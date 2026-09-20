# core_v4 -> OpenSearch: how to run the export, the transfer and the load

Design and decisions: `SERVING_DESIGN.md` (numbers in section 6). Code: `export/`. Everything runs with only the **serving dependency group**
(duckdb, pyarrow, opensearch-py; no torch/CUDA): `uv sync --frozen --only-group serving`.

```
cluster: core_v4_noworkenrichment.duckdb --export.py--> Parquet (~9 GB)  --rsync-->  Mac  --rsync-->  VM  --load.py-->  OpenSearch (VM)
```

## 0. Test the whole chain on the mini DB (laptop, 1 minute)
```bash
bash src/pipelines/core_v4/serving/export/run_all.sh          # export -> local OpenSearch :9201 (prefix hm_) -> 42 use-case checks
```
Needs Docker (starts `infra/docker-compose.yml` opensearch on 9201 if it is not up). Expect `42/42 passed`.

## 1. Export on the cluster (read-only on the 132 GB file)
Work only in `/vast/lu72hip/hm_pipeline`, never `/home`. The pyproject/uv.lock with the `serving` group must be synced there (git).
```bash
cd /vast/lu72hip/hm_pipeline
# smoke first (minutes): 0.1% of the works, same SQL
uv run --frozen --only-group serving python src/pipelines/core_v4/serving/export/export.py \
    --db /work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb --out data/serving_export_smoke --works-sample 1000 --works-chunks 1 \
    --memory-limit 120GB --threads 16 --temp-dir tmp_session/duck_tmp
# full run as a slurm job (partition fat, 16 cpus, 150 GB, 6 h ceiling; edit paths inside if needed)
sbatch src/pipelines/core_v4/serving/export/export.sbatch
```
**Text cleaning (D27):** all text fields go through `hm_clean` (entities decoded in 3 passes, whitelisted HTML/MathML tags stripped, whitespace collapsed; see `export/sql/00_macros.sql`).
If you change the macro or any `sql/*.sql`, **rerun the whole export with `--force` or into a fresh `--out`**: existing files are skipped, and works, projects, organisations, minorities, grants and
`api/publishers.json` all depend on it. Check the result on the real data without the cluster: `export/verify_clean.py` (before/after counts on the dry-run Parquet, `agent_job/CLEAN_VERIFY.md`).
Output (`data/serving_export/`): `organisations/`, `projects/`, `minorities/`, `grants/`, `works/works_00..09.parquet`, `api/topics.json`, `api/publishers.json`,
`export_manifest.json` (rows, bytes, seconds per file). Files are written as `.tmp` and renamed, existing files are skipped, so a crashed/timed-out job just continues when
resubmitted (delete a file to redo it; `--force` redoes all).
Expected sizes (from the dry run + slice): works ~8 GB (10 files), projects ~0.75 GB, organisations ~60 MB, minorities/grants < 1 MB. Runtime on the real file is **not measured yet**;
the smoke run tells you (`export_manifest.json`).

Check before downloading (numbers from the cluster analysis): works 50,000,000 rows, projects 3,893,065, organisations 494,099, minorities 278, grants 6,120;
works with `is_ch_via_project` 35,975; projects with `coordinator_ids` 81,043; 103 distinct `funder` values; projects with a non-empty `minority_qids` **6,503** (manifest `minority_source`).

### Minority tags: stored tags minus a small deny-list (`--minority-exclude`, part of the cluster run)
Decision D32 (2026-09-20): the minority tags ship as stored in the database, **minus a conservative deny-list of obviously wrong (project, minority) pairs**; keyword-style hits
(adjectives like "Russian", "Turkish") stay, and **all 278 groups stay in the `minorities` index** even if they end with 0 projects. The list is `export/minority_exclusions.csv`
(2,878 pairs, committed, human-reviewable; columns project_id, minority_qid, group_name_en, rule, matched_text, title) and is already in `export.sbatch`:
`--minority-exclude src/pipelines/core_v4/serving/export/minority_exclusions.csv`. Rules removed: R1 typo variants of multi-word keywords ("many people" -> "manx people"),
R2 the word "same" for Sámi, R5 "Hebrew" only inside an institution name. Effect: 9,293 -> **6,503** projects with a minority, 9,528 -> 6,650 tags, 9 groups end with 0 projects
(report with per-group before/after and examples: `agent_job/MINORITY_EXCLUSIONS.md`). Semantics: subtractive; a project not listed keeps its stored tags; a project that loses all
tags gets an empty list; works/minorities rollups/project docs all use the reduced tags; the export aborts if the file is missing, or lists a project/pair that is not in this database.
`export_manifest.json` -> `minority_source` records the mode, the counts and `groups_emptied`; no flag = the stored tags exactly as before.
```bash
.venv/bin/python src/pipelines/core_v4/serving/export/minority_override.py      # regenerates the CSV + report (profile obvious = default, ~15 s; main .venv: needs spaCy)
MINORITY_EXCLUDE=src/pipelines/core_v4/serving/export/minority_exclusions.csv bash src/pipelines/core_v4/serving/export/run_all.sh   # laptop test on the mini DB
```
The CSV must be generated from the same database it is applied to (it was built from `agent_job/out/projects_full.parquet`, an export of the cluster file).

Optional, NOT used: `--profile strict` + `--minority-override` (full replacement of the tags with the strictly corrected list, `agent_job/MINORITY_OVERRIDE.md`; Parquet, git-ignored,
mutually exclusive with `--minority-exclude`).

## 2. Download and upload (order = load order)
```bash
# cluster -> Mac (small ones first, they are enough to bring the site up; works last)
rsync -av --partial cluster:/vast/lu72hip/hm_pipeline/data/serving_export/{api,organisations,projects,minorities,grants,export_manifest.json} ./serving_export/
rsync -av --partial cluster:/vast/lu72hip/hm_pipeline/data/serving_export/works ./serving_export/
# Mac -> VM (50 Mbps home upload ~ 20 GB/h: everything except works is ~1 GB = minutes, works ~8 GB ~ 25 min)
rsync -av --partial ./serving_export/ digicher:~/serving_export/
```

## 3. Load on the VM (OpenSearch in Docker, HDD, 8 cores, 31 GB RAM)
```bash
# one-time: the code (git) + the light environment
cd ~/hm_pipeline && uv sync --frozen --only-group serving
export OPENSEARCH_USERNAME=admin OPENSEARCH_PASSWORD=...        # prod has basic auth on plain http (see heritagemonitor/infra/PRODUCTION.md)
cd src/pipelines/core_v4/serving/export
# small indexes first: the app can use them while works loads
uv run --frozen --only-group serving python load.py --parquet ~/serving_export --host 127.0.0.1 --port 9200 --only organisations,projects,minorities,grants
# works last (long): 4 shards, best_compression, refresh -1 while loading. Run inside tmux/nohup; safe to interrupt and rerun (resumes per file).
nohup uv run --frozen --only-group serving python load.py --parquet ~/serving_export --host 127.0.0.1 --port 9200 --only works --threads 4 > ~/load_works.log 2>&1 &
```
What `load.py` does per index: create (mapping from `mappings.py`, prod shard counts works 4 / projects 2 / others 1, replicas 0, refresh -1) -> bulk each Parquet file with `parallel_bulk`
-> restore refresh 30 s -> refresh -> forcemerge to 1 segment per shard (`--no-forcemerge` to skip, `--segments N`) -> verify the doc count against the Parquet metadata (exit code 1 on mismatch).
Useful flags: `--recreate` (drop and rebuild), `--suffix _v1` (build `<name>_v1` and switch the alias `<name>` at the end = rebuild without downtime), `--threads/--chunk` (tune bulk),
`--finalize-only` (only refresh/forcemerge/verify after an interrupted run), `--title-shingle 2` (smaller project title autocomplete), `--shards works=1` (laptop tests).
An existing index without a load state is refused unless `--recreate`. Load state: `<parquet>/.load_state/<index>.json`.

Expected: laptop (1 GB heap) did works 20k docs/s and projects 9k docs/s; the HDD VM will be several times slower. Plan hours for works, ~30 min for the rest. Watch:
`curl -s localhost:9200/_cat/indices?v&bytes=gb` and `~/load_works.log`. Final sizes ~ projects 7 GB, works 20-26 GB, organisations < 1 GB.

## 4. After the load
- Re-time the api-critical queries on the VM (works typo fallback, first-query latency): `real_smoke.py --prefix "" --port 9200` (uses the exported Parquet only to pick test words; run it against a
  Parquet copy on the VM). Tune `queries.TYPO["works"]` (threshold, max_expansions, timeout) from the result; disable the works fallback if it is too slow.
- The api holds `api/topics.json` and `api/publishers.json` in memory (topic tree modal, publisher typeahead). Corpus-aware topic counts: `queries.topic_modal_aggs()` with the `is_ch` filter, cached per corpus.
- api rules to implement (all in `queries.py`): `rewrite_query`/`sqs` (Google-like syntax), `search_typo_tolerant` (fallback + did-you-mean), `page_window` (10,000 window) and `total_of` (`10,000+`),
  `work_filters` / `project_filters` / `org_filters`, `org_autocomplete_body`, `orgs_body` (rank blend), `merge_by_name_key` (duplicate institutions), `query_network_body`, `funding_aggs`, `experts_aggs`.

## Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `strict_dynamic_mapping_exception` while loading | a column in the Parquet has no mapping in `mappings.py` (dynamic strict): add it there, rerun with `--recreate` |
| `index exists but there is no load state` | previous index from another run: `--recreate` |
| bulk 429 / timeouts | lower `--threads` (2) or `--chunk` (500); the VM heap is 12 GB, works docs are small |
| export job hits the 6 h limit | rerun the same sbatch, finished files are skipped |
| `uv run` tries to build torch | use `--only-group serving` (and `--frozen`); do not run plain `uv sync` on the Mac/VM |
| Python 3.14: `got more than 100 headers` | only with `_reindex`; the loader does not use it |
