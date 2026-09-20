# Local load report (laptop, 2026-09-21)

**Scope change during this task:** the user's Mac RAM is shared with other work, so the full local build (50M works, 3.9M projects) was **stopped** and replaced by a small
*coherent sample*. Full-scale checks (50M works typo fallback, first-query global ordinals on `org_ids`, biggest-org aggregations, funding agg over 3.9M projects) can only be
done on the VM: `export/vm_smoke.py` was written for that and tested on the sample.

## 1. State left behind
| | |
|---|---|
| OpenSearch | container `infra-opensearch-1`, `localhost:9201`, OpenSearch 3.8.0, **heap 2 GB** (`OPENSEARCH_JAVA_OPTS=-Xms2g -Xmx2g`), security disabled, container left running |
| Indices (empty prefix) | `projects` 25,797 docs (2 shards), `organisations` 28,137 (1), `works` 28,653 (4), `minorities` 278 docs (299 in `_cat`: nested subgroup docs), `grants` 6,119 (1); replicas 0; ~150 MB in total |
| Sample data | `data/serving_export_sample/` (23 MB, git-ignored via `/data`), same layout and schema as `data/serving_export/`, plus `sample_manifest.json` |
| Untouched | heritagemonitor and its OpenSearch on 9200 (and volume `heritagemonitor_os-data`), `data/serving_export/` (the 7.8 GB real export, read-only) |
| Free disk | 272 GiB at the start, 277-282 GiB during the partial full load, 280 GiB now (nothing is above the 60 GiB floor) |
| Docker | Docker Desktop VM 8 GiB (`MemoryMiB: 8092`), 12 CPUs; other containers (hm-web/api/libs/postgres + heritagemonitor's opensearch) use ~3.4 GiB, so 2 GB is the largest safe heap here. Raise Docker's memory in Docker Desktop settings (the Mac has 32 GB) if you want a bigger heap for larger tests |
| Compose change | `infra/docker-compose.yml`: `OPENSEARCH_JAVA_OPTS=${OPENSEARCH_JAVA_OPTS:--Xms1g -Xmx1g}` (default unchanged, override per run) |

## 2. What the sample contains (`export/build_sample.py`, seed 7)
| | count | notes |
|---|---|---|
| projects | 25,797 | 5,580 DCH, 5,004 minority-tagged, 2,996 with coordinators, 25 without topic; **all 2,123 projects of one anchor org** (National Research Council, the org with the most collaboration partners among orgs with 300-2,500 projects: org-network / experts tests have 500+ partners) + one project with 797 works (works tab) + random rest |
| organisations | 28,137 | every org referenced by a sampled project (cap 30k), 11,092 with geo, 10,895 with a DCH project |
| works | 28,653 | tier 0: 22,848 (22,114 with a project link), tier 1: 5,805; 11,604 DCH-proxy, 6,674 with minority tags, 9,562 with `pdf_url`; 797 works of the anchor project, 2,633 works of the anchor org |
| minorities / grants / api | 278 / 6,119 / topics.json, publishers.json | complete copies |

Coherence: `projects.org_ids` -> all exist in the sample organisations (0 dangling); `works.project_ids` / `organisation_ids` are **filtered** to ids in the sample (0 dangling, a link to a
project/org outside the sample is dropped). Denormalised numbers (`project.work_count`, `organisation.project_count/work_count/total_funding_eur/rank_*`, `org_count`, the `publishers.json` counts,
grants `project_count`) keep their FULL-dataset values, so they do not equal the number of docs in the sample (the tests know: `test_usecases.py` has a `SAMPLE` mode).
Also: works of a minority project outside the sample keep the minority tag but lose that project id (a sample artefact, 7 works for the top group).

Rebuild: `.venv-serving/bin/python src/pipelines/core_v4/serving/export/build_sample.py` (35 s, needs `data/serving_export/`). Options: `--projects-random --org-cap --works-t0 --works-t1 --seed`.

## 3. Results
- **Load** (`load.py --parquet data/serving_export_sample --port 9201 --recreate --threads 2 --chunk 1000`, prefix empty): all five indices in 26 s, every doc count verified against the Parquet metadata.
- **Correctness suite** (`test_usecases.py --parquet data/serving_export_sample --port 9201 --prefix ""`): **44/44 passed** (all use cases in SERVING_DESIGN.md section 5: DCH corpus, minority, funder/programme/topic facets and the corpus-aware topic modal,
  org and title autocomplete, typo fallback, google-style syntax, works by `project_ids` / `organisation_ids`, org network incl. `name_key` de-dup, query network payload, funding agg, experts, minorities text + two-step institution search, unknown buckets,
  entity/tag cleaning, currency rules, all 278 minority groups present). The mini-DB chain (`run_all.sh`) also passes 44/44 after the changes below.
- **`vm_smoke.py`** on the sample: 51 checks, 0 errors, 0 red flags. **Latencies on the sample are NOT representative** (25-30k docs per index, everything in the page cache); they only prove the checks run.

## 4. Findings and changes made (all under `export/`, nothing committed)
1. **Approximate facet counts with 2+ shards** (real finding). Projects have 2 shards, works 4; a terms aggregation with the default `shard_size` (size*1.5+10) returns slightly too low counts:
   topic facet size 50 on three queries: 4-5 of 50 counts 1-2 too low (`doc_count_error_upper_bound` 2-6); with `shard_size: 500` all exact (bound 0). Fixed with `queries.terms_agg()` (shard_size = max(10*size, 500))
   used by every facet / experts / org-network / funding aggregation and by the tests. **The api (heritagemonitor) must send `shard_size` on its facet aggregations too.**
2. **`eager_global_ordinals: true` on `projects.org_ids`**: mapping update accepted, experts / org-network / funding aggregation results identical before and after. Set in `mappings.py` (and `mappings/projects.json`) so a fresh
   VM load has it. The latency benefit (no slow first query after a restart or refresh) can only be measured at scale: on the VM restart the container, then
   `vm_smoke.py --runs 1 --only "experts,org network,funding"`, compare with and without (`--set-eager-ordinals` toggles it on an existing index).
3. **Tests made sample-aware** (they compared FULL-dataset counts): `SAMPLE` mode in `test_usecases.py`; `WORD`/typo test no longer picks words with doubled letters at positions 3/4 (transposition typo == word);
   entity regex restricted to real entities (prose like `R&D;`, `DD&AS;` is legitimate); minorities blob test uses groups with <= 150 projects (the blob holds only the top 200 projects per group).
4. **Data findings, not fixed (need an export rerun, ~9 min on the cluster):** `works.language` keeps non-ISO source values after `hm_lang` (0.13% of works: `sr (latin script)`, `sr (cyrillic script)`, `lv-lv`, `el_gr`, `sr_lat`, `inglese`, `ng`);
   harmless for search, normalise to `^[a-z]{3}$` or NULL next time. And: the minority `project_title_blob` covers only the top 200 projects (by `pred`) of each group, so for Russians (2,242), Turkish (1,676), Jewish (1,344) most project titles
   are only reachable through the two-step route (already planned).
5. **Real-scale numbers from the stopped full load** (organisations complete, projects partial, 2 GB heap, 4 threads, chunk 1000):
   organisations 494,099 docs -> **1.29 GB** primary store (2.6 KB/doc, more than the 0.6-0.75 GB estimate, still small), 11.9k docs/s (41 s), forcemerge 38 s;
   projects 1.17M of 3.89M docs -> 2.6 GB unmerged (2.2 KB/doc) => ~7-9 GB at full size (estimate 6.9 GB), 9.5-9.8k docs/s with the merge scheduler throttling indexing ("segment writing can't keep up") every few seconds.
   Works were not loaded at full scale; the sample gives 498 B/doc (forcemerged), consistent with the 20-26 GB estimate.
6. Latency red flags: **none measurable here** (the sample is too small). Every full-scale question stays open until `vm_smoke.py` runs on the VM.

## 5. What to change / check for the VM
- **Heap:** `PRODUCTION.md` has 12g heap in a 24g container on a 31 GB box. The laptop needed a 2 GB heap only because Docker had 8 GB; the load itself is off-heap heavy (Lucene mmap), so the 12g heap is generous; if page cache pressure
  shows on the 30 GB works index, try 8g heap (leaves ~16 GB of the container as page cache). Measure with `vm_smoke.py` before and after.
- **Loader:** projects/orgs loaded at 9.5-12k docs/s with `--threads 4 --chunk 1000` on an SSD laptop with throttled merges. On the HDD start with `--threads 4 --chunk 1000` for the small indexes and `--threads 2..4` for works; watch for 429s
  (lower `--threads` / `--chunk 500`). Keep `refresh -1` during the load and the forcemerge at the end (the loader does both). `index.merge.scheduler.max_thread_count: 1` is already set for the HDD.
- **Shards:** works 4 / projects 2 / others 1 stay (mappings.py); replicas 0.
- **After the load:** `vm_smoke.py` (all groups, `--runs 15`), then once more right after a container restart with `--runs 1` for the cold global-ordinals and cold-page-cache numbers.
- **api:** use `shard_size` on facet/network aggregations (finding 1); URLs/ids are strings everywhere.

## 6. Commands
```bash
# start / restart the 9201 node with a 2 GB heap
export OPENSEARCH_INITIAL_ADMIN_PASSWORD='Proto-Local-9201!x' OPENSEARCH_JAVA_OPTS='-Xms2g -Xmx2g'
docker compose -f infra/docker-compose.yml up -d --force-recreate opensearch
# build the sample from the real export and load it (empty prefix)
PY=.venv-serving/bin/python; E=src/pipelines/core_v4/serving/export
$PY $E/build_sample.py                                                             # -> data/serving_export_sample/  (35 s)
(cd $E && ../../../../../$PY load.py --parquet ../../../../../data/serving_export_sample --port 9201 --recreate --threads 2 --chunk 1000)
# correctness (sample mode is detected from sample_manifest.json) and the full-scale smoke script
(cd $E && ../../../../../$PY test_usecases.py --parquet ../../../../../data/serving_export_sample --port 9201 --prefix "")
(cd $E && ../../../../../$PY vm_smoke.py --port 9201 --runs 5)
# on the VM after the real load (basic auth from env, prod OpenSearch on 9200):
#   cd ~/hm_pipeline/src/pipelines/core_v4/serving/export && export OPENSEARCH_USERNAME=admin OPENSEARCH_PASSWORD=...
#   uv run --frozen --only-group serving python vm_smoke.py --host 127.0.0.1 --port 9200 --runs 15 --json ~/vm_smoke.json
# mini-DB regression of the whole chain (creates hm_* indices on 9201; delete them afterwards)
bash src/pipelines/core_v4/serving/export/run_all.sh
```

## 7. Files changed or added (uncommitted)
`infra/docker-compose.yml` (heap env var); `export/queries.py` (`terms_agg`, used by experts/network/funding aggs); `export/mappings.py` + `export/mappings/projects.json` (`eager_global_ordinals` on `org_ids`);
`export/test_usecases.py` (sample mode, robust word/entity/blob tests); new `export/build_sample.py`, `export/vm_smoke.py`; `SERVING_DESIGN.md` (new 6.3b, 6.3c, section 7); this report.
Untracked/ignored artefacts: `data/serving_export_sample/`, `data/serving_logs/` (load logs), `.venv-serving/`.
