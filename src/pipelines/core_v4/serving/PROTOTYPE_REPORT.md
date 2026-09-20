# Local prototype report: does the SERVING_DESIGN.md topology work?

Run of 2026-09-20/21 on the Mac, mini DB (`core_v4_noworkenrichment-min.duckdb`, 1000 projects / 1000 organisations / 1000 works),
scratch OpenSearch 3.8.0 on `localhost:9201` (hm_pipeline's own compose; heritagemonitor's 9200 untouched). Indices are named `proto_*`.
Nothing was committed; `pyproject.toml` / `uv.lock` are untouched.

**Verdict: the topology works.** 30/30 use-case checks pass, every count is cross-checked against DuckDB, not just "returns something".
Things that broke or need a decision are in section 4.

## 1. What was built (all in `serving/prototype/`)
| File | What |
|---|---|
| `sql/00_common.sql` + `projects/organisations/works/minorities/grants/topics.sql` | the **export views**: DuckDB SQL, each ends in `COPY ... TO '__OUT__/x.parquet' (zstd)`. Parametrised only by DB path, so the same SQL runs on the 132 GB file. |
| `export.py` | runs the SQL read-only (`--db`, `--out`, `--only`, `--memory-limit`, `--threads`). Mini DB: <1 s. |
| `mappings.py` | settings + mappings for 5 indices (`dynamic: strict`, HDD settings, `hm_text` / `hm_name` analyzers, `search_as_you_type`, `geo_point`, `index:false` stored-only fields). |
| `load.py` | streaming Parquet -> `parallel_bulk` (constant memory, no LIMIT/OFFSET). Recreates index, `refresh_interval -1`, then 30s + forcemerge. |
| `queries.py` | what apps/api would do: `rewrite_query`, `simple_query_string` builder, filters, **typo-tolerant search** (strict, fuzzy fallback, did-you-mean). |
| `test_usecases.py` | the 30 checks (`last_run.txt` = output of the final run). |
| `measure.py`, `ablate.py`, `synth_works.py` | size per index, per-field size by ablation (OpenSearch has **no** `_disk_usage` API), synthetic 500k-works load. |
| `run_all.sh` | rebuild + test from scratch (section 7). |

Doc shape decisions taken while building (flag if you disagree): raw columns keep their DuckDB names (`legalName`, `startDate`...), derived fields are
snake_case (`org_ids`, `topic_id`, `funded_eur_per_org`...); ids are strings; `geolocation` becomes `geo` `{lat, lon}`; `pillars` also as `pillar_list` keywords;
`fundings`/`pids` are kept in `_source` only (`enabled:false`), with `funder`, `programme`, `funding_stream_ids` flattened for filters.
Design changes from the coordinator are all in: D4 `is_ch_via_project`, D4b `works.minority_qids`, D8 `pred` not indexed, D13 equal split, D14 ECB table
(section 3b, HRK/BGN as given), D17 `funder`/`programme` on projects and grants, D18 works filters, pdf/landing rule, D6 typo tolerance.

## 2. Use-case results (mini scale, so timings are ~ms; they prove behaviour, not speed)
| Use case | Result | Checked against |
|---|---|---|
| Query rewrite (`AND/OR/NOT`, `-x`, quotes, `~N`) + garbage never throws (16 hostile strings) | pass | semantics: AND narrows, `-x` = exact complement, `OR` = a+b-both, wildcard/fuzzy inert |
| Projects search + facets: topic terms agg (count desc), year histogram, regions, themes, pillars, budget sort, DCH filter | pass | topic counts **exactly equal** DuckDB over the hit set |
| Project overview by id | pass | all 45 fields incl. `pred`, `openaireId`, `fundings` come back |
| Project -> works tab (`works.project_ids`) / org -> works (`organisation_ids`) / org -> projects | pass | counts == `work_count` / `project_count` rollups |
| Works search + PDF/landing in `_source`, `-negation`, year filter, sort `_score, citation_count` | pass | |
| Works filters year + OA colour + language + publisher + DCH proxy | pass | == DuckDB (filter built from a real DCH-proxy work, non-empty) |
| Works DCH corpus (`is_ch_via_project`) | pass | 46 works == DuckDB; tier-1 = 0 |
| Works by minority (`works.minority_qids`) | pass | 99 works == DuckDB == "projects with qid -> works with those project_ids" |
| Org autocomplete (`bool_prefix` on `sayt`, `function_score` by `project_count`) incl. acronym | pass | top org in top 3 for 2 prefixes |
| Project autocomplete (acronym / title) | pass | |
| Typo tolerance projects/orgs (4 typo kinds, 2-word, negation kept, clean query stays strict) | pass | 0 strict hits -> 162 fuzzy hits; did-you-mean `european` |
| Typo tolerance works at scale (500k synthetic works) | pass | table in 2.1 |
| Experts (agg `org_ids` -> mget orgs -> rerank) | pass | top bucket == DuckDB; scripted `max(_score)` ordering also works |
| Org network (agg partners -> mget geo; lazy project-pair query) | pass | pair count == bucket `doc_count` |
| Query network (top-N projects -> edges in python, `max_edges` cap) | pass | payload 28.8 KB for 1000 edges |
| Funding map (per-org `sum(funded_eur_per_org)`, top N, mget geo) | pass | agg sum == `organisations.total_funding_eur` == DuckDB (D13 is consistent) |
| Minorities: text via title blob, institution via two-step, facets, map (2-level agg) | pass, with caveat | 4.2 |
| Topic modal SCI vs DCH (agg `topic_id` size 5000 + rollup by field) | pass | DCH counts == DuckDB, field rollup == field agg |
| Grants list / `funder` + `programme` facets / project filter by stream | pass | facets == DuckDB, `project_count` == project filter |
| Org filters (region + rorTypes + geo) with sort funding > projects > works | pass | |
| `pred` display-only | pass | in `_source`, `range` on it matches nothing |
| Currency (ECB table, unlisted -> NULL) | pass | GBP / HRK conversion exact |
| Deep pagination | limit confirmed | `from+size > 10000` is rejected |

### 2.1 Typo-tolerance fallback timing (works, 500k synthetic docs, 360k-term Zipf vocabulary, 1 shard)
| Query | strict | fuzzy fallback (max_expansions 10 / 50) |
|---|---|---|
| 1 typo, mid-frequency word | 3 ms | 11 / 6 ms (86 hits) |
| 1 typo, very frequent word (9.4k docs) | 3 ms | 18 / 10 ms (9.6k / 9.8k hits) |
| 2 words, one typo each | 3 ms | 14 / 16 ms (8 / 31 hits) |
| typo + negation | 4 ms | 8 ms |
| clean query | 5-10 ms | not run (stays strict) |

Read this cautiously: it is 1% of the real corpus and a pseudo-vocabulary. Fuzzy cost grows with term-dictionary size and the postings of the
matched expansions, so at 50M docs on an HDD expect **10-100x** (hundreds of ms up to seconds, cold cache worse). Mitigations that are already
cheap: fallback only when strict returns fewer than a small threshold (use 1-3 on works, 5 on projects/orgs), `prefix_length 2`, `max_expansions ~20`,
and a request `timeout` (e.g. 1500 ms). Note `max_expansions 10` vs `50` changed recall on the 2-word case (8 vs 31 hits): the cap trades recall for speed.
**Measure this on the VM once works are loaded**, it is the one query path I could not size locally.

## 3. Sizes and extrapolation
Store size after forcemerge, `best_compression`, per-field cost from ablation. **The mini DB is denser than prod** (12 orgs/project, 20 orgs/work, 16 authors/work), so
each field was scaled by prod averages, not taken raw.

| Index | mini B/doc | dominated by | prod docs | extrapolated |
|---|---|---|---|---|
| works | 1072 | authors 425 (16.4/doc, ~26 B each), organisation_ids 256 (20/doc, ~13 B each), title 153, project_ids 50 (~56 B each), publisher+container 47, doi 40, urls 32 | 50M | **~515-645 B/doc, 26-32 GB** at 2.8 orgs/work (READ: 141M/50M), 0.15 project ids/work and 5-10 authors/work (authors/work in prod is unknown: agent_job F). Synthetic prod-density docs (500k) gave 387 B/doc = 19 GB, but the pseudo-text has less entropy, so treat **19 GB as a floor and plan for 25-40 GB**. |
| projects | 4019 | summary 1398, **title 1582 of which the autocomplete `title.sayt` is 1498**, org_names 288, org_ids 194 | 3.9M | with `title.sayt`: ~3.6 KB/doc = **~14 GB**; without it ~2.1 KB/doc = **~8 GB** (org fields shrink ~9x in prod: 1.4 orgs/project) |
| organisations | 2271 | alternativeNames 896 (all its autocomplete subfields ~1475 together), rorLocations+rorRelationships 292, pids 95 | 494k | ~1.1 GB |
| minorities / grants | 762 / 1149 | | 278 / ~100s | < 5 MB |

Total on disk ~ **35-55 GB**. The VM container has 24g limit, 12g heap, so ~12g of page cache: only 25-35% of the index fits in RAM, works will be cold-read from the HDD.

Throughput (laptop SSD, 4 bulk threads, works-sized docs): **~18k docs/s**, forcemerge of 500k docs 7-11 s. 50M works ~ **45 min on this Mac**; on the HDD VM with
`merge.scheduler.max_thread_count: 1` I would guess 3-5x slower (2-4 h), which is a guess, not a measurement.

**Recommendations**
- `works`: **4 primary shards** (~6-10 GB each), not 8. On an HDD each extra shard adds random IO to every query; 4 shards already parallelise across 8 cores and leave cores for the other indexes. `projects`: 2 shards, `organisations`/`minorities`/`grants`: 1.
- Heap: 12g is more than needed (no fielddata, aggregations are `size`-capped keyword terms aggs). Consider **8g heap and keep the 24g container limit**, so ~16g is page cache; on a spinning disk cache beats heap. Judgement call, verify with the real load.
- `works.id` is mapped `index:false` (get-by-id/mget use `_id`): saves 16-58 B/doc (1-3 GB at 50M). Same could be done for `projects.id`/`organisations.id` (55 B/doc).
- Never `track_total_hits: true` on works for generic queries (it counts every match); use the default 10,000 cap and show "10,000+" (not measured here, standard OpenSearch behaviour).

## 4. Problems found and what I recommend

### 4.1 Bugs fixed while building (already reflected in the code)
1. **`word~2` and `"a b"~3` silently return wrong results** with the restricted flags: with FUZZY/SLOP off, `~` is dropped and the `2` becomes an extra AND term
   (`european~2`: 79 hits instead of 434). Fix: `rewrite_query` strips `~N`. Unit-tested.
2. **`index_options: docs` on `org_names` breaks phrase search** ("University of Bath" found nothing, silently). Mapping now keeps positions (norms still off).
3. **hm_pipeline `infra/docker-compose.yml` does not start without `OPENSEARCH_INITIAL_ADMIN_PASSWORD`** (default is empty; the container exits after "No custom admin password found",
   even with the security plugin disabled). Start with `OPENSEARCH_INITIAL_ADMIN_PASSWORD='Proto-Local-9201!x' docker compose -f infra/docker-compose.yml up -d opensearch`
   (`run_all.sh` does this). Suggest defaulting it in the compose file.

### 4.2 Design points to decide (deviations from SERVING_DESIGN.md)
1. **Project title autocomplete costs 37% of the projects index** (1.5 KB/doc, ~6 GB at 3.9M). Options: (a) accept it, (b) autocomplete on `acronym.sayt` (78 B/doc) + org names only, and
   titles via `match_phrase_prefix` on `title` (no extra index, slower on HDD). I recommend (b) unless title autocomplete is a must.
2. **"Did you mean": use the term suggester per word, not the phrase suggester.** Phrase suggester fails on `search_as_you_type` shingle fields ("At least one unigram is required").
   Term suggester on the unstemmed `title.sayt` / `legalName.sayt` works (`euorpean` -> `european`). **Works has no unstemmed title field, so no did-you-mean on works** (only the fuzzy fallback);
   adding one is another ~150 B/doc.
3. **Minorities title blob does not cover institution names** ("University of Bath": blob finds nothing, projects two-step finds it). Recommendation: keep the blob (free, 278 docs) for text,
   and do institution/topic-aware minority search through the **two-step** (projects query with `exists(minority_qids)` -> terms agg `minority_qids` -> mget minorities; 4-8 ms here, exact counts),
   or add the org names of the minority's top projects to the blob.
4. **`max_result_window` = 10,000**: the paginated list cannot go past result 10,000 (works: page ~1000). UI must cap or the API must use `search_after`.
5. **Multiple coordinators exist** (1 project in the mini DB has 2): `coordinator_id` is currently `any_value`. Consider `coordinator_ids[]`.
6. **`granted.currency` NULL with `fundedAmount` > 0** (3 of 1000 projects in the mini DB): `funded_amount_eur` is NULL by design; check the prod share (agent_job B).
7. SERVING_DESIGN.md section 5 still says the corpus selector has "no effect on works"; D4 makes it filter on `is_ch_via_project`. Needs a wording update.
8. Experts ranking: `terms` order by doc_count then rerank with org rollups is cheap; ordering by scripted `max(_score)` also works. Use doc_count first, compare cost on the VM before switching.
9. Collaboration (D11): no edge index was needed. Org network = one agg + one mget (62 KB for 421 nodes incl. names, 5+64 ms); query network = 483 projects -> 33,886 raw pairs -> capped 1,000 edges / 186 nodes, 28.8 KB.
   Cap the orgs per project (I used 40: a 98-org consortium alone is 4,753 pairs). At prod density (1.4 orgs/project) this is far cheaper than in the mini DB.

## 5. macOS / VM install without CUDA torch (proposal, verified on copies, real files untouched)
`uv run`/`uv sync` fail on macOS because `tool.uv.sources` pins `torch` to the `pytorch-cu126` index, which has no macOS wheel.
- **A (recommended, minimal, Linux-safe):** add `serving = ["duckdb", "pyarrow", "opensearch-py"]` under `[dependency-groups]`, run `uv lock`
  (verified: the lock diff is only the new group, +10 lines, **no version changes**, the cluster resolution is identical), then on the Mac and on the VM:
  `uv sync --frozen --only-group serving` (verified `--dry-run` on macOS: installs only ~10 small packages, no torch). Then run `.venv/bin/python ...`
  (or `uv run --no-sync`), a bare `uv run` would try to sync the full project again.
- **B (only if you want the full pipeline importable on the Mac):** `torch = { index = "pytorch-cu126", marker = "sys_platform == 'linux'" }`.
  Verified: `uv lock` changes only torch (`2.11.0+cu126` stays for Linux, plain PyPI `2.11.0` added for other platforms) and `uv sync --frozen --dry-run` then succeeds on macOS.
  It changes `uv.lock`, and the VM (Linux) would still pull cu126 torch + CUDA libs unless it also uses A.
What I actually used: a separate venv, `uv venv .venv-serving && uv pip install duckdb pyarrow opensearch-py` (untracked; `.gitignore` covers `.venv` but not `.venv-serving`).
Also: the existing `common/search/index_duckdb_table.py` pages with `LIMIT/OFFSET` and pandas, do not use it for works (`load.py` replaces it).

## 6. Not verified locally (needs the cluster / the VM)
Real value distributions (agent_job A-M: authors/work, stream count, currency mix, pdf coverage, orgs/project tail), fuzzy and text-search latency at 50M docs on an HDD,
actual works index size, load time on the VM, heap needs under concurrent use. The export SQL has not run on the 132 GB file: on the cluster set `--memory-limit`/`--threads`,
and note `works.sql` joins `relation` (154M rows) with `project` for the DCH/minority proxies (should be fine, but time it first on `--only works` with a small limit).

## 7. Rerun
```bash
# from the repo root, Docker running
bash src/pipelines/core_v4/serving/prototype/run_all.sh                  # mini DB -> parquet -> proto_* indices -> 30 checks
bash src/pipelines/core_v4/serving/prototype/run_all.sh <path/to/other.duckdb>
# pieces (from prototype/, venv = ../../../../../.venv-serving/bin/python):
python export.py --db <duckdb> --out <dir> [--only works,projects]      # SQL -> Parquet
python load.py --parquet <dir> [--only works] [--shards 4]               # Parquet -> OpenSearch (host/port flags for the VM)
python test_usecases.py --parquet <dir>                                  # checks (needs the parquet dir for cross-checks)
python measure.py ; python ablate.py works|projects|organisations        # sizes
python synth_works.py 500000                                             # synthetic works scale test (used by the typo test)
```
Local OpenSearch is left running on `localhost:9201` (container `infra-opensearch-1`), with `proto_*` indices loaded (`proto_works_synth` = 500k synthetic works).
