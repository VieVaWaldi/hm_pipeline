# core_v4 serving design (OpenSearch): the one place for decisions

Status of 2026-09-20. Deadline: 2 days. Source: `core_v4_noworkenrichment.duckdb` (see `../READ_CORE_V4_FINISHED.md`).
Consumer: `/Users/wehrenberger/Code/DIGICHer/heritagemonitor` (api + web). Use cases: `apps/web/src/common/catalog/useCases.ts`.
Field-by-field mappings are the **final goal** of this file (section 6, still empty). Everything else is decided or marked OPEN.

## 1. Locations and constraints
- **Cluster (Draco)**: has the 132.6 GB DuckDB. Work only in `/vast/lu72hip/hm_pipeline`, never `/home`. Heavy work as slurm jobs.
- **Mac (home)**: relay. ~10 GB upload from home; the final works index goes up from the university (~10x faster).
- **VM "DIGICHerVM"**: 8 cores, 31 GB RAM, **HDD** (`rota=1`), 5 TB disk. Prod compose: OpenSearch heap 12g, container limit 24g
  (page cache is charged to the container, see `heritagemonitor/infra/PRODUCTION.md`). Old app: Postgres, all projects fine,
  10M works "slow but usable" -> 50M works will be slower, so the works doc must be as small as possible.
- **Cluster and VM cannot talk to each other.** Cluster -> Mac -> VM.
- OpenSearch version must be identical in both repos (currently `opensearchproject/opensearch:3.8.0` in both composes).
  hm_pipeline's dev instance is on port **9201** (heritagemonitor's own dev OpenSearch holds 9200, do not touch its indexes).
- **No CUDA/torch on Mac or VM.** The serving code needs only duckdb, pyarrow, opensearch-py, not the full pipeline
  (the `torch` cu126 wheel in `pyproject.toml` has no macOS build, `uv run` fails on the Mac).

## 2. Transfer process (decided)
1. On the cluster, one **export query per index** flattens the DuckDB into the exact document shape (joins, arrays, rollups) and
   writes zstd Parquet, only the fields the index needs. Joins run on the cluster, never on the VM.
2. Download to the Mac, upload to the VM, a loader bulk-indexes the Parquet into OpenSearch on the VM.
3. **Order: everything except works first** (minutes, site can go live), then works (tier 0 = 5.0M project-linked first, then tier 1).
4. Loader must stream Parquet and use `parallel_bulk`. The existing `common/search/index_duckdb_table.py` uses `LIMIT/OFFSET`
   paging + pandas: fine for the minorities index, quadratic and far too slow for works.
5. Load-time settings: `refresh_interval: -1`, `number_of_replicas: 0`, then restore refresh (30s) and `_forcemerge` after.
6. The 132 GB file itself is never moved. The mini DB (`data/duckdb/core/core_v4_noworkenrichment-min.duckdb`) is only for testing the chain.
   **The mini DB is denser than prod** (12 orgs/project, 20 orgs/work vs ~1.4 and ~2.8 in prod): use it for logic, not for size estimates.

## 3. Decision log
| # | Decision | Status |
|---|---|---|
| D1 | Staged transfer/load (section 2) | decided |
| D2 | **Links live on the "many" side.** `works` carry `project_ids[]` + `organisation_ids[]`; a project stores only `work_count`. A project's works tab = `works` filtered by `project_ids`. Ids are strings (UBIGINT > signed long). | decided |
| D3 | Exactly 1 topic per project. Project stores `topic_id, subfield_id, field_id, domain_id` (keywords). Topic facet = `terms` agg on `topic_id` over the matched projects, ordered by count (exact). Names come from the api (4.5k topics held in api memory, ~1-2 MB), not from OpenSearch. Tree counts = same agg rolled up by subfield/field. Corpus-aware topic modal = the agg with the `is_ch` filter, cached per corpus (SCI/DCH) in the api. | decided |
| D4 | **No abstracts in works** (v1). Works get no `is_ch`/`minority`/topic. `is_ch` is NOT copied from project to work (a project's work is not necessarily DCH). Corpus selector therefore does not filter works. | decided |
| D5 | Works are trimmed hard (section 4). Keep a `pdf_url` (+ `landing_url`) so the list row can send the user straight to the PDF. Extraction rule from `instances[].urls` needs prod analysis. | decided, rule OPEN |
| D6 | Query syntax: `simple_query_string`, `default_operator: AND`, restricted `flags` (no PREFIX/FUZZY/SLOP/NEAR, i.e. no expensive wildcard/fuzzy), api rewrites literal `AND`/`OR`/`NOT` outside quotes to `+`/`|`/`-`. Never throws on bad syntax. Reason: `query_string` throws on unbalanced quotes/colons, allows `field:value` and leading wildcards (perf + abuse). | provisional, verify perf in prototype |
| D7 | **Grants are derived, not a table.** Group `unnest(project.fundings)` by `fundingStream.id` (e.g. `EC::H2020::RIA` -> funder EC, programme H2020, action RIA). Small index `grants`. Per-grant-code entity dropped. | decided, size OPEN (prod) |
| D8 | Projects/orgs/minorities: index **all** columns (incl. `is_translated`, `pred`, `openaireId`, `minority_qid`), the UI overview shows everything. `pred` lets the UI change the `is_ch` threshold live. | decided |
| D9 | No index for experts. Experts = query `projects`, `terms` agg on `org_ids`, fetch org docs, rank with org rollups. Works are not part of experts. | decided |
| D10 | Funding map = aggregation over the **projects matching the query + all project filters**: per org `sum(funded amount share)`, top-N orgs, then `mget` org geo. List shows the same orgs. | decided |
| D11 | Collaboration: **edge index is OPEN**. Default: org network from a `terms` agg on `projects.org_ids`; query network builds edges in the api from top-N projects with a hard cap (`max_edges`). Payload shape is compact: `nodes[{id,name,lat,lng,w}]`, `edges[{a,b,w}]` (indices into nodes, project ids fetched lazily on click). An edge index (org-pair docs) cannot serve query/filter-aware networks and would be tens of millions of docs on an HDD box. Revisit only if the prototype shows it is too slow. | OPEN |
| D12 | Country-centroid fallback for orgs without geolocation: dropped (60k project-connected orgs with geo, mostly the higher-budget projects, is enough). | decided |
| D13 | Org funding: sum over the org's projects. **Attribution rule OPEN**: (a) full project amount to every participant (over-counts), (b) `amount / org_count` (proposed), (c) Cordis contribution where known (only 392k relations). Whatever is chosen is precomputed once and used by org rollup and the funding agg alike. | OPEN |
| D14 | Currency: `granted.currency` is mixed (EUR, GBP, USD, HRK, ...). Proposal: fixed conversion table applied at export, store `funded_amount` (raw), `currency`, `funded_amount_eur` (approximate, labelled so in the UI). Rates are a static table with a stated date (no live rates). | OPEN |
| D15 | Coordinators: only 392k of 5.5M project->org rows carry `cordis_type` (`coordinator`, `participant`, ...). Store `coordinator_id` on projects where known, plus `org_ids` ordered coordinator first. UI must not promise a coordinator for every project. | decided |
| D16 | Dev env: no CUDA on Mac/VM. | decided |

## 4. Index topology (top level)
| Index | Docs | Purpose |
|---|---|---|
| `projects` | 3.9M | search, facets, experts, collaboration, funding map. 2 shards. |
| `organisations` | 494k | search, autocomplete, overview, geo. 1 shard. |
| `works` | 50M | plain text search + overview. No facets. Sharded (start 4-6, decide from prototype). |
| `minorities` | 278 | search, facets, map. 1 shard. |
| `grants` | ? (derived) | funding search. 1 shard. |
| topics | 4.5k | **not an index**: static table held by the api. |

All: `number_of_replicas: 0`. HDD settings from `PRODUCTION.md` apply (`merge.scheduler.max_thread_count: 1`, `best_compression`).

### projects (sketch, not the final mapping)
Own columns (all, see READ_CORE_V4_FINISHED.md) + denormalised: `org_ids[]` (coordinator first), `org_names[]` (text, low boost),
`org_regions[]`, `org_countries[]`, `coordinator_id`, `org_count`, `work_count`, `topic_id/subfield_id/field_id/domain_id`,
`minority_qids[]` (all), `funder/funding_stream` keywords, `funded_amount(_eur)`, `funded_eur_per_org`, `year`, `is_ch`, `pred`, `is_translated`,
`pillars` (bits -> also 5 booleans or a keyword array for facets), `theme`.
Search fields: `acronym`, `title`, `summary` (+ `keywords`, `grantId`). `search_as_you_type` on `acronym`/`title` for autocomplete.

### organisations
All columns + rollups `project_count`, `work_count`, `total_funding_eur`, `has_dch_project`/`dch_project_count`.
`geolocation` as `geo_point`, `region`, `countryCode`, `nuts3`, `rorTypes`. Autocomplete on `legalName`, `legalShortName`, `alternativeNames`,
ranked by `project_count`. Default ranking: `total_funding_eur`, `project_count`, `work_count`.

### works (trimmed)
Keep: `id`, `title`, `authors` (first ~20 names + `author_count`), `publication_date`/`year`, `publisher`, `container_name`,
`open_access_color`, `best_access_right`, `language`, `citation_count`, `doi`, `pdf_url`, `landing_url` (both `index: false`),
`project_ids[]`, `organisation_ids[]`, `link_tier`.
Drop: descriptions/abstract, influence, views, subjects, instances (avg 12 per work in the mini DB!), formats, sources, other pids, container details.
Ranking: BM25, `citation_count`. Filters: year, OA colour, language. No facets, no autocomplete.

### minorities
All 278 rows, all columns, plus rollups from their projects: `project_count`, `topic_ids`/topic counts, `org_ids`/geo for the map, and a
**title blob** (a text field concatenating the titles/acronyms of the minority's projects, capped) so a text query like "bread" also finds
minorities through their projects. Default ranking: seed groups first (`source_class` `manual_seed`), then project count / work count.

### grants (derived)
One doc per funding stream: funder, jurisdiction, programme, action, description, `project_count`, `total_funded_eur`, `dch_project_count`.
Number of docs unknown until the prod analysis (mini DB: 121 distinct streams in 1000 projects).

## 5. UI implications (per index, for heritagemonitor)
- **Global**: ids are strings everywhere; routes use ids. Corpus selector SCI/DCH = `is_ch` filter on `projects`; on `organisations` via
  `has_dch_project`; on `minorities` via a DCH project count; **no effect on works** (UI should say so or grey it out). Topic modal and
  its counts are corpus-aware (D3). Search box: D6 syntax, tell users about `"phrase"`, `-term`, `AND`.
- **projects**: tabs = overview (every column; `openaireId` links to OpenAIRE, `doi` links out, `is_translated` shown as a badge, `pred` shown),
  organisations (coordinator first *if known*, else ordered by relation), works (`works` filtered by `project_ids`, paged). Clicking an
  org/work routes to `/search/organisations|works` by id. Ranking: BM25 or budget (no alphabetic). Filters: year, theme, pillar, topic, grant,
  region. Facets: topic count (D3), org count, date histogram. Budget sort caveat: currency (D14).
- **organisations**: tabs = overview, projects list, works list (`works` filtered by `organisation_ids`). Ranking: funding, project count, work count.
  Filters: region, `ror_types`. Autocomplete ranked by `project_count`. Only ~18% of project-connected orgs have coordinates: map views show those only.
- **works**: overview + project/organisation tabs (fetch by `project_ids`/`organisation_ids`). List row has a "PDF" button (`pdf_url`, fall back to
  `landing_url`, hide if none). No facets, no autocomplete, only year/OA/language filters. Abstract is not shown (not indexed) -> overview has no abstract in v1.
- **experts**: results are orgs from the projects agg (D9); facets: topic count; filters: reuse project filters (they apply to the underlying project query).
- **minorities**: existing UI mostly stays, DTOs change to the new column names. Detail tabs list projects/orgs/topics/grants/works through the
  other indexes (`projects.minority_qids`), not embedded. Map = second request: orgs + minority qids + geo.
- **funding**: paged list + map columns from the same query (D10), 2nd request for the map.
- **collaboration**: organisationNetwork autocomplete on organisations; map arcs from D11; queryNetwork needs a `max_edges` parameter in the UI.

## 6. Field-by-field mappings
_TODO once the prototype and the prod analysis (`tmp_session/AGENT_JOB_RESULTS.md`) are in. One subsection per index: mapping JSON, settings,
export SQL, api query examples, facet tiers._

## 7. Open questions
Currency (D14), org funding attribution (D13), collaboration edge index (D11), work `pdf_url` extraction rule (D5), grants count (D7),
works shard count and bytes/doc (prototype), `simple_query_string` cost on works (D6).
