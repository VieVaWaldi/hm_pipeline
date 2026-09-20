# core_v4 serving design (OpenSearch): the one place for decisions

Status of 2026-09-20. Deadline: 2 days. Source: `core_v4_noworkenrichment.duckdb` (see `../READ_CORE_V4_FINISHED.md`).
Consumer: `/Users/wehrenberger/Code/DIGICHer/heritagemonitor` (api + web). Use cases: `apps/web/src/common/catalog/useCases.ts`.
Field-by-field mappings are the **final goal** of this file (section 6, still empty). Everything else is decided or marked OPEN.

## 1. Locations and constraints
- **Cluster (Draco)**: has the 132.6 GB DuckDB. Work only in `/vast/lu72hip/hm_pipeline`, never `/home`. Heavy work as slurm jobs.
- **Mac (home)**: relay. **50 Mbps upload** (~6 MB/s, ~20 GB/hour realistic). Everything except works goes up from home; works goes up from
  the university network only if the trimmed works Parquet (no abstracts) turns out big (decide from the dry-run sizes in agent_job section M).
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
| D4 | **No abstracts in works** (v1). Works get no topic/minority enrichment. **`is_ch` on works is a proxy**: a tier-0 work gets `is_ch_via_project = true` if ANY linked project has `is_ch` (tier-1 works: false, never in the DCH corpus). The name says it is a proxy; the UI/DTO must not present it as a classification of the work. DCH corpus on works = tier-0 works of DCH projects. | decided (reverses the first draft) |
| D4b | Same proxy logic for minorities: tier-0 works get `minority_qids[]` = union of the `minority_qid` of all linked projects (empty for tier 1). Works then take part in the minorities use case (`works.minority_qids` filter). Caveat as D4: inherited from the project, not a property of the work. | decided |
| D5 | Works are trimmed hard (section 4). Keep a `pdf_url` (+ `landing_url`) so the list row can send the user straight to the PDF. Extraction rule from `instances[].urls` needs prod analysis. | decided, rule OPEN |
| D6 | Query syntax: `simple_query_string`, `default_operator: AND`, restricted `flags` (no PREFIX/SLOP/NEAR, no user wildcards), api rewrites literal `AND`/`OR`/`NOT` outside quotes to `+`/`|`/`-`. Never throws on bad syntax. Reason: `query_string` throws on unbalanced quotes/colons, allows `field:value` and leading wildcards (perf + abuse). **Typo tolerance is required** but is NOT user syntax: strict query first, and if it returns few hits (< threshold) the api reruns a `match` with `fuzziness: AUTO`, `prefix_length: 2`, capped `max_expansions` (on works this fallback is the expensive path, measure it), plus a "did you mean" term/phrase suggester on titles/names. **Autocomplete** everywhere except works (D-list in section 5). | provisional, verify perf in prototype |
| D7 | **Grants are derived, not a table.** Group `unnest(project.fundings)` by `fundingStream.id` (e.g. `EC::H2020::RIA` -> funder EC, programme H2020, action RIA). Small index `grants`. Per-grant-code entity dropped. | decided, size OPEN (prod) |
| D8 | Projects/orgs/minorities: index **all** columns (incl. `is_translated`, `pred`, `openaireId`, `minority_qid`), the UI overview shows everything. **`pred` is display-only** (overview of a project): no filter, no slider, users never set it; only the pipeline owner changes the `is_ch` threshold, at export time. | decided |
| D9 | No index for experts. Experts = query `projects`, `terms` agg on `org_ids`, fetch org docs, rank with org rollups. Works are not part of experts. | decided |
| D10 | Funding map = aggregation over the **projects matching the query + all project filters**: per org `sum(funded amount share)`, top-N orgs, then `mget` org geo. List shows the same orgs. | decided |
| D11 | Collaboration: **edge index is OPEN**. Default: org network from a `terms` agg on `projects.org_ids`; query network builds edges in the api from top-N projects with a hard cap (`max_edges`). Payload shape is compact: `nodes[{id,name,lat,lng,w}]`, `edges[{a,b,w}]` (indices into nodes, project ids fetched lazily on click). An edge index (org-pair docs) cannot serve query/filter-aware networks and would be tens of millions of docs on an HDD box. Revisit only if the prototype shows it is too slow. | OPEN |
| D12 | Country-centroid fallback for orgs without geolocation: dropped (60k project-connected orgs with geo, mostly the higher-budget projects, is enough). | decided |
| D13 | Org funding: sum over the org's projects with an **equal split**: each participant gets `funded_amount_eur / org_count` (`funded_eur_per_org` on the project). Precomputed once, used by the org rollup and the funding agg alike. | decided |
| D14 | Currency: `granted.currency` is mixed (EUR, GBP, USD, HRK, ...). Proposal: fixed conversion table applied at export, store `funded_amount` (raw), `currency`, `funded_amount_eur` (approximate, labelled so in the UI). Rates are a static table with a stated date (no live rates). Table: section 3b. | decided, table to be trimmed to the currencies that matter after agent_job section B |
| D15 | Coordinators: only 392k of 5.5M project->org rows carry `cordis_type` (`coordinator`, `participant`, ...). Store `coordinator_ids[]` (a project can have several coordinators, seen in the mini DB) on projects where known, plus `org_ids` ordered coordinators first. UI must not promise a coordinator for every project. | decided |
| D16 | Dev env: no CUDA on Mac/VM. | decided |
| D17 | Projects carry **funder** and **programme** as two separate keyword facets (arrays, a project can have several fundings), derived from `fundings[].shortName`/`fundingStream.id` levels (funder = level 1, programme = level 2, e.g. `EC` / `H2020`); `frameworkProgrammes` stays as a raw field. `grants` has the same two facets. | decided, exact split rule from agent_job section A |
| D18 | Works filters: year, OA colour, language, publisher. Not `isInDiamondJournal`. No facets on works (a publisher filter needs a value picker, see UI). | decided |

### 3a. Findings from the prod analysis (`agent_job/AGENT_JOB_RESULTS.md`, 2026-09-20) and the decisions they force
Corrections to earlier assumptions, then decisions **D19-D33** (status "default" = baked into the export unless the user objects).
- **`link_tier = 0` does NOT mean "has a project"**: 922,801 of the 5.0M tier-0 works (18.4%) have no project relation. Use `len(project_ids) > 0`
  for "linked". `is_ch_via_project` / `minority_qids` are therefore only true/non-empty for works with a project; DCH corpus on works = **35,975 works**.
  (READ_CORE_V4_FINISHED.md is wrong on this point.)
- 316,230 projects (8.1%) have no org: `funded_eur_per_org` is NULL when `org_count = 0` (D13 would divide by zero); 443,648 tier-0 works have 0 orgs.
- D11 numbers: 7.35M (project, org-pair) edges, **4.41M distinct org pairs** (153,616 for DCH). Much smaller than feared, but the busiest pairs are the same institution
  under two org ids (see D19), so the decision stays: no pair index for v1; fallback = a 4.4M-doc pair index built from the same Parquet if the live agg is too slow on the VM.
- Collaboration/funding maps are effectively **EU maps**: 26% of projects (49% of DCH projects) have >= 1 geolocated org; NIH/NSF/SNSF projects almost never.
- Trimmed works doc: 684 B JSON avg, ~34 GB raw, **7.6 GB Parquet** for 50M -> **works go up from home (~0.4 h at 50 Mbps), no university detour**. Everything else 0.7 GB.

| # | Decision | Status |
|---|---|---|
| D19 | **Duplicate organisations** (46,065 duplicated names, e.g. Johns Hopkins under 2 ids, 23k shared projects "with itself"). v1: org doc gets `name_key` = normalised `legalName` + `countryCode` (lowercase, no punctuation, `&amp;` unescaped); collaboration/experts use it to drop self-pairs and group duplicates in the api. Real merge (canonical id remapped in projects/works/rollups) = after the deadline. | default, user may pick full merge |
| D20 | Funder = `fundings[].shortName` (103 values, never NULL); programme = level 2 of `fundingStream.id` (= `frameworkProgrammes`, 4,552 values, NULL for 293k projects). Funder facet = normal terms facet; **programme = top-N + typeahead picker** (4.5k values). `&amp;` unescaped in stream ids/names. Grants index = 6,074 docs + 1 bucket "no stream id". | default |
| D21 | Currency rules: table 3b + `NULL` currency with funder SNSF -> CHF; `$` -> USD except funder NHMRC -> AUD; `funded_amount_eur > 1e9` set to NULL (four 2.5bn EC rows, Wellcome INR 3.64bn, MAX IV SEK 1.55bn are junk); other unknown currencies NULL. `totalCost` is not indexed (0 for 98.8%). Only 58% of projects have an amount: UI says so. | default |
| D22 | Works `organisation_ids[]` capped at 100 (keep `org_count`); p99 is 41, max 2,660. Projects: no cap. | default |
| D23 | Works `language`: normalise the 3-letter codes (`fra/fre` -> `fra`, `esl/spa` -> `spa`); `und` (36%) -> NULL/"unknown". A filter on it is fine after that. | default |
| D24 | Works `publisher` filter: 310,891 distinct values, a free facet is unreasonable. The export also writes a small `publishers` list (top ~3,000 by works, covers 88%); the api holds it in memory (like topics) and serves the typeahead; the filter is an exact `term` on the keyword. No normalisation of publisher variants in v1. | default |
| D25 | Works UI button: `pdf_url` exists for 13.4% of works (36.8% tier 0), `landing_url` for 99.65%. Row shows "PDF" when `pdf_url`, else "DOI/page" via `landing_url`. Rule and macros are final (results section G6, `&amp;` unescaped). | decided |
| D26 | "Unknown" buckets: `region` NULL for 43% of project-connected orgs (US institutions without countryCode) -> export `region = 'Unknown'`; `rorTypes` empty for 74% -> `ror_types = ['unknown']`. Filters show them. | default |
| D27 | Text cleaning at export: unescape `&amp;` in project title/summary, org names, stream ids; strip HTML tags from summaries (2,026). Projects: `summary` is NULL for 86%, `acronym` NULL for 97%: UI must not promise abstract search. | default |
| D28 | Dates: `year` = NULL outside 1950..2040 (data has 1900..2125 junk); `startDate/endDate` kept raw for the overview; UI year facet range clamped to 1980..current year + 3. | default |
| D29 | Topics: `topic_id` nullable (8,795 projects have none), topic agg gets a `missing` bucket; DCH corpus shows 1,715 of 4,513 topics. | decided |
| D30 | Coordinators: EC-only (81,043 projects, 63% of EC, 1,800 DCH). UI hides the coordinator column for other funders. | decided |
| D31 | `pred` vs `is_ch`: `is_ch` is NOT `pred > 0.55` (1,170 flagged projects are below it). Never recompute `is_ch` from `pred` in the api/UI. | decided |
| D32 | Minorities: 182 of 278 have no project (default ranking puts them last after the seeds); **Manx (1,787), Russians (2,242), Turkish (1,676) look like keyword false positives** (unreviewed): review a sample before showing project counts prominently. | OPEN (review) |
| D33 | Org rollups are heavy-tailed (CNRS 840,577 works, one org 48,337 projects): rank with a log/saturation function (`rank_feature`/`log1p`), not raw sums. | default |

### 3b. Currency table (D14): units of currency per 1 EUR
Source: ECB euro foreign exchange reference rates, reference date **2026-09-18** (`ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml`,
fetched 2026-09-20). Approximate on purpose: one date applied to grants of all years, shown in the UI as "approx. EUR".
`funded_amount_eur = funded_amount / rate[currency]`. Currencies not in the table -> `funded_amount_eur = NULL` (never guess).

| EUR | USD | GBP | CHF | SEK | NOK | DKK | PLN | CZK | HUF | RON | ISK | TRY |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.1460 | 0.85880 | 0.9462 | 11.2915 | 10.8095 | 7.4754 | 4.3635 | 24.339 | 364.28 | 5.2647 | 139.40 | 55.9077 |

Others in the ECB list (only add if agent_job B shows they matter): JPY 180.94, AUD 1.6095, BRL 5.8857, CAD 1.6056, CNY 7.6755, HKD 8.9903,
IDR 20424.81, ILS 3.4812, INR 109.8755, KRW 1590.76, MXN 19.6855, MYR 4.6763, NZD 2.0068, PHP 71.972, SGD 1.4651, THB 38.225, ZAR 18.6482.
Not in the ECB list: **HRK** (Croatia is in the euro since 2023, fixed conversion 7.53450 HRK = 1 EUR, appears in the mini DB) and **BGN**
(fixed peg 1.95583 BGN = 1 EUR). These two fixed rates are from memory of the official fixings, verify before shipping.
The values were read via a page-summary fetch, not a raw download: **spot-check 2-3 rates against the ECB page before the export runs.**

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
`project_ids[]`, `organisation_ids[]`, `link_tier`, `is_ch_via_project` (D4, proxy), `minority_qids[]` (D4b, proxy).
`pdf_url` rule (decided order): open-access `.pdf` url, else any `.pdf` url, else NULL; `landing_url`: `https://doi.org/<doi>`, else first OPEN url, else first url.
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
  `has_dch_project`; on `minorities` via a DCH project count; on `works` via `is_ch_via_project` (proxy: only works of DCH projects; UI should
  say "via linked project" and tier-1 works are never in DCH). Topic modal and
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
  other indexes (`projects.minority_qids`, `works.minority_qids` for tier-0 works, labelled "via linked project"), not embedded. Map = second request: orgs + minority qids + geo.
- **funding**: paged list + map columns from the same query (D10), 2nd request for the map.
- **collaboration**: organisationNetwork autocomplete on organisations; map arcs from D11; queryNetwork needs a `max_edges` parameter in the UI.

## 6. Field-by-field mappings
_TODO once the prototype (`prototype/`, `PROTOTYPE_REPORT.md`) and the prod analysis (`agent_job/AGENT_JOB_RESULTS.md`) are in. One subsection per index: mapping JSON, settings,
export SQL, api query examples, facet tiers._

## 7. Open questions
Currency table trim + HRK/BGN check (D14), collaboration edge index (D11), real `pdf_url` coverage (D5), grants count (D7), funder/programme split (D17),
works shard count and bytes/doc (prototype), typo-tolerance cost on works (D6), publisher filter UI (distinct publisher count from section F),
nothing else blocking the mappings.
