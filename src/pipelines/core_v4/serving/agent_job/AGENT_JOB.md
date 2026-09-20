# AGENT JOB: analyse core_v4 on the cluster (read-only) to size and design the OpenSearch indexes

## Rules (read first)
- Work **only from `/vast/lu72hip/hm_pipeline`**. Never read or write under `/home`. Write scratch/output only under
  `/vast/lu72hip/hm_pipeline/tmp_session/` (create it).
- **Use slurm (`sbatch`) for every query/job**, never run heavy DuckDB on the login node. Look at how `merge_single.py` was run
  (job 8981036) and at `slurm-*.out` in the repo root for the sbatch header conventions used here (partition, memory, time). One job per
  analysis group is fine; run independent groups in parallel. Set DuckDB `memory_limit` below the job memory, `threads` = cpus, and
  `temp_directory` under `/vast/lu72hip/hm_pipeline/tmp_session/duck_tmp`.
- The database is `/work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb` (132 GB). Open it **read-only**
  (`duckdb.connect(path, read_only=True)`). Never write to it. Do not touch any other file under `/work`.
- Read `../../READ_CORE_V4_FINISHED.md` (schema and counts) and `src/pipelines/core_v4/serving/SERVING_DESIGN.md`
  (why we ask). Do not change any code in the repo, do not git commit or push.
- Write results **incrementally** to `/vast/lu72hip/hm_pipeline/tmp_session/AGENT_JOB_RESULTS.md` (one section per analysis letter below,
  after each job finishes, not at the very end). For each section include: the exact SQL, the numbers as small markdown tables, the slurm
  job id and runtime, and one or two sentences of what it means for the index design. Numbers, not prose. If something is too slow, say so,
  sample it (say how) and go on. The user is on a deadline: do A-G first, H-M after.
- Terminology: a work is a `product` in `relation`. `link_tier` 0 = linked to a project (5.0M works), 1 = only org-linked.

## What we want to know

### A. Funding streams / grants (a "grant" is a derived funding stream, there is no grants table)
- `count(distinct fundingStream.id)` over `unnest(project.fundings)`; number of distinct `fundings[].name`/`shortName`/`jurisdiction`.
- Split `fundingStream.id` on `::` into levels (funder :: programme :: action ...): distinct count per level, max depth, and the top 50 streams by
  project count with description. Show the full hierarchy for `EC` (EC :: H2020/FP7/HE/ERASMUS+ ... :: ...).
- Projects with no funding at all; projects with more than one funding entry (distribution).
- `count(distinct grantId)`, `count(distinct frameworkProgrammes)` and the top 30 values of `frameworkProgrammes`.
- Per top-20 funder: project count, projects with `granted.fundedAmount > 0`, sum fundedAmount.

### B. Currency and budget
- Distribution of `granted.currency` (count, count with `fundedAmount > 0`, sum, min, median, max).
- Share of projects with NULL currency / 0 or NULL `fundedAmount` / 0 `totalCost`; per funder for the top 10 funders.
- The 20 largest `fundedAmount` values with currency and funder (outliers).
- Which currencies cover 99% of the money (the list we need a conversion table for).

### C. Geolocation and regions
- `organization.region` distribution for all orgs and for project-connected orgs; NULL share; `countryCode` NULL share.
- Of the 5,500,329 project->organization relations: share whose organization has a `geolocation`; share of projects with >=1 geolocated org;
  the same for projects with `is_ch` true; the same weighted by `granted.fundedAmount`.
- `geolocation_source` split for project-connected orgs (should be 63,885 in total, check).

### D. Cardinalities and tails (drives array sizes, the collaboration design and the response sizes)
- Orgs per project: min/avg/p50/p90/p99/max, count of projects with >50 and >200 orgs, the 10 biggest with title.
- Works per project (project->product): same statistics + top 20 projects. Projects per work; orgs per work (`product`->organization, source is the work).
- Projects per org and works per org: percentiles, max, top 20 orgs with `legalName` (project count, work count).
- **Collaboration size:** exact `sum(n*(n-1)/2)` over projects (n = orgs in the project) = number of (project, org-pair) edges. And the number of
  distinct unordered org pairs that share >=1 project (exact if it fits in a job, else a documented sample/estimate). Also for `is_ch` projects only.
- Number of (project, org) relations per `relType`/`provenance`.

### E. Coordinators
- `cordis_type` distribution on project->organization relations; number of projects with a `coordinator`; of the `is_ch` projects; of the
  top-10 funders' projects. Does any project have more than one coordinator?

### F. Works: sizing of a trimmed document
We plan to index per work only: `id`, `title`, first 20 author `fullName`s + author count, `publicationDate`, `publisher`, `container.name`,
`openAccessColor`, `bestAccessRight.label`, `language.code`, `citationCount`, doi, pdf_url, landing_url, `project_ids[]`, `organisation_ids[]`, `link_tier`.
- Average and p50/p90/p99/max for: `length(title)`, `len(authors)`, sum of lengths of the first 20 `authors[].fullName`, `length(publisher)`,
  `length(container.name)`, `len(instances)`, orgs per work. Split by `link_tier`.
- Estimated raw JSON bytes per trimmed document (sum of the above + ids as 20-char strings), per tier, and total for 50M.
- NULL shares: `publicationDate`, `title`, `citationCount`, `language`, `publisher`. Distribution of year (histogram), `openAccessColor`, `bestAccessRight.label`,
  `language.code` (top 20), `citationCount` (percentiles, share 0/NULL). Distinct `publisher` count (is a facet/filter on it reasonable?).
- Are titles possibly not in English (share by `language.code`)? Works were not translated.

### G. Works: PDF / landing URL (the list row gets a "PDF" button)
`instances` is `STRUCT(accessRight, alternateIdentifiers, articleProcessingCharge, license, pids, publicationDate, refereed, type, urls VARCHAR[])[]`.
- Share of works (per tier) with: any instance url; any url ending in `.pdf` (case-insensitive, ignoring query string) or containing `/pdf`;
  any url from an instance whose `accessRight.label = 'OPEN'`; an open-access instance with a `.pdf` url; a doi pid (`pids` scheme doi/DOI).
- Propose **one extraction rule** for `pdf_url` (e.g. first `.pdf` url among OPEN instances, else first `.pdf`, else NULL) and for `landing_url`
  (e.g. `https://doi.org/<doi>`, else first OPEN url, else first url). Report the coverage of each rule per tier, the size added per work, and give 15 example
  rows (`title`, chosen `pdf_url`, `landing_url`) across tiers. Mention any weird url hosts that dominate (e.g. sci-hub-like or dead patterns, top 20 hosts).
- Write the rule as a DuckDB SQL expression that can be reused in the export view.

### H. Projects: text and classifier
- Percentiles of `length(summary)`, `length(title)`, `length(keywords)`; NULL summary share; `acronym` NULL share.
- `pred` histogram (counts above 0.3, 0.4, 0.5, 0.55, 0.6, 0.7, 0.8, 0.9), `is_ch` count, `is_translated` count.
- `startDate` year histogram and NULL share; `theme` counts; `pillars` counts per bit and combos; projects with `openAccessMandate*` true.

### I. Topics
- Projects per topic (percentiles, top 20 with names, number of topics with 0 projects); split by `is_ch`. Confirm exactly one topic per project
  (`max(count(*)) group by source_id` in `relation_topic`). Number of topics per domain/field/subfield level actually used.

### J. Minorities
- Projects per minority qid (all 96 used groups) with `group_name_en`; per group: total `length(title)` of its projects (for a title-blob cap),
  number of distinct organizations, of which geolocated; the projects that carry more than one minority.

### K. Organizations
- `rorTypes` distribution; percentiles of `len(alternativeNames)` and name lengths; share with NULL `legalName`; duplicates of `legalName`
  (top 20 duplicated names and their counts); `countryCode` top 20; share of orgs with `rorId`; orgs connected to projects vs works only, by region.

### L. Sanity
- `id` uniqueness in `project`, `organization`, `work`; every `relation` endpoint exists (already checked at merge, just re-confirm cheaply for a 1% sample).
- Max `id` per table and confirm all fit in `UBIGINT` (we export ids as strings).

### M. Export dry run (measures transfer size and time, writes a few GB at most)
Under `/vast/lu72hip/hm_pipeline/serving/agent_job/out/`, using `COPY (...) TO ... (FORMAT parquet, COMPRESSION zstd)`:
1. Works, trimmed as in F + the url rules from G, **tier 0 only (5.0M)**, with `project_ids` and `organisation_ids` as `VARCHAR[]` (aggregated from
   `relation`). Report: runtime, file size, bytes per row, and extrapolated size for all 50M works (tier 1 sampled: 1M rows).
2. A 3% random sample of projects with all columns + `org_ids[]` + `topic_id` + `work_count`: report size and extrapolate to 3.89M.
3. All organizations with all columns + rollups `project_count`, `work_count`: report size (should be small).
Report the exact SQL for each; those become the export views.

## Deliverable
`/vast/lu72hip/hm_pipeline/serving/agent_job/AGENT_JOB_RESULTS.md`, sections A-M as above, then a final section **"Surprises"** (anything in the data
that contradicts `READ_CORE_V4_FINISHED.md` or would break a design assumption: many-to-few links, 1 topic per project, id types, missing values).
The user copies this file back to their laptop, so keep it self-contained.
