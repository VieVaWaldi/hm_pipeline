# core_v4 -> OpenSearch export: results (2026-09-20)

Output: `/vast/lu72hip/hm_pipeline/data/serving_export/` (7.9 GB total). No code fixes were needed (no `EXPORT_FIXES.md`). Git: HEAD 81fc098, working tree clean before the run; nothing committed.

## Jobs
| step | job id | state | elapsed | peak mem (MaxRSS) |
|---|---|---|---|---|
| smoke (works-sample 1000, 1 chunk) | 8981218 | COMPLETED 0:0 | 2:00 (export 87 s) | 64.6 GB |
| full export (10 works chunks) | 8981219 | COMPLETED 0:0 | 3:42 (export 217 s) | 39.1 GB |
| verification (DuckDB over the Parquet) | 8981221 | COMPLETED 0:0 | 0:12 | n/a |

The full run needed no resubmit and no changes to `export.sbatch`. The smoke-run extrapolation (works ~1000x of 52 s = up to 14 h) was far too pessimistic: the real works time is ~18-19 s per 5M-row chunk, so the fixed cost dominated the smoke run.

Runtime per index (full run): organisations 2.4 s, projects 20.6 s, minorities 0.2 s, grants 0.2 s, works 17.6-19.2 s per chunk.

Minority source (manifest): mode `exclude`, 2,878 excluded pairs, 9,293 stored -> 6,503 projects with a minority, 6,650 project-group tags, 9 groups emptied (Q18690619, Q208551, Q214361, Q257528, Q2656122, Q415693, Q63884107, Q7654710, Q846578).

## Verification
| check | expected | actual | |
|---|---|---|---|
| works rows | 50,000,000 | 50,000,000 | OK |
| projects rows | 3,893,065 | 3,893,065 | OK |
| organisations rows | 494,099 | 494,099 | OK |
| minorities rows | 278 | 278 | OK |
| grants rows | ~6,120 | 6,120 | OK |
| works `is_ch_via_project` | 35,975 | 35,975 | OK |
| tier-0 works with empty `project_ids` | 922,801 | 922,801 | OK |
| projects with non-empty `coordinator_ids` | ~81,043 | 81,043 | OK |
| projects with NULL `topic_id` | ~8,795 | 8,795 | OK |
| distinct funders in projects | ~103 | 103 | OK |
| projects with non-empty `minority_qids` | 6,503 | 6,503 | OK |
| minorities with `project_count` 0 | 191 | 191 (file has all 278 rows) | OK |
| works with `pdf_url` / `landing_url` | ~13.4% / 99.65% | 13.42% / 99.65% | OK |
| NULL ids (all 5 sets) | 0 | 0 | OK |
| duplicate ids (all 5 sets) | 0 | 0 (distinct = total everywhere) | OK |
| id type | VARCHAR | VARCHAR in all 5 sets | OK |
| works `organisation_ids` max length | <= 100 | 100 (0 works above 100) | OK |
| `api/topics.json` | valid, ~4,516 | valid list, 4,516 | OK |
| `api/publishers.json` | valid, ~3,000 | valid list, 3,000 | OK |
| total size | works ~8 GB, projects ~0.75 GB, orgs ~60 MB, min/grants < 1 MB | works 7.1 GB (7.57 GB decimal), projects 719 MB, orgs 59 MB, minorities 99 KB, grants 227 KB, api 1.2 MB; total 7.9 GB | OK |

Extra: `link_tier` distribution: tier 0 = 5,001,873, tier 1 = 44,998,127. Max `org_ids` per project = 380 (no limit applies to projects).

## Files
| file | size | rows |
|---|---|---|
| api/publishers.json | 0.19 MB | 3,000 entries |
| api/topics.json | 1.04 MB | 4,516 entries |
| export_manifest.json | 2.5 KB | |
| grants/grants.parquet | 0.23 MB | 6,120 |
| minorities/minorities.parquet | 0.10 MB | 278 |
| organisations/organisations.parquet | 61.64 MB | 494,099 |
| projects/projects.parquet | 753.72 MB | 3,893,065 |
| works/works_00.parquet | 757.18 MB | 5,000,377 |
| works/works_01.parquet | 757.32 MB | 4,998,827 |
| works/works_02.parquet | 756.85 MB | 4,998,770 |
| works/works_03.parquet | 756.44 MB | 4,997,399 |
| works/works_04.parquet | 756.95 MB | 4,999,353 |
| works/works_05.parquet | 756.93 MB | 5,000,209 |
| works/works_06.parquet | 757.88 MB | 5,003,975 |
| works/works_07.parquet | 756.98 MB | 4,998,584 |
| works/works_08.parquet | 756.96 MB | 4,999,115 |
| works/works_09.parquet | 757.60 MB | 5,003,391 |

## Warnings (data quality, not export bugs; nothing changed)
- **HTML entities `&amp;` in source text**: works.container_name 1,766,178 rows (e.g. "ACS Applied Materials &amp; Interfaces"), works.title 9,208, works.publisher 3,095, projects.title 2, projects.summary 31, projects.acronym 6. Organisations: 0. Titles also contain raw markup such as `<sub>3</sub>`. The frontend either decodes/strips these or the export SQL gets an unescape step (decision for the laptop session; a rerun takes ~4 minutes).
- Empty strings: none found in the checked text columns (title, publisher, container_name, doi, summary, acronym, legalName, name_key). NULLs are used instead (works.container_name 12.6M NULL, publisher 4.0M NULL, doi 6.8M NULL; projects.summary 3.36M NULL).
- Sample rows (5 works, 5 projects, 5 organisations) looked fine: arrays are real arrays, types are as expected. Small source quirks: NIH projects have `org_regions` ['Unknown'] with empty `org_countries`, duplicated org names in `org_names` (e.g. 'YALE UNIVERSITY' twice for two org ids), and many `pending_org_` organisations have no geo/ror.
- Smoke output `data/serving_export_smoke/` is still on disk (small, git-ignored); it can be deleted.

## Doc counts to compare after loading
works 50,000,000 | projects 3,893,065 | organisations 494,099 | minorities 278 | grants 6,120
