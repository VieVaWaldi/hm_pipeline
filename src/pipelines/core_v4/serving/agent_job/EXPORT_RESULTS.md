# core_v4 -> OpenSearch export, rerun 2 after the D27 text-cleaning fix (2026-09-21)

Output: `/vast/lu72hip/hm_pipeline/data/serving_export/` (7.8 GB total), rewritten in place with `--force`; every file is from this run (works/projects/organisations/minorities/grants/publishers.json).
Git: HEAD 38b780e ("serving export 3"), working tree clean; nothing committed. No code changes, no `EXPORT_FIXES.md`.
The rerun used `tmp_session/export_full_force.sbatch` = `export/export.sbatch` + `--force` (the committed script is untouched).

## Jobs
| step | job id | state | elapsed | peak mem (MaxRSS) |
|---|---|---|---|---|
| smoke (`--works-sample 1000 --force`) | 8981227 | COMPLETED 0:0 | 2:38 (export 132 s) | 62.9 GB |
| full export, `--force` | 8981228 | COMPLETED 0:0 | 8:51 (export 515 s) | 40.0 GB |
| verification | 8981229 | COMPLETED | 0:30 | n/a |
| residue inspection / grants / raw-title checks | 8981230-8981234 | small read-only jobs (8981230 failed on an unsupported regex lookahead in my script and was redone) | seconds | n/a |

Per index (full run): organisations 4.7 s, projects 33.8 s, minorities 0.4 s, grants 0.2 s, api/publishers.json ~25 s, works 42.6-44.7 s per 5M-row chunk (about 2.4x slower than run 1 because of the cleaning).
Minority source (manifest): mode `exclude`, 2,878 pairs, 6,503 projects, 9 groups emptied. Smoke printed the expected minority line.

## Verification
| check | expected | actual | |
|---|---|---|---|
| works rows | 50,000,000 | 50,000,000 | OK |
| projects rows | 3,893,065 | 3,893,065 | OK |
| organisations rows | 494,099 | 494,099 | OK |
| minorities rows | 278 | 278 | OK |
| grants rows | about 6,120 | **6,119** | OK (explained, see below) |
| works `is_ch_via_project` | 35,975 | 35,975 | OK |
| tier-0 works with empty `project_ids` | 922,801 | 922,801 | OK |
| projects with `coordinator_ids` | about 81,043 | 81,043 | OK |
| projects with NULL `topic_id` | about 8,795 | 8,795 | OK |
| distinct funders | about 103 | 103 | OK |
| projects with `minority_qids` | 6,503 | 6,503 | OK |
| minorities with `project_count` 0 | 191 | 191 (278 rows) | OK |
| `pdf_url` / `landing_url` | about 13.4% / 99.65% | 13.42% / 99.65% | OK |
| `&lt;` `&gt;` `&quot;` `&nbsp;` in works.title/publisher/container_name, projects.title/summary | 0 | works.title: **1** `&lt;`, **1** `&gt;`; all other columns 0 | **DEVIATION (tiny)** |
| `&amp;` in the same columns | 0 | works.title **9**; works.publisher, works.container_name, projects.title, projects.summary all 0 (was 1,766,178 in container_name) | **DEVIATION (tiny)** |
| other entity-like residue | a handful of look-alikes | see "Residue" below | OK |
| residual `<tag` in works.title / projects.title / projects.summary | counts, no `<sub>` `<i>` `<mi>` `<p>` `<br>` | works.title 3,281, projects.title 53, projects.summary 725; no real `<sub>`/`<i>`/`<mi>`/`<p>`/`<br>`/MathML tag left (0 matches for `<tag>` / `</tag>` / `<tag/>` of those names in all three columns); details below | OK |
| `api/publishers.json` values | all occur in works.publisher, none with `&amp;` | 0 values missing from works.publisher, 0 with `&amp;` (3,000 entries) | OK |
| NULL ids / duplicate ids (all 5 sets) | 0 / 0 | 0 / 0 | OK |
| id type | VARCHAR | VARCHAR everywhere | OK |
| works `organisation_ids` max length | <= 100 | 100 (none above) | OK |
| `api/topics.json`, `api/publishers.json` | valid, ~4,516 / ~3,000 | valid, 4,516 / 3,000 | OK |
| sizes | works ~8 GB, projects ~0.75 GB, orgs ~60 MB, min/grants < 1 MB | works 7.1 GB, projects 719 MB, orgs 59 MB, minorities 99 KB, grants 227 KB, api 1.2 MB; total 7.8 GB | OK |

### Grants 6,119 instead of 6,120
Not a bug. The grant id is the cleaned funding-stream id. In the database two raw ids, `SFI::SFI Stokes Professorship &amp; Lectureship Programme` and
`SFI::SFI Stokes Professorship &amp;amp; Lectureship Programme`, now clean to the same `... & Lectureship Programme`. Raw distinct stream ids 6,074 -> cleaned 6,073, plus the per-funder placeholders = 6,119 (ids distinct, none NULL).

### Residue after cleaning (nothing was changed, decision for the laptop session)
- **11 works titles** still contain `&amp;` (9), `&lt;` (1), `&gt;` (1). Cause (checked against the raw table): the source is escaped 4-5 levels deep in these rows (e.g. `Potential &amp;amp;amp;amp;amp; Associated`, `&amp;amp;amp;lt;italic&amp;amp;amp;gt;`), `hm_clean` decodes 3 passes. 11 of 50,000,000 rows; a fourth/fifth decode pass in `hm_dec3` would fix them (rerun ~9 min).
- **Other `&name;` residue** (strict `&xxx;` pattern): works.title has case variants and rare entities the map does not know (`&Apos;` 70, `&8217;` 50, `&Amp;` 30, `&NBSP;` 17, `&Nbsp;` 16, `&dot;` 9, `&8211;` 6, `&769;` 5, `&plus;` 4 ...; the numeric ones like `&8217;` are malformed, no `#`); works.container_name 1 (`&dtrif;`); works.publisher 0; projects.title `&955;` 6, `&RETINA;` 3, `&ELDERLY;` 3, `&IMMUNITY;` 3, `&945;` 2 (plain-text look-alikes); projects.summary ~16 look-alikes (`&Va00;`, `&al2022;`, `&Li99;` ...: citation-like text). All in the tens or low hundreds of rows.
- **Loose "any `&xx`" counts** (`works.title` 5,302, `container_name` 14,574, `projects.title` 38,986, `summary` 1,822) are plain-text ampersands like `R&CPS`, `M&IS`, not entities, and are expected.
- **Residual `<tag`**: what remains are unknown pseudo-tags and comparisons (`<z`, `<x`, `<A Narrative Review>`, `<Background`, `<Objectives`, `<alpha`, `<Zea mays` ...), reversed/mangled source tags (`>i<Xylella>/i<`, `>sup<32>/sup<P`), and a few malformed real tags with attributes (works.title: `<span ` 6, `<p ` 6, `<a ` 21, `</i ` 4, `<em ` 2, `<b ` 2, `<sub article>` 1, `<mi ` 1; projects.summary `<i ` 2, `</p ` 1). About 45 works titles and 3 summaries in total.

## Sample rows
5 works, 5 projects, 5 organisations printed (see the slurm log `tmp_session/hm_export_verify2-8981229.out`). Nothing broken: real arrays, correct types, no empty strings (empty -> NULL), typographic characters preserved (`’`, `İ`, `§`), no `&amp;`. Source quirks unchanged: NIH projects have empty `org_countries`, duplicated org names for two org ids, many `pending_org_` orgs have no geo/ror.

## Files
| file | size | rows |
|---|---|---|
| api/publishers.json | 0.19 MB | 3,000 |
| api/topics.json | 1.04 MB | 4,516 |
| export_manifest.json | 2.5 KB | |
| grants/grants.parquet | 0.23 MB | 6,119 |
| minorities/minorities.parquet | 0.10 MB | 278 |
| organisations/organisations.parquet | 61.64 MB | 494,099 |
| projects/projects.parquet | 753.70 MB | 3,893,065 |
| works/works_00.parquet | 755.57 MB | 5,000,377 |
| works/works_01.parquet | 755.71 MB | 4,998,827 |
| works/works_02.parquet | 755.24 MB | 4,998,770 |
| works/works_03.parquet | 754.84 MB | 4,997,399 |
| works/works_04.parquet | 755.33 MB | 4,999,353 |
| works/works_05.parquet | 755.32 MB | 5,000,209 |
| works/works_06.parquet | 756.26 MB | 5,003,975 |
| works/works_07.parquet | 755.35 MB | 4,998,584 |
| works/works_08.parquet | 755.36 MB | 4,999,115 |
| works/works_09.parquet | 756.00 MB | 5,003,391 |

## Warnings
- Not done: the optional `verify_clean.py` cross-check (`agent_job/CLEAN_VERIFY.md`); the verification above was run on the exported Parquet only.
- `data/serving_export_smoke/` is still on disk (small, git-ignored, overwritten with cleaned smoke data); it can be deleted.

## Doc counts to compare after loading
works 50,000,000 | projects 3,893,065 | organisations 494,099 | minorities 278 | grants **6,119**
