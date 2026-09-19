# core_v4 phase 1: investigation log

Written by Claude on 2026-09-19. Everything below is what was done, what was found, and what is still open.

## Status against the exit criteria

| criterion | state |
|---|---|
| `openaire_staging_v4.duckdb` exists | **done.** `/work/lu72hip/data/duckdb/sources/openaire_staging_v4.duckdb`, 356,442.5 MB, built by SLURM job 8980403 (31 min, exit 0). Row counts equal v3: organization 494,099, project 3,893,065, work 218,421,450, relation 296,915,370 |
| `work.countries` populated | **verified on samples only.** Column is `VARCHAR[]`; rows read back as e.g. `['HR']`. The exact non-null count needs the report |
| v4 report exists | **not yet.** SLURM job 8980627 is PENDING (reason: Priority; the `fat` partition is full, scheduler estimate 2026-09-22). Snakemake started from `/vast/lu72hip/hm_pipeline_v4` submitted it, so the file will appear in `/vast/lu72hip/hm_pipeline_v4/reports/sources/dumps/openaire_dump_staging_v4_2026_06_05.md`. I did not run the 128 GB report myself inside your interactive allocation (job 8979757) |
| README complete and consistent | **done** apart from the exact `work.countries` count, which points at the pending report. All numbers are copied from the run below |
| no change to any core_v3 file or output | **holds.** `openaire_staging_2.duckdb` mtime is still 2026-09-19 12:35; the main checkout has no edits from me (see "Where the changes live") |

## Where the changes live: a separate checkout, not the main one

The core_v3 Snakemake run was still submitting jobs from `/vast/lu72hip/hm_pipeline` (its jobs' WorkDir; new ones were still appearing as I
worked), so I followed the brief's "work in a separate checkout" option: `git clone /vast/lu72hip/hm_pipeline /vast/lu72hip/hm_pipeline_v4`
(plus a copy of `.env`; the project venv is shared, with `PYTHONPATH=<clone>/src` so the clone's code is what runs). Nothing was committed
or pushed there or here. A separate `.snakemake` state also avoids fighting the running workflow's lock.

Changes in `/vast/lu72hip/hm_pipeline_v4` (`git status`):

* `config/dumps.yaml`: `path_duck_staging_v4` under `openaire_dump`.
* `src/sources/dumps/openaire/staging.py`: `--target {v3,v4}`, default `v3`; v4 writes the new path and adds
  `list_distinct(list_transform(countries, c -> c.code)) AS countries` to the work SELECT. The v3 path still uses `path_duck_staging_2`
  with the "pids temp" comment kept. In v3 mode the added SQL fragment is an empty string, so the statement is unchanged.
* `orchestration/rules/dumps.smk`: rule `stage_openaire_dump_v4`, `REPORT_NAME_OPENAIRE_DUMP_STAGING_V4`, its `DUMP_REPORT_INPUTS` entry.
  I also added it to `LARGE_DUMP_REPORTS` (same set, one more member) so its report gets the 128 GB allocation instead of 16 GB. It is in no
  existing target (`all`, `ingest_dumps`, `core_v3`).
* `src/pipelines/core_v4/investigation/phase1_measurements.py` (+ `.json` output): new, read-only.
* `src/pipelines/core_v4/README.md`: rewritten per section C; the old strategy notes are kept verbatim at the bottom.
* `core_v4_phase1.patch` in that directory: the complete diff, for applying to the main checkout.

**To bring it into the main checkout** (not done, because core_v3 was still active there): once core_v3 is idle or you are fine with additive
edits, `cd /vast/lu72hip/hm_pipeline && git apply /vast/lu72hip/hm_pipeline_v4/core_v4_phase1.patch`, then copy the v4 report over when it
exists. All edits are additive, but one caution: a `generate_reports` run without `--only` will now also try the v4 file; it skips DBs that
do not exist, and every core_v3 report job uses `--only`.

Unrelated, noticed in the main checkout: `git status` shows `tmp_reports_core_v3/{main,staging,staging_2}.md` as deleted. I did not touch
them; it looks like something in core_v3's own run.

## What was checked

* `staging.py --target v4` on a local sample (2,000 orgs, 2,000 projects, 4,000 works, 5,000 relations, LIMIT-sampled READ_ONLY from prod raw
  into the clone's dev `data/`): `work.countries` is `VARCHAR[]`, 3,000 of 4,000 rows non-empty (3,000 sampled to have countries).
  v3 mode on the same sample: organization, project, relation schemas identical to v4; work differs only by `countries`.
* `_duck_label("path_duck_staging_v4")` gives `staging_v4`, so the report name is `openaire_dump_staging_v4_2026_06_05.md` (checked with
  `_iter_dumps()` in prod env). It cannot collide with `--only ..._2026_06_05.md` of the raw or staging_2 reports.
* Snakemake dry run of the v4 report target scheduled exactly two jobs (`stage_openaire_dump_v4`, `report_dump`); `load_openaire_dump` and
  `download_openaire_dump` were not touched. The stage rule's shell line is `... staging --target v4 ...`.
* The measurement script's cap logic was checked against a hand-written SQL ground truth on the sample (cutoff date and rows newer than it match).

## Things I did that the brief did not spell out

* Three measurement runs, all on SLURM, all READ_ONLY (jobs 8980402, 8980615, 8980617; ~4-7 min each). Run 2 extended the country mapping
  (alpha-3 codes and non-country values that only showed up in `work.countries`); run 3 fixed a determinism bug (how the cutoff date's ties were
  filled depended on DuckDB's row order, which moved the "% descriptions" column by up to 1.9 points). The output below is run 3.
* Extra variants beyond the brief: "name + country, null allowed"; the PIC join as a third org match variant, plus the "confirmed by OpenAire
  `hasParticipant`" column, which is what separates the variants; caps of 25M, 75M, 100M next to 50M; the Mapbox split into "Cordis already
  has coordinates" vs "needs Mapbox".
* The measurement script creates `/work/lu72hip/duckdb_tmp/phase1` as DuckDB spill space (it did not need it).
* Country mapping in `phase1_measurements.py` is a proposal (`COUNTRY_MAPPING`); nothing in the pipeline uses it.
* Report of the raw dump (step C1): read as far as it was available. It did not exist when I started; core_v3's job produced it at 17:01.

## Findings that change plans

1. **Cap at 50M.** All 5.0M project-linked works fit; the cutoff falls inside the org-only group at 2018-05-01 (106,004 works share that date,
   2,750 dropped). No work without an org or project link survives. Kept set: 99.99% title, 74.3% description, 99.5% valid date.
2. **Bad dates.** 16,879 works are dated after 2026, up to year 2999 (staging only nulls <1000 and >3000). "Newest first" would rank them first.
3. **PIC beats name for the Cordis org merge**: 89.7% of triplets vs 52.0% (name + country), 81.9% vs 40.0% confirmed by an OpenAire relation.
   Only half of PIC pairs coincide with name + country pairs, and I did not look into why (open item in the README).
4. **Mapbox is ~16k orgs, not ~60k**, if Cordis coordinates are used first.
5. **Cordis project match fell to 61.2%** from the 68.6% recorded in `core_v3/READ_TRANSFORMATION.md`; not investigated.
6. **NLLB:** 33% of works have language `und`; descriptions have p95 2,441 chars but max 5.18M.

## Not done / open

* v4 report (queued) and the exact `work.countries` coverage from it.
* Applying the patch to the main checkout.
* Section B item "Cordis: org match yield ... Cordis address but no ROR coordinates": done at org level (not restricted to matched projects), stated in the tables.
* Not measured: "keep all works connected to minorities" (phase 2 material).

---

# Raw output of `phase1_measurements.py` (run 3, SLURM 8980617)

Copied verbatim from `data/logs/phase1_measurements.log` of the clone. The JSON with the same numbers is
`src/pipelines/core_v4/investigation/phase1_measurements.json` there.

# core_v4 phase 1 measurements

| source | path |
|---|---|
| staging_db | /work/lu72hip/data/duckdb/sources/openaire_staging_2.duckdb |
| raw_db | /work/lu72hip/data/duckdb/sources/openaire_raw.duckdb |
| ror_db | /work/lu72hip/data/duckdb/sources/ror_raw.duckdb |
| cordis_db | /work/lu72hip/data/duckdb/sources/cordis_full_projects_no_pdfs_raw.duckdb |

## Relation types in openaire_staging_2


_(relation overview: 1s)_
| sourceType | relType.name | targetType | rows |
|---|---|---|---|
| product | hasAuthorInstitution | organization | 282,790,150 |
| project | produces | product | 8,574,474 |
| project | hasParticipant | organization | 5,550,746 |

## Works: link groups


_(build wk: 98s)_
Total works: **218,421,450**  
Distinct works referenced by a project_produces relation: 5,001,873; by a product_hasAuthorInstitution relation: 120,014,927 (the difference to the counts below is relation targets that are not in `work`).

| group | works | % of all | title | % | descriptions[1] | % | valid date | % |
|---|---|---|---|---|---|---|---|---|
| both | 4,558,270 | 2.09 | 4,557,962 | 99.99 | 4,271,199 | 93.70 | 4,531,598 | 99.41 |
| project_only | 443,603 | 0.20 | 443,448 | 99.97 | 384,678 | 86.72 | 212,956 | 48.01 |
| org_only | 115,456,657 | 52.86 | 115,421,607 | 99.97 | 84,870,744 | 73.51 | 114,925,597 | 99.54 |
| neither | 97,962,920 | 44.85 | 96,535,301 | 98.54 | 33,817,825 | 34.52 | 92,815,090 | 94.75 |

Linked to a project (any): **5,001,873** (2.29%); linked to an org (any): **120,014,927** (54.95%).

## Works: publication year, 5-year buckets (rows = bucket, cells = works)

| bucket | both | project_only | org_only | neither | all |
|---|---|---|---|---|---|
| <1900 | 47 | 151 | 292,692 | 1,873,638 | 2,166,528 |
| 1900-1904 | 9 | 20 | 26,885 | 316,423 | 343,337 |
| 1905-1909 | 3 | 27 | 37,043 | 351,823 | 388,896 |
| 1910-1914 | 2 | 31 | 42,227 | 393,927 | 436,187 |
| 1915-1919 | 6 | 16 | 36,490 | 332,155 | 368,667 |
| 1920-1924 | 9 | 22 | 49,647 | 414,305 | 463,983 |
| 1925-1929 | 13 | 22 | 69,385 | 519,937 | 589,357 |
| 1930-1934 | 4 | 25 | 85,769 | 567,446 | 653,244 |
| 1935-1939 | 11 | 29 | 101,585 | 583,671 | 685,296 |
| 1940-1944 | 7 | 38 | 93,188 | 486,321 | 579,554 |
| 1945-1949 | 14 | 29 | 115,412 | 511,930 | 627,385 |
| 1950-1954 | 15 | 36 | 216,216 | 739,409 | 955,676 |
| 1955-1959 | 30 | 48 | 309,681 | 903,709 | 1,213,468 |
| 1960-1964 | 61 | 68 | 472,273 | 1,146,352 | 1,618,754 |
| 1965-1969 | 165 | 62 | 765,495 | 1,898,134 | 2,663,856 |
| 1970-1974 | 418 | 108 | 1,145,336 | 2,428,101 | 3,573,963 |
| 1975-1979 | 781 | 170 | 1,476,131 | 2,631,576 | 4,108,658 |
| 1980-1984 | 2,345 | 364 | 1,988,042 | 2,916,248 | 4,906,999 |
| 1985-1989 | 4,864 | 381 | 2,844,915 | 3,172,505 | 6,022,665 |
| 1990-1994 | 8,968 | 446 | 4,154,661 | 3,320,858 | 7,484,933 |
| 1995-1999 | 43,577 | 1,760 | 5,428,218 | 4,249,774 | 9,723,329 |
| 2000-2004 | 88,741 | 2,735 | 7,689,903 | 5,789,555 | 13,570,934 |
| 2005-2009 | 198,661 | 6,483 | 11,477,663 | 7,289,457 | 18,972,264 |
| 2010-2014 | 629,567 | 24,988 | 16,504,689 | 11,091,112 | 28,250,356 |
| 2015-2019 | 1,275,681 | 80,350 | 21,885,637 | 14,501,929 | 37,743,597 |
| 2020-2024 | 1,959,432 | 78,898 | 29,722,193 | 18,521,196 | 50,281,719 |
| 2025-2029 | 318,146 | 15,641 | 7,893,290 | 5,850,614 | 14,077,691 |
| 2030-2034 | 10 | 0 | 191 | 338 | 539 |
| 2035-2039 | 5 | 2 | 227 | 196 | 430 |
| 2040-2044 | 0 | 0 | 6 | 95 | 101 |
| 2045-2049 | 1 | 0 | 2 | 23 | 26 |
| 2050-2054 | 0 | 0 | 4 | 19 | 23 |
| 2055-2059 | 0 | 0 | 1 | 22 | 23 |
| 2060-2064 | 0 | 0 | 1 | 5 | 6 |
| 2065-2069 | 0 | 0 | 2 | 2 | 4 |
| 2070-2074 | 0 | 0 | 2 | 6 | 8 |
| 2075-2079 | 0 | 0 | 4 | 8 | 12 |
| 2080-2084 | 0 | 0 | 2 | 30 | 32 |
| 2085-2089 | 0 | 0 | 11 | 17 | 28 |
| 2090-2094 | 0 | 1 | 3 | 3 | 7 |
| 2095-2099 | 0 | 0 | 19 | 20 | 39 |
| 2100-2104 | 4 | 0 | 40 | 60 | 104 |
| 2105-2109 | 1 | 0 | 76 | 59 | 136 |
| 2110-2114 | 0 | 1 | 1 | 39 | 41 |
| 2115-2119 | 0 | 0 | 3 | 40 | 43 |
| 2120-2124 | 0 | 0 | 30 | 9 | 39 |
| 2150-2154 | 0 | 0 | 0 | 1 | 1 |
| 2200-2204 | 0 | 0 | 2 | 9 | 11 |
| 2205-2209 | 0 | 0 | 2 | 0 | 2 |
| 2220-2224 | 0 | 0 | 0 | 1 | 1 |
| 2300-2304 | 0 | 0 | 0 | 1 | 1 |
| 2320-2324 | 0 | 0 | 0 | 2 | 2 |
| 2515-2519 | 0 | 0 | 0 | 1 | 1 |
| 2525-2529 | 0 | 0 | 5 | 166 | 171 |
| 2545-2549 | 0 | 0 | 0 | 115 | 115 |
| 2550-2554 | 0 | 1 | 14 | 754 | 769 |
| 2555-2559 | 0 | 2 | 148 | 4,073 | 4,223 |
| 2560-2564 | 0 | 0 | 103 | 4,028 | 4,131 |
| 2565-2569 | 0 | 0 | 7 | 2,626 | 2,633 |
| 2570-2574 | 0 | 0 | 0 | 14 | 14 |
| 2575-2579 | 0 | 0 | 8 | 140 | 148 |
| 2580-2584 | 0 | 0 | 2 | 33 | 35 |
| 2600-2604 | 0 | 1 | 0 | 0 | 1 |
| 2625-2629 | 0 | 0 | 0 | 1 | 1 |
| 2630-2634 | 0 | 0 | 0 | 1 | 1 |
| 2635-2639 | 0 | 0 | 0 | 1 | 1 |
| 2640-2644 | 0 | 0 | 0 | 1 | 1 |
| 2645-2649 | 0 | 0 | 0 | 2 | 2 |
| 2700-2704 | 0 | 0 | 0 | 1 | 1 |
| 2800-2804 | 0 | 0 | 0 | 1 | 1 |
| 2805-2809 | 0 | 0 | 1 | 9 | 10 |
| 2895-2899 | 0 | 0 | 0 | 1 | 1 |
| 2900-2904 | 0 | 0 | 3 | 1 | 4 |
| 2910-2914 | 0 | 0 | 1 | 2 | 3 |
| 2915-2919 | 0 | 0 | 8 | 2 | 10 |
| 2920-2924 | 0 | 0 | 2 | 2 | 4 |
| 2925-2929 | 0 | 0 | 0 | 3 | 3 |
| 2995-2999 | 0 | 0 | 0 | 2 | 2 |
| no date | 26,672 | 230,647 | 531,060 | 5,147,830 | 5,936,209 |

Works dated after 2026: 16,879

## Works: where the cap falls (strict order: project link, then org link, then newest date)

Order: `both` > `project_only` > `org_only` > `neither`; inside the tier that straddles the cap, newest `publicationDate` first, works without a date last. Ties on the cutoff date are split arbitrarily.


_(cap histogram: 1s)_
| cap | cutoff falls in tier | cutoff date | rows on that date | kept | % title | % descr | % date |
|---|---|---|---|---|---|---|---|
| 25M | org_only | 2023-01-01 | 1,464,323 | 25,000,000 | 99.99 | 71.82 | 98.97 |
| 50M | org_only | 2018-05-01 | 106,004 | 50,000,000 | 99.99 | 74.29 | 99.49 |
| 75M | org_only | 2012-01-01 | 1,068,430 | 75,000,000 | 99.98 | 75.50 | 99.66 |
| 100M | org_only | 2000-01-01 | 460,884 | 100,000,000 | 99.98 | 75.52 | 99.74 |

Kept per tier at cap 50,000,000:

| tier | kept | of |
|---|---|---|
| both | 4,558,270 | 4,558,270 |
| project_only | 443,603 | 443,603 |
| org_only | 44,998,127 | 115,456,657 |
| neither | 0 | 97,962,920 |

## Works for NLLB: top 20 language.code

| language.code | works | % |
|---|---|---|
| eng | 113,151,213 | 51.80 |
| und | 72,282,170 | 33.09 |
| deu/ger | 6,454,174 | 2.95 |
| fra/fre | 4,497,994 | 2.06 |
| rus | 3,613,354 | 1.65 |
| jpn | 3,230,744 | 1.48 |
| ita | 2,455,427 | 1.12 |
| esl/spa | 1,766,785 | 0.81 |
| tur | 1,537,325 | 0.70 |
| por | 1,201,218 | 0.55 |
| spa | 1,150,978 | 0.53 |
| pol | 757,518 | 0.35 |
| dut/nld | 702,909 | 0.32 |
| hrv | 597,896 | 0.27 |
| ces/cze | 507,360 | 0.23 |
| ukr | 457,047 | 0.21 |
| ara | 356,862 | 0.16 |
| fin | 327,132 | 0.15 |
| per | 266,689 | 0.12 |
| cat | 265,846 | 0.12 |

## Works for NLLB: text length in characters

| text | non-null | avg | p95 | max |
|---|---|---|---|---|
| title | 216,958,318 | 79.56 | 157.00 | 119,750 |
| descriptions[1] | 123,344,446 | 1,152.90 | 2,441.00 | 5,179,731 |

## Organizations and geolocation

| metric | orgs | % of orgs |
|---|---|---|
| organizations | 494,099 | 100.00 |
| with rorId | 126,401 | 25.58 |
| rorId found in ror_raw | 126,400 | 25.58 |
| with ROR coordinates | 126,400 | 25.58 |
| without any coordinates (no ROR coords) | 367,699 | 74.42 |
| with a PIC | 69,883 | 14.14 |
| countryCode null | 157,221 | 31.82 |

Top 20 countryCode:

| countryCode | orgs | % |
|---|---|---|
| (null) | 157,221 | 31.82 |
| US | 61,119 | 12.37 |
| FR | 31,567 | 6.39 |
| GB | 21,730 | 4.40 |
| DE | 20,700 | 4.19 |
| IT | 15,531 | 3.14 |
| ES | 15,140 | 3.06 |
| NL | 8,622 | 1.74 |
| JP | 7,382 | 1.49 |
| GR | 7,263 | 1.47 |
| CH | 7,141 | 1.45 |
| CN | 6,925 | 1.40 |
| PL | 6,701 | 1.36 |
| BE | 6,695 | 1.35 |
| TR | 6,656 | 1.35 |
| PT | 5,756 | 1.16 |
| IN | 5,754 | 1.16 |
| CA | 5,599 | 1.13 |
| CZ | 5,259 | 1.06 |
| SE | 5,197 | 1.05 |

## Cordis institutions

| metric | institutions | % |
|---|---|---|
| institutions | 180,813 | 100.00 |
| real coordinates (geolocation::varchar <> 'null') | 129,843 | 71.81 |
| street and city | 165,708 | 91.65 |

## Cordis project match on grantId

| metric | value | % |
|---|---|---|
| Cordis projects | 142,773 | 100.00 |
| Cordis projects matched (id_original = grantId) | 87,439 | 61.24 |
| OpenAire projects matched | 98,326 | 2.53 |
| match pairs | 98,326 |  |
| Cordis projects matching >1 OpenAire project | 9,250 |  |

## Cordis org match yield (triplets = Cordis project x institution inside matched projects)

Triplets in matched projects: **480,133**

| variant | triplets matched | % of triplets | pairs (fan-out) | pairs / triplet | Cordis institutions | OpenAire orgs | triplets also in OA hasParticipant | % of triplets |
|---|---|---|---|---|---|---|---|---|
| name only | 293,479 | 61.12 | 735,075 | 2.50 | 63,703 | 75,113 | 195,607 | 40.74 |
| name + country | 249,755 | 52.02 | 376,227 | 1.51 | 61,716 | 63,576 | 191,835 | 39.95 |
| name + country (null on either side allowed) | 292,160 | 60.85 | 715,086 | 2.45 | 63,520 | 74,122 | 194,180 | 40.44 |
| PIC | 430,591 | 89.68 | 553,115 | 1.28 | 68,515 | 68,045 | 393,063 | 81.87 |

PIC-matched pairs that name+country also finds (same OpenAire org): 276,587 / 553,115 (50.01%). Triplets found by PIC but not by name+country: 191,980; not by name only: 151,893.

## Cordis PIC overlap

| metric | value | % |
|---|---|---|
| j_project_institution rows | 761,774 | 100.00 |
| rows with organization_id (PIC) | 465,970 | 61.17 |
| distinct Cordis PICs | 81,136 |  |
| distinct OpenAire org PICs | 69,730 |  |
| PICs in both | 67,922 | 83.71 |

## Mapbox candidates: matched orgs with a Cordis address but no ROR coordinates

Matching is org level (not restricted to matched projects). `Cordis coords` = at least one matched institution already has real coordinates, so Mapbox is not needed for it.

| match | matched orgs | no ROR coords | **Mapbox candidates** (+ address) | of which Cordis coords | of which need Mapbox | no ROR coords, any Cordis coords |
|---|---|---|---|---|---|---|
| name only | 81,848 | 74,924 | 73,022 | 53,616 | 19,406 | 54,867 |
| name + country | 67,234 | 61,344 | 60,278 | 44,230 | 16,048 | 44,932 |
| PIC | 68,125 | 60,364 | 59,644 | 43,557 | 16,087 | 44,088 |

## Duplicate legal names with different addresses

| metric | names | institution rows |
|---|---|---|
| distinct lower(trim(legal_name)) | 172,930 | 180,813 |
| names on >1 institution | 7,528 | 15,411 |
| ... with >1 distinct (street, city, country) | 6,912 | 14,172 |
| ... with >1 distinct (city, country) | 3,610 | 7,467 |

OpenAire orgs, same lower(trim(legalName)): 60,019 names on >1 org (134,711 orgs), 1,285 of them with >1 distinct countryCode.

## Country codes


_(country codes: 2s)_
| source | distinct values | null rows | non-ISO distinct | non-ISO rows | unmapped after mapping |
|---|---|---|---|---|---|
| openaire org.countryCode | 241 | 157,221 | 4 | 174 | 0 |
| cordis institution.country | 218 | 186 | 9 | 21,646 | 0 |
| ror location country_code | 233 | 0 | 0 | 0 | 0 |
| openaire work.countries | 115 | 0 | 8 | 737 | 0 |

Distinct values across all four sources: **253** (ISO alpha-2 has 249).

Non-ISO in **openaire org.countryCode**:

| value | rows | proposed |
|---|---|---|
| EU | 169 | NULL (drop) |
| OC | 2 | NULL (drop) |
| AN | 2 | CW |
| YU | 1 | RS |

Non-ISO in **cordis institution.country**:

| value | rows | proposed |
|---|---|---|
| UK | 16,050 | GB |
| EL | 5,456 | GR |
| YU | 100 | RS |
| ZZ | 28 | NULL (drop) |
| ZR | 4 | CD |
| CS | 3 | RS |
| AN | 3 | CW |
| DC | 1 | NULL (drop) |
| EU | 1 | NULL (drop) |

Non-ISO in **openaire work.countries**:

| value | rows | proposed |
|---|---|---|
| QAT | 510 | QA |
| EU | 149 | NULL (drop) |
| LIE | 31 | LI |
| EUROPE | 26 | NULL (drop) |
| WORLD | 18 | NULL (drop) |
| DD | 1 | NULL (drop) |
| CS | 1 | RS |
| YU | 1 | RS |

Proposed mapping (applied by `norm_cc` above): `{'EL': 'GR', 'UK': 'GB', 'ZR': 'CD', 'YU': 'RS', 'CS': 'RS', 'AN': 'CW', 'QAT': 'QA', 'LIE': 'LI', 'ZZ': None, 'EU': None, 'DC': None, 'DD': None, 'OC': None, 'EUROPE': None, 'WORLD': None}`; kept as-is: `['XK']`.

Wrote /vast/lu72hip/hm_pipeline_v4/src/pipelines/core_v4/investigation/phase1_measurements.json
