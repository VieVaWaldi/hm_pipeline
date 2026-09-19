# Core_v4 Strategy

! Not a target yet! WIP !

---

This document specifies core_v4 and records what phase 1 measured.
See the [ADR](...) for more.

Core_v4 builds on core_v3, because we probably made decisions there that were important for the project, but we didnt document them.
OpenAire is reinvestigated and the import is updated where the data says so (see the column changelog).
Cordis is merged deeper and enrichment expanded. OpenSearch is served from now instead of Postgres.

Numbers in this file come from `investigation/phase1_measurements.py` (JSON next to it: `investigation/phase1_measurements.json`),
run on prod on 2026-09-19 against `openaire_staging_2.duckdb` (core_v3's own staging, ids already hashed), `ror_raw.duckdb`
and `cordis_full_projects_no_pdfs_raw.duckdb`. Every database is attached READ_ONLY. Rerun: see the docstring of the script.

## 1. Sources (report paths, relative to the repo root)

| Source | Report |
|---|---|
| OpenAire raw | `reports/sources/dumps/openaire_dump_2026_06_05.md` |
| OpenAire staging (v3, read by core_v3) | `reports/sources/dumps/openaire_dump_staging_2_2026_06_05.md` |
| OpenAire staging v4 (adds `work.countries`) | `reports/sources/dumps/openaire_dump_staging_v4_2026_06_05.md` |
| ROR | `reports/sources/dumps/ror_dump_2026_08_03.md` |
| Cordis | `reports/sources/apis/cordis/full_projects_no_pdfs.md` |
| Minorities | `reports/sources/dumps/minorities.md` (a dump, not under `apis/cordis/`) |
| OATopics | `reports/sources/dumps/oa_topics.md` (a dump, not under `apis/cordis/`) |

`reports/` is gitignored: these files exist on the machine that ran the report rules.

Staging targets. `python -m sources.dumps.openaire.staging --target {v3,v4}`, default `v3`:

| target | writes | rule | used by |
|---|---|---|---|
| v3 (default) | `openaire_staging_2.duckdb` (deleted and rebuilt on every run) | `stage_openaire_dump` | core_v3 |
| v4 | `openaire_staging_v4.duckdb` | `stage_openaire_dump_v4` (not part of `all`, `ingest_dumps` or `core_v3`; run it by name) | core_v4 |

v4 differs from v3 only by the extra `work.countries` column.

## 2. Column changelog: OpenAire raw to staging (prod coverage)

Coverage is non-null rows. Raw numbers from the raw report, staging numbers from `staging_2`. `id` columns are `hash(id)` as
UBIGINT in staging, the original string is kept as `openaireId`.

### organization: 494,099 rows raw and staging

| raw | staging | non-null | note |
|---|---|---|---|
| id | id (hash), openaireId | 100% | |
| legalName | legalName | 100% | sanitize_name |
| legalShortName | legalShortName | 475,410 (96%) | sanitize_name |
| websiteUrl | websiteUrl | 188,616 (38%) | sanitize_url |
| alternativeNames | alternativeNames | 181,580 (37%) | sanitized, nulls removed |
| country.code | countryCode | 336,878 (68%) | `country.label` dropped |
| pids | pids | 204,609 (41%) | kept whole |
| pids[scheme=ROR] | rorId | 126,401 (26%) | new |
| pids[scheme=Wikidata] | wikiId | 55,825 (11%) | new |

### project: 3,909,902 rows raw, 3,893,065 staging

Row filter: title NULL (16,590 rows) or `'unidentified'` (247 rows) removed: 16,837 rows.

| raw | staging | non-null (raw to staging) | note |
|---|---|---|---|
| id | id (hash), openaireId | 100% | |
| code | grantId | 100% | join key to Cordis `project.id_original` |
| title | title | 3,893,312 to 3,893,065 (100%) | |
| acronym | acronym | 128,220 to 120,629 (3%) | |
| websiteUrl | websiteUrl | 86,933 (2%) | |
| startDate / endDate | startDate / endDate | 94% / 90% | years outside 1000-3000 set NULL |
| callIdentifier, keywords, subjects, summary | same | 3% / 13% / 8% / 14% | sanitized |
| openAccessMandateFor{Publications,Dataset}, granted | same | 100% | |
| fundings | fundings | 3,891,009 (100%) | |
| fundings[].fundingStream.id | frameworkProgrammes | 3,598,016 (92%) | new, derived |
| h2020Programmes | dropped | 32,249 (1%) | |
| indicators | dropped | 671,037 (17%) | citationImpact JSON and usageCounts |
| pids | dropped | 100,110 (3%) | project DOI |

### relation: 296,915,370 rows raw and staging

| raw | staging | non-null | note |
|---|---|---|---|
| source, target | source, target | 100% | `hash()` to UBIGINT, same hash as `id` |
| sourceType, targetType, relType, provenance, validated | same | 100% | |
| validationDate | dropped | 942,916 (0%) | |

Only three relation types are loaded (`RELATION_DIRS` in the loader), each stored once:

| sourceType | relType.name | targetType | rows |
|---|---|---|---|
| product | hasAuthorInstitution | organization | 282,790,150 |
| project | produces | product | 8,574,474 |
| project | hasParticipant | organization | 5,550,746 |

### work: 218,421,450 rows raw and staging

| raw | staging | non-null | note |
|---|---|---|---|
| id | id (hash), openaireId | 100% | |
| mainTitle | title | 216,958,318 (99%) | sanitize_title |
| publicationDate | publicationDate | 212,526,830 to 212,485,241 (97%) | 41,589 set NULL: year <1000 or >3000 |
| publisher | publisher | 184,650,335 (85%) | |
| openAccessColor | openAccessColor | 55,657,366 (25%) | |
| isGreen, isInDiamondJournal, publiclyFunded | same | 215,716,232 (99%) | |
| language | language | 100% | STRUCT(code, label) |
| bestAccessRight | bestAccessRight | 138,405,272 (63%) | |
| authors | authors | 182,013,144 (83%) | |
| subjects | subjects | 131,308,586 (60%) | |
| descriptions | descriptions | 123,346,737 to 123,344,446 (56%) | sanitize_content, nulls removed |
| pids | pids | 193,292,417 (88%) | |
| sources | sources | 168,711,105 (77%) | |
| formats | formats | 31,014,123 (14%) | |
| instances | instances | 218,421,450 (100%) | |
| indicators.citationImpact.citationCount / .influence | citationCount / influence | 100% | |
| indicators.usageCounts.views | views | 3,549,274 (2%) | |
| container | container | 147,556,829 (68%) | |
| **countries** | **countries (v4 only)** | raw 34,507,649 (16%) | `list_distinct(list_transform(countries, c -> c.code))`, `VARCHAR[]`. `label` and `provenance` dropped as redundant |
| contributors | dropped | 14,224,780 (7%) | |
| dateOfCollection | dropped | 100% | |
| embargoEndDate | dropped | 5,507,358 (3%) | |
| lastUpdateTimeStamp | dropped | 203,003,069 (93%) | |
| originalIds | dropped | 218,412,841 (100%) | |
| subTitle | dropped | 9,416,412 (4%) | |
| type | dropped | 100% | one value: the table only holds `publication/` |
| codeRepositoryUrl, contactGroups, contactPeople, coverages, documentationUrls, geoLocations, programmingLanguage, size, tools, version | dropped | 0 (0%) | empty JSON columns |

v4 `work.countries`: the column exists (`VARCHAR[]`, e.g. `['HR']`). The raw non-null share (34,507,649, 16%) is the upper bound; the exact
non-null count is in the v4 report once it has been generated (see open items). Row counts of the v4 build equal v3: organization 494,099,
project 3,893,065, work 218,421,450, relation 296,915,370; file size 356,442.5 MB.

## 3. Decisions based on Cordis (proposed, from the measurements below)

Cordis: 142,773 projects, 180,813 institutions, 761,774 project-institution rows. A *triplet* below is a (Cordis project, Cordis institution)
pair inside a project that matched OpenAire on `id_original = grantId`. There are 480,133 of them.

1. **Project match stays `cordis.project.id_original = openaire.project.grantId`.** 87,439 of 142,773 Cordis projects match (61.2%).
   9,250 of them match more than one OpenAire project (98,326 match pairs), so the org step must disambiguate, e.g. by requiring the
   `hasParticipant` relation between the matched OpenAire project and the org.
2. **Org match: PIC first, name + normalised country as the fallback, never name only.**

   | variant | triplets matched | % of 480,133 | pairs per triplet | triplets also in OpenAire `hasParticipant` | % |
   |---|---|---|---|---|---|
   | name only (core_v3 rule) | 293,479 | 61.1 | 2.50 | 195,607 | 40.7 |
   | name + country | 249,755 | 52.0 | 1.51 | 191,835 | 40.0 |
   | name + country, null allowed | 292,160 | 60.9 | 2.45 | 194,180 | 40.4 |
   | PIC | 430,591 | 89.7 | 1.28 | 393,063 | 81.9 |

   * PIC finds 191,980 triplets that name + country does not, and twice as many confirmed by an OpenAire relation.
   * Name only inflates every match 2.5x (duplicate names) and gains only 3,772 confirmed triplets over name + country.
   * PIC coverage: 465,970 of 761,774 `j_project_institution` rows carry an `organization_id` (61%). 67,922 of 81,136 distinct Cordis PICs
     (83.7%) exist on an OpenAire org (69,730 distinct OpenAire PICs).
3. **Country must be normalised before it is compared** (mapping in section 5): 21,506 Cordis institutions (11.9%) use the EU codes
   `UK` (16,050) and `EL` (5,456).
4. **Geolocation: ROR coordinates first, then Cordis, Mapbox only for the rest.** ROR gives coordinates to 126,400 orgs (25.6%).
   Cordis has real coordinates for 129,843 of 180,813 institutions (71.8%) and street + city for 165,708 (91.6%).
   Orgs matched to a Cordis institution that have no ROR coordinates but a Cordis address (the Mapbox candidates):

   | match | matched orgs | no ROR coords | **candidates** (with address) | of which Cordis already has coordinates | **need Mapbox** |
   |---|---|---|---|---|---|
   | name only | 81,848 | 74,924 | 73,022 | 53,616 | 19,406 |
   | name + country | 67,234 | 61,344 | 60,278 | 44,230 | 16,048 |
   | PIC | 68,125 | 60,364 | 59,644 | 43,557 | 16,087 |

   Using Cordis coordinates directly cuts Mapbox from ~60k to ~16k orgs. Matching is org level, not limited to matched projects.
5. **Duplicate legal names make a name match ambiguous.** Cordis: 7,528 lower-cased names sit on more than one institution
   (15,411 rows, 8.5%); 6,912 of them with more than one distinct (street, city, country), 3,610 with more than one (city, country).
   OpenAire: 60,019 names sit on more than one org (134,711 orgs), 1,285 of them with more than one `countryCode`.

## 4. Measurement results (prod, 2026-09-19)

### Works, for the 50M cap

Groups from `project_produces` (project link) and `product_hasAuthorInstitution` (org link). Total works: **218,421,450**.
Linked to a project (any): 5,001,873 (2.29%); linked to an org (any): 120,014,927 (54.95%).

| group | works | % of all | title | % | descriptions[1] | % | valid date | % |
|---|---|---|---|---|---|---|---|---|
| project + org | 4,558,270 | 2.09 | 4,557,962 | 99.99 | 4,271,199 | 93.70 | 4,531,598 | 99.41 |
| project only | 443,603 | 0.20 | 443,448 | 99.97 | 384,678 | 86.72 | 212,956 | 48.01 |
| org only | 115,456,657 | 52.86 | 115,421,607 | 99.97 | 84,870,744 | 73.51 | 114,925,597 | 99.54 |
| neither | 97,962,920 | 44.85 | 96,535,301 | 98.54 | 33,817,825 | 34.52 | 92,815,090 | 94.75 |

"Valid date" = `publicationDate` not NULL after staging (staging only removes years outside 1000-3000, see open items).

Publication year, 5-year buckets. Everything before 1990 and everything from 2030 on is collapsed here; the full histogram is in the JSON:

| bucket | project + org | project only | org only | neither |
|---|---|---|---|---|
| before 1990 | 8,804 | 1,647 | 10,168,412 | 22,187,610 |
| 1990-1994 | 8,968 | 446 | 4,154,661 | 3,320,858 |
| 1995-1999 | 43,577 | 1,760 | 5,428,218 | 4,249,774 |
| 2000-2004 | 88,741 | 2,735 | 7,689,903 | 5,789,555 |
| 2005-2009 | 198,661 | 6,483 | 11,477,663 | 7,289,457 |
| 2010-2014 | 629,567 | 24,988 | 16,504,689 | 11,091,112 |
| 2015-2019 | 1,275,681 | 80,350 | 21,885,637 | 14,501,929 |
| 2020-2024 | 1,959,432 | 78,898 | 29,722,193 | 18,521,196 |
| 2025-2029 | 318,146 | 15,641 | 7,893,290 | 5,850,614 |
| 2030 and later (invalid) | 21 | 8 | 931 | 12,985 |
| no date | 26,672 | 230,647 | 531,060 | 5,147,830 |

**Where the cap falls** (strict order: project link, then org link, then newest date; works without a date last):

| cap | tier holding the cutoff | cutoff date | works on that date | kept % title | % descriptions | % date |
|---|---|---|---|---|---|---|
| 25M | org only | 2023-01-01 | 1,464,323 | 99.99 | 71.82 | 98.97 |
| **50M** | org only | **2018-05-01** | 106,004 | 99.99 | 74.29 | 99.49 |
| 75M | org only | 2012-01-01 | 1,068,430 | 99.98 | 75.50 | 99.66 |
| 100M | org only | 2000-01-01 | 460,884 | 99.98 | 75.52 | 99.74 |

At 50M: all 4,558,270 + 443,603 project-linked works fit (5.0M), plus 44,998,127 of the 115,456,657 org-only works: everything dated
after 2018-05-01 (44,894,873) and 103,254 of the 106,004 works dated exactly 2018-05-01. No work from the `neither` group is kept.
The ties on the cutoff date need a tie-break rule (open items). At a cutoff date the title/description shares are split pro rata.

### Works, for NLLB

| language.code | works | % | | language.code | works | % |
|---|---|---|---|---|---|---|
| eng | 113,151,213 | 51.80 | | spa | 1,150,978 | 0.53 |
| und | 72,282,170 | 33.09 | | pol | 757,518 | 0.35 |
| deu/ger | 6,454,174 | 2.95 | | dut/nld | 702,909 | 0.32 |
| fra/fre | 4,497,994 | 2.06 | | hrv | 597,896 | 0.27 |
| rus | 3,613,354 | 1.65 | | ces/cze | 507,360 | 0.23 |
| jpn | 3,230,744 | 1.48 | | ukr | 457,047 | 0.21 |
| ita | 2,455,427 | 1.12 | | ara | 356,862 | 0.16 |
| esl/spa | 1,766,785 | 0.81 | | fin | 327,132 | 0.15 |
| tur | 1,537,325 | 0.70 | | per | 266,689 | 0.12 |
| por | 1,201,218 | 0.55 | | cat | 265,846 | 0.12 |

The language code is not normalised: `spa` and `esl/spa`, `deu/ger`, `fra/fre`, `dut/nld`, `ces/cze` are single languages under two spellings.

| text (characters) | non-null | avg | p95 | max |
|---|---|---|---|---|
| title | 216,958,318 | 79.56 | 157 | 119,750 |
| descriptions[1] | 123,344,446 | 1,152.90 | 2,441 | 5,179,731 |

### Organizations and geolocation

| metric | orgs | % of 494,099 |
|---|---|---|
| with rorId | 126,401 | 25.58 |
| rorId found in `ror_raw` | 126,400 | 25.58 |
| with ROR coordinates | 126,400 | 25.58 |
| without any coordinates (no ROR coords) | 367,699 | 74.42 |
| with a PIC | 69,883 | 14.14 |
| countryCode NULL | 157,221 | 31.82 |

Top 20 countryCode: NULL 157,221 (31.82%), US 61,119, FR 31,567, GB 21,730, DE 20,700, IT 15,531, ES 15,140, NL 8,622, JP 7,382, GR 7,263,
CH 7,141, CN 6,925, PL 6,701, BE 6,695, TR 6,656, PT 5,756, IN 5,754, CA 5,599, CZ 5,259, SE 5,197.

### Cordis

| metric | value | % |
|---|---|---|
| institutions with real coordinates (`geolocation::varchar <> 'null'`) | 129,843 | 71.81 of 180,813 |
| institutions with street and city | 165,708 | 91.65 |
| Cordis projects matched on grantId | 87,439 | 61.24 of 142,773 |
| OpenAire projects matched | 98,326 | 2.53 of 3,893,065 |
| Cordis projects matching more than one OpenAire project | 9,250 | |
| `j_project_institution` rows with PIC | 465,970 | 61.17 of 761,774 |
| distinct Cordis PICs / OpenAire PICs / in both | 81,136 / 69,730 / 67,922 | 83.71 of Cordis PICs |

Org match yield, Mapbox candidates and duplicate names: section 3.
PIC-matched pairs that name + country also finds (same OpenAire org): 276,587 of 553,115 (50.0%).

### Country codes

| source | distinct values | NULL rows | non-ISO distinct | non-ISO rows |
|---|---|---|---|---|
| OpenAire org.countryCode | 241 | 157,221 | 4 | 174 |
| Cordis institution.country | 218 | 186 | 9 | 21,646 |
| ROR location country_code | 233 | 0 | 0 | 0 |
| OpenAire work.countries (raw) | 115 | 0 | 8 | 737 |

253 distinct values across the four sources (ISO 3166-1 alpha-2 has 249). ROR is clean.

## 5. Country code mapping (proposal)

Implemented as `COUNTRY_MAPPING` in `phase1_measurements.py`; nothing in the pipeline uses it yet.

| value | where | rows | maps to |
|---|---|---|---|
| UK | Cordis | 16,050 | GB |
| EL | Cordis | 5,456 | GR |
| YU | Cordis 100, work 1, org 1 | 102 | RS (judgement call, split state) |
| CS | Cordis 3, work 1 | 4 | RS (judgement call, split state) |
| ZR | Cordis | 4 | CD |
| AN | Cordis 3, org 2 | 5 | CW (judgement call, split territory) |
| QAT | work.countries | 510 | QA (alpha-3) |
| LIE | work.countries | 31 | LI (alpha-3) |
| EU | org 169, work 149, Cordis 1 | 319 | NULL, not a country |
| EUROPE, WORLD | work.countries | 26, 18 | NULL |
| ZZ | Cordis | 28 | NULL |
| DC, DD | Cordis 1, work 1 | 2 | NULL |
| OC | org | 2 | NULL (Oceania) |
| XK | in use in the data | | kept: not ISO 3166-1, but user-assigned and used by the EU |

After this mapping nothing is left unmapped in any of the four sources.

## 6. Open items

* **v4 staging check.** `openaire_staging_v4.duckdb` was built on 2026-09-19 (SLURM 8980403, 31 min, exit 0). Its report
  `reports/sources/dumps/openaire_dump_staging_v4_2026_06_05.md` was still queued when this was written (SLURM 8980627, waiting for a free
  `fat` node): confirm it exists and that `work.countries` is populated in it. Report jobs write into the checkout the job was launched from.
* **Future-dated works.** 16,879 works are dated after 2026 (13,945 of them in 2030 or later, a cluster around 2550-2569, a tail up to 2999).
  Staging only removes years outside 1000-3000. Under "newest first" they would be kept before everything else. Decide a clamp
  (e.g. year > 2026 becomes NULL) before ordering. Effect on the 50M set is at most 16,879 works.
* **Tie-break at the cutoff.** 106,004 works are dated exactly 2018-05-01 and 2,750 of them are dropped. Many dates look like month or
  year placeholders (2023-01-01 alone has 1.46M works). Pick a secondary key (has description, citationCount, ...).
* **Undated works.** 5,936,209 works have no date (project-only group: 230,647 of 443,603). Under strict ordering this only matters
  for the `neither` group, which is not kept at 50M.
* **NLLB.** 72.3M works (33%) have language `und`; 113.2M are `eng`. NLLB needs the source language: detect it or skip `und`. Text
  length is very skewed: descriptions p95 = 2,441 chars, max 5.18M; titles max 119,750. Truncate or chunk. Language codes need
  normalising (see above).
* **Cordis project match rate.** 61.2% now; `core_v3/READ_TRANSFORMATION.md` records 96,056 of 139,925 (68.6%) on the earlier Cordis
  database. Not investigated why it fell (different Cordis extract?).
* **PIC vs name disagreement.** Only 50.0% of PIC-matched (project, institution, org) pairs are also found by name + country. Not
  inspected: it mixes duplicate OpenAire orgs sharing a PIC (1.28 pairs per PIC triplet) and differently spelled names. Sample before
  trusting PIC for the org identity.
* **Residual geolocation.** After ROR (126,400 orgs) and Cordis coordinates via name + country (44,932 more orgs without ROR coordinates),
  roughly 322.8k orgs (65%) still have no coordinates, most without any Cordis link. Only `countryCode` (68% present) exists for
  them: country centroid fallback or leave empty.
* **Mapbox scope.** 16,048 orgs (name + country) or 16,087 (PIC), not the 60k candidates. Decide which match defines the set.
* **Country mapping.** Confirm the YU, CS, AN judgement calls (about 110 rows) and the NULL drops.
* **Not measured in phase 1.** The "keep all works connected to minorities" rule (needs the minorities dump joined to works).
  One `rorId` is missing from `ror_raw`.
* **Dropped raw columns** (section 2) were dropped by core_v3 staging and are unchanged in v4. Candidates to revisit: project `pids`
  (DOI), `h2020Programmes`, work `subTitle`.
* **Files not yet in the prod checkout.** Phase 1 was developed in a separate checkout because core_v3 jobs were starting up.

---

## Strategy notes (previous README, unchanged)

**Idempotence**:
* ... how?
* Just make a new duckdb for each big step, that u always tear down and rebuild from scratch? @Claude if you know better tell me lol

**Reports**:
* For each duckdb created make a report

**Limit runs**:
* parameter --limit -n default 1000 per entity (if thats not snakemake namespace) to make one real but fast test run

Open:
* Can we measure/ note (here in this readme or where? maybe we can add that to reports but reports is static rn/ depends on no state, which is great, not sure how we d add process run time to that)

### ... core_v4 process? ...

* we may just copy the high level base process from core_v3 verbatim
* target 1 single duckdb file for core_v4? how big can duckdb files get? 

@Claude argue with me here:
1. Add ROR to OpenAIRE -> core_v4_staging.duckdb
2. Merge Cordis
- Needs to be analysed in core_v3 first to see what we already have
- I feel like we only found matching project -> relation -> organization triplets and added 

* Merging Cordis 

    * From Cordis only institution level funding information for projects is needed from `j_project_institution`
    * From ROR organization information like geolocation and others are needed.
2. Enrichment
    * TFIdf Topic Classification using `../../enrichment/topic_modelling` -> I did this before step 5 so the intermediate core file is called path_duck_staging_2 in the config.
    * CH Classification
3. And finally a deployment from duckdb to postgresdb

--- This is new and not done ---

...

### Drop Works

-> Because our VM sucks and cant handle 200M

* Create a new core_v3 duckdb that removes works and respective relations
* Hard Limit by 50 Million works
* Keep all works connected to minorities
* Keep as many works connected to projects, works as possible
* Drop works with many nulls or that are older

### Enrichment - WIP

- Parallelize what can be safely and easily parallelized!
- Target projects for all enrichments before doing works, as works takes very long and i present this in 3 days

1. Geolocations 
-> for organisations

2. NLLB
-> for projects and works
-> Must happen before all text based enrichment, so we got english for all texts.

3. Topics TF-IDF
-> for projects and works

4. Keyword Matching
-> for projects and works
* Theme & Pillars (Needs Topics)
* Regions Mapping
* Minorities? (thats a merge later with projects right?)

5. DCH Classification (broke last time after 2 mil projects)
-> for projects and works

### Serving - WIP

* Each index served gets a version number prefix: /corev3/{version}/indexreports:
