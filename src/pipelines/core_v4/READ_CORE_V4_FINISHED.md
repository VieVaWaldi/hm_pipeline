# core_v4_noworkenrichment.duckdb: the finished core_v4 database (report of 2026-09-20)

**This is the file we use for serving (OpenSearch).** 

One DuckDB file with every entity and every relation:
50M works, 3.89M projects, 494k organizations, topics, the minority table. 

**Projects and organizations are fully enriched. Works are deliberately NOT enriched** 
(decision of 2026-09-20: the deadline is the OpenSearch serve; work enrichment is expensive and not needed for it).

```
/work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb      132.6 GB, written 2026-09-20 16:46
```

Built by `src/pipelines/core_v4/merge_single.py` (302 s, sbatch job 8981036) from three finished files, which are
untouched and still exist:

| Source file | Contribution |
|---|---|
| `core_v4_works_base.duckdb` (130.8 GB, copied as a file) | `work`, works' `relation` rows (project->work, work->organization), empty `relation_topic` |
| `core_v4_projects.duckdb` (1.8 GB) | `project`, `organization`, `topic`, project->organization `relation` rows, project `relation_topic` |
| `data/duckdb/sources/minorities_raw.duckdb` | table `minority` (278 groups, all columns of `minorities_raw`) |

Merge checks (all zero): work rows with NULL `link_tier`, `relation_topic` rows that are not projects, relations pointing at a
missing project/organization/work, `relation_topic` pointing at a missing project/topic, project `minority_qid` values
missing from `minority`. Open it **read-only** (`duckdb.connect(path, read_only=True)`); one writer at a time otherwise.

## Tables and columns

| Table | Rows | Notes |
|---|---|---|
| `work` | 50,000,000 | 5,001,873 project-linked (`link_tier` 0) + 44,998,127 org-only (`link_tier` 1). No enrichment (see below). |
| `project` | 3,893,065 | fully enriched |
| `organization` | 494,099 | enriched (regions, coordinates from ROR / Cordis / core_v2, Cordis addresses) |
| `topic` | 4,516 | OpenAlex topic taxonomy, `id` is the primary key |
| `minority` | 278 | the minority groups (Wikidata QIDs) |
| `relation` | 153,750,065 | 140,970,846 product(work)->organization, 7,278,890 project->product(work), 5,500,329 project->organization |
| `relation_topic` | 3,884,270 | project->topic only (`type = 'project'`), no work rows |

Type names in `relation`: a work is called `product` in `sourceType` / `targetType`, ids are `UBIGINT`.

**project**: `id UBIGINT, openaireId, grantId, title, acronym, websiteUrl, startDate DATE, endDate DATE, callIdentifier, keywords,
openAccessMandateForPublications BOOL, openAccessMandateForDataset BOOL, subjects VARCHAR[], fundings STRUCT[], frameworkProgrammes
VARCHAR[], summary, doi, granted STRUCT(currency, fundedAmount, totalCost)` and the enrichment columns
`is_translated BOOL, is_ch BOOL, pred FLOAT, minority_qid VARCHAR[], pillars UTINYINT, theme VARCHAR`.

**organization**: `id UBIGINT, openaireId, legalName, legalShortName, websiteUrl, alternativeNames VARCHAR[], countryCode, rorId, wikiId,
pids STRUCT[], rorStatus, rorEstablished INT, rorTypes VARCHAR[], rorLocations JSON, geolocation DOUBLE[] ([lat, lng]),
geolocation_source, rorRelationships JSON, address_street, address_postalcode, address_city, address_country, nuts3, region`.

**topic**: `id INT, subfield_id, field_id, domain_id, topic_name, subfield_name, field_name, domain_name, keywords, summary,
wikipedia_url, created_at, updated_at`. 

**relation_topic**: `type, source_id UBIGINT, topic_id INT, score FLOAT, created_at`.

**minority**: `qid, merged_qids VARCHAR[], group_name_en, countries VARCHAR[], source_class VARCHAR[], population DOUBLE, religions,
native_languages, part_of, subclass_of, diaspora, ancestral_home, admin_territory, has_parts (all VARCHAR[]), known_subgroups
STRUCT(name, qid)[], search_keywords VARCHAR[]`. A project's `minority_qid` list holds `minority.qid` values (also see `merged_qids`).

**relation**: `source UBIGINT, sourceType, target UBIGINT, targetType, relType STRUCT(name, type), provenance STRUCT(provenance, trust),
validated BOOL, cordis_ec_contribution DOUBLE, cordis_type` (the two Cordis columns are filled on project->organization rows only).

**work**: `id, openaireId, title, publicationDate, publisher, openAccessColor, isGreen, isInDiamondJournal, publiclyFunded, language,
bestAccessRight, authors, subjects, descriptions VARCHAR[], pids, sources, formats, instances, citationCount, influence, views, countries,
container, link_tier SMALLINT` plus the enrichment columns (below, all at defaults).

## Enrichment numbers

### Projects (3,893,065, all enriched)
| Enrichment | Column | Result |
|---|---|---|
| NLLB translation (title + summary to English; a language detector decides, ~3.6% of fields) | `is_translated`, `title`/`summary` overwritten | 136,473 projects translated (3.5%) |
| DCH classifier (BERT, English text) | `is_ch`, `pred` | probability for all 3,893,065; 21,280 flagged cultural heritage (0.55%) |
| Topics (TF-IDF against the OpenAlex taxonomy) | `topic`, `relation_topic` | 3,884,270 projects have a topic link, 4,513 distinct topics used |
| Theme (best topic above score 0.1, provisional) | `theme` | 61,600 projects (1.6% of all projects): Economy 57,248, Tourism 4,352; the other 98.4% are NULL |
| Minorities (keyword matching, Wikidata QIDs) | `minority_qid` | 9,293 projects have at least one minority (0.24%), 96 distinct groups used |
| Pillars (5 bits, lowest first: inclusive, sustainable, resilient, innovative, global) | `pillars` | 228,592 projects with any pillar (5.9%): inclusive 29,944 (0.8%), sustainable 71,808 (1.8%), resilient 20,273 (0.5%), innovative 117,250 (3.0%), global 71,981 (1.8%) |

### Organizations (494,099, all enriched; 348,578 are connected to a project, 145,521 only to works)
| Enrichment | Result |
|---|---|
| Coordinates `geolocation` | 172,739 organizations (35%): ROR 126,400, Cordis 44,978, core_v2 1,361 (`geolocation_source`); 321,360 have none |
| **Mapbox geocoding** | **skipped on purpose** (costs money); the other 321,360 stay without coordinates |
| Regions (`region`, static country table) | 336,831 organizations |
| Cordis merge (addresses, PIC match) | address city on 69,754, `nuts3` on 43,232, ROR ids on 126,401; project match rate of Cordis 61.2% (87,439 of 142,773 grants), 392,682 project->organization relations carry Cordis contribution/type |

### Works (50,000,000): NO enrichment, on purpose
`is_translated` = false, `is_ch` = NULL, `pred` = NULL, `minority_qid` = `[]`, `pillars` = 0, `theme` = NULL for every work;
`relation_topic` has no work rows. Works keep their original title and descriptions (no translation). Works have `link_tier`
(0 = linked to a project, 1 = only linked to organizations; the 50M were trimmed newest first from 218M, cutoff date 2018-04-30 in the
tier-1 part, all 5.0M project-linked works kept). The translations for works that were already computed (NLLB tier 0 and tier 1)
are kept under `/work/lu72hip/data/enrichment/core_v4/nllb/` (not used in this file), so work enrichment can be resumed later.

## The questions of 2026-09-20

1. **Distinct organizations with a geolocation that are connected to a project: 63,885** (of 348,578 organizations connected to any
   project, 18.3%). By source: Cordis 44,740, ROR 17,941, core_v2 1,204. Most of the 172,739 geolocated organizations are linked to works only.
2. **Projects linked to Jewish, Ladin or Sami minorities: 2,465** distinct projects (the three groups overlap a little):
   Jewish people (Q7325) 1,870, Ladins (Q1799968) 19, Sámi people (Q48199) 590. Adding the sub-groups Georgian Jews, Kemi Sami, Kildin Saami and the
   Jewish Community of Messina adds no further project. **Of those 2,465, 378 use translated text** (`is_translated`): Jewish 264, Ladins 6, Sámi 108.
   (For all 9,293 projects with any minority: 1,410 translated.)
3. Is this better for the OpenSearch serve? Yes: one read-only file holds all entities and relations, so a view per index can join everything
   without attaching several files.

The SQL (read-only):
```sql
-- 1) distinct organizations with a geolocation connected to a project
SELECT count(*) FROM organization o
WHERE o.geolocation IS NOT NULL AND o.id IN (SELECT target FROM relation WHERE sourceType = 'project' AND targetType = 'organization');
-- 2) projects with a Jewish / Ladin / Sami minority, and how many are translated
SELECT count(*) AS projects, count(*) FILTER (WHERE is_translated) AS translated
FROM project WHERE list_has_any(minority_qid, ['Q7325', 'Q1799968', 'Q48199']);
```

## Things to know when serving
- Ids are `UBIGINT` up to ~1.8e19, larger than OpenSearch's signed `long` (9.2e18): export them as strings (or `unsigned_long` if the cluster supports it).
- `geolocation` is `[lat, lng]`. The minority matches are keyword-based (translated title/summary); their precision has not been reviewed.
- Theme covers 1.6% of projects (threshold 0.1 is provisional) and is kept as is; Cordis matching (61.2%) was accepted.
- Reference numbers in the staging: 218,421,450 works before trimming, 3,893,065 projects, 494,099 organizations; every count above matches staging.

## Provenance
Run of 2026-09-19/20 on Draco (Snakemake `core_v4_base`, then the merge). Fixes made on the way (patches in `fixes/`, log in `FIXES.md`, status in `STATUS.md`):
the tmp-file race in `enrichment/side_outputs.py`, the DuckDB memory cap in `open_staging()` (`enrichment/text_sources.py`, `regions.py`,
`geolocation.py`), and memory settings in `orchestration/rules/pipeline/core_v4/enrichment.smk`. These changes and `merge_single.py` are not
committed (the user commits). To rebuild the single file after changing a source: `sbatch ... uv run python -m pipelines.core_v4.merge_single` (it refuses to
overwrite an existing file).
