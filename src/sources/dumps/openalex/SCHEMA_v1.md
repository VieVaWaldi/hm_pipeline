# OpenAlex Dump

## First Dump | Published 03.02.2026 | without xpac

- **Docs**: https://docs.openalex.org/download-all-data/snapshot-data-format
- **Location**: `/work/lu72hip/data/pile/openalex_2026_02_03_dump/`
- **Total size**: ~626 GB compressed (works dominate)
- **Format**: Hive-partitioned directories (`updated_date=YYYY-MM-DD/part_XXXX.gz`), newline-delimited JSON, gzip-compressed

## Entities and Scale

Complete entity list with exact row counts (counted from disk):

| Entity | Files | Rows | Notes |
|---|---|---|---|
| `authors` | 359 | **109,695,206** | from manifest |
| `awards` | 1 | **11,823,532** | funding grants/awards |
| `concepts` | 14 | **65,026** | **deprecated** by OpenAlex; use `topics` instead |
| `continents` | 2 | **7** | reference data |
| `countries` | 3 | **247** | reference data |
| `domains` | 1 | **4** | top of topic hierarchy |
| `fields` | 1 | **26** | second level of topic hierarchy |
| `funders` | 15 | **32,437** | funding organizations |
| `institution-types` | 1 | **8** | reference data |
| `institutions` | 20 | **120,658** | universities, labs, etc. |
| `keywords` | 16 | **65,004** | |
| `languages` | 13 | **176** | reference data |
| `licenses` | 2 | **10** | OA license types tracked |
| `publishers` | 14 | **10,703** | |
| `sdgs` | 1 | **17** | UN Sustainable Development Goals |
| `source-types` | 1 | **6** | reference data (journal, repository, etc.) |
| `sources` | 17 | **279,377** | journals, repositories, etc. |
| `subfields` | 1 | **252** | third level of topic hierarchy |
| `topics` | 3 | **4,516** | finest granularity classification |
| `work-types` | 3 | **19** | reference data |
| `works` | 2,236 | **482,451,464** | from manifest |

**Topic hierarchy**: domains (4) → fields (26) → subfields (252) → topics (4,516)

---

## DuckDB Loading

**No extraction needed.** DuckDB globs `.gz` files directly from the hive directories:

```python
con.execute("""
    SELECT * FROM read_json(
        '/work/.../openalex_2026_02_03_dump/works/**/*.gz',
        format='newline_delimited',
        compression='gzip',
        hive_partitioning=true,
        union_by_name=true
    )
""")
```

`hive_partitioning=true` auto-parses `updated_date` as a DATE column. `union_by_name=true` required for type drift across partitions (see schema drift section).

---

## Schema (live-tested, DuckDB 1.2.2)

### authors (18 columns — 110M rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `display_name_alternatives` | VARCHAR[] |
| `orcid` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `summary_stats` | STRUCT(2yr_mean_citedness DOUBLE, h_index BIGINT, i10_index BIGINT) |
| `ids` | STRUCT(openalex VARCHAR, orcid VARCHAR) |
| `affiliations` | STRUCT(institution STRUCT(id, ror, display_name, country_code, type, lineage VARCHAR[]), years BIGINT[])[] |
| `last_known_institutions` | STRUCT(id, ror, display_name, country_code, type, lineage VARCHAR[])[] |
| `topics` | STRUCT(id, display_name, count BIGINT, score DOUBLE, subfield STRUCT, field STRUCT, domain STRUCT)[] |
| `topic_share` | STRUCT(id, display_name, value DOUBLE, subfield STRUCT, field STRUCT, domain STRUCT)[] |
| `x_concepts` | STRUCT(id, wikidata, display_name, level BIGINT, score DOUBLE, count BIGINT)[] — **deprecated** |
| `sources` | STRUCT(id, display_name, issn_l, issn VARCHAR[], is_oa, is_in_doaj, is_core, host_organization, ...)[] |
| `counts_by_year` | STRUCT(year BIGINT, works_count BIGINT, oa_works_count BIGINT, cited_by_count BIGINT)[] |
| `works_api_url` | VARCHAR |
| `updated_date` | DATE |
| `created_date` | VARCHAR |

### awards (22 columns — 11.8M rows)

Funding awards/grants, each linked to a funder and a list of funded works.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR (nullable) |
| `description` | VARCHAR (nullable) |
| `funder_award_id` | VARCHAR — funder's own grant ID |
| `amount` | DOUBLE (nullable) |
| `currency` | VARCHAR (nullable) |
| `funder` | STRUCT(id VARCHAR, display_name VARCHAR, ror_id VARCHAR, doi VARCHAR) |
| `funding_type` | VARCHAR (nullable) |
| `funder_scheme` | VARCHAR (nullable) |
| `provenance` | VARCHAR — e.g. `crossref_work.grants` |
| `start_date` | DATE (nullable) |
| `end_date` | DATE (nullable) |
| `start_year` | BIGINT (nullable) |
| `end_year` | BIGINT (nullable) |
| `lead_investigator` | VARCHAR (nullable) |
| `co_lead_investigator` | VARCHAR (nullable) |
| `investigators` | VARCHAR[] |
| `landing_page_url` | VARCHAR (nullable) |
| `doi` | VARCHAR (nullable) |
| `works_api_url` | VARCHAR |
| `funded_outputs` | VARCHAR[] — OpenAlex work IDs |
| `funded_outputs_count` | BIGINT |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### concepts (16 columns — 65K rows — **DEPRECATED**)

OpenAlex is phasing out concepts in favour of `topics`. Still in the dump for backwards compatibility. Use `topics` for new work.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `level` | BIGINT — 0 (broadest) to 5 |
| `description` | VARCHAR |
| `wikidata` | VARCHAR |
| `image_url` | VARCHAR |
| `image_thumbnail_url` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `ids` | STRUCT(openalex, wikidata, wikipedia, umls_aui, umls_cui, mag) |
| `works_api_url` | VARCHAR |
| `summary_stats` | STRUCT (nullable in dump) |
| `international` | JSON (nullable) |
| `ancestors` | JSON (nullable in dump) |
| `related_concepts` | JSON (nullable in dump) |
| `counts_by_year` | JSON (nullable in dump) |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### continents (10 columns — 7 rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `wikidata_id` | VARCHAR |
| `wikidata_url` | VARCHAR |
| `wikipedia_url` | VARCHAR |
| `display_name_alternatives` | VARCHAR[] |
| `description` | VARCHAR |
| `ids` | STRUCT(openalex VARCHAR, wikidata VARCHAR) |
| `countries` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### countries (18 columns — 247 rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `country_code` | VARCHAR — ISO 2-letter |
| `display_name` | VARCHAR |
| `continent_id` | BIGINT |
| `is_global_south` | BOOLEAN |
| `wikidata_url` | VARCHAR |
| `wikipedia_url` | VARCHAR |
| `display_name_alternatives` | VARCHAR[] |
| `description` | VARCHAR |
| `alpha_3` | VARCHAR — ISO 3-letter |
| `numeric` | BIGINT — ISO numeric code |
| `full_name` | VARCHAR |
| `works_count` | BIGINT (nullable) |
| `cited_by_count` | BIGINT (nullable) |
| `authors_api_url` | VARCHAR |
| `institutions_api_url` | VARCHAR |
| `works_api_url` | VARCHAR |
| `ids` | STRUCT(openalex, iso, wikidata, wikipedia) |
| `continent` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### domains (9 columns — 4 rows)

Top of the topic hierarchy (domains → fields → subfields → topics).

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `ids` | STRUCT(openalex, wikidata, wikipedia) |
| `display_name_alternatives` | VARCHAR[] |
| `fields` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `siblings` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### fields (9 columns — 26 rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `ids` | STRUCT(openalex, wikidata, wikipedia) |
| `display_name_alternatives` | VARCHAR[] |
| `domain` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `subfields` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `siblings` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### funders (15 columns — 32K rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `alternate_titles` | VARCHAR[] |
| `country_code` | VARCHAR |
| `description` | VARCHAR (nullable) |
| `homepage_url` | VARCHAR (nullable) |
| `image_url` | VARCHAR (nullable) |
| `image_thumbnail_url` | VARCHAR (nullable) |
| `ids` | STRUCT(openalex VARCHAR, ror VARCHAR, wikidata VARCHAR, crossref BIGINT, doi VARCHAR) |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `awards_count` | BIGINT |
| `roles` | STRUCT(role VARCHAR, id VARCHAR, works_count BIGINT)[] |
| `counts_by_year` | STRUCT(year BIGINT, works_count BIGINT, oa_works_count BIGINT, cited_by_count BIGINT)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### institution-types (7 columns — 8 rows)

Values: company, education, facility, government, healthcare, nonprofit, archive, other.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `works_api_url` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### institutions (27 columns — 120,658 rows — exact, tested)

| column | type |
|---|---|
| `id` | VARCHAR |
| `ror` | VARCHAR |
| `display_name` | VARCHAR |
| `country_code` | VARCHAR |
| `type` | VARCHAR — matches institution-types |
| `type_id` | VARCHAR |
| `lineage` | VARCHAR[] |
| `is_super_system` | BOOLEAN |
| `homepage_url` | VARCHAR |
| `image_url` | VARCHAR |
| `image_thumbnail_url` | VARCHAR |
| `display_name_acronyms` | VARCHAR[] |
| `display_name_alternatives` | VARCHAR[] |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `ids` | STRUCT(openalex, ror, grid, wikipedia, wikidata) |
| `roles` | STRUCT(role VARCHAR, id VARCHAR, works_count BIGINT)[] |
| `repositories` | STRUCT(...)[] |
| `geo` | STRUCT(city, geonames_city_id, region, country_code, country, latitude DOUBLE, longitude DOUBLE) |
| `topics` | STRUCT(id, display_name, count BIGINT, score DOUBLE, subfield STRUCT, field STRUCT, domain STRUCT)[] |
| `associated_institutions` | STRUCT(id, ror, display_name, country_code, type, relationship VARCHAR)[] |
| `counts_by_year` | STRUCT(year BIGINT, works_count BIGINT, oa_works_count BIGINT, cited_by_count BIGINT)[] |
| `summary_stats` | STRUCT(2yr_mean_citedness DOUBLE, h_index BIGINT, i10_index BIGINT) |
| `works_api_url` | VARCHAR |
| `updated_date` | DATE |
| `created_date` | VARCHAR |

### keywords (6 columns — 65K rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `works_api_url` | VARCHAR |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### languages (6 columns — 176 rows)

| column | type |
|---|---|
| `id` | VARCHAR — e.g. `https://openalex.org/languages/mt` |
| `display_name` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `works_api_url` | VARCHAR |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### licenses (8 columns — 10 rows)

Only tracks licenses that OpenAlex explicitly recognizes (e.g. CC-BY, MIT). Not exhaustive.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `url` | VARCHAR — SPDX URL |
| `description` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `works_api_url` | VARCHAR |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### publishers (16 columns — 10K rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `lineage` | VARCHAR[] — parent publisher chain |
| `alternate_titles` | VARCHAR[] |
| `country_codes` | VARCHAR[] |
| `hierarchy_level` | BIGINT — 0 = top-level |
| `parent_publisher` | VARCHAR (nullable) |
| `ids` | STRUCT(openalex, ror, wikidata) |
| `ror_id` | VARCHAR (nullable) |
| `wikidata_id` | VARCHAR (nullable) |
| `homepage_url` | VARCHAR (nullable) |
| `image_url` | VARCHAR (nullable) |
| `image_thumbnail_url` | VARCHAR (nullable) |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `summary_stats` | STRUCT(2yr_mean_citedness DOUBLE, h_index BIGINT, i10_index BIGINT) |
| `roles` | STRUCT(role VARCHAR, id VARCHAR, works_count BIGINT)[] |
| `counts_by_year` | STRUCT(year BIGINT, works_count BIGINT, cited_by_count BIGINT)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### sdgs (9 columns — 17 rows)

UN Sustainable Development Goals.

| column | type |
|---|---|
| `id` | VARCHAR |
| `ids` | STRUCT(openalex VARCHAR, un VARCHAR, wikidata VARCHAR) |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `image_url` | VARCHAR |
| `image_thumbnail_url` | VARCHAR |
| `works_api_url` | VARCHAR |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### source-types (7 columns — 6 rows)

Values: journal, repository, conference, ebook platform, book series, other.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `works_api_url` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### sources (22 columns — 279K rows)

Journals, repositories, conference series, ebook platforms.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `issn_l` | VARCHAR (nullable) |
| `issn` | VARCHAR[] (nullable) |
| `type` | VARCHAR — journal / repository / conference / etc. |
| `host_organization` | BIGINT (nullable) — publisher ID as integer |
| `host_organization_name` | VARCHAR (nullable) |
| `host_organization_lineage` | VARCHAR[] |
| `ids` | STRUCT(openalex, issn_l, issn VARCHAR[], mag, wikidata) |
| `homepage_url` | VARCHAR (nullable) |
| `country_code` | VARCHAR (nullable) |
| `alternate_titles` | VARCHAR[] |
| `apc_prices` | STRUCT(price BIGINT, currency VARCHAR)[] |
| `apc_usd` | BIGINT (nullable) |
| `societies` | STRUCT(...)[] |
| `is_oa` | BOOLEAN |
| `is_in_doaj` | BOOLEAN |
| `is_in_doaj_since_year` | BIGINT (nullable) |
| `is_high_oa_rate` | BOOLEAN |
| `is_in_scielo` | BOOLEAN |
| `is_ojs` | BOOLEAN |
| `is_core` | BOOLEAN |
| `oa_flip_year` | BIGINT (nullable) |
| `first_publication_year` | BIGINT (nullable) |
| `last_publication_year` | BIGINT (nullable) |
| `works_count` | BIGINT |
| `oa_works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `summary_stats` | STRUCT(2yr_mean_citedness DOUBLE, h_index BIGINT, i10_index BIGINT) |
| `topics` | STRUCT(id, display_name, count BIGINT, score DOUBLE, subfield STRUCT, field STRUCT, domain STRUCT)[] |
| `counts_by_year` | STRUCT(year BIGINT, works_count BIGINT, oa_works_count BIGINT, cited_by_count BIGINT)[] |
| `created_date` | VARCHAR |
| `updated_date` | DATE |

### subfields (10 columns — 252 rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `ids` | STRUCT(openalex, wikidata, wikipedia) |
| `display_name_alternatives` | VARCHAR[] |
| `field` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `domain` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `topics` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `siblings` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### topics (11 columns — 4,516 rows)

Finest-granularity classification. Prefer over deprecated `concepts`.

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `keywords` | VARCHAR[] |
| `ids` | STRUCT(openalex VARCHAR, wikipedia VARCHAR) |
| `subfield` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `field` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `domain` | STRUCT(id VARCHAR, display_name VARCHAR) |
| `siblings` | STRUCT(id VARCHAR, display_name VARCHAR)[] |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `works_api_url` | VARCHAR |
| `updated_date` | VARCHAR |
| `created_date` | VARCHAR |

### work-types (7 columns — 19 rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `display_name` | VARCHAR |
| `description` | VARCHAR |
| `works_api_url` | VARCHAR |
| `works_count` | BIGINT |
| `cited_by_count` | BIGINT |
| `created_date` | VARCHAR |
| `updated_date` | VARCHAR |

### works (49 columns — 482M rows)

| column | type |
|---|---|
| `id` | VARCHAR |
| `doi` | VARCHAR |
| `title` / `display_name` | VARCHAR |
| `ids` | STRUCT(openalex, doi, mag, pmid) |
| `indexed_in` | VARCHAR[] |
| `publication_date` | DATE |
| `publication_year` | BIGINT |
| `language` | VARCHAR |
| `type` | VARCHAR |
| `authorships` | STRUCT(author STRUCT(display_name, id, orcid), author_position, affiliations STRUCT(institution_ids, raw_affiliation_string)[], countries, raw_author_name, is_corresponding, raw_affiliation_strings, institutions STRUCT(...)[])[] |
| `authors_count` | BIGINT |
| `corresponding_author_ids` | VARCHAR[] |
| `corresponding_institution_ids` | VARCHAR[] |
| `primary_topic` | STRUCT(id, display_name, score, subfield, field, domain) |
| `topics` | STRUCT(id, display_name, score, subfield, field, domain)[] |
| `keywords` | STRUCT(id, display_name, score DOUBLE)[] |
| `concepts` | STRUCT(id, wikidata, display_name, level, score)[] — deprecated |
| `locations` | STRUCT(id, source STRUCT, is_oa, is_published, landing_page_url, pdf_url, raw_source_name, raw_type, provenance, license, license_id, version, is_accepted)[] |
| `locations_count` | BIGINT |
| `primary_location` | STRUCT (same as locations element) |
| `best_oa_location` | STRUCT (same as locations element) |
| `sustainable_development_goals` | STRUCT(id, display_name, score DOUBLE)[] |
| `awards` | STRUCT(id, display_name, funder_award_id, funder_id, funder_display_name)[] |
| `funders` | STRUCT(id, display_name, ror)[] |
| `institutions` | JSON[] — inconsistent sub-structure |
| `countries_distinct_count` | BIGINT |
| `institutions_distinct_count` | BIGINT |
| `open_access` | STRUCT(is_oa, oa_status, any_repository_has_fulltext, oa_url) |
| `is_paratext` | BOOLEAN |
| `is_retracted` | BOOLEAN |
| `is_xpac` | BOOLEAN |
| `biblio` | STRUCT(volume, issue, first_page, last_page) |
| `referenced_works` | VARCHAR[] |
| `referenced_works_count` | BIGINT |
| `related_works` | VARCHAR[] |
| `abstract_inverted_index` | MAP(VARCHAR, BIGINT[]) — word→position map; ~60% of works |
| `cited_by_count` | BIGINT |
| `counts_by_year` | STRUCT(year BIGINT, cited_by_count BIGINT)[] |
| `apc_list` | STRUCT(value BIGINT, currency, value_usd BIGINT) |
| `apc_paid` | STRUCT(value DOUBLE, currency, value_usd DOUBLE) |
| `fwci` | DOUBLE |
| `citation_normalized_percentile` | STRUCT(value, is_in_top_1_percent, is_in_top_10_percent) |
| `cited_by_percentile_year` | STRUCT(min, max) |
| `mesh` | STRUCT(descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic)[] |
| `has_content` | STRUCT(pdf BOOLEAN, grobid_xml BOOLEAN) |
| `has_fulltext` | BOOLEAN |
| `created_date` | VARCHAR |
| `updated_date` | DATE — hive partition column |

Notes:
- `abstract_inverted_index`: word → position-array map. Present in ~60% of works. Consistently `MAP(VARCHAR, BIGINT[])` — no type conflict.
- `authorships` capped at 100 authors per docs.
- `institutions` (top-level) inferred as `JSON[]` — inconsistent sub-structure across records.
- `content_url`: **not in the snapshot** (API-only).
- Deprecated fields (`grants`, `host_venue`, `alternate_host_venues`) are absent from the snapshot entirely.

---

## Schema Drift Analysis (live-tested)

Compared oldest partition (2016-06-24) vs newest (2026-02-25) for works:

- **Column count**: 49 vs 49 — **zero column drift** across 10 years.
- **Type conflicts** (3 fields evolved from sparse JSON to typed STRUCT in older partitions):

| Field | 2016 type | 2026 type |
|---|---|---|
| `sustainable_development_goals` | `JSON[]` | `STRUCT(id, display_name, score)[]` |
| `awards` | `JSON[]` | `STRUCT(id, display_name, funder_award_id, funder_id, funder_display_name)[]` |
| `apc_paid` | `JSON` | `STRUCT(value DOUBLE, currency, value_usd DOUBLE)` |

**Resolution**: `union_by_name=true` across 4 mixed-date partitions (2016, 2016, 2026, 2026) resolves all 3 automatically — DuckDB promotes JSON to the richer STRUCT type. Tested and confirmed.

---

## Loading Strategy

### Reference entities (continents, countries, domains, fields, subfields, topics, source-types, institution-types, work-types, sdgs, licenses)

Tiny — under 1K rows each. Single glob, done in seconds.

```python
con.execute(f"""
    CREATE TABLE {entity} AS
    SELECT * FROM read_json(
        '/work/.../openalex_2026_02_03_dump/{entity}/**/*.gz',
        format='newline_delimited', compression='gzip',
        hive_partitioning=true, union_by_name=true
    )
""")
```

### Medium entities (institutions, keywords, concepts, funders, publishers, sources, awards)

Up to a few hundred thousand rows. Same single-glob pattern. All load trivially.

### Authors (67 GB, 110M rows)

Same glob pattern. Feasible as a single call; monitor memory. COPY TO Parquet recommended over materializing into `.duckdb`.

### Works (626 GB, 482M rows)

**Option A — COPY TO Parquet (recommended)**
```sql
COPY (
    SELECT * FROM read_json(
        '/work/.../works/**/*.gz',
        format='newline_delimited', compression='gzip',
        hive_partitioning=true, union_by_name=true
    )
) TO 'works.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
```
DuckDB streams lazily; does not materialize all 482M rows into memory at once.

**Option B — Per-partition Parquet**
Write one Parquet per `updated_date` partition. Allows incremental updates.

**Option C — Chunked by year**
Loop over year ranges and COPY TO separate Parquet files.

### Recommendation
- Reference + medium entities: single glob → DuckDB table.
- Works + authors: COPY TO Parquet (zstd). Keep `hive_partitioning=true` and `union_by_name=true` on all queries.

---

## Open Questions (remaining)

- [ ] Does `union_by_name=true` hold across ALL 2,236 work files (only tested 4 partitions)?
- [ ] Actual decompressed size of works — estimate 300–600 GB for Parquet output depending on compression.
- [ ] `institutions` (top-level work field) is `JSON[]` — are sub-fields consistent enough to cast to a typed STRUCT, or leave as JSON?
- [ ] `counts_by_year` in works only has `year` + `cited_by_count`, but in authors/sources has `works_count`, `oa_works_count`, `cited_by_count` — confirmed different schemas per entity.
- [ ] Time to COPY all works to Parquet on this node (estimate: hours; schedule as a slurm job).
- [ ] `awards` entity (11.8M rows) — `funded_outputs` links awards to works; useful for funding analysis. Confirm the `awards` field in works records matches these IDs.
