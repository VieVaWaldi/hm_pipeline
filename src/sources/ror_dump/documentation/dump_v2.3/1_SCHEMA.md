# ROR Dump

## Second Dump | Released 2026-08-03 | v2.11 schema

- **Docs**: https://ror.readme.io/docs/ror-data-structure-v2
- **Download**: https://zenodo.org/records/21773148
- **Schema**: https://github.com/ror-community/ror-schema/blob/schema-v2-1/ror_schema_v2_1.json 
- **Location**: `/work/lu72hip/data/pile/ror_2026_08_03_dump/`
- **File**:                                     # ---  `v2.3-2026-02-24-ror-data.json` (~275 MB, single JSON file)---
- **Format**: A single top-level JSON array of organization records.
- **Total records**: **135,710**

---

## First Dump | Released 2026-02-24 | v2.3 schema

- **Docs**: https://ror.readme.io/docs/ror-data-structure-v2
- **Download**: https://zenodo.org/records/18761279
- **Schema**: https://github.com/ror-community/ror-schema/blob/schema-v2-1/ror_schema_v2_1.json
- **Location**: `/work/lu72hip/data/pile/ror_2026_02_24_dump/`
- **File**: `v2.3-2026-02-24-ror-data.json` (~275 MB, single JSON file)
- **Format**: A single top-level JSON array of organization records.
- **Total records**: **122,388**

---

## Scale and Status Breakdown

| Status | Count |
|---|---|
| `active` | 119,962 |
| `inactive` | 1,087 |
| `withdrawn` | 1,339 |

## Organization Types (non-exclusive — an org can have multiple)

| Type | Count |
|---|---|
| `company` | 31,836 |
| `education` | 24,462 |
| `nonprofit` | 16,842 |
| `funder` | 17,483 |
| `healthcare` | 14,622 |
| `facility` | 13,816 |
| `other` | 9,609 |
| `government` | 8,186 |
| `archive` | 3,198 |

---

## DuckDB Loading

DuckDB reads the single JSON file directly:

```python
con.execute("""
    SELECT * FROM read_json('/work/.../ror_2026_02_24_dump/v2.3-2026-02-24-ror-data.json',
        format='array')
""")
```

All nested arrays (names, locations, external_ids, etc.) are inferred as structs/arrays. Use `unnest()` to flatten them for joins.

---

## Schema (live-inspected, 122,388 records)

Each record is a JSON object with the following top-level fields:

| field | type | notes |
|---|---|---|
| `id` | VARCHAR | ROR URL e.g. `https://ror.org/04ttjf776` |
| `status` | VARCHAR | `active` / `inactive` / `withdrawn` |
| `established` | INTEGER | founding year; null for ~18% of records |
| `types` | VARCHAR[] | one or more of: education, company, healthcare, funder, facility, nonprofit, government, archive, other |
| `domains` | VARCHAR[] | internet domains associated with org; empty for ~84% of records |
| `names` | STRUCT[] | see below |
| `locations` | STRUCT[] | see below |
| `links` | STRUCT[] | see below |
| `external_ids` | STRUCT[] | see below |
| `relationships` | STRUCT[] | see below |
| `admin` | STRUCT | metadata: created/last_modified dates and schema version |

### names

All 122,388 records have exactly one `ror_display` name. Multiple name entries per org are common.

| subfield | type | notes |
|---|---|---|
| `value` | VARCHAR | the name string |
| `types` | VARCHAR[] | one or more of: `ror_display`, `label`, `acronym`, `alias` |
| `lang` | VARCHAR | ISO 639-1 language code; null for acronyms |

**Name type counts (across all orgs):**

| Name type | Occurrences |
|---|---|
| `ror_display` | 122,388 (always exactly 1 per org) |
| `label` | 160,684 (can be multiple per org, one per language) |
| `acronym` | 54,673 |
| `alias` | 50,300 |

### locations

One or more geonames-backed locations per org.

| subfield | type | notes |
|---|---|---|
| `geonames_id` | INTEGER | geonames.org numeric ID |
| `geonames_details.continent_code` | VARCHAR | e.g. `EU`, `NA`, `OC` |
| `geonames_details.continent_name` | VARCHAR | |
| `geonames_details.country_code` | VARCHAR | ISO 3166-1 alpha-2 |
| `geonames_details.country_name` | VARCHAR | |
| `geonames_details.country_subdivision_code` | VARCHAR | state/province code; nullable |
| `geonames_details.country_subdivision_name` | VARCHAR | nullable |
| `geonames_details.lat` | DOUBLE | |
| `geonames_details.lng` | DOUBLE | |
| `geonames_details.name` | VARCHAR | city/place name |

### links

| subfield | type | notes |
|---|---|---|
| `type` | VARCHAR | `website` or `wikipedia` |
| `value` | VARCHAR | URL |

**Coverage:** `website` present for 119,512 orgs; `wikipedia` for 40,611 orgs.

### external_ids

Cross-references to other authority databases. Each entry:

| subfield | type | notes |
|---|---|---|
| `type` | VARCHAR | `grid` / `isni` / `fundref` / `wikidata` |
| `all` | VARCHAR[] | all known IDs of this type for the org |
| `preferred` | VARCHAR | preferred ID; nullable |

**Coverage by type:**

| External ID type | Orgs with this ID |
|---|---|
| `grid` | 103,680 |
| `isni` | 58,851 |
| `wikidata` | 55,836 |
| `fundref` | 17,769 |

### relationships

Links to other ROR records (parent/child/related org hierarchy).

| subfield | type | notes |
|---|---|---|
| `type` | VARCHAR | `parent` / `child` / `related` / `predecessor` / `successor` |
| `label` | VARCHAR | display name of the related org |
| `id` | VARCHAR | ROR URL of the related org |

**Relationship counts (across all orgs):**

| Rel type | Count |
|---|---|
| `parent` | 26,206 |
| `child` | 25,830 |
| `related` | 11,168 |
| `successor` | 1,884 |
| `predecessor` | 410 |

Note: parent/child counts are symmetric — each child relation on org A corresponds to a parent relation on the child's record.

### admin

Metadata about the ROR record itself.

| subfield | type | notes |
|---|---|---|
| `admin.created.date` | VARCHAR | ISO date string e.g. `2018-11-14` |
| `admin.created.schema_version` | VARCHAR | ROR schema version at creation |
| `admin.last_modified.date` | VARCHAR | ISO date string |
| `admin.last_modified.schema_version` | VARCHAR | ROR schema version at last modification |

---

## Loading Strategy

### Flat table (one row per org)

```sql
CREATE TABLE ror AS
SELECT
    id,
    status,
    established,
    types,
    domains,
    admin.created.date AS created_date,
    admin.last_modified.date AS last_modified_date
FROM read_json('/work/.../ror_2026_02_24_dump/v2.3-2026-02-24-ror-data.json', format='array')
```

### Extract primary display name

```sql
SELECT id,
    (SELECT n.value FROM unnest(names) AS n WHERE 'ror_display' = ANY(n.types) LIMIT 1) AS display_name
FROM ror_raw
```

### Join with OpenAIRE organizations via GRID ID

Both OpenAIRE `organization.pids` and ROR `external_ids` carry GRID IDs. Joining on GRID is the most reliable cross-reference (103K ROR orgs have one).

```sql
-- Extract GRID from ROR
SELECT id AS ror_id,
    (SELECT eid.all[1] FROM unnest(external_ids) AS eid WHERE eid.type = 'grid' LIMIT 1) AS grid_id
FROM ror_raw

-- OpenAIRE org pids: STRUCT(scheme, value)[] where scheme = 'grid'
```

---

## Key Differences from OpenAIRE organization

| | ROR | OpenAIRE organization |
|---|---|---|
| Record count | 122,388 | 448,161 |
| Format | Single JSON array | Directory of part files (json.gz) |
| Coverage | Global authority file (all org types) | Research-active orgs linked to OpenAIRE outputs |
| Name structure | Typed name list (ror_display, label, acronym, alias) | `legalName` + `legalShortName` + `alternativeNames[]` |
| Geography | Geonames-linked with lat/lng | Country code + label only |
| Cross-refs | grid, isni, fundref, wikidata | pids[] with scheme/value |
| Hierarchy | parent/child/related relationships | None |

---

## Key Notes

1. **Single file** — no partitioning; load in one shot. At ~275 MB it fits easily in memory.

2. **`id` is a URL** — `https://ror.org/<id>`. Extract the bare ID with `split_part(id, '/', -1)` if needed for joins.

3. **`types` is an array** — an org can be both `education` and `funder`. Filter with `list_contains(types, 'education')` in DuckDB SQL.

4. **Names require unnesting** to get the display name. Always use the `ror_display`-typed entry as the canonical name — exactly one exists per org.

5. **`domains` sparsely populated** — only ~16% of orgs have domain entries. Do not rely on it for matching.

6. **No direct affiliation links** — ROR is a reference dataset only. To link orgs to publications, use OpenAIRE `relation` (type=`affiliation`) and join on GRID or ISNI.

7. **`established` null for ~18%** — treat as nullable integer.

8. **Schema v2.x** — this dump uses ROR schema v2.x which introduced the `names[]` array (replacing flat `name`/`aliases` fields from v1). Do not confuse with v1 schema documented elsewhere.
