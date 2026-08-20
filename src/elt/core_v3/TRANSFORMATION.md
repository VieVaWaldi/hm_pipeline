# Core_v3 Transformation

This document describes how to merge Cordis and ROR with OpenAire to create the core_v3 data model.
Script: `src/elt/core_v3/transformation.py`

## General

The core_v3 duckdb (`/work/lu72hip/data/duckdb/core/core_v3.duckdb`) currently contains the OpenAire staging
tables (`organization`, `project`, `work`, `relation`) plus `topic` and `relation_topic` from TF-IDF enrichment.

This transformation adds columns to existing tables — it does not create new tables.

---

## 1. Merging ROR into `core.organization`

### Goal

Enrich each organization that has a ROR id with geolocation, org type, status, founding year, and
parent/child/related org hierarchy from ROR.

### Join key

```
core.organization.rorId = ror.id
```

Both fields store the full ROR URL (e.g. `https://ror.org/04ttjf776`). Direct equality join.
Only organizations that already have a `rorId` will be updated (~subset of 448K orgs).

### Source

`/work/lu72hip/data/duckdb/sources/ror_raw.duckdb` — table `ror`

ROR schema reference: `src/sources/ror_dump/documentation/dump_v2.3/1_SCHEMA.md`

### Columns to add to `core.organization`

| column | type | source | notes |
|---|---|---|---|
| `rorStatus` | VARCHAR | `ror.status` | `active` / `inactive` / `withdrawn` |
| `rorEstablished` | INTEGER | `ror.established` | founding year; null for ~18% |
| `rorTypes` | VARCHAR[] | `ror.types` | e.g. `['education', 'funder']` |
| `rorLocations` | JSON | `ror.locations` | full geonames struct array serialized as JSON |
| `geolocation` | DOUBLE[] | `ror.locations[1].geonames_details.lat/lng` | `[lat, lng]` of primary location |
| `rorRelationships` | JSON | `ror.relationships` | parent/child/related/successor/predecessor links |

### SQL pattern

```sql
ALTER TABLE organization ADD COLUMN rorStatus       VARCHAR;
ALTER TABLE organization ADD COLUMN rorEstablished  INTEGER;
ALTER TABLE organization ADD COLUMN rorTypes        VARCHAR[];
ALTER TABLE organization ADD COLUMN rorLocations    JSON;
ALTER TABLE organization ADD COLUMN geolocation     DOUBLE[];
ALTER TABLE organization ADD COLUMN rorRelationships JSON;

UPDATE organization o
SET
    rorStatus        = r.status,
    rorEstablished   = r.established,
    rorTypes         = r.types,
    rorLocations     = r.locations::JSON,
    geolocation      = [r.locations[1].geonames_details.lat,
                        r.locations[1].geonames_details.lng],
    rorRelationships = r.relationships::JSON
FROM ror r
WHERE o.rorId = r.id;
```

---

## 2. Merging Cordis into `core.relation`

### Goal

For each project–institution relationship in Cordis, find the matching triplet in OpenAire and copy
the institution's EC funding share and participation type onto the `core.relation` row.
OpenAire does not carry per-institution funding data; Cordis does.

### Matching strategy (validated in EDA — `src/sources/cordis/eda.ipynb`)

Two-step exact join:

1. **Project match**: `cordis.project.id_original = core.project.grantId`
   - 96,056 / 139,925 cordis projects matched (68.6%)
2. **Institution match**: `LOWER(TRIM(cordis.institution.legal_name)) = LOWER(TRIM(core.organization.legalName))`
   - 317,818 / 534,817 triplets matched (59.4%)
   - Case-insensitive exact match works well; e.g. "UNIVERSITY OF TWENTE" ↔ "University of Twente"
   - Duplicate org names in core (~58K) are naturally disambiguated by the final relation join

The final join against `core.relation` yields **~193K enrichable rows per direction**, ~386K total
(relation is bidirectional: both `source=project/target=org` and `source=org/target=project` exist).

### Source

`/work/lu72hip/data/duckdb/sources/cordis_raw.duckdb` — tables `project`, `institution`, `j_project_institution`

Cordis schema reference: see column lists in `src/sources/cordis/eda.ipynb`

### Columns to add to `core.relation`

| column | type | source | notes |
|---|---|---|---|
| `cordis_ec_contribution` | DOUBLE | `j_project_institution.ec_contribution` | EC funding share for this institution; 55.5% non-null |
| `cordis_type` | VARCHAR | `j_project_institution.type` | `coordinator` or `participant`; 100% non-null |

`ec_contribution` was chosen over `total_cost` (40.5% coverage) and `net_ec_contribution` (40.5%)
because it has the best coverage (55.5%) and correctly represents "what the EC paid to this institution."
`total_cost = 0.0` is common for recent ERC grants where the EC covers everything.

### SQL pattern

```sql
-- Attach cordis
ATTACH '/work/lu72hip/data/duckdb/sources/cordis_raw.duckdb' AS cordis (READ_ONLY);

ALTER TABLE relation ADD COLUMN cordis_ec_contribution DOUBLE;
ALTER TABLE relation ADD COLUMN cordis_type            VARCHAR;

-- Build triplet lookup: core_project_id + core_org_id + cordis funding fields
CREATE TEMP TABLE _cordis_triplets AS
SELECT
    op.id                  AS core_project_id,
    co.id                  AS core_org_id,
    jpi.ec_contribution    AS cordis_ec_contribution,
    jpi.type               AS cordis_type
FROM cordis.project          cp
JOIN project                 op  ON cp.id_original = op.grantId
JOIN cordis.j_project_institution jpi ON jpi.project_id = cp.id
JOIN cordis.institution      ci  ON ci.id = jpi.institution_id
JOIN organization            co  ON LOWER(TRIM(co.legalName)) = LOWER(TRIM(ci.legal_name));

-- Update project → organization direction only (org → project is the same fact reversed,
-- enriching both would cause double-counting in any SUM(cordis_ec_contribution) query)
UPDATE relation r
SET
    cordis_ec_contribution = ct.cordis_ec_contribution,
    cordis_type            = ct.cordis_type
FROM _cordis_triplets ct
WHERE r.source = ct.core_project_id
  AND r.target = ct.core_org_id;
```

### Expected yield

| metric | value |
|---|---|
| Cordis projects matched to core | 96,056 |
| Triplets with institution name match | 317,818 |
| Relation rows enriched (project→org direction only) | ~193K |
| `cordis_ec_contribution` non-null after enrichment | ~55% of enriched rows |
| `cordis_type` non-null after enrichment | 100% of enriched rows |
