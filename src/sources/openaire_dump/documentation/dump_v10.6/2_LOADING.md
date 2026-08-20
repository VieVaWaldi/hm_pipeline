# OpenAire Model

This document explains what entities and columns to ingest from the OpenAIRE dump and how to load them into DuckDB.

---

## Entities to load

| Entity | Rows | Notes |
|---|---|---|
| `organization` | 448,161 | - |
| `project` | 3,673,360 | - |
| `work` | ~165M | Loaded from `publication/` directory; table named `work` |
| `relation` | ~2B (raw) → filtered | See filter rules below |

## Entities to ignore

| Entity | Notes |
|---|---|
| `communities_infrastructures` | Not relevant |
| `dataset` | Excluded for now, may revisit |
| `datasource` | Not needed |
| `otherresearchproduct` | Not needed |
| `software` | Not needed |

---

## DuckDB target

**Single file: `openaire.duckdb`** — all four tables live in one DuckDB file.
Cross-entity joins (works ↔ relations ↔ organizations ↔ projects) require a single file to use DuckDB's query optimizer. Separate files would require ATTACH and complicate joins.

## Column selection

### Keep all columns for all entities

We do not drop columns at load time. It is easier to drop data later after inspecting it than to re-run the load.
Exception: the 9 sparse JSON fields at the bottom of the work schema that are near-null for publication-type records are likely wasteful, but are kept for now.

## Nested data strategy

DuckDB stores STRUCTs natively and supports dot-notation queries (e.g. `language.code`, `indicators.citationImpact.citationCount`). No need to unnest at load time. All nested fields are kept as-is and flattened in the sanitization step if needed.

**Note on `organization.pids`:** ROR values are stored as full URLs (`https://ror.org/00mesrk97`). When merging with ROR data, strip the prefix to get the bare ID, or normalize both sides to the same format.

---

## Relation filter rules

The raw `relation` directory has ~2B rows. We filter to only what is needed.

### Step 1: filter by relation type

```sql
WHERE relType.type IN ('affiliation', 'outcome', 'participation')
```

This excludes:
- `citation` (work↔work, not needed)
- `provision` (product↔datasource, dominant by volume)
- `similarity` (top-N similar docs)

Relation types kept:

| relType.type | relType.name examples | Entities |
|---|---|---|
| `affiliation` | hasAuthorInstitution, isAuthorInstitutionOf | work ↔ organization |
| `outcome` | produces, isProducedBy | project ↔ work |
| `participation` | isParticipant | organization ↔ project |

### Step 2: filter product-side to works only

The `relation` table uses `sourceType = 'product'` / `targetType = 'product'` for all research output types (publications, datasets, software, other). It does not carry the product subtype.

**We cannot filter to publication-only products during the initial relation scan.** The work IDs are needed first.

**Loading order:**
1. `organization`
2. `project`
3. `work` → after load, the work ID set is available in DuckDB
4. `relation` → filter by relType.type (step 1) + semi-join against work IDs

The `work` table is loaded from the `publication/` directory only, so the semi-join implicitly restricts products to publications — datasets, software, and other research products are excluded.

```sql
-- Step 4: load relations filtered to publication products only
CREATE TABLE relation AS
SELECT r.*
FROM read_json('.../relation/*.json.gz', format='newline_delimited', compression='gzip') r
WHERE relType.type IN ('affiliation', 'outcome', 'participation')
  AND (
    (r.sourceType = 'product' AND r.source IN (SELECT id FROM work))
    OR (r.targetType = 'product' AND r.target IN (SELECT id FROM work))
    OR (r.sourceType != 'product' AND r.targetType != 'product')  -- org↔project participation
  )
```

Note: `participation` (organization↔project) has no product side, so it passes through the type filter without needing the work semi-join.

---

## Data quality notes

- **`publicationDate`**: values like `0001-12-30` exist. Load as VARCHAR, clean in the sanitization step.
- **`dump uses 'product'`**: the relation table uses `product` as sourceType/targetType for research outputs; the API docs say `result`.
- **ROR URL format**: `pids.value` for scheme=ROR is a full URL (`https://ror.org/XXXXX`), not a bare ID.