# OpenAire Dump

This document explains the source data schema as available in the dump.

## First Dump | Published 05.06.2026 | v11

- **Docs**: https://graph.openaire.eu/docs/data-model/ | https://zenodo.org/records/20428976
- **Location**: `/work/lu72hip/data/pile/openaire_2026_06_05_dump/`
- **Total size**: ~____ GB compressed
- **Format**: Each entity is a directory of `part-XXXXX.json.gz` files (newline-delimited JSON, gzip-compressed).

---


## First Dump | Published 01.12.2025 | v10.6.0

- **Docs**: https://graph.openaire.eu/docs/data-model/ | https://zenodo.org/records/14608526
- **Location**: `/work/lu72hip/data/pile/openaire_2025_12_01_dump/`
- **Total size**: ~309 GB compressed
- **Format**: Each entity is a directory of `part-XXXXX.json.gz` files (newline-delimited JSON, gzip-compressed).

---

## Entities and Scale

This is the complete list of all entity types in the dump. All are present as directories:

| Entity | Parts on disk | Row count | Notes |
|---|---|---|---|
| `communities_infrastructures` | 1 | 36 | tiny — single part |
| `dataset` | 573 | ~74M | same schema as publication |
| `datasource` | 2 | ~216K | journals, repos, etc. |
| `organization` | 120 | 448,161 | |
| `otherresearchproduct` | 250 | ~32M | same schema as publication |
| `project` | 12 | 3,673,360 | funding grants |
| `publication` | 1500 | ~165M | |
| `relation` | 2934 | ~2B | all cross-entity edges; see breakdown below |
| `software` | 18 | ~862K | same schema as publication |

**Note:** The API docs list fields like `projects`, `organizations`, `communities` as embedded cross-references *within* research product records. **Those embedded fields are absent from the dump.** All cross-entity links live exclusively in the `relation/` directory. The entity types themselves (`project/`, `organization/`, etc.) all exist as separate directories.

## DuckDB Loading

DuckDB reads `.json.gz` parts directly:
```python
con.execute("""
    SELECT * FROM read_json('/work/.../openaire_2025_12_01_dump/organization/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")
```

Tested on `organization/`: schema inferred correctly, 448,161 rows loaded without errors.

---

## Schema (live-tested, DuckDB 1.2.2)

### communities_infrastructures (6 columns — tiny, fully flat)

Only 36 rows. Research communities and e-infrastructures registered in OpenAIRE.
See more: https://graph.openaire.eu/docs/data-model/entities/community

| column | type | notes |
|---|---|---|
| `id` | VARCHAR | |
| `acronym` | VARCHAR | |
| `name` | VARCHAR | |
| `type` | VARCHAR | e.g. "Research Infrastructure" |
| `description` | VARCHAR | |
| `zenodoCommunity` | VARCHAR | nullable — URL to Zenodo community |

### dataset / otherresearchproduct / publication / software (37 columns — shared research product schema)

All four research product types share the same schema. `type` field distinguishes them.
See more: https://graph.openaire.eu/docs/10.6.0/data-model/entities/research-product#sub-types. In the OpenAire docs all of these are called Result or ResearchProduct.

| column | type | notes |
|---|---|---|
| `id` | VARCHAR | |
| `type` | VARCHAR | publication / data / software / other |
| `mainTitle` | VARCHAR | |
| `subTitle` | VARCHAR | nullable |
| `publicationDate` | DATE | bad values exist e.g. `0001-12-30` |
| `publisher` | VARCHAR | |
| `embargoEndDate` | DATE | nullable |
| `dateOfCollection` | VARCHAR | |
| `lastUpdateTimeStamp` | BIGINT | |
| `openAccessColor` | VARCHAR | bronze / gold / hybrid / null |
| `isGreen` | BOOLEAN | nullable |
| `isInDiamondJournal` | BOOLEAN | nullable |
| `publiclyFunded` | BOOLEAN | nullable |
| `language` | STRUCT(code VARCHAR, label VARCHAR) | |
| `bestAccessRight` | STRUCT(code, label, scheme) | nullable |
| `authors` | STRUCT(fullName, name, surname, rank BIGINT, pid STRUCT(id STRUCT(scheme,value), provenance STRUCT(provenance,trust)))[] | |
| `countries` | STRUCT(code, label, provenance STRUCT)[] | nullable |
| `subjects` | STRUCT(subject STRUCT(scheme,value), provenance STRUCT)[] | nullable |
| `descriptions` | VARCHAR[] | nullable |
| `originalIds` | VARCHAR[] | |
| `pids` | STRUCT(scheme VARCHAR, value VARCHAR)[] | nullable |
| `sources` | VARCHAR[] | nullable |
| `formats` | VARCHAR[] | nullable |
| `contributors` | VARCHAR[] | nullable |
| `coverages` | JSON | inferred as JSON due to inconsistent structure |
| `container` | STRUCT(name, issnPrinted, issnOnline, issnLinking JSON, ep, iss, sp, vol, edition JSON, conferencePlace JSON, conferenceDate JSON) | journal info; nullable |
| `instances` | STRUCT(license, accessRight STRUCT, type, urls[], publicationDate, refereed, alternateIdentifiers STRUCT[], pids STRUCT[], articleProcessingCharge STRUCT)[] | |
| `indicators` | STRUCT(citationImpact STRUCT(citationCount, influence, popularity, impulse, citationClass, influenceClass, impulseClass, popularityClass), usageCounts STRUCT(downloads, views)) | nullable |
| `documentationUrls` | JSON | nullable; intended for software |
| `codeRepositoryUrl` | JSON | nullable; intended for software |
| `programmingLanguage` | JSON | nullable; intended for software |
| `contactPeople` | JSON | nullable |
| `contactGroups` | JSON | nullable |
| `tools` | JSON | nullable |
| `size` | JSON | nullable; intended for datasets |
| `version` | JSON | nullable |
| `geoLocations` | JSON | nullable; intended for datasets |

The 9 `JSON`-typed fields at the bottom are null in the sampled publication and software records — they appear to be sparsely populated across all types. `union_by_name=true` is required because these nullable fields may be entirely absent from some part files.

`publicationDate` bad values (e.g. `0001-12-30`) — load as VARCHAR or filter during load.

### datasource (23 columns — journals, repositories, data sources)

~216K rows. Represents the publishing venues / repositories that provide research products to OpenAIRE.

| column | type | notes |
|---|---|---|
| `id` | VARCHAR | |
| `originalIds` | VARCHAR[] | |
| `pids` | STRUCT(scheme, value)[] | nullable |
| `type` | STRUCT(scheme VARCHAR, value VARCHAR) | e.g. scheme=pubsrepository::journal, value=Journal |
| `openaireCompatibility` | VARCHAR | e.g. "Not yet registered", "OpenAIRE 3.0 (OA, funding)" |
| `officialName` | VARCHAR | |
| `englishName` | VARCHAR | |
| `websiteUrl` | VARCHAR | nullable |
| `logoUrl` | VARCHAR | nullable |
| `dateOfValidation` | VARCHAR | nullable |
| `description` | VARCHAR | nullable |
| `subjects` | VARCHAR[] | nullable (note: may contain `{NULL}` string artifacts) |
| `languages` | VARCHAR[] | nullable |
| `contentTypes` | VARCHAR[] | nullable |
| `releaseStartDate` | VARCHAR | nullable |
| `releaseEndDate` | VARCHAR | nullable |
| `missionStatementUrl` | VARCHAR | nullable |
| `accessRights` | VARCHAR | nullable |
| `uploadRights` | VARCHAR | nullable |
| `databaseAccessRestriction` | VARCHAR | nullable |
| `dataUploadRestriction` | VARCHAR | nullable |
| `versioning` | BOOLEAN | |
| `citationGuidelineUrl` | VARCHAR | nullable |
| `pidSystems` | VARCHAR | nullable |
| `certificates` | VARCHAR | nullable |
| `policies` | JSON | nullable |
| `journal` | STRUCT(name, issnPrinted, issnOnline, issnLinking, ep, iss, sp, vol, edition, conferencePlace, conferenceDate) | nullable; populated for journal-type datasources |

### organization (7 columns — flat, very easy)

448,161 rows exact.

| column | type |
|---|---|
| `id` | VARCHAR |
| `legalShortName` | VARCHAR |
| `legalName` | VARCHAR |
| `websiteUrl` | VARCHAR |
| `alternativeNames` | VARCHAR[] |
| `country` | STRUCT(code VARCHAR, label VARCHAR) |
| `pids` | STRUCT(scheme VARCHAR, value VARCHAR)[] |

### project (15 columns — funding grants)

3,673,360 rows exact.

| column | type | notes |
|---|---|---|
| `id` | VARCHAR | |
| `code` | VARCHAR | funder's grant ID |
| `acronym` | VARCHAR | nullable |
| `title` | VARCHAR | |
| `websiteUrl` | VARCHAR | nullable |
| `startDate` | DATE | nullable |
| `endDate` | DATE | nullable |
| `callIdentifier` | VARCHAR | nullable |
| `keywords` | VARCHAR | nullable; single string, not an array |
| `openAccessMandateForPublications` | BOOLEAN | |
| `openAccessMandateForDataset` | BOOLEAN | |
| `subjects` | VARCHAR[] | nullable |
| `fundings` | STRUCT(shortName VARCHAR, name VARCHAR, jurisdiction VARCHAR, fundingStream VARCHAR)[] | funder info |
| `summary` | VARCHAR | nullable |
| `granted` | STRUCT(currency VARCHAR, totalCost DOUBLE, fundedAmount DOUBLE) | nullable |
| `h2020Programmes` | STRUCT(code VARCHAR, description VARCHAR)[] | nullable; H2020-specific programme codes |

### relation (7 columns — flat schema, very large scale)

~2B rows across 2934 parts.

| column | type | notes |
|---|---|---|
| `source` | VARCHAR | OpenAIRE entity ID |
| `sourceType` | VARCHAR | **`product`** / organization / project / datasource / community |
| `target` | VARCHAR | OpenAIRE entity ID |
| `targetType` | VARCHAR | same vocabulary |
| `relType` | STRUCT(name VARCHAR, type VARCHAR) | see taxonomy below |
| `provenance` | STRUCT(provenance VARCHAR, trust VARCHAR) | |
| `validated` | BOOLEAN | |

**Important:** The dump uses `product` (not `result`) for research output sourceType/targetType. The API docs say `result` — the dump says `product`.

#### Relation type taxonomy (sampled from actual data)

The relation table covers **all cross-entity relationships**, not just citations:

| relType.type | relType.name examples | src → tgt |
|---|---|---|
| `provision` | isProvidedBy, provides, isHostedBy, hosts | product↔datasource |
| `citation` | Cites, IsCitedBy | product↔product |
| `affiliation` | hasAuthorInstitution, isAuthorInstitutionOf | product↔organization |
| `outcome` | produces, isProducedBy | project↔product |
| `similarity` | IsAmongTopNSimilarDocuments, HasAmongTopNSimilarDocuments | product↔product |
| `part` | IsPartOf, HasPart | product↔product |
| `participation` | isParticipant | organization↔project |
| `relationship` | IsRelatedTo, IsSourceOf, IsDerivedFrom, IsReferencedBy, References | product↔product, product↔community, project↔community, etc. |

**Dominant type by volume is `provision`** (product↔datasource links).

---

## Loading Strategy

### Small entities

Single `read_json` glob. Schema is simple and fully flat.

```python
con.execute("""
    CREATE TABLE organizations AS
    SELECT * FROM read_json('/work/.../openaire_2025_12_01_dump/organization/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")
```

Same pattern for `project/`, `datasource/`, `communities_infrastructures/`.

### Research products (publication, dataset, software, otherresearchproduct)

All share the same schema — can be loaded uniformly. Single glob per entity type. Use `union_by_name=true` for the nullable JSON-typed fields. Consider selecting only needed columns to avoid 9 sparse JSON fields.

```sql
SELECT * FROM read_json('/work/.../openaire_2025_12_01_dump/publication/*.json.gz',
    format='newline_delimited', compression='gzip', union_by_name=true)
```

### Relations (~2B rows, 2934 parts) — the hard problem

Filter to useful relation types only**
```sql
COPY (
    SELECT * FROM read_json('/work/.../openaire_2025_12_01_dump/relation/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
    WHERE relType.type IN ('affiliation', 'outcome', 'citation', 'participation')
) TO 'relations_filtered.parquet' (FORMAT parquet, COMPRESSION zstd)
```
Excludes `provision` and `similarity` edges which dominate by count but are unlikely needed.