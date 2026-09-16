# OpenAire Staging

This document explains what columns and rows to drop from the 4 chosen entities from OpenAire and how to clean the remaining ones. This process will transform `openaire_raw.duckdb` to `openaire_staging.duckdb`.

## ID's

Because BIGINT is faster than VARCHAR for this kinda scale. 200 million hash(work.id) produced 0 collisions

* Transform column id in all 4 tables from id to openaireId
* The new PK id for each table will be be hash(id), so the hash of the old id. It will replace the old column id
* This also means we need to hash the ids in relation.source and relation.target in place

## Transformation

### organization

| column | type |
|---|---|
| `id` | VARCHAR |
| `legalShortName` | VARCHAR |
| `legalName` | VARCHAR |
| `websiteUrl` | VARCHAR |
| `alternativeNames` | VARCHAR[] |
| `country` | STRUCT(code VARCHAR, label VARCHAR) |
| `pids` | STRUCT(scheme VARCHAR, value VARCHAR)[] |

Transformation:
* country
    * Drop country
    * Transform country.code into column organization.countryCode
* pids
    * Drop pids
    * Where pids.scheme == ROR, use pids.value and transform pids.value into column organization.rorId
    * Where pids.scheme == Wikidata, use pids.value and transform pids.value into column organization.wikiId
    * All organizations that do not have pids.scheme == ROR OR pids.scheme == Wikidata should null these 2 columns

### project

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
| `keywords` | VARCHAR[] | nullable |
| `openAccessMandateForPublications` | BOOLEAN | |
| `openAccessMandateForDataset` | BOOLEAN | |
| `subjects` | VARCHAR[] | nullable |
| `fundings` | STRUCT(shortName VARCHAR, name VARCHAR, jurisdiction VARCHAR, fundingStream VARCHAR)[] | funder info |
| `summary` | VARCHAR | nullable |
| `granted` | STRUCT(currency VARCHAR, totalCost DOUBLE, fundedAmount DOUBLE) | nullable |
| `h2020Programmes` | STRUCT(code VARCHAR, description VARCHAR)[] | nullable; H2020-specific programme codes |

Transformation:
* Drop all rows WHERE title == 'unidentified' OR WHERE title is NULL
* Rename code to grantId
* Create column frameworkProgrammes 
    * Pattern: fundings[].fundingStream.id = "EC::H2020::RIA" → extract 2nd segment (H2020, FP7, HE, FP6)
    * Implementation: Use Python UDF during staging to split by :: and take index 1, or a DuckDB approach if thats easy
* Drop column h2020Programmes


### work

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

Transformation:
* Drop columns: type, subTitle, embargoEndDate, dateOfCollection, lastUpdateTimeStamp, countries, originalIds, contributors, coverages, documentationUrls, codeRepositoryUrl, programmingLanguage, contactPeople, contactGroups, tools, size, version, geoLocations
* Rename column mainTitle to title
* Drop indicators, but extract citationCount, influence and views as their own columns for work


### relation

| column | type | notes |
|---|---|---|
| `source` | VARCHAR | OpenAIRE entity ID |
| `sourceType` | VARCHAR | **`product`** / organization / project / datasource / community |
| `target` | VARCHAR | OpenAIRE entity ID |
| `targetType` | VARCHAR | same vocabulary |
| `relType` | STRUCT(name VARCHAR, type VARCHAR) | see taxonomy below |
| `provenance` | STRUCT(provenance VARCHAR, trust VARCHAR) | |
| `validated` | BOOLEAN | |

* Inplace hash() of source and target

---

## Sanitization Steps Detail

All sanitizers live in `src/lib/sanitizers/`. Scalar UDFs are registered via `con.create_function(...)` and applied with `UPDATE ... SET col = udf(col)`. Array columns use DuckDB's `list_transform(col, x -> udf(x))` and then filter out NULLs with `list_filter(..., x -> x IS NOT NULL)`.

* Some OpenAire columns like organization.legalName seem to have BOM related issues which also need to be sanitized eg value.strip('\ufeff\u200b\u200c\u200d\ufffe').
* 

### organization

| column | sanitizer | notes |
|---|---|---|
| `legalName` | `parse_names_and_identifiers` | |
| `legalShortName` | `parse_names_and_identifiers` | |
| `websiteUrl` | `parse_web_resources` | |
| `alternativeNames` | `parse_names_and_identifiers` per element | `list_filter(list_transform(...), x -> x IS NOT NULL)` |

### project

| column | sanitizer | notes |
|---|---|---|
| `title` | `parse_titles_and_labels` | |
| `acronym` | `parse_string` | acronyms should not be heavily normalized; minimal whitespace cleanup only |
| `callIdentifier` | `parse_string` | |
| `keywords` | `parse_string` | raw type is VARCHAR (single string), not VARCHAR[] — apply directly |
| `subjects` | `parse_string` per element | VARCHAR[] → elementwise, filter NULLs |
| `summary` | `parse_content` | preserves paragraph structure |
| `startDate` | range filter | already DATE type; NULL if `year < 1000 OR year > 3000` |
| `endDate` | range filter | NULL if `year < 1000 OR year > 3000`; 3,837 rows have year=9999 (placeholder) |
| `websiteUrl` | `parse_web_resources` | |

### work

| column | sanitizer | notes |
|---|---|---|
| `mainTitle` | `parse_titles_and_labels` | |
| `publisher` | `parse_names_and_identifiers` | |
| `descriptions` | `parse_content` per element | VARCHAR[] → elementwise, filter NULLs |
| `publicationDate` | range filter | NULL if `year < 1000 OR year > 3000`; 14,178 below 1000, 7,198 above 3000 (incl. 9999 placeholder) |