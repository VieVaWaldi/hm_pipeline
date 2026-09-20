# works_limit - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_limit_works.duckdb`
- **Size:** 11.5 MB
- **Tables:** 3

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| relation | 24,858 | 9 |
| relation_topic | 1,818 | 5 |
| work | 1,818 | 30 |

## **relation** - 24,858 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 24,858 (100%) | 5024052685280941 |
| sourceType | VARCHAR | 24,858 (100%) | project |
| target | UBIGINT | 24,858 (100%) | 2612856331438482797 |
| targetType | VARCHAR | 24,858 (100%) | product |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 24,858 (100%) | {"name": "produces", "type": "outcome"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 24,858 (100%) | {"provenance": "Harvested", "trust": "0.9"} |
| validated | BOOLEAN | 24,858 (100%) | True |
| cordis_ec_contribution | DOUBLE | 0 (0%) |  |
| cordis_type | VARCHAR | 0 (0%) |  |

## **relation_topic** - 1,818 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| type | VARCHAR | 1,818 (100%) | work |
| source_id | UBIGINT | 1,818 (100%) | 6293587086115586768 |
| topic_id | INTEGER | 1,818 (100%) | 13607 |
| score | FLOAT | 1,818 (100%) | 0.23641769587993622 |
| created_at | TIMESTAMP | 1,818 (100%) | 2026-09-19 20:40:53 |

## **work** - 1,818 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 1,818 (100%) | 13214985131959579648 |
| openaireId | VARCHAR | 1,818 (100%) | od______3063::a5246a35c1333642e08e93516fdf8ea5 |
| title | VARCHAR | 1,818 (100%) | Sojourn time estimation in partially observed piecewise deterministic Markov processes - application to myeloma modeling |
| publicationDate | DATE | 1,657 (91%) | 2024-01-01 |
| publisher | VARCHAR | 1,551 (85%) | Zenodo |
| openAccessColor | VARCHAR | 988 (54%) | gold |
| isGreen | BOOLEAN | 1,802 (99%) | True |
| isInDiamondJournal | BOOLEAN | 1,802 (99%) | False |
| publiclyFunded | BOOLEAN | 1,802 (99%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 1,818 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 1,775 (98%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 1,662 (91%) | [{"fullName": "Cleynen, Alice", "name": "Alice", "pid": {"id": {"scheme": "orcid_pending", "value": "0000-0001-8083-0204"}, "provenance": {"provenance": "Harvested", "trust": "0.9"}}, "rank": 1, "surn… |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 1,594 (88%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "Survie Piecewise Deterministic Markov Processes"}}, {"provenance": {"provenance": "Harvested", "… |
| descriptions | VARCHAR[] | 1,730 (95%) | ["We consider a problem of estimating relapse time in Particle Deterministic Markov Processes (PDMP) whose Euclidean component is a surrogate biomarker for the status of myeloma patients. One of the m… |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 1,566 (86%) | [{"scheme": "handle", "value": "11441/142334"}] |
| sources | VARCHAR[] | 1,479 (81%) | ["reponame:idUS. Dep\u00f3sito de Investigaci\u00f3n de la Universidad de Sevilla", "instname:Universidad de Sevilla (US)"] |
| formats | VARCHAR[] | 897 (49%) | ["application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 1,818 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": null, "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "articl… |
| citationCount | DOUBLE | 1,818 (100%) | 0.0 |
| influence | DOUBLE | 1,818 (100%) | 2.3384854e-09 |
| views | BIGINT | 187 (10%) | 5 |
| countries | VARCHAR[] | 1,172 (64%) | ["FR"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 1,346 (74%) | {"edition": null, "ep": "111", "iss": null, "issnLinking": null, "issnOnline": null, "issnPrinted": "0149-7634", "name": "Neuroscience &amp; Biobehavioral Reviews", "sp": "102", "vol": "91"} |
| link_tier | SMALLINT | 1,818 (100%) | 0 |
| is_translated | BOOLEAN | 1,818 (100%) | True |
| is_ch | BOOLEAN | 1,818 (100%) | False |
| pred | FLOAT | 1,818 (100%) | 0.004755199886858463 |
| minority_qid | VARCHAR[] | 2 (0%) | ["Q84072"] |
| pillars | UTINYINT | 1,818 (100%) | 0 |
| theme | VARCHAR | 31 (2%) | Economy |

