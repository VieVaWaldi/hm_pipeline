# works_base - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_works_base.duckdb`
- **Size:** 124764.0 MB
- **Tables:** 3

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| relation | 148,249,736 | 9 |
| relation_topic | 0 | 5 |
| work | 50,000,000 | 30 |

## **relation** - 148,249,736 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 148,249,736 (100%) | 2867888019284631038 |
| sourceType | VARCHAR | 148,249,736 (100%) | project |
| target | UBIGINT | 148,249,736 (100%) | 3897101154594171350 |
| targetType | VARCHAR | 148,249,736 (100%) | product |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 148,249,736 (100%) | {"name": "produces", "type": "outcome"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 148,249,736 (100%) | {"provenance": "Inferred by OpenAIRE", "trust": "0.72"} |
| validated | BOOLEAN | 148,249,736 (100%) | False |
| cordis_ec_contribution | DOUBLE | 0 (0%) |  |
| cordis_type | VARCHAR | 0 (0%) |  |

## **relation_topic** - 0 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| type | VARCHAR | 0 (0%) |  |
| source_id | UBIGINT | 0 (0%) |  |
| topic_id | INTEGER | 0 (0%) |  |
| score | FLOAT | 0 (0%) |  |
| created_at | TIMESTAMP | 0 (0%) |  |

## **work** - 50,000,000 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 50,000,000 (100%) | 16309358753131835344 |
| openaireId | VARCHAR | 50,000,000 (100%) | doi_dedup___::4cd47eb23154d569301169819a5c6da8 |
| title | VARCHAR | 49,994,566 (100%) | Effective strategies for working with paraeducators |
| publicationDate | DATE | 49,742,547 (99%) | 2018-11-18 |
| publisher | VARCHAR | 46,001,342 (92%) | Informa UK Limited |
| openAccessColor | VARCHAR | 20,889,172 (42%) | bronze |
| isGreen | BOOLEAN | 48,831,612 (98%) | False |
| isInDiamondJournal | BOOLEAN | 48,831,612 (98%) | False |
| publiclyFunded | BOOLEAN | 48,831,612 (98%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 50,000,000 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 42,022,254 (84%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 48,031,156 (96%) | [{"fullName": "Sarah N. Douglas", "name": "Sarah N.", "pid": null, "rank": 1, "surname": "Douglas"}] |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 35,190,151 (70%) | [{"provenance": {"provenance": "Inferred by OpenAIRE", "trust": null}, "subject": {"scheme": "SDG", "value": "4. Education"}}, {"provenance": {"provenance": "Inferred by OpenAIRE", "trust": null}, "su… |
| descriptions | VARCHAR[] | 37,144,047 (74%) | ["Paraeducators, also referred to as educational assistants, paraprofessionals, and instructional aides, have become an important part of the education of children with disabilities in many countries.… |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 45,929,605 (92%) | [{"scheme": "doi", "value": "10.1080/20473869.2018.1509494"}] |
| sources | VARCHAR[] | 42,372,219 (85%) | ["Crossref"] |
| formats | VARCHAR[] | 10,709,610 (21%) | ["application/xml"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 50,000,000 (100%) | [{"accessRight": null, "alternateIdentifiers": null, "articleProcessingCharge": null, "license": null, "pids": [{"scheme": "doi", "value": "10.1080/20473869.2018.1509494"}], "publicationDate": "2018-1… |
| citationCount | DOUBLE | 50,000,000 (100%) | 0.0 |
| influence | DOUBLE | 50,000,000 (100%) | 2.3384854e-09 |
| views | BIGINT | 1,147,100 (2%) | 17 |
| countries | VARCHAR[] | 12,534,806 (25%) | ["HR"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 37,433,342 (75%) | {"edition": null, "ep": "314", "iss": null, "issnLinking": null, "issnOnline": "2047-3877", "issnPrinted": "2047-3869", "name": "International Journal of Developmental Disabilities", "sp": "313", "vol… |
| link_tier | SMALLINT | 50,000,000 (100%) | 1 |
| is_translated | BOOLEAN | 50,000,000 (100%) | False |
| is_ch | BOOLEAN | 0 (0%) |  |
| pred | FLOAT | 0 (0%) |  |
| minority_qid | VARCHAR[] | 0 (0%) |  |
| pillars | UTINYINT | 50,000,000 (100%) | 0 |
| theme | VARCHAR | 0 (0%) |  |

