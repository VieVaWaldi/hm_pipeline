# works_base_limit - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_limit_works_base.duckdb`
- **Size:** 11.3 MB
- **Tables:** 3

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| relation | 24,858 | 9 |
| relation_topic | 0 | 5 |
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

## **relation_topic** - 0 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| type | VARCHAR | 0 (0%) |  |
| source_id | UBIGINT | 0 (0%) |  |
| topic_id | INTEGER | 0 (0%) |  |
| score | FLOAT | 0 (0%) |  |
| created_at | TIMESTAMP | 0 (0%) |  |

## **work** - 1,818 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 1,818 (100%) | 1924676956609295952 |
| openaireId | VARCHAR | 1,818 (100%) | doi_dedup___::b2bd59a17d03544d2bbb69232e2149cd |
| title | VARCHAR | 1,818 (100%) | Interaction between striatal volume and DAT1 polymorphism predicts working memory development during adolescence |
| publicationDate | DATE | 1,657 (91%) | 2018-04-01 |
| publisher | VARCHAR | 1,551 (85%) | Elsevier BV |
| openAccessColor | VARCHAR | 988 (54%) | gold |
| isGreen | BOOLEAN | 1,802 (99%) | True |
| isInDiamondJournal | BOOLEAN | 1,802 (99%) | False |
| publiclyFunded | BOOLEAN | 1,802 (99%) | True |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 1,818 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 1,775 (98%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 1,662 (91%) | [{"fullName": "Bokde, Arun", "name": "Arun", "pid": {"id": {"scheme": "orcid", "value": "0000-0003-0114-4914"}, "provenance": {"provenance": "ORCID_ENRICHMENT", "trust": "0.9"}}, "rank": 1, "surname":… |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 1,594 (88%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "Neurophysiology and neuropsychology"}}, {"provenance": {"provenance": "Harvested", "trust": "0.9… |
| descriptions | VARCHAR[] | 1,730 (95%) | ["There is considerable inter-individual variability in the rate at which working memory (WM) develops during childhood and adolescence, but the neural and genetic basis for these differences are poor… |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 1,566 (86%) | [{"scheme": "doi", "value": "10.1016/j.dcn.2018.03.006"}, {"scheme": "pmid", "value": "29567584"}, {"scheme": "pmc", "value": "PMC6969124"}, {"scheme": "handle", "value": "2262/91694"}] |
| sources | VARCHAR[] | 1,479 (81%) | ["Crossref", "Dev Cogn Neurosci", "Developmental Cognitive Neuroscience, Vol 30, Iss , Pp 191-199 (2018)", "Developmental Cognitive Neuroscience"] |
| formats | VARCHAR[] | 897 (49%) | ["191-199", "application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 1,818 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": "gold", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "arti… |
| citationCount | DOUBLE | 1,818 (100%) | 11.0 |
| influence | DOUBLE | 1,818 (100%) | 2.4951923e-09 |
| views | BIGINT | 187 (10%) | 40 |
| countries | VARCHAR[] | 1,172 (64%) | ["IE"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 1,346 (74%) | {"edition": null, "ep": "199", "iss": null, "issnLinking": null, "issnOnline": null, "issnPrinted": "1878-9293", "name": "Developmental Cognitive Neuroscience", "sp": "191", "vol": "30"} |
| link_tier | SMALLINT | 1,818 (100%) | 0 |
| is_translated | BOOLEAN | 1,818 (100%) | False |
| is_ch | BOOLEAN | 0 (0%) |  |
| pred | FLOAT | 0 (0%) |  |
| minority_qid | VARCHAR[] | 0 (0%) |  |
| pillars | UTINYINT | 1,818 (100%) | 0 |
| theme | VARCHAR | 0 (0%) |  |

