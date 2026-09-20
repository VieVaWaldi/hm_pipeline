# projects - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_projects.duckdb`
- **Size:** 1709.3 MB
- **Tables:** 5

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 494,099 | 23 |
| project | 3,893,065 | 24 |
| relation | 5,500,329 | 9 |
| relation_topic | 3,884,270 | 5 |
| topic | 4,516 | 13 |

## **organization** - 494,099 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 494,099 (100%) | 10898555399556535076 |
| openaireId | VARCHAR | 494,099 (100%) | pending_org_::fbcc4425988fd7f67b81250c18e9c7cc |
| legalName | VARCHAR | 494,099 (100%) | TYÖ- JA ELINKEINOMINISTERIÖ |
| legalShortName | VARCHAR | 475,410 (96%) | TEM |
| websiteUrl | VARCHAR | 188,616 (38%) | http://www.tem.fi |
| alternativeNames | VARCHAR[] | 181,580 (37%) | ["TEM"] |
| countryCode | VARCHAR | 336,707 (68%) | FI |
| rorId | VARCHAR | 126,401 (26%) | https://ror.org/02b2fsy61 |
| wikiId | VARCHAR | 55,825 (11%) | Q4902926 |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 204,609 (41%) | [{"scheme": "PIC", "value": "999826143"}] |
| rorStatus | VARCHAR | 126,400 (26%) | active |
| rorEstablished | INTEGER | 103,541 (21%) | 1965 |
| rorTypes | VARCHAR[] | 126,400 (26%) | ["education"] |
| rorLocations | JSON | 126,400 (26%) | [{"geonames_id":2643097,"geonames_details":{"continent_code":"EU","continent_name":"Europe","country_code":"GB","country_name":"United Kingdom","country_subdivision_code":"ENG","country_subdivision_na… |
| geolocation | DOUBLE[] | 172,739 (35%) | [60.163419, 24.9446336] |
| geolocation_source | VARCHAR | 172,739 (35%) | cordis |
| rorRelationships | JSON | 126,400 (26%) | [] |
| address_street | VARCHAR | 69,450 (14%) | ALEKSANTERINKATU 4 |
| address_postalcode | VARCHAR | 65,823 (13%) | 00023 |
| address_city | VARCHAR | 69,754 (14%) | Helsinki |
| address_country | VARCHAR | 70,494 (14%) | FI |
| nuts3 | VARCHAR | 43,232 (9%) | FI1B1 |
| region | VARCHAR | 336,831 (68%) | Northern Europe |

## **project** - 3,893,065 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 3,893,065 (100%) | 3914582936788389563 |
| openaireId | VARCHAR | 3,893,065 (100%) | nwo_________::e3453d2b535730c79bf71859d1d2688e |
| grantId | VARCHAR | 3,893,065 (100%) | KIEM.K20.01.063 |
| title | VARCHAR | 3,893,065 (100%) | Interprofessional Community of Practice Healthy return to work after CVA |
| acronym | VARCHAR | 120,629 (3%) | BGPR |
| websiteUrl | VARCHAR | 86,933 (2%) | http://purl.org/au-research/grants/arc/LP0455615 |
| startDate | DATE | 3,669,190 (94%) | 2020-11-15 |
| endDate | DATE | 3,516,080 (90%) | 2021-11-15 |
| callIdentifier | VARCHAR | 129,893 (3%) | Apuraha |
| keywords | VARCHAR | 507,115 (13%) | Teknologi |
| openAccessMandateForPublications | BOOLEAN | 3,893,065 (100%) | False |
| openAccessMandateForDataset | BOOLEAN | 3,893,065 (100%) | False |
| subjects | VARCHAR[] | 294,676 (8%) | ["Materials Research"] |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 3,891,009 (100%) | [{"fundingStream": null, "jurisdiction": "NL", "name": "Netherlands Organisation for Scientific Research (NWO)", "shortName": "NWO"}, {"fundingStream": {"description": "KIEM KIEM 2020 KIEM 2020 KIEM 2… |
| frameworkProgrammes | VARCHAR[] | 3,598,016 (92%) | ["KIEM KIEM 2020 KIEM 2020 KIEM 2020 - Batch 1"] |
| summary | VARCHAR | 533,148 (14%) | In the chronic phase after rehabilitation, a large proportion of people who have suffered a stroke (CVA) return to their homes. Many people after a stroke still experience long-term limitations in the… |
| doi | VARCHAR | 99,887 (3%) | 10.61686/pjqfz35282 |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 3,893,065 (100%) | {"currency": null, "fundedAmount": 0.0, "totalCost": 0.0} |
| is_translated | BOOLEAN | 3,893,065 (100%) | True |
| is_ch | BOOLEAN | 3,893,065 (100%) | False |
| pred | FLOAT | 3,893,065 (100%) | 0.0064882696606218815 |
| minority_qid | VARCHAR[] | 9,293 (0%) | ["Q125564"] |
| pillars | UTINYINT | 3,893,065 (100%) | 0 |
| theme | VARCHAR | 61,600 (2%) | Economy |

## **relation** - 5,500,329 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 5,500,329 (100%) | 9607202781264065739 |
| sourceType | VARCHAR | 5,500,329 (100%) | project |
| target | UBIGINT | 5,500,329 (100%) | 15488786585350802359 |
| targetType | VARCHAR | 5,500,329 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 5,500,329 (100%) | {"name": "hasParticipant", "type": "participation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 5,500,329 (100%) | {"provenance": "Harvested", "trust": "0.900"} |
| validated | BOOLEAN | 5,500,329 (100%) | False |
| cordis_ec_contribution | DOUBLE | 384,988 (7%) | 113944.5 |
| cordis_type | VARCHAR | 392,682 (7%) | participant |

## **relation_topic** - 3,884,270 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| type | VARCHAR | 3,884,270 (100%) | project |
| source_id | UBIGINT | 3,884,270 (100%) | 2907467956262843504 |
| topic_id | INTEGER | 3,884,270 (100%) | 11463 |
| score | FLOAT | 3,884,270 (100%) | 0.3120538890361786 |
| created_at | TIMESTAMP | 3,884,270 (100%) | 2026-09-19 21:39:04 |

## **topic** - 4,516 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 4,516 (100%) | 10020 |
| subfield_id | VARCHAR | 4,516 (100%) | 1702 |
| field_id | VARCHAR | 4,516 (100%) | 17 |
| domain_id | VARCHAR | 4,516 (100%) | 3 |
| topic_name | VARCHAR | 4,516 (100%) | Quantum Information and Cryptography |
| subfield_name | VARCHAR | 4,516 (100%) | Artificial Intelligence |
| field_name | VARCHAR | 4,516 (100%) | Computer Science |
| domain_name | VARCHAR | 4,516 (100%) | Physical Sciences |
| keywords | VARCHAR | 4,516 (100%) | ["Quantum","Entanglement","Cryptography","Computation","Metrology","Superconducting Circuits","Photon","Measurement","Security","Communication"] |
| summary | VARCHAR | 4,516 (100%) | This cluster of papers covers a wide range of topics in the field of quantum information, including quantum entanglement, cryptography, computation, metrology, superconducting circuits, photon manipul… |
| wikipedia_url | VARCHAR | 4,514 (100%) | https://en.wikipedia.org/wiki/Quantum_information_science |
| created_at | TIMESTAMP | 4,516 (100%) | 2026-09-19 21:39:04 |
| updated_at | TIMESTAMP | 0 (0%) |  |

