# projects_limit - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_limit_projects.duckdb`
- **Size:** 8.0 MB
- **Tables:** 5

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 4,247 | 23 |
| project | 50 | 24 |
| relation | 287 | 9 |
| relation_topic | 50 | 5 |
| topic | 4,516 | 13 |

## **organization** - 4,247 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 4,247 (100%) | 14258442750171362618 |
| openaireId | VARCHAR | 4,247 (100%) | pending_org_::1368fb3eb618ea55221c47a66b307f00 |
| legalName | VARCHAR | 4,247 (100%) | Université de Bordeaux |
| legalShortName | VARCHAR | 4,244 (100%) | Université de Bordeaux |
| websiteUrl | VARCHAR | 2,785 (66%) | https://www.u-bordeaux.com |
| alternativeNames | VARCHAR[] | 2,599 (61%) | ["College of Natural and Computational Sciences"] |
| countryCode | VARCHAR | 3,591 (85%) | FR |
| rorId | VARCHAR | 2,392 (56%) | https://ror.org/003109y17 |
| wikiId | VARCHAR | 1,854 (44%) | Q39918611 |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 3,023 (71%) | [{"scheme": "mag_id", "value": "98702875"}, {"scheme": "GRID", "value": "grid.412043.0"}] |
| rorStatus | VARCHAR | 2,392 (56%) | active |
| rorEstablished | INTEGER | 2,203 (52%) | 1606 |
| rorTypes | VARCHAR[] | 2,392 (56%) | ["education", "funder"] |
| rorLocations | JSON | 2,392 (56%) | [{"geonames_id":2525473,"geonames_details":{"continent_code":"EU","continent_name":"Europe","country_code":"IT","country_name":"Italy","country_subdivision_code":"88","country_subdivision_name":"Sardi… |
| geolocation | DOUBLE[] | 2,707 (64%) | [39.23054, 9.11917] |
| geolocation_source | VARCHAR | 2,707 (64%) | ror |
| rorRelationships | JSON | 2,392 (56%) | [{"type":"related","label":"Istituto Nazionale di Fisica Nucleare, Sezione di Cagliari","id":"https://ror.org/03paz5966"}] |
| address_street | VARCHAR | 1,041 (25%) | Al. Prymasa Tysiaclecia 83 |
| address_postalcode | VARCHAR | 872 (21%) | 01-242 |
| address_city | VARCHAR | 1,059 (25%) | Warsaw |
| address_country | VARCHAR | 1,086 (26%) | PL |
| nuts3 | VARCHAR | 369 (9%) | SE110 |
| region | VARCHAR | 3,593 (85%) | Western Europe |

## **project** - 50 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 50 (100%) | 5024052685280941 |
| openaireId | VARCHAR | 50 (100%) | corda__h2020::b31c44b101f13488585c9da12536431b |
| grantId | VARCHAR | 50 (100%) | 773430 |
| title | VARCHAR | 50 (100%) | CROSS BOrder management of variable renewable energies and storage units enabling a transnational Wholesale market |
| acronym | VARCHAR | 48 (96%) | CROSSBOW |
| websiteUrl | VARCHAR | 2 (4%) | www.insidefood.eu |
| startDate | DATE | 50 (100%) | 2017-11-01 |
| endDate | DATE | 50 (100%) | 2022-04-30 |
| callIdentifier | VARCHAR | 48 (96%) | H2020-LCE-2017-SGS |
| keywords | VARCHAR | 2 (4%) | Seed Award in Science |
| openAccessMandateForPublications | BOOLEAN | 50 (100%) | True |
| openAccessMandateForDataset | BOOLEAN | 50 (100%) | True |
| subjects | VARCHAR[] | 29 (58%) | ["Demonstration of system integration with smart transmission grid and storage technologies with increasing share of renewables", "Demonstration of system integration with smart transmission grid and … |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 50 (100%) | [{"fundingStream": {"description": "Horizon 2020 Framework Programme - Innovation action", "id": "EC::H2020::IA"}, "jurisdiction": "EU", "name": "European Commission", "shortName": "EC"}] |
| frameworkProgrammes | VARCHAR[] | 50 (100%) | ["H2020"] |
| summary | VARCHAR | 31 (62%) | CROSSBOW will propose the shared use of resources to foster cross-border management of variable renewable energies and storage units, enabling a higher penetration of clean energies whilst reducing ne… |
| doi | VARCHAR | 29 (58%) | 10.3030/773430 |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 50 (100%) | {"currency": "EUR", "fundedAmount": 17287700.0, "totalCost": 21685200.0} |
| is_translated | BOOLEAN | 50 (100%) | False |
| is_ch | BOOLEAN | 50 (100%) | False |
| pred | FLOAT | 50 (100%) | 0.006192990578711033 |
| minority_qid | VARCHAR[] | 0 (0%) |  |
| pillars | UTINYINT | 50 (100%) | 2 |
| theme | VARCHAR | 4 (8%) | Economy |

## **relation** - 287 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 287 (100%) | 6471310952829871 |
| sourceType | VARCHAR | 287 (100%) | project |
| target | UBIGINT | 287 (100%) | 16246178485959616483 |
| targetType | VARCHAR | 287 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 287 (100%) | {"name": "hasParticipant", "type": "participation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 287 (100%) | {"provenance": "Harvested", "trust": "0.900"} |
| validated | BOOLEAN | 287 (100%) | False |
| cordis_ec_contribution | DOUBLE | 267 (93%) | 678861.875 |
| cordis_type | VARCHAR | 274 (95%) | participant |

## **relation_topic** - 50 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| type | VARCHAR | 50 (100%) | project |
| source_id | UBIGINT | 50 (100%) | 1590952030806931 |
| topic_id | INTEGER | 50 (100%) | 10808 |
| score | FLOAT | 50 (100%) | 0.28554287552833557 |
| created_at | TIMESTAMP | 50 (100%) | 2026-09-19 20:36:28 |

## **topic** - 4,516 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 4,516 (100%) | 10011 |
| subfield_id | VARCHAR | 4,516 (100%) | 1208 |
| field_id | VARCHAR | 4,516 (100%) | 12 |
| domain_id | VARCHAR | 4,516 (100%) | 2 |
| topic_name | VARCHAR | 4,516 (100%) | Literature: history, themes, analysis |
| subfield_name | VARCHAR | 4,516 (100%) | Literature and Literary Theory |
| field_name | VARCHAR | 4,516 (100%) | Arts and Humanities |
| domain_name | VARCHAR | 4,516 (100%) | Social Sciences |
| keywords | VARCHAR | 4,516 (100%) | ["Postcolonialism","Race","Gender","Literature","History","Identity","Power","Colonialism","Society","Public Sphere"] |
| summary | VARCHAR | 4,516 (100%) | This cluster of papers explores the intersection of postcolonialism, race, gender, literature, and history within the context of power dynamics, identity formation, and the public sphere. It delves in… |
| wikipedia_url | VARCHAR | 4,514 (100%) | https://en.wikipedia.org/wiki/Cultural_studies |
| created_at | TIMESTAMP | 4,516 (100%) | 2026-09-19 20:36:28 |
| updated_at | TIMESTAMP | 0 (0%) |  |

