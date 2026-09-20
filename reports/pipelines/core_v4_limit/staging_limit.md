# staging_limit - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_limit_staging.duckdb`
- **Size:** 18.0 MB
- **Tables:** 4

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 4,247 | 22 |
| project | 50 | 18 |
| relation | 25,145 | 9 |
| work | 1,818 | 24 |

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

## **project** - 50 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 50 (100%) | 8508783237659591 |
| openaireId | VARCHAR | 50 (100%) | corda_______::8fedcfb152ae68f3be095979acd990bb |
| grantId | VARCHAR | 50 (100%) | 329268 |
| title | VARCHAR | 50 (100%) | A Kantian Approach to Current Tensions between Modern Law and Religious Commitments |
| acronym | VARCHAR | 48 (96%) | LAW AND RELIGION |
| websiteUrl | VARCHAR | 2 (4%) | http://techeese.eu/ |
| startDate | DATE | 50 (100%) | 2013-09-23 |
| endDate | DATE | 50 (100%) | 2015-09-22 |
| callIdentifier | VARCHAR | 48 (96%) | FP7-PEOPLE-2012-IEF |
| keywords | VARCHAR | 2 (4%) | Seed Award in Science |
| openAccessMandateForPublications | BOOLEAN | 50 (100%) | False |
| openAccessMandateForDataset | BOOLEAN | 50 (100%) | False |
| subjects | VARCHAR[] | 29 (58%) | ["Demonstration of system integration with smart transmission grid and storage technologies with increasing share of renewables", "Demonstration of system integration with smart transmission grid and … |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 50 (100%) | [{"fundingStream": {"description": "SEVENTH FRAMEWORK PROGRAMME - SP3-People - Marie-Curie Actions", "id": "EC::FP7::SP3::PEOPLE"}, "jurisdiction": "EU", "name": "European Commission", "shortName": "E… |
| frameworkProgrammes | VARCHAR[] | 50 (100%) | ["FP7"] |
| summary | VARCHAR | 31 (62%) | CROSSBOW will propose the shared use of resources to foster cross-border management of variable renewable energies and storage units, enabling a higher penetration of clean energies whilst reducing ne… |
| doi | VARCHAR | 29 (58%) | 10.3030/773430 |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 50 (100%) | {"currency": null, "fundedAmount": 0.0, "totalCost": 0.0} |

## **relation** - 25,145 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 25,145 (100%) | 6471310952829871 |
| sourceType | VARCHAR | 25,145 (100%) | project |
| target | UBIGINT | 25,145 (100%) | 16246178485959616483 |
| targetType | VARCHAR | 25,145 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 25,145 (100%) | {"name": "hasParticipant", "type": "participation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 25,145 (100%) | {"provenance": "Harvested", "trust": "0.900"} |
| validated | BOOLEAN | 25,145 (100%) | False |
| cordis_ec_contribution | DOUBLE | 267 (1%) | 678861.875 |
| cordis_type | VARCHAR | 274 (1%) | participant |

## **work** - 1,818 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 1,818 (100%) | 13450476068920131355 |
| openaireId | VARCHAR | 1,818 (100%) | doi_dedup___::12bce855df547ab84961feaa58884ab8 |
| title | VARCHAR | 1,818 (100%) | Reactogenicity to major tuberculosis antigens absent in BCG is linked to improved protection against Mycobacterium tuberculosis |
| publicationDate | DATE | 1,657 (91%) | 2017-07-14 |
| publisher | VARCHAR | 1,551 (85%) | Springer Science and Business Media LLC |
| openAccessColor | VARCHAR | 988 (54%) | gold |
| isGreen | BOOLEAN | 1,802 (99%) | True |
| isInDiamondJournal | BOOLEAN | 1,802 (99%) | False |
| publiclyFunded | BOOLEAN | 1,802 (99%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 1,818 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 1,775 (98%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 1,662 (91%) | [{"fullName": "Nacho Aguilo", "name": "Nacho", "pid": {"id": {"scheme": "orcid", "value": "0000-0001-7897-9173"}, "provenance": {"provenance": "ORCID_ENRICHMENT", "trust": "0.9"}}, "rank": 1, "surname… |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 1,594 (88%) | [{"provenance": {"provenance": "Inferred by OpenAIRE", "trust": null}, "subject": {"scheme": "FOS", "value": "0301 basic medicine"}}, {"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subje… |
| descriptions | VARCHAR[] | 1,730 (95%) | ["<jats:title>Abstract</jats:title><jats:p>MTBVAC is a live-attenuated <jats:italic>Mycobacterium tuberculosis</jats:italic> vaccine, currently under clinical development, that contains the major anti… |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 1,566 (86%) | [{"scheme": "doi", "value": "10.1038/ncomms16085"}, {"scheme": "pmid", "value": "28706226"}, {"scheme": "pmc", "value": "PMC5519979"}] |
| sources | VARCHAR[] | 1,479 (81%) | ["Crossref", "Nat Commun", "reponame:Zagu\u00e1n. Repositorio Digital de la Universidad de Zaragoza", "instname:Universidad de Zaragoza", "instname:", "Nature Communications, Vol 8, Iss 1, Pp 1-11 (20… |
| formats | VARCHAR[] | 897 (49%) | ["application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 1,818 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": "gold", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "arti… |
| citationCount | DOUBLE | 1,818 (100%) | 117.0 |
| influence | DOUBLE | 1,818 (100%) | 5.5565885e-09 |
| views | BIGINT | 187 (10%) | 7 |
| countries | VARCHAR[] | 1,172 (64%) | ["ES"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 1,346 (74%) | {"edition": null, "ep": null, "iss": null, "issnLinking": null, "issnOnline": "2041-1723", "issnPrinted": null, "name": "Nature Communications", "sp": null, "vol": "8"} |
| link_tier | SMALLINT | 1,818 (100%) | 0 |

