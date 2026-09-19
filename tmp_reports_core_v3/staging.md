# staging - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v3_staging.duckdb`
- **Size:** 2.5 MB
- **Tables:** 4

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 500 | 16 |
| project | 500 | 17 |
| relation | 500 | 9 |
| work | 500 | 22 |

## **organization** - 500 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 500 (100%) | 11759239505163002302 |
| openaireId | VARCHAR | 500 (100%) | mk0_________::3a0574383de69b97813edc1c5d1be45f |
| legalName | VARCHAR | 500 (100%) | Masarykova univerzita / Rektorát |
| legalShortName | VARCHAR | 478 (96%) | BRI |
| websiteUrl | VARCHAR | 196 (39%) | http://www.bonfiglioli.com |
| alternativeNames | VARCHAR[] | 193 (39%) | ["BRI"] |
| countryCode | VARCHAR | 345 (69%) | IT |
| rorId | VARCHAR | 126 (25%) | https://ror.org/03vcm6439 |
| wikiId | VARCHAR | 65 (13%) | Q3214408 |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 216 (43%) | [{"scheme": "PIC", "value": "952996095"}] |
| rorStatus | VARCHAR | 126 (25%) | active |
| rorEstablished | INTEGER | 109 (22%) | 1968 |
| rorTypes | VARCHAR[] | 126 (25%) | ["facility"] |
| rorLocations | JSON | 126 (25%) | [{"geonames_id":2972315,"geonames_details":{"continent_code":"EU","continent_name":"Europe","country_code":"FR","country_name":"France","country_subdivision_code":"OCC","country_subdivision_name":"Occ… |
| geolocation | DOUBLE[] | 126 (25%) | [43.60426, 1.44367] |
| rorRelationships | JSON | 126 (25%) | [{"type":"parent","label":"Institut National des Sciences Appliquées de Toulouse","id":"https://ror.org/01h8pf755"},{"type":"parent","label":"Institut des Sciences de l'Information et de leurs Interac… |

## **project** - 500 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 500 (100%) | 5074298163293383572 |
| openaireId | VARCHAR | 500 (100%) | 100010414___::2383ea6f558581c1a8dcd370ea931aae |
| grantId | VARCHAR | 500 (100%) | SS-2018-165 |
| title | VARCHAR | 500 (100%) | An investigation into the extent of dynamic hyperinflation in COPD patients attending a community based pulmonary rehabilitation programme. |
| acronym | VARCHAR | 0 (0%) |  |
| websiteUrl | VARCHAR | 0 (0%) |  |
| startDate | DATE | 500 (100%) | 2018-01-01 |
| endDate | DATE | 472 (94%) | 2016-08-31 |
| callIdentifier | VARCHAR | 472 (94%) | Postdoctoral Researcher TT |
| keywords | VARCHAR | 82 (16%) | Educational sciences |
| openAccessMandateForPublications | BOOLEAN | 500 (100%) | False |
| openAccessMandateForDataset | BOOLEAN | 500 (100%) | False |
| subjects | VARCHAR[] | 0 (0%) |  |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 500 (100%) | [{"fundingStream": {"description": "Summer Student Scholarships - Capacity Building and Leadership Enhancement", "id": "100010414::Summer Student Scholarships::Capacity Building and Leadership Enhance… |
| frameworkProgrammes | VARCHAR[] | 28 (6%) | ["Summer Student Scholarships"] |
| summary | VARCHAR | 61 (12%) | Information provides a solid and constructive foundation for dialogue among people, for the individuals choice-making and broadly for decision-making in society. A key challenge with regard to the rel… |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 500 (100%) | {"currency": "EUR", "fundedAmount": 2400.0, "totalCost": 0.0} |

## **relation** - 500 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 500 (100%) | 9314380870723311381 |
| sourceType | VARCHAR | 500 (100%) | product |
| target | UBIGINT | 500 (100%) | 3914739875998513666 |
| targetType | VARCHAR | 500 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 500 (100%) | {"name": "hasAuthorInstitution", "type": "affiliation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 500 (100%) | {"provenance": "Inferred by OpenAIRE", "trust": "1.0"} |
| validated | BOOLEAN | 500 (100%) | False |
| cordis_ec_contribution | DOUBLE | 0 (0%) |  |
| cordis_type | VARCHAR | 0 (0%) |  |

## **work** - 500 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 500 (100%) | 571845499676661081 |
| openaireId | VARCHAR | 500 (100%) | 1287497f2a65::447b34cd09ac3c3962e86c8a044ef22d |
| title | VARCHAR | 448 (90%) | MERCEDES CABRERA y FERNANDO DEL REY: El poder de los empresarios. Política y economía en la España contemporánea (1875-2000), Madrid, Taurus, 2002 |
| publicationDate | DATE | 455 (91%) | 2008-01-01 |
| publisher | VARCHAR | 281 (56%) | Centro de Estudios Políticos y Constitucionales |
| openAccessColor | VARCHAR | 33 (7%) | gold |
| isGreen | BOOLEAN | 482 (96%) | False |
| isInDiamondJournal | BOOLEAN | 482 (96%) | False |
| publiclyFunded | BOOLEAN | 482 (96%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 500 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 411 (82%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 476 (95%) | [{"fullName": ", por Zira Box", "name": null, "pid": null, "rank": 1, "surname": null}] |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 385 (77%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "RECENSIONES"}}] |
| descriptions | VARCHAR[] | 327 (65%) | ["Autori ovog rada analiziraju grupni okvir u konkretnoj grupno-analiti\u010dkoj situaciji tijekom faza i kriza grupe, u grupi kojoj je voditelj po\u010detnik u edukaciji za grupnog analiti\u010dara. … |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 145 (29%) | [{"scheme": "handle", "value": "10357/37292"}] |
| sources | VARCHAR[] | 185 (37%) | ["Historia y Pol\u00edtica"] |
| formats | VARCHAR[] | 273 (55%) | ["application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 500 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": "gold", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "arti… |
| citationCount | DOUBLE | 500 (100%) | 0.0 |
| influence | DOUBLE | 500 (100%) | 2.3384854e-09 |
| views | BIGINT | 26 (5%) | 97 |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 76 (15%) | {"edition": null, "ep": null, "iss": "09", "issnLinking": null, "issnOnline": "1989-063X", "issnPrinted": "1575-0361", "name": "Historia y Pol\u00edtica", "sp": null, "vol": null} |

