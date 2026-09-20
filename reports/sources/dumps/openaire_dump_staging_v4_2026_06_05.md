# openaire_dump_staging_v4_2026_06_05 - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/sources/openaire_staging_v4.duckdb`
- **Size:** 356419.5 MB
- **Tables:** 4

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 494,099 | 10 |
| project | 3,893,065 | 18 |
| relation | 296,915,370 | 7 |
| work | 218,421,450 | 23 |

## **organization** - 494,099 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 494,099 (100%) | 11759239505163002302 |
| openaireId | VARCHAR | 494,099 (100%) | mk0_________::3a0574383de69b97813edc1c5d1be45f |
| legalName | VARCHAR | 494,099 (100%) | Masarykova univerzita / Rektorát |
| legalShortName | VARCHAR | 475,410 (96%) | BRI |
| websiteUrl | VARCHAR | 188,616 (38%) | http://www.bonfiglioli.com |
| alternativeNames | VARCHAR[] | 181,580 (37%) | ["BRI"] |
| countryCode | VARCHAR | 336,878 (68%) | IT |
| rorId | VARCHAR | 126,401 (26%) | https://ror.org/03f3r3r28 |
| wikiId | VARCHAR | 55,825 (11%) | Q6840980 |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 204,609 (41%) | [{"scheme": "PIC", "value": "952996095"}] |

## **project** - 3,893,065 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 3,893,065 (100%) | 5074298163293383572 |
| openaireId | VARCHAR | 3,893,065 (100%) | 100010414___::2383ea6f558581c1a8dcd370ea931aae |
| grantId | VARCHAR | 3,893,065 (100%) | SS-2018-165 |
| title | VARCHAR | 3,893,065 (100%) | An investigation into the extent of dynamic hyperinflation in COPD patients attending a community based pulmonary rehabilitation programme. |
| acronym | VARCHAR | 120,629 (3%) | CONCEPT |
| websiteUrl | VARCHAR | 86,933 (2%) | http://purl.org/au-research/grants/arc/DP0451399 |
| startDate | DATE | 3,669,190 (94%) | 2018-01-01 |
| endDate | DATE | 3,516,080 (90%) | 2016-08-31 |
| callIdentifier | VARCHAR | 129,893 (3%) | Postdoctoral Researcher TT |
| keywords | VARCHAR | 507,115 (13%) | Educational sciences |
| openAccessMandateForPublications | BOOLEAN | 3,893,065 (100%) | False |
| openAccessMandateForDataset | BOOLEAN | 3,893,065 (100%) | False |
| subjects | VARCHAR[] | 294,676 (8%) | ["EIC Accelerator Open 2024"] |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 3,891,009 (100%) | [{"fundingStream": {"description": "Summer Student Scholarships - Capacity Building and Leadership Enhancement", "id": "100010414::Summer Student Scholarships::Capacity Building and Leadership Enhance… |
| frameworkProgrammes | VARCHAR[] | 3,598,016 (92%) | ["Summer Student Scholarships"] |
| summary | VARCHAR | 533,148 (14%) | Information provides a solid and constructive foundation for dialogue among people, for the individuals choice-making and broadly for decision-making in society. A key challenge with regard to the rel… |
| doi | VARCHAR | 99,887 (3%) | 10.3030/101217760 |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 3,893,065 (100%) | {"currency": "EUR", "fundedAmount": 2400.0, "totalCost": 0.0} |

## **relation** - 296,915,370 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 296,915,370 (100%) | 9314380870723311381 |
| sourceType | VARCHAR | 296,915,370 (100%) | product |
| target | UBIGINT | 296,915,370 (100%) | 3914739875998513666 |
| targetType | VARCHAR | 296,915,370 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 296,915,370 (100%) | {"name": "hasAuthorInstitution", "type": "affiliation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 296,915,370 (100%) | {"provenance": "Inferred by OpenAIRE", "trust": "1.0"} |
| validated | BOOLEAN | 296,915,370 (100%) | False |

## **work** - 218,421,450 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 218,421,450 (100%) | 571845499676661081 |
| openaireId | VARCHAR | 218,421,450 (100%) | 1287497f2a65::447b34cd09ac3c3962e86c8a044ef22d |
| title | VARCHAR | 216,958,318 (99%) | MERCEDES CABRERA y FERNANDO DEL REY: El poder de los empresarios. Política y economía en la España contemporánea (1875-2000), Madrid, Taurus, 2002 |
| publicationDate | DATE | 212,485,241 (97%) | 2008-01-01 |
| publisher | VARCHAR | 184,650,335 (85%) | Centro de Estudios Políticos y Constitucionales |
| openAccessColor | VARCHAR | 55,657,366 (25%) | gold |
| isGreen | BOOLEAN | 215,716,232 (99%) | False |
| isInDiamondJournal | BOOLEAN | 215,716,232 (99%) | False |
| publiclyFunded | BOOLEAN | 215,716,232 (99%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 218,421,450 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 138,405,272 (63%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 182,013,144 (83%) | [{"fullName": ", por Zira Box", "name": null, "pid": null, "rank": 1, "surname": null}] |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 131,308,586 (60%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "RECENSIONES"}}] |
| descriptions | VARCHAR[] | 123,344,446 (56%) | ["Autori ovog rada analiziraju grupni okvir u konkretnoj grupno-analiti\u010dkoj situaciji tijekom faza i kriza grupe, u grupi kojoj je voditelj po\u010detnik u edukaciji za grupnog analiti\u010dara. … |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 193,292,417 (88%) | [{"scheme": "handle", "value": "10357/37292"}] |
| sources | VARCHAR[] | 168,711,105 (77%) | ["Historia y Pol\u00edtica"] |
| formats | VARCHAR[] | 31,014,123 (14%) | ["application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 218,421,450 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": "gold", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "arti… |
| citationCount | DOUBLE | 218,421,450 (100%) | 0.0 |
| influence | DOUBLE | 218,421,450 (100%) | 2.3384854e-09 |
| views | BIGINT | 3,549,274 (2%) | 97 |
| countries | VARCHAR[] | 34,507,649 (16%) | ["HR"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 147,556,829 (68%) | {"edition": null, "ep": null, "iss": "09", "issnLinking": null, "issnOnline": "1989-063X", "issnPrinted": "1575-0361", "name": "Historia y Pol\u00edtica", "sp": null, "vol": null} |

