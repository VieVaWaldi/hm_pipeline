# openaire_dump_2026_06_05 - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/sources/openaire_raw.duckdb`
- **Size:** 387269.0 MB
- **Tables:** 4

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 494,099 | 7 |
| project | 3,909,902 | 18 |
| relation | 296,915,370 | 8 |
| work | 218,421,450 | 37 |

## **organization** - 494,099 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| alternativeNames | VARCHAR[] | 181,580 (37%) | ["BRI"] |
| country | STRUCT(code VARCHAR, "label" VARCHAR) | 336,878 (68%) | {"code": "IT", "label": "Italy"} |
| id | VARCHAR | 494,099 (100%) | mk0_________::3a0574383de69b97813edc1c5d1be45f |
| legalName | VARCHAR | 494,099 (100%) | Masarykova univerzita / Rektorát |
| legalShortName | VARCHAR | 475,410 (96%) | BRI |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 204,609 (41%) | [{"scheme": "PIC", "value": "952996095"}] |
| websiteUrl | VARCHAR | 188,616 (38%) | http://www.bonfiglioli.com |

## **project** - 3,909,902 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| acronym | VARCHAR | 128,220 (3%) | SKIPPI |
| callIdentifier | VARCHAR | 129,893 (3%) | Postdoctoral Researcher TT |
| code | VARCHAR | 3,909,902 (100%) | SS-2018-165 |
| endDate | DATE | 3,521,733 (90%) | 2016-08-31 |
| fundings | STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[] | 3,907,791 (100%) | [{"fundingStream": {"description": "Summer Student Scholarships - Capacity Building and Leadership Enhancement", "id": "100010414::Summer Student Scholarships::Capacity Building and Leadership Enhance… |
| granted | STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE) | 3,909,902 (100%) | {"currency": "EUR", "fundedAmount": 2400.0, "totalCost": 0.0} |
| h2020Programmes | STRUCT(code VARCHAR, description VARCHAR)[] | 32,249 (1%) | [{"code": "H2020-EU.1.1.", "description": "European Research Council (ERC)"}, {"code": "H2020-EU.1.1.", "description": "European Research Council (ERC)"}] |
| id | VARCHAR | 3,909,902 (100%) | 100010414___::2383ea6f558581c1a8dcd370ea931aae |
| indicators | STRUCT(citationImpact JSON, usageCounts STRUCT(downloads BIGINT, "views" BIGINT)) | 671,037 (17%) | {"citationImpact": null, "usageCounts": null} |
| keywords | VARCHAR | 516,119 (13%) | Educational sciences |
| openAccessMandateForDataset | BOOLEAN | 3,909,902 (100%) | False |
| openAccessMandateForPublications | BOOLEAN | 3,909,902 (100%) | False |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 100,110 (3%) | [{"scheme": "doi", "value": "10.3030/101217760"}] |
| startDate | DATE | 3,685,695 (94%) | 2018-01-01 |
| subjects | VARCHAR[] | 294,676 (8%) | ["EIC Accelerator Open 2024"] |
| summary | VARCHAR | 534,364 (14%) | Information provides a solid and constructive foundation for dialogue among people, for the individuals choice-making and  broadly for decision-making in society. A key challenge with regard to the re… |
| title | VARCHAR | 3,893,312 (100%) | An investigation into the extent of dynamic hyperinflation in COPD patients attending a community based pulmonary rehabilitation programme. |
| websiteUrl | VARCHAR | 86,933 (2%) | http://purl.org/au-research/grants/arc/DP0451399 |

## **relation** - 296,915,370 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 296,915,370 (100%) | {"provenance": "Inferred by OpenAIRE", "trust": "1.0"} |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 296,915,370 (100%) | {"name": "hasAuthorInstitution", "type": "affiliation"} |
| source | VARCHAR | 296,915,370 (100%) | doi_dedup___::bde5fae6de1b9423fe93f538809b25ee |
| sourceType | VARCHAR | 296,915,370 (100%) | product |
| target | VARCHAR | 296,915,370 (100%) | openorgs____::01bf2bf3375b3fbaf8d2dac6dad08c84 |
| targetType | VARCHAR | 296,915,370 (100%) | organization |
| validated | BOOLEAN | 296,915,370 (100%) | False |
| validationDate | DATE | 942,916 (0%) | 2025-06-26 |

## **work** - 218,421,450 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 182,013,144 (83%) | [{"fullName": ", por Zira Box", "name": null, "pid": null, "rank": 1, "surname": null}] |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 138,405,272 (63%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| codeRepositoryUrl | JSON | 0 (0%) |  |
| contactGroups | JSON | 0 (0%) |  |
| contactPeople | JSON | 0 (0%) |  |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 147,556,829 (68%) | {"edition": null, "ep": null, "iss": "09", "issnLinking": null, "issnOnline": "1989-063X", "issnPrinted": "1575-0361", "name": "Historia y Pol\u00edtica", "sp": null, "vol": null} |
| contributors | VARCHAR[] | 14,224,780 (7%) | ["Gregurek, Rudolf"] |
| countries | STRUCT(code VARCHAR, "label" VARCHAR, provenance STRUCT(provenance VARCHAR, trust VARCHAR))[] | 34,507,649 (16%) | [{"code": "HR", "label": "Croatia", "provenance": {"provenance": "Inferred by OpenAIRE", "trust": "0.85"}}] |
| coverages | JSON | 0 (0%) |  |
| dateOfCollection | VARCHAR | 218,421,450 (100%) | 2026-04-25T02:35:46.920098214 |
| descriptions | VARCHAR[] | 123,346,737 (56%) | ["Autori ovog rada analiziraju grupni okvir u konkretnoj grupno-analiti\u010dkoj situaciji tijekom faza i kriza grupe, u grupi kojoj je voditelj po\u010detnik u edukaciji za grupnog analiti\u010dara. … |
| documentationUrls | JSON | 0 (0%) |  |
| embargoEndDate | VARCHAR | 5,507,358 (3%) | 1938-06-01 |
| formats | VARCHAR[] | 31,014,123 (14%) | ["application/pdf"] |
| geoLocations | JSON | 0 (0%) |  |
| id | VARCHAR | 218,421,450 (100%) | 1287497f2a65::447b34cd09ac3c3962e86c8a044ef22d |
| indicators | STRUCT(citationImpact STRUCT(citationClass VARCHAR, citationCount DOUBLE, impulse DOUBLE, impulseClass VARCHAR, influence DOUBLE, influenceClass VARCHAR, popularity DOUBLE, popularityClass VARCHAR), usageCounts STRUCT(downloads BIGINT, "views" BIGINT)) | 218,421,450 (100%) | {"citationImpact": {"citationClass": "C5", "citationCount": 0.0, "impulse": 0.0, "impulseClass": "C5", "influence": 2.3384854e-09, "influenceClass": "C5", "popularity": 1.9599772e-10, "popularityClass… |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 218,421,450 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": "gold", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "arti… |
| isGreen | BOOLEAN | 215,716,232 (99%) | False |
| isInDiamondJournal | BOOLEAN | 215,716,232 (99%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 218,421,450 (100%) | {"code": "eng", "label": "English"} |
| lastUpdateTimeStamp | BIGINT | 203,003,069 (93%) | 1776279010530 |
| mainTitle | VARCHAR | 216,958,318 (99%) | MERCEDES CABRERA y FERNANDO DEL REY: El poder de los empresarios. Política y economía en la España contemporánea (1875-2000), Madrid, Taurus, 2002 |
| openAccessColor | VARCHAR | 55,657,366 (25%) | gold |
| originalIds | VARCHAR[] | 218,412,841 (100%) | ["50\|1287497f2a65::447b34cd09ac3c3962e86c8a044ef22d", "oai:ojs.pkp.sfu.ca:article/44833"] |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 193,292,417 (88%) | [{"scheme": "handle", "value": "10357/37292"}] |
| programmingLanguage | JSON | 0 (0%) |  |
| publicationDate | DATE | 212,526,830 (97%) | 0001-12-30 |
| publiclyFunded | BOOLEAN | 215,716,232 (99%) | False |
| publisher | VARCHAR | 184,650,335 (85%) | Centro de Estudios Políticos y Constitucionales |
| size | JSON | 0 (0%) |  |
| sources | VARCHAR[] | 168,711,105 (77%) | ["Historia y Pol\u00edtica"] |
| subTitle | VARCHAR | 9,416,412 (4%) | zbMATH Open Web Interface contents unavailable due to conflicting licenses. |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 131,308,586 (60%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "RECENSIONES"}}] |
| tools | JSON | 0 (0%) |  |
| type | VARCHAR | 218,421,450 (100%) | publication |
| version | JSON | 0 (0%) |  |

