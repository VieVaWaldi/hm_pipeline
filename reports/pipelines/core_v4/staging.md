# staging - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/core/core_v4_staging.duckdb`
- **Size:** 124329.8 MB
- **Tables:** 4

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| organization | 494,099 | 22 |
| project | 3,893,065 | 18 |
| relation | 153,750,065 | 9 |
| work | 50,000,000 | 24 |

## **organization** - 494,099 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 494,099 (100%) | 832354188336545630 |
| openaireId | VARCHAR | 494,099 (100%) | pending_org_::df87a412db6828ae8c45d1ba7a4586f0 |
| legalName | VARCHAR | 494,099 (100%) | ASFAR CIC |
| legalShortName | VARCHAR | 475,410 (96%) | ASFAR CIC |
| websiteUrl | VARCHAR | 188,616 (38%) | http://www.digital.brantner.com |
| alternativeNames | VARCHAR[] | 181,580 (37%) | ["Brantner Digital Solutions GmbH"] |
| countryCode | VARCHAR | 336,707 (68%) | GB |
| rorId | VARCHAR | 126,401 (26%) | https://ror.org/05bhhjy59 |
| wikiId | VARCHAR | 55,825 (11%) | Q74432432 |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 204,609 (41%) | [{"scheme": "PIC", "value": "886891856"}] |
| rorStatus | VARCHAR | 126,400 (26%) | active |
| rorEstablished | INTEGER | 103,541 (21%) | 1965 |
| rorTypes | VARCHAR[] | 126,400 (26%) | ["education"] |
| rorLocations | JSON | 126,400 (26%) | [{"geonames_id":2643097,"geonames_details":{"continent_code":"EU","continent_name":"Europe","country_code":"GB","country_name":"United Kingdom","country_subdivision_code":"ENG","country_subdivision_na… |
| geolocation | DOUBLE[] | 172,739 (35%) | [53.13333, -1.2] |
| geolocation_source | VARCHAR | 172,739 (35%) | ror |
| rorRelationships | JSON | 126,400 (26%) | [] |
| address_street | VARCHAR | 69,450 (14%) | NO.22 MAIZIDIAN STREET CHAOYANG DISTRICT |
| address_postalcode | VARCHAR | 65,823 (13%) | 100125 |
| address_city | VARCHAR | 69,754 (14%) | BEIJING |
| address_country | VARCHAR | 70,494 (14%) | CN |
| nuts3 | VARCHAR | 43,232 (9%) | PT1A0 |

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

## **relation** - 153,750,065 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| source | UBIGINT | 153,750,065 (100%) | 3009160125860437562 |
| sourceType | VARCHAR | 153,750,065 (100%) | project |
| target | UBIGINT | 153,750,065 (100%) | 2516589739106412851 |
| targetType | VARCHAR | 153,750,065 (100%) | organization |
| relType | STRUCT("name" VARCHAR, "type" VARCHAR) | 153,750,065 (100%) | {"name": "hasParticipant", "type": "participation"} |
| provenance | STRUCT(provenance VARCHAR, trust VARCHAR) | 153,750,065 (100%) | {"provenance": "Harvested", "trust": "0.900"} |
| validated | BOOLEAN | 153,750,065 (100%) | False |
| cordis_ec_contribution | DOUBLE | 384,988 (0%) | 250396.828125 |
| cordis_type | VARCHAR | 392,682 (0%) | participant |

## **work** - 50,000,000 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | UBIGINT | 50,000,000 (100%) | 4746385561214483529 |
| openaireId | VARCHAR | 50,000,000 (100%) | RECOLECTA___::20041a949de0d3635060b807dc4e3be4 |
| title | VARCHAR | 49,994,566 (100%) | Flying Like Geese |
| publicationDate | DATE | 49,742,547 (99%) | 2022-01-01 |
| publisher | VARCHAR | 46,001,342 (92%) | Array |
| openAccessColor | VARCHAR | 20,889,172 (42%) | gold |
| isGreen | BOOLEAN | 48,831,612 (98%) | True |
| isInDiamondJournal | BOOLEAN | 48,831,612 (98%) | False |
| publiclyFunded | BOOLEAN | 48,831,612 (98%) | False |
| language | STRUCT(code VARCHAR, "label" VARCHAR) | 50,000,000 (100%) | {"code": "eng", "label": "English"} |
| bestAccessRight | STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR) | 42,022,254 (84%) | {"code": "c_abf2", "label": "OPEN", "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"} |
| authors | STRUCT(fullName VARCHAR, "name" VARCHAR, pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)), rank BIGINT, surname VARCHAR)[] | 48,031,156 (96%) | [{"fullName": "Bracons Escarre, Oriol\|\|", "name": "Oriol", "pid": null, "rank": 1, "surname": "Bracons Escarre"}, {"fullName": "De Urrengoechea Cantavenera, Tom\u00e1s\|\|", "name": "Toma\u0301s", "… |
| subjects | STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[] | 35,190,151 (70%) | [{"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": {"scheme": "keyword", "value": "Atmospheric modeling"}}, {"provenance": {"provenance": "Harvested", "trust": "0.9"}, "subject": … |
| descriptions | VARCHAR[] | 37,144,047 (74%) | ["Formation flights hold significant potential for reducing the environmental impact of aviation and offer a means to streamline airspace complexity. The selection of candidates for formation pairing … |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | 45,929,605 (92%) | [{"scheme": "arXiv", "value": "2204.10036"}] |
| sources | VARCHAR[] | 42,372,219 (85%) | ["reponame:Dip\u00f2sit Digital de Documents de la UAB", "instname:Universitat Aut\u00f2noma de Barcelona"] |
| formats | VARCHAR[] | 10,709,610 (21%) | ["application/pdf"] |
| instances | STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR), alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[], articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR, pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR, "type" VARCHAR, urls VARCHAR[])[] | 50,000,000 (100%) | [{"accessRight": {"code": "c_abf2", "label": "OPEN", "openAccessRoute": null, "scheme": "http://vocabularies.coar-repositories.org/documentation/access_rights/"}, "alternateIdentifiers": null, "articl… |
| citationCount | DOUBLE | 50,000,000 (100%) | 0.0 |
| influence | DOUBLE | 50,000,000 (100%) | 2.3384854e-09 |
| views | BIGINT | 1,147,100 (2%) | 719 |
| countries | VARCHAR[] | 12,534,806 (25%) | ["IT"] |
| container | STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR, "name" VARCHAR, sp VARCHAR, vol VARCHAR) | 37,433,342 (75%) | {"edition": null, "ep": null, "iss": null, "issnLinking": null, "issnOnline": null, "issnPrinted": null, "name": "CoRR", "sp": null, "vol": "abs/1201.4899"} |
| link_tier | SMALLINT | 50,000,000 (100%) | 0 |

