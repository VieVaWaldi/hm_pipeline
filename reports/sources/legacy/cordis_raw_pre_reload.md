# cordis_raw_pre_reload - Last updated: 18-09-2026

- **Path:** `/work/lu72hip/data/duckdb/sources/cordis_raw.duckdb`
- **Size:** 1295.3 MB
- **Tables:** 17

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| fundingprogramme | 13,176 | 9 |
| institution | 177,748 | 19 |
| j_institution_person | 82,380 | 2 |
| j_project_fundingprogramme | 307,456 | 2 |
| j_project_institution | 741,535 | 9 |
| j_project_researchoutput | 778,828 | 2 |
| j_project_topic | 1,054,828 | 2 |
| j_project_weblink | 15,773 | 2 |
| j_researchoutput_institution | 10,710 | 2 |
| j_researchoutput_person | 1,974,269 | 3 |
| j_researchoutput_topic | 0 | 2 |
| j_researchoutput_weblink | 1,107 | 2 |
| person | 1,295,289 | 8 |
| project | 139,925 | 17 |
| researchoutput | 778,828 | 20 |
| topic | 1,052 | 5 |
| weblink | 265,646 | 5 |

## **fundingprogramme** - 13,176 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 13,176 (100%) | 1 |
| code | VARCHAR | 13,176 (100%) | FP2-CAMAR |
| title | VARCHAR | 13,176 (100%) | Research and technological development programme (EEC) in the field of competitiveness of agriculture and management of agricultural resources, 1989-1993 |
| short_title | VARCHAR | 204 (2%) | Competiveness of agriculture and management of agricultural resources |
| framework_programme | VARCHAR | 13,176 (100%) | FP2 |
| pga | VARCHAR | 503 (4%) | FP2-CAMAR |
| rcn | VARCHAR | 13,176 (100%) | 147 |
| created_at | TIMESTAMP | 13,176 (100%) | 2026-03-13 11:55:46.281801 |
| updated_at | TIMESTAMP | 13,176 (100%) | 2026-03-13 11:55:46.281807 |

## **institution** - 177,748 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 177,748 (100%) | 1 |
| legal_name | VARCHAR | 177,748 (100%) | TEAGASC (AGRICULTURE AND FOOD DEVELOPMENT AUTHORITY) |
| sme | BOOLEAN | 44,899 (25%) | False |
| url | VARCHAR | 38,321 (22%) | http://www.roma.ccr.it/circe |
| short_name | VARCHAR | 68,759 (39%) | VW |
| vat_number | VARCHAR | 45,269 (25%) | DK18159104 |
| street | VARCHAR | 163,021 (92%) | Sandymount Ave. 19 |
| postbox | VARCHAR | 14,814 (8%) | BP 311 |
| postalcode | VARCHAR | 139,638 (79%) | 4 |
| city | VARCHAR | 170,628 (96%) | DUBLIN |
| country | VARCHAR | 177,562 (100%) | IE |
| geolocation | JSON | 177,748 (100%) | [-7.9034194, 53.0968609] |
| type_title | VARCHAR | 72,457 (41%) | Public bodies (excluding Research Organisations and Secondary or Higher Education Establishments) |
| nuts_level_0 | VARCHAR | 54,032 (30%) | UK |
| nuts_level_1 | VARCHAR | 47,450 (27%) | UKI |
| nuts_level_2 | VARCHAR | 47,450 (27%) | UKI3 |
| nuts_level_3 | VARCHAR | 48,420 (27%) | UKI32 |
| created_at | TIMESTAMP | 177,748 (100%) | 2026-03-13 11:55:46.283836 |
| updated_at | TIMESTAMP | 177,748 (100%) | 2026-03-13 11:55:46.283840 |

## **j_institution_person** - 82,380 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| institution_id | INTEGER | 82,380 (100%) | 75015 |
| person_id | INTEGER | 82,380 (100%) | 7780 |

## **j_project_fundingprogramme** - 307,456 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 307,456 (100%) | 1 |
| fundingprogramme_id | INTEGER | 307,456 (100%) | 1 |

## **j_project_institution** - 741,535 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 741,535 (100%) | 1 |
| institution_id | INTEGER | 741,535 (100%) | 1 |
| institution_position | INTEGER | 741,535 (100%) | 1 |
| ec_contribution | FLOAT | 411,767 (56%) | 447972.21875 |
| net_ec_contribution | FLOAT | 300,303 (40%) | 230000.0 |
| total_cost | FLOAT | 300,122 (40%) | 230000.0 |
| type | VARCHAR | 741,535 (100%) | participant |
| organization_id | VARCHAR | 445,731 (60%) | 999994535 |
| rcn | INTEGER | 741,535 (100%) | 51846 |

## **j_project_researchoutput** - 778,828 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 778,828 (100%) | 7 |
| researchoutput_id | INTEGER | 778,828 (100%) | 1 |

## **j_project_topic** - 1,054,828 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 1,054,828 (100%) | 1 |
| topic_id | INTEGER | 1,054,828 (100%) | 1 |

## **j_project_weblink** - 15,773 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 15,773 (100%) | 437 |
| weblink_id | INTEGER | 15,773 (100%) | 1 |

## **j_researchoutput_institution** - 10,710 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 10,710 (100%) | 35 |
| institution_id | INTEGER | 10,710 (100%) | 582 |

## **j_researchoutput_person** - 1,974,269 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 1,974,269 (100%) | 49 |
| person_id | INTEGER | 1,974,269 (100%) | 26 |
| person_position | INTEGER | 1,974,269 (100%) | 0 |

## **j_researchoutput_topic** - 0 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 0 (0%) |  |
| topic_id | INTEGER | 0 (0%) |  |

## **j_researchoutput_weblink** - 1,107 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 1,107 (100%) | 22198 |
| weblink_id | INTEGER | 1,107 (100%) | 3854 |

## **person** - 1,295,289 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 1,295,289 (100%) | 1 |
| title | VARCHAR | 77,203 (6%) | Ms. |
| name | VARCHAR | 1,216,831 (94%) | PLESTENJAK A |
| first_name | VARCHAR | 78,457 (6%) | Brooke |
| last_name | VARCHAR | 78,457 (6%) | Alasya |
| telephone_number | VARCHAR | 78,213 (6%) | +44 20 7594 1181 |
| created_at | TIMESTAMP | 1,295,289 (100%) | 2026-03-13 11:55:57.247473 |
| updated_at | TIMESTAMP | 1,295,289 (100%) | 2026-03-13 11:55:57.247477 |

## **project** - 139,925 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 139,925 (100%) | 1 |
| id_original | VARCHAR | 139,925 (100%) | 80010001 |
| doi | VARCHAR | 54,987 (39%) | 10.3030/632927 |
| title | VARCHAR | 139,925 (100%) | EUROPEAN RESEARCH AND MULTIMEDIA SYSTEM TO DISSEMINATE NEW MANAGEMENT KNOW-HOW LINKED WITH NEW INFORMATION TECHNOLOGIES TO ADVISERS AND FARMERS |
| acronym | VARCHAR | 111,625 (80%) | EFIT |
| status | VARCHAR | 81,342 (58%) | CLOSED |
| start_date | DATE | 139,925 (100%) | 1991-01-01 |
| end_date | DATE | 138,975 (99%) | 1994-01-01 |
| ec_signature_date | DATE | 55,176 (39%) | 2014-07-17 |
| total_cost | FLOAT | 115,722 (83%) | 54000.0 |
| ec_max_contribution | FLOAT | 119,023 (85%) | 27000.0 |
| objective | VARCHAR | 128,705 (92%) | This project, more commonly known as Euro-Farmers Information Technology (EFIT), is concerned with the actual use of computers and telecommunication in farm production and management. Its main objecti… |
| call_identifier | VARCHAR | 90,835 (65%) | NOT AVAILABLE |
| call_title | VARCHAR | 85,876 (61%) | NOT AVAILABLE |
| call_rcn | VARCHAR | 90,835 (65%) | 90927 |
| created_at | TIMESTAMP | 139,925 (100%) | 2026-03-13 11:55:46.259765 |
| updated_at | TIMESTAMP | 139,925 (100%) | 2026-03-13 11:55:46.259774 |

## **researchoutput** - 778,828 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 778,828 (100%) | 1 |
| id_original | VARCHAR | 778,828 (100%) | BREU0053_444_EXPL |
| from_pdf | BOOLEAN | 778,828 (100%) | False |
| type | VARCHAR | 778,828 (100%) | relatedResult |
| doi | VARCHAR | 382,403 (49%) | 10.1093/nar/gku1061 |
| title | VARCHAR | 778,828 (100%) | Biodegradable and resorbable polymeric coatings for drug delivery |
| publication_date | DATE | 778,828 (100%) | 1996-10-23 |
| journal | VARCHAR | 433,363 (56%) | Extract: COST 94: The post-harvest treatment of fruit and vegetables: Current status and future prospects, Oosterbeek, Netherlands, 19-22 October 1994: Proceedings (1998) pp. 23-30 |
| summary | VARCHAR | 269,791 (35%) | The post-harvest treatment of fruits and vegetables is a subject of great economic importance. In order to meet consumer demands for constant availability and good quality throughout the year adequate… |
| comment | VARCHAR | 83,623 (11%) | Drug targeting using polymeric microspheres injected into the blood circulation could have considerable impact in many disease conditions, especially cancer chemotherapy. The project aims to develop n… |
| fulltext | VARCHAR | 0 (0%) |  |
| funding_number | VARCHAR | 0 (0%) |  |
| journal_number | VARCHAR | 275,745 (35%) | 43/D1 |
| journal_title | VARCHAR | 433,363 (56%) | Extract: COST 94: The post-harvest treatment of fruit and vegetables: Current status and future prospects, Oosterbeek, Netherlands, 19-22 October 1994: Proceedings (1998) pp. 23-30 |
| published_pages | VARCHAR | 206,841 (27%) | 23-30 |
| published_year | VARCHAR | 466,902 (60%) | 1998 |
| publisher | VARCHAR | 467,740 (60%) | Oxford University Press |
| issn | VARCHAR | 307,733 (40%) | 0305-1048 |
| created_at | TIMESTAMP | 778,828 (100%) | 2026-03-13 11:55:46.904304 |
| updated_at | TIMESTAMP | 778,828 (100%) | 2026-03-13 11:55:46.904310 |

## **topic** - 1,052 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 1,052 (100%) | 1 |
| name | VARCHAR | 1,052 (100%) | social sciences |
| level | INTEGER | 1,052 (100%) | 0 |
| created_at | TIMESTAMP | 1,052 (100%) | 2026-03-13 11:55:46.265394 |
| updated_at | TIMESTAMP | 1,052 (100%) | 2026-03-13 11:55:46.265399 |

## **weblink** - 265,646 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 265,646 (100%) | 1 |
| url | VARCHAR | 265,646 (100%) | http://homepages.physik.uni-muenchen.de/~Dieter.Luest/forcesuniverse.html |
| title | VARCHAR | 6,229 (2%) | project website |
| created_at | TIMESTAMP | 265,646 (100%) | 2026-03-13 11:56:08.184633 |
| updated_at | TIMESTAMP | 265,646 (100%) | 2026-03-13 11:56:08.184638 |

