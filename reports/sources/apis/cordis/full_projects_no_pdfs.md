# full_projects_no_pdfs - Last updated: 18-09-2026

- **Path:** `/work/lu72hip/data/duckdb/sources/cordis_full_projects_no_pdfs_raw.duckdb`
- **Size:** 1539.0 MB
- **Tables:** 17

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| fundingprogramme | 13,594 | 9 |
| institution | 180,813 | 19 |
| j_institution_person | 82,315 | 2 |
| j_project_fundingprogramme | 314,097 | 2 |
| j_project_institution | 761,774 | 9 |
| j_project_researchoutput | 898,565 | 2 |
| j_project_topic | 1,070,834 | 2 |
| j_project_weblink | 15,941 | 2 |
| j_researchoutput_institution | 12,769 | 2 |
| j_researchoutput_person | 2,666,200 | 3 |
| j_researchoutput_topic | 0 | 2 |
| j_researchoutput_weblink | 46,139 | 2 |
| person | 1,446,648 | 8 |
| project | 142,773 | 17 |
| researchoutput | 898,210 | 20 |
| topic | 1,059 | 5 |
| weblink | 300,479 | 5 |

## **fundingprogramme** - 13,594 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 13,594 (100%) | 1 |
| code | VARCHAR | 13,594 (100%) | FP2-CAMAR |
| title | VARCHAR | 13,594 (100%) | Research and technological development programme (EEC) in the field of competitiveness of agriculture and management of agricultural resources, 1989-1993 |
| short_title | VARCHAR | 204 (2%) | Competiveness of agriculture and management of agricultural resources |
| framework_programme | VARCHAR | 13,594 (100%) | FP2 |
| pga | VARCHAR | 504 (4%) | FP2-CAMAR |
| rcn | VARCHAR | 13,594 (100%) | 147 |
| created_at | TIMESTAMP | 13,594 (100%) | 2026-09-18 02:08:36.328641 |
| updated_at | TIMESTAMP | 13,594 (100%) | 2026-09-18 02:08:36.328644 |

## **institution** - 180,813 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 180,813 (100%) | 1 |
| legal_name | VARCHAR | 180,813 (100%) | TEAGASC (AGRICULTURE AND FOOD DEVELOPMENT AUTHORITY) |
| sme | BOOLEAN | 47,951 (27%) | False |
| url | VARCHAR | 38,434 (21%) | http://www.roma.ccr.it/circe |
| short_name | VARCHAR | 70,601 (39%) | VW |
| vat_number | VARCHAR | 48,694 (27%) | DK18159104 |
| street | VARCHAR | 166,077 (92%) | Sandymount Ave. 19 |
| postbox | VARCHAR | 14,759 (8%) | BP 311 |
| postalcode | VARCHAR | 142,691 (79%) | 4 |
| city | VARCHAR | 173,684 (96%) | DUBLIN |
| country | VARCHAR | 180,627 (100%) | IE |
| geolocation | JSON | 180,813 (100%) | [-7.9034194, 53.0968609] |
| type_title | VARCHAR | 75,536 (42%) | Public bodies (excluding Research Organisations and Secondary or Higher Education Establishments) |
| nuts_level_0 | VARCHAR | 56,457 (31%) | UK |
| nuts_level_1 | VARCHAR | 49,572 (27%) | UKI |
| nuts_level_2 | VARCHAR | 49,565 (27%) | UKI3 |
| nuts_level_3 | VARCHAR | 51,157 (28%) | UKI32 |
| created_at | TIMESTAMP | 180,813 (100%) | 2026-09-18 02:08:36.329955 |
| updated_at | TIMESTAMP | 180,813 (100%) | 2026-09-18 02:08:36.329958 |

## **j_institution_person** - 82,315 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| institution_id | INTEGER | 82,315 (100%) | 75015 |
| person_id | INTEGER | 82,315 (100%) | 7780 |

## **j_project_fundingprogramme** - 314,097 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 314,097 (100%) | 1 |
| fundingprogramme_id | INTEGER | 314,097 (100%) | 1 |

## **j_project_institution** - 761,774 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 761,774 (100%) | 1 |
| institution_id | INTEGER | 761,774 (100%) | 1 |
| institution_position | INTEGER | 761,774 (100%) | 1 |
| ec_contribution | FLOAT | 429,364 (56%) | 447972.21875 |
| net_ec_contribution | FLOAT | 320,542 (42%) | 230000.0 |
| total_cost | FLOAT | 320,366 (42%) | 230000.0 |
| type | VARCHAR | 761,774 (100%) | participant |
| organization_id | VARCHAR | 465,970 (61%) | 999994535 |
| rcn | INTEGER | 761,774 (100%) | 51846 |

## **j_project_researchoutput** - 898,565 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 898,565 (100%) | 7 |
| researchoutput_id | INTEGER | 898,565 (100%) | 1 |

## **j_project_topic** - 1,070,834 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 1,070,834 (100%) | 1 |
| topic_id | INTEGER | 1,070,834 (100%) | 1 |

## **j_project_weblink** - 15,941 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| project_id | INTEGER | 15,941 (100%) | 437 |
| weblink_id | INTEGER | 15,941 (100%) | 1 |

## **j_researchoutput_institution** - 12,769 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 12,769 (100%) | 35 |
| institution_id | INTEGER | 12,769 (100%) | 582 |

## **j_researchoutput_person** - 2,666,200 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 2,666,200 (100%) | 49 |
| person_id | INTEGER | 2,666,200 (100%) | 26 |
| person_position | INTEGER | 2,666,200 (100%) | 0 |

## **j_researchoutput_topic** - 0 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 0 (0%) |  |
| topic_id | INTEGER | 0 (0%) |  |

## **j_researchoutput_weblink** - 46,139 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| researchoutput_id | INTEGER | 46,139 (100%) | 22198 |
| weblink_id | INTEGER | 46,139 (100%) | 3854 |

## **person** - 1,446,648 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 1,446,648 (100%) | 1 |
| title | VARCHAR | 77,123 (5%) | Ms. |
| name | VARCHAR | 1,446,648 (100%) | PLESTENJAK A |
| first_name | VARCHAR | 78,373 (5%) | Brooke |
| last_name | VARCHAR | 78,373 (5%) | Alasya |
| telephone_number | VARCHAR | 78,129 (5%) | +44 20 7594 1181 |
| created_at | TIMESTAMP | 1,446,648 (100%) | 2026-09-18 02:08:52.860177 |
| updated_at | TIMESTAMP | 1,446,648 (100%) | 2026-09-18 02:08:52.860180 |

## **project** - 142,773 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 142,773 (100%) | 1 |
| id_original | VARCHAR | 142,773 (100%) | 80010001 |
| doi | VARCHAR | 57,811 (40%) | 10.3030/632927 |
| title | VARCHAR | 142,773 (100%) | EUROPEAN RESEARCH AND MULTIMEDIA SYSTEM TO DISSEMINATE NEW MANAGEMENT KNOW-HOW LINKED WITH NEW INFORMATION TECHNOLOGIES TO ADVISERS AND FARMERS |
| acronym | VARCHAR | 114,473 (80%) | EFIT |
| status | VARCHAR | 84,190 (59%) | CLOSED |
| start_date | DATE | 142,773 (100%) | 1991-01-01 |
| end_date | DATE | 141,823 (99%) | 1994-01-01 |
| ec_signature_date | DATE | 58,025 (41%) | 2014-07-17 |
| total_cost | FLOAT | 118,570 (83%) | 54000.0 |
| ec_max_contribution | FLOAT | 121,871 (85%) | 27000.0 |
| objective | VARCHAR | 131,553 (92%) | This project, more commonly known as Euro-Farmers Information Technology (EFIT), is concerned with the actual use of computers and telecommunication in farm production and management. Its main objecti… |
| call_identifier | VARCHAR | 93,683 (66%) | NOT AVAILABLE |
| call_title | VARCHAR | 88,723 (62%) | NOT AVAILABLE |
| call_rcn | VARCHAR | 93,683 (66%) | 90927 |
| created_at | TIMESTAMP | 142,773 (100%) | 2026-09-18 02:08:36.313450 |
| updated_at | TIMESTAMP | 142,773 (100%) | 2026-09-18 02:21:30.240098 |

## **researchoutput** - 898,210 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 898,210 (100%) | 1 |
| id_original | VARCHAR | 898,210 (100%) | BREU0053_444_EXPL |
| from_pdf | BOOLEAN | 898,210 (100%) | False |
| type | VARCHAR | 898,210 (100%) | relatedResult |
| doi | VARCHAR | 455,038 (51%) | 10.1093/nar/gku1061 |
| title | VARCHAR | 898,210 (100%) | Biodegradable and resorbable polymeric coatings for drug delivery |
| publication_date | DATE | 898,210 (100%) | 1996-10-23 |
| journal | VARCHAR | 506,990 (56%) | Extract: COST 94: The post-harvest treatment of fruit and vegetables: Current status and future prospects, Oosterbeek, Netherlands, 19-22 October 1994: Proceedings (1998) pp. 23-30 |
| summary | VARCHAR | 306,509 (34%) | The post-harvest treatment of fruits and vegetables is a subject of great economic importance. In order to meet consumer demands for constant availability and good quality throughout the year adequate… |
| comment | VARCHAR | 86,565 (10%) | Drug targeting using polymeric microspheres injected into the blood circulation could have considerable impact in many disease conditions, especially cancer chemotherapy. The project aims to develop n… |
| fulltext | VARCHAR | 0 (0%) |  |
| funding_number | VARCHAR | 0 (0%) |  |
| journal_number | VARCHAR | 325,439 (36%) | 43/D1 |
| journal_title | VARCHAR | 506,990 (56%) | Extract: COST 94: The post-harvest treatment of fruit and vegetables: Current status and future prospects, Oosterbeek, Netherlands, 19-22 October 1994: Proceedings (1998) pp. 23-30 |
| published_pages | VARCHAR | 237,300 (26%) | 23-30 |
| published_year | VARCHAR | 546,801 (61%) | 1998 |
| publisher | VARCHAR | 548,934 (61%) | Oxford University Press |
| issn | VARCHAR | 365,066 (41%) | 0305-1048 |
| created_at | TIMESTAMP | 898,210 (100%) | 2026-09-18 02:08:37.268294 |
| updated_at | TIMESTAMP | 898,210 (100%) | 2026-09-18 02:20:40.904721 |

## **topic** - 1,059 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 1,059 (100%) | 1 |
| name | VARCHAR | 1,059 (100%) | social sciences |
| level | INTEGER | 1,059 (100%) | 0 |
| created_at | TIMESTAMP | 1,059 (100%) | 2026-09-18 02:08:36.318100 |
| updated_at | TIMESTAMP | 1,059 (100%) | 2026-09-18 02:08:36.318103 |

## **weblink** - 300,479 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| id | INTEGER | 300,479 (100%) | 1 |
| url | VARCHAR | 300,479 (100%) | http://homepages.physik.uni-muenchen.de/~Dieter.Luest/forcesuniverse.html |
| title | VARCHAR | 6,409 (2%) | project website |
| created_at | TIMESTAMP | 300,479 (100%) | 2026-09-18 02:09:06.962095 |
| updated_at | TIMESTAMP | 300,479 (100%) | 2026-09-18 02:09:06.962099 |

