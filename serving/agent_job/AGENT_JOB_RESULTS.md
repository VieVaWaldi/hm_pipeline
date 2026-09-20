# AGENT_JOB_RESULTS: core_v4 analysis for the OpenSearch design

Database `/work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb`, opened **read-only** in every job. Every query ran as an sbatch job on partition `fat` (8-16 cpus, 64-150 GB, DuckDB `memory_limit` below the job memory, `threads` = cpus, `temp_directory` under `tmp_session/duck_tmp`). Run date 2026-09-20. The heavy relations (`relation` 153.7M rows) and per-work features (50M rows) were materialised **once** as Parquet under `tmp_session/prep/` (appendix), so each analysis ran in seconds (all analyses A-L: seconds each, no sampling except where stated; F2b, G3c/G3d, G4, G5 use hash samples, marked in the title).

Exports of section M are in `serving/agent_job/out/` (`works_tier0`, `works_tier1_sample`, `projects_sample3pct`, `projects_full`, `organizations`, `grants`; ~1.8 GB total). No repo code changed, nothing committed; scripts are in `tmp_session/jobs/`.

## TL;DR for the index design
| Question | Answer |
|---|---|
| grants docs | **6,074** streams (+318k funding entries without stream id); funder key = `fundings.shortName` (103 values), programme = level 2 of the id (4,552, == `frameworkProgrammes`) |
| currencies to convert | USD, EUR, GBP, SEK, AUD, CHF (NULL currency of SNSF), `$`, HRK = 99.6%; 58% of projects have a positive amount |
| geolocated share | 63,885 orgs (18.3% of project-connected); 26.4% of projects, 49.2% of DCH projects have >= 1 geolocated org |
| array sizes | orgs/project p99 8 (max 380); orgs/work p99 41 (max 2,660): cap `organisation_ids` at ~100 |
| collaboration | 7.35M (project, org-pair) edges, **4.41M distinct pairs** (153,616 for DCH) |
| coordinators | 81,043 projects, EC only (63% of EC); 1,800 DCH projects |
| trimmed work doc | 684 B JSON avg (tier 0: 813, tier 1: 670) -> ~34 GB raw for 50M; **7.6 GB Parquet** |
| pdf_url coverage | 36.8% tier 0 / 10.8% tier 1 / 13.4% all; landing_url 99.65% |
| Parquet sizes | works 50M 7.6 GB, projects 0.66 GB, orgs 45 MB, grants 0.2 MB |
| biggest surprise | 922,801 tier-0 works have no project link; 316,230 projects have no org; duplicate orgs |

Sections: A funding, B currency, C geo, D cardinalities (+ D8 pairs), E coordinators, F works sizing, G pdf/landing, H projects text, I topics, J minorities, K organisations, L sanity, M export dry run, **Surprises**, Appendix (prep SQL, macros, job list). Where a title says "job" the slurm id and the runtime are printed per section (`_N s_` under each query is the DuckDB time, 0 s = < 0.5 s).


---

# A. Funding streams / grants

**Design meaning (A).**
- `grants` index = **6,074 docs** (distinct non-NULL `fundingStream.id`) + 1 bucket for the 318,401 funding entries whose stream id is NULL (NSF 109,992, FCT 56,595, NWO 40,597, AKA 32,292, ANR 27,760, NIH 21,669 ...). Tiny, 1 shard, export = 0.2 MB.
- **Funder key for D17 = `fundings[].shortName`**: never NULL/empty, 103 distinct values (also 103 after `upper()`). Level 1 of the stream id is NOT usable alone: NULL for 293k projects, plus variants (`tubitak` vs `TUBITAK`, `INCA` vs `INCa`, `100010414` for HRB). 103 funders, 26 jurisdictions, 75 distinct level-1 ids.
- **Programme = level 2 of the stream id** (`H2020`, `FP7`, `HE`, `ERASMUS+`, `EPSRC`, ...): 4,552 distinct; identical to the distinct `frameworkProgrammes` values (4,552), so `frameworkProgrammes` is the programme level for free. It is NULL for the 293k projects without stream id. A programme facet needs a typeahead/top-N picker (4.5k values); only EC has a small clean hierarchy (ERASMUS+ 39 streams, H2020 66, FP7 24, HE 59; 188 EC streams).
- Max depth 4 (`funder::programme::action::subaction`); 4,183 streams have depth 2, 1,345 depth 3, 546 depth 4.
- A project has 1 funder except 3 projects with 2. 143,396 projects (3.7%) have >1 funding entry (mostly 2 streams of the same funder), 2,056 have none. Store `funder` as keyword array anyway.
- `&amp;` HTML entities sit in 156 stream ids (195,309 entries), e.g. NIH child-health institute exists twice; unescape when building `grants`.

slurm job `8981166`

### A1 counts of distinct stream ids / names / shortNames / jurisdictions

```sql
SELECT count(*) fundings_entries, count(DISTINCT project_id) projects_with_funding, count(DISTINCT sid) streams, count(DISTINCT fname) n_names, count(DISTINCT short) n_shortnames, count(DISTINCT jur) n_jurisdictions,
 count(*) FILTER (WHERE sid IS NULL) null_sid FROM pfl
```

| fundings_entries | projects_with_funding | streams | n_names | n_shortnames | n_jurisdictions | null_sid |
|---|---|---|---|---|---|---|
| 4,042,458 | 3,891,009 | 6074 | 104 | 103 | 26 | 318,401 |

_0 s_

### A2 levels of fundingStream.id (split on '::')

```sql
SELECT count(DISTINCT l1) distinct_level1_funder, count(DISTINCT (l1,l2)) distinct_l1_l2_programme, count(DISTINCT l2) distinct_level2_alone, count(DISTINCT (l1,l2,l3)) distinct_l1_l2_l3, max(depth) max_depth FROM pfl
```

| distinct_level1_funder | distinct_l1_l2_programme | distinct_level2_alone | distinct_l1_l2_l3 | max_depth |
|---|---|---|---|---|
| 75 | 4610 | 4552 | 5724 | 4 |

_0 s_

### A2b depth distribution

```sql
SELECT depth, count(DISTINCT sid) streams, count(*) entries FROM pfl GROUP BY depth ORDER BY depth
```

| depth | streams | entries |
|---|---|---|
| 2 | 4183 | 2,982,629 |
| 3 | 1345 | 626,938 |
| 4 | 546 | 114,490 |
| None | 0 | 318,401 |

_0 s_

### A3 top 50 streams by project count

```sql
SELECT sid, any_value(descr) description, any_value(short) shortName, any_value(jur) jurisdiction, count(DISTINCT project_id) projects FROM pfl GROUP BY sid ORDER BY projects DESC LIMIT 50
```

| sid | description | shortName | jurisdiction | projects |
|---|---|---|---|---|
| None | None | NIH | US | 318,399 |
| NIH::NATIONAL_CANCER_INSTITUTE | NATIONAL CANCER INSTITUTE | NIH | US | 288,456 |
| NIH::NATIONAL_INSTITUTE_OF_GENERAL_MEDICAL_SCIENCES | NATIONAL INSTITUTE OF GENERAL MEDICAL SCIENCES | NIH | US | 231,417 |
| NIH::NATIONAL_HEART,_LUNG,_AND_BLOOD_INSTITUTE | NATIONAL HEART, LUNG, AND BLOOD INSTITUTE | NIH | US | 217,261 |
| NIH::NATIONAL_INSTITUTE_OF_ALLERGY_AND_INFECTIOUS_DISEASES | NATIONAL INSTITUTE OF ALLERGY AND INFECTIOUS DISEASES | NIH | US | 214,742 |
| NIH::NATIONAL_INSTITUTE_OF_DIABETES_AND_DIGESTIVE_AND_KIDNEY_DISEASES | NATIONAL INSTITUTE OF DIABETES AND DIGESTIVE AND KIDNEY DISEASES | NIH | US | 168,044 |
| NIH::NATIONAL_INSTITUTE_OF_NEUROLOGICAL_DISORDERS_AND_STROKE | NATIONAL INSTITUTE OF NEUROLOGICAL DISORDERS AND STROKE | NIH | US | 155,841 |
| NIH::NATIONAL_INSTITUTE_OF_MENTAL_HEALTH | NATIONAL INSTITUTE OF MENTAL HEALTH | NIH | US | 129,131 |
| NIH::NATIONAL_INSTITUTE_ON_AGING | NATIONAL INSTITUTE ON AGING | NIH | US | 101,012 |
| NIH::NATIONAL_INSTITUTE_ON_DRUG_ABUSE | NATIONAL INSTITUTE ON DRUG ABUSE | NIH | US | 82,242 |
| NIH::EUNICE_KENNEDY_SHRIVER_NATIONAL_INSTITUTE_OF_CHILD_HEALTH_&amp;HUMAN_DEVELOPMENT | EUNICE KENNEDY SHRIVER NATIONAL INSTITUTE OF CHILD HEALTH &amp;HUMAN DEVELOPMENT | NIH | US | 71,571 |
| NIH::NATIONAL_EYE_INSTITUTE | NATIONAL EYE INSTITUTE | NIH | US | 67,583 |
| NIH::EUNICE_KENNEDY_SHRIVER_NATIONAL_INSTITUTE_OF_CHILD_HEALTH_&amp;_HUMAN_DEVELOPMENT | EUNICE KENNEDY SHRIVER NATIONAL INSTITUTE OF CHILD HEALTH &amp; HUMAN DEVELOPMENT | NIH | US | 53,938 |
| NIH::NATIONAL_INSTITUTE_OF_ARTHRITIS_AND_MUSCULOSKELETAL_AND_SKIN_DISEASES | NATIONAL INSTITUTE OF ARTHRITIS AND MUSCULOSKELETAL AND SKIN DISEASES | NIH | US | 52,921 |
| NIH::NATIONAL_INSTITUTE_OF_ENVIRONMENTAL_HEALTH_SCIENCES | NATIONAL INSTITUTE OF ENVIRONMENTAL HEALTH SCIENCES | NIH | US | 47,710 |
| UKRI::EPSRC | EPSRC | UKRI | GB | 44,100 |
| SNSF::Projects::Project funding | Projects - Project funding | SNSF | CH | 40,041 |
| NIH::NATIONAL_INSTITUTE_ON_ALCOHOL_ABUSE_AND_ALCOHOLISM | NATIONAL INSTITUTE ON ALCOHOL ABUSE AND ALCOHOLISM | NIH | US | 39,999 |
| NIH::NATIONAL_INSTITUTE_ON_DEAFNESS_AND_OTHER_COMMUNICATION_DISORDERS | NATIONAL INSTITUTE ON DEAFNESS AND OTHER COMMUNICATION DISORDERS | NIH | US | 39,771 |
| NSF::MPS/OAD::MPS/DMS | Directorate for Mathematical &amp; Physical Sciences - Division of Mathematical Sciences | NSF | US | 38,395 |
| UKRI::Innovate UK | Innovate UK | UKRI | GB | 35,028 |
| NIH::NATIONAL_CENTER_FOR_RESEARCH_RESOURCES | NATIONAL CENTER FOR RESEARCH RESOURCES | NIH | US | 34,569 |
| NIH::VETERANS_AFFAIRS | VETERANS AFFAIRS | NIH | US | 25,048 |
| NIH::NATIONAL_INSTITUTE_OF_DENTAL_&amp;CRANIOFACIAL_RESEARCH | NATIONAL INSTITUTE OF DENTAL &amp;CRANIOFACIAL RESEARCH | NIH | US | 24,466 |
| NSF::GEO/OAD::GEO/EAR | Directorate for Geosciences - Division of Earth Sciences | NSF | US | 23,926 |
| VINNOVA::Project grant | Project grant | VINNOVA | SE | 23,922 |
| NSF::ENG/OAD::ENG/CMMI | Directorate for Engineering - Division of Civil, Mechanical &amp; Manufacturing Innovation | NSF | US | 22,605 |
| NSF::ENG/OAD::ENG/CBET | Directorate for Engineering - Division of Chemical, Bioengineering, Environmental, and Transport Systems | NSF | US | 22,364 |
| UKRI::BBSRC | BBSRC | UKRI | GB | 21,523 |
| NSF::MPS/OAD::MPS/CHE | Directorate for Mathematical &amp; Physical Sciences - Division of Chemistry | NSF | US | 21,503 |
| NIH::NATIONAL_INSTITUTE_OF_BIOMEDICAL_IMAGING_AND_BIOENGINEERING | NATIONAL INSTITUTE OF BIOMEDICAL IMAGING AND BIOENGINEERING | NIH | US | 20,750 |
| NSF::EHR/OAD::EHR/DUE | Directorate for Education &amp; Human Resources - Division of Undergraduate Education | NSF | US | 20,680 |
| NSF::GEO/OAD::GEO/OCE | Directorate for Geosciences - Division of Ocean Sciences | NSF | US | 20,611 |
| NIH::SUBSTANCE_ABUSE_AND_MENTAL_HEALTH_SERVICES_ADMINISTRATION | SUBSTANCE ABUSE AND MENTAL HEALTH SERVICES ADMINISTRATION | NIH | US | 20,025 |
| NSF::MPS/OAD::MPS/DMR | Directorate for Mathematical &amp; Physical Sciences - Division of Materials Research | NSF | US | 19,606 |
| NSF::SBE/OAD::SBE/BCS | Directorate for Social, Behavioral &amp; Economic Sciences - Division of Behavioral and Cognitive Sciences | NSF | US | 19,314 |
| NSF::OD::OD/OIA | Office of the Director - Office of Integrative Activities | NSF | US | 19,209 |
| NSF::BIO/OAD::BIO/DEB | Directorate for Biological Sciences - Division of Environmental Biology | NSF | US | 18,711 |
| UKRI::MRC | MRC | UKRI | GB | 18,691 |
| VR::Project grant | Project grant | VR | SE | 18,585 |
| NSF::SBE/OAD::SBE/SES | Directorate for Social, Behavioral &amp; Economic Sciences - Division of Social and Economic Sciences | NSF | US | 17,856 |
| GA0::GA | GA | GA0 | CZ | 17,739 |
| NIH::NATIONAL_HUMAN_GENOME_RESEARCH_INSTITUTE | NATIONAL HUMAN GENOME RESEARCH INSTITUTE | NIH | US | 17,520 |
| NIH::NATIONAL_INSTITUTE_OF_DENTAL_&amp;_CRANIOFACIAL_RESEARCH | NATIONAL INSTITUTE OF DENTAL &amp; CRANIOFACIAL RESEARCH | NIH | US | 17,421 |
| NSF::BIO/OAD::BIO/IOS | Directorate for Biological Sciences - Division of Integrative Organismal Systems | NSF | US | 17,128 |
| ARC::Discovery Projects | Discovery Projects | ARC | AU | 17,125 |
| DFG::f9bf162fb5082d223d8b42678b1a28d2 | Sachbeihilfen | DFG | DE | 17,114 |
| UKRI::NERC | NERC | UKRI | GB | 16,991 |
| NSF::BIO/OAD::BIO/MCB | Directorate for Biological Sciences - Division of Molecular &amp; Cellular Biosciences | NSF | US | 16,447 |
| NSF::CISE/OAD::CISE/CCF | Directorate for Computer &amp; Information Science &amp; Engineering - Division of Computing and Communication Foundations | NSF | US | 16,088 |

_0 s_

### A4 EC hierarchy: level2 (programme) with projects and #streams below

```sql
SELECT l2 programme, count(DISTINCT sid) streams, count(DISTINCT project_id) projects FROM pfl WHERE l1='EC' GROUP BY l2 ORDER BY projects DESC
```

| programme | streams | projects |
|---|---|---|
| ERASMUS+ | 39 | 44,461 |
| H2020 | 66 | 35,434 |
| FP7 | 24 | 25,891 |
| HE | 59 | 22,788 |

_0 s_

### A4b EC hierarchy: all streams (top 80 by projects)

```sql
SELECT sid, any_value(descr) description, count(DISTINCT project_id) projects FROM pfl WHERE l1='EC' GROUP BY sid ORDER BY projects DESC
```

| sid | description | projects |
|---|---|---|
| EC::FP7::SP3::PEOPLE | SEVENTH FRAMEWORK PROGRAMME - SP3-People - Marie-Curie Actions | 11,129 |
| EC::H2020::MSCA-IF-EF-ST | Horizon 2020 Framework Programme - Standard European Fellowships | 7262 |
| EC::HE::HORIZON-TMA-MSCA-PF-EF | Horizon Europe Framework Programme - HORIZON TMA MSCA Postdoctoral Fellowships - European Fellowships | 6264 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::School Exchange Partnerships | ERASMUS+ - Cooperation for innovation and the exchange of good practices - School Exchange Partnerships | 5789 |
| EC::FP7::SP2::ERC | SEVENTH FRAMEWORK PROGRAMME - SP2-Ideas - ERC | 4561 |
| EC::HE::ERC::HORIZON-ERC | Horizon Europe Framework Programme - European Research Council - HORIZON ERC Grants | 4517 |
| EC::H2020::SME-1 | Horizon 2020 Framework Programme - SME instrument phase 1 | 4234 |
| EC::H2020::RIA | Horizon 2020 Framework Programme - Research and Innovation action | 3981 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for vocational education and training | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for vocational education and training | 3136 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for adult education | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for adult education | 2954 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for school education | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for school education | 2924 |
| EC::H2020::ERC::ERC-STG | Horizon 2020 Framework Programme - European Research Council - Starting Grant | 2771 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Small-scale partnerships in school education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Small-scale partnerships in school education | 2625 |
| EC::HE::HORIZON-RIA | Horizon Europe Framework Programme - HORIZON  Research and Innovation Actions | 2617 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for Schools Only | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for Schools Only | 2566 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Small-scale partnerships in adult education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Small-scale partnerships in adult education | 2330 |
| EC::FP7::SP1::ICT | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Information and Communication Technologies | 2328 |
| EC::H2020::CSA | Horizon 2020 Framework Programme - Coordination and support action | 2317 |
| EC::H2020::ERC::ERC-COG | Horizon 2020 Framework Programme - European Research Council - Consolidator Grant | 2254 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for youth | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for youth | 2247 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Small-scale partnerships in youth | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Small-scale partnerships in youth | 2157 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Cooperation partnerships in school education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Cooperation partnerships in school education | 1874 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Cooperation partnerships in vocational education and training | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Cooperation partnerships in vocational education and training | 1836 |
| EC::H2020::IA | Horizon 2020 Framework Programme - Innovation action | 1763 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Cooperation partnerships in youth | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Cooperation partnerships in youth | 1737 |
| EC::HE::HORIZON-EIC-ACC | Horizon Europe Framework Programme - HORIZON EIC Accelerator | 1713 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships for higher education | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships for higher education | 1677 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Cooperation partnerships in higher education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Cooperation partnerships in higher education | 1658 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Small-scale partnerships in vocational education and training | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Small-scale partnerships in vocational education and training | 1607 |
| EC::H2020::ERC::ERC-ADG | Horizon 2020 Framework Programme - European Research Council - Advanced Grant | 1584 |
| EC::HE::HORIZON-CSA | Horizon Europe Framework Programme - HORIZON Coordination and Support Actions | 1442 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Cooperation partnerships in adult education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Cooperation partnerships in adult education | 1408 |
| EC::HE::HORIZON-AG | Horizon Europe Framework Programme - HORIZON Action Grant Budget-Based | 1328 |
| EC::HE::HORIZON-AG-UN | Horizon Europe Framework Programme - HORIZON Unit Grant | 1218 |
| EC::H2020::SME-2 | Horizon 2020 Framework Programme - SME instrument phase 2 | 1208 |
| EC::H2020::MSCA-IF-GF | Horizon 2020 Framework Programme - Global Fellowships | 1160 |
| EC::HE::ERC::HORIZON-ERC-POC | Horizon Europe Framework Programme - European Research Council - HORIZON ERC Proof of Concept Grants | 1067 |
| EC::HE::HORIZON-IA | Horizon Europe Framework Programme - HORIZON Innovation Actions | 1059 |
| EC::FP7::SP4::SME | SEVENTH FRAMEWORK PROGRAMME - SP4-Capacities - Research for the benefit of SMEs | 1036 |
| EC::FP7::SP1::HEALTH | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Health | 1008 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Capacity Building in higher education | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Capacity Building in higher education | 902 |
| EC::FP7::SP1::NMP | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Nanosciences, Nanotechnologies, Materials and new Production Technologies - NMP | 804 |
| EC::H2020::MSCA-ITN-ETN | Horizon 2020 Framework Programme - European Training Networks | 803 |
| EC::HE::ERC::HORIZON-ERC\HORIZON-AG | Horizon Europe Framework Programme - European Research Council - HORIZON-ERC\HORIZON Action Grant Budget-Based | 797 |
| EC::FP7::SP1::SP1-JTI | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Joint Technology Initiatives (Annex IV-SP1) | 795 |
| EC::H2020::ERC::ERC-POC | Horizon 2020 Framework Programme - European Research Council - Proof of Concept Grant | 760 |
| EC::H2020::MSCA-IF-EF-RI | Horizon 2020 Framework Programme - Reintegration panel | 725 |
| EC::FP7::SP1::TPT | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Transport (including Aeronautics) | 719 |
| EC::HE::HORIZON-TMA-MSCA-PF-GF | Horizon Europe Framework Programme - HORIZON TMA MSCA Postdoctoral Fellowships - Global Fellowships | 702 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Capacity Building in higher education | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Capacity Building in higher education | 680 |
| EC::HE::HORIZON-TMA-MSCA-PF-EF\HORIZON-AG-UN | Horizon Europe Framework Programme - HORIZON-TMA-MSCA-PF-EF\HORIZON Unit Grant | 649 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Partnerships for Digital Education Readiness | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Partnerships for Digital Education Readiness | 632 |
| EC::HE::HORIZON-EIC | Horizon Europe Framework Programme - HORIZON EIC Grants | 628 |
| EC::H2020::MSCA-RISE | Horizon 2020 Framework Programme - RISE | 586 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Partnerships for Creativity | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Partnerships for Creativity | 572 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Capacity Building for youth in ACP countries, Latin America and Asia | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Capacity Building for youth in ACP countries, Latin America and Asia | 522 |
| EC::FP7::SP1::KBBE | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Food, Agriculture and Fisheries, and Biotechnology | 516 |
| EC::HE::HORIZON-EIC-ACC-BF | Horizon Europe Framework Programme - HORIZON EIC Accelerator Blended Finance | 509 |
| EC::HE::HORIZON-TMA-MSCA-DN | Horizon Europe Framework Programme - HORIZON TMA MSCA Doctoral Networks | 507 |
| EC::FP7::SP1::ENV | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Environment (including Climate Change) | 495 |
| EC::HE::HORIZON-JU-RIA | Horizon Europe Framework Programme - HORIZON JU Research and Innovation Actions | 475 |
| EC::H2020::MSCA-IF-EF-CAR | Horizon 2020 Framework Programme - Career Restart panel | 435 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Capacity Building for youth in neighbouring and enlargement countries | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Capacity Building for youth in neighbouring and enlargement countries | 385 |
| EC::FP7::SP1::ENERGY | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Energy | 379 |
| EC::H2020::ERC::ERC-POC-LS | Horizon 2020 Framework Programme - European Research Council - ERC Proof of Concept Lump Sum Pilot | 365 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Erasmus Mundus Design Measures | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Erasmus Mundus Design Measures | 353 |
| EC::H2020::H2020-EEN-SGA | Horizon 2020 Framework Programme - Specific Grant Agreement  Enterprise Europe Network (EEN) | 342 |
| EC::FP7::SP4::INFRA | SEVENTH FRAMEWORK PROGRAMME - SP4-Capacities - Research Infrastructures | 341 |
| EC::H2020::CS2-IA | Horizon 2020 Framework Programme - Innovation action | 333 |
| EC::HE::HORIZON-TMA-MSCA-SE | Horizon Europe Framework Programme - HORIZON TMA MSCA Staff Exchanges | 330 |
| EC::FP7::SP1::SEC | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Security | 321 |
| EC::HE::HORIZON-AG-LS | Horizon Europe Framework Programme - HORIZON Lump Sum Grant | 303 |
| EC::H2020::SME-2b | Horizon 2020 Framework Programme - SME Instrument (grant only and blended finance) | 292 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Capacity Building in Vocational Education and Training | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Capacity Building in Vocational Education and Training | 273 |
| EC::ERASMUS+::Partnerships for cooperation and exchanges of practices::Capacity Building in the field of Youth | ERASMUS+ - Partnerships for cooperation and exchanges of practices - Capacity Building in the field of Youth | 272 |
| EC::FP7::SP1::SPA | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Space | 267 |
| EC::H2020::MSCA-IF-EF-SE | Horizon 2020 Framework Programme - Society and Enterprise panel | 266 |
| EC::FP7::SP1::SSH | SEVENTH FRAMEWORK PROGRAMME - SP1-Cooperation - Socio-economic sciences and Humanities | 253 |
| EC::ERASMUS+::Cooperation for innovation and the exchange of good practices::Strategic Partnerships addressing more than one field | ERASMUS+ - Cooperation for innovation and the exchange of good practices - Strategic Partnerships addressing more than one field | 242 |
| EC::H2020::CS2-RIA | Horizon 2020 Framework Programme - Research and Innovation action | 222 |
| ... 108 more rows |  | 

_0 s_

### A4c EC: count of level-3 values per programme

```sql
SELECT l2, count(DISTINCT l3) level3_values, list(DISTINCT l3)[:12] sample_l3 FROM pfl WHERE l1='EC' GROUP BY l2 ORDER BY count(DISTINCT project_id) DESC LIMIT 20
```

| l2 | level3_values | sample_l3 |
|---|---|---|
| ERASMUS+ | 2 | ['Cooperation for innovation and the exchange of good practices', 'Partnerships for cooperation and exchanges of practices'] |
| H2020 | 60 | ['SME-1', '', 'SESAR-CSA', 'IA-LS', 'Shift2Rail-CSA', 'MSCA-ITN-ETN', 'IA', 'ERC', 'MSCA-IF-GF', 'H2020-EEN-SGA', 'SME-2b', 'RIA-LS'] |
| FP7 | 6 | ['SP5', 'SP2', '', 'SP1', 'SP4', 'SP3'] |
| HE | 54 | ['HORIZON-CSA\\HORIZON-AG', 'HORIZON-TMA-MSCA-PF-GF\\HORIZON-AG-UN', 'ERC', 'EURATOM-CSA', 'HORIZON-TMA-MSCA-DN-JD\\HORIZON-AG-UN', 'MSCA-PF', 'HORIZON-TMA-MSCA-DN', 'HORIZON-TMA-MSCA-PF-EF\\HORIZON-AG-UN', 'HORIZON-AG-LS', 'HORIZON-RIA', 'HORIZON-AG', 'HORIZON-EIC'] |

_0 s_

### A2c funder key check: level1 vs fundings.shortName (rows where they differ)

```sql
SELECT l1, short, count(*) entries, count(DISTINCT project_id) projects FROM pfl WHERE coalesce(l1,'') <> coalesce(short,'') GROUP BY 1,2 ORDER BY 4 DESC LIMIT 30
```

| l1 | short | entries | projects |
|---|---|---|---|
| None | NSF | 109,992 | 109,992 |
| None | FCT | 56,595 | 56,595 |
| None | NWO | 40,597 | 40,597 |
| None | AKA | 32,292 | 32,292 |
| None | ANR | 27,760 | 27,760 |
| None | NIH | 21,669 | 21,669 |
| tubitak | TUBITAK | 16,609 | 16,609 |
| None | FCF | 4441 | 4441 |
| None | FORMAS | 3459 | 3459 |
| None | GSRI | 3244 | 3244 |
| None | FORTE | 2656 | 2656 |
| None | WT | 2443 | 2443 |
| None | MZOS | 2388 | 2388 |
| None | IBF | 2264 | 2264 |
| INCA | INCa | 2205 | 2205 |
| None | HRZZ | 1911 | 1911 |
| 100010414 | HRB | 1890 | 1890 |
| None | KF | 1362 | 1362 |
| None | RIF | 1164 | 1164 |
| None | SK | 773 | 773 |
| None | SNSA | 443 | 443 |
| None | HFRI | 377 | 377 |
| None | SNSF | 331 | 331 |
| None | KAUTE | 297 | 297 |
| None | KI | 286 | 286 |
| None | PF | 171 | 171 |
| None | MVT | 160 | 160 |
| None | VR | 151 | 151 |
| None | MTNF | 146 | 146 |
| None | FMF | 144 | 144 |

_0 s_

### A2d distinct funders after fallback; funders with NULL/empty everything

```sql
SELECT count(DISTINCT funder) distinct_funders, count(DISTINCT lower(funder)) distinct_funders_lowercased, count(*) FILTER (WHERE funder IS NULL) entries_without_funder, count(*) FILTER (WHERE sid LIKE '%&amp;%') streams_with_html_entity FROM pfl
```

| distinct_funders | distinct_funders_lowercased | entries_without_funder | streams_with_html_entity |
|---|---|---|---|
| 103 | 103 | 0 | 195,309 |

_0 s_

### A2f candidate funder key = fundings.shortName: NULL/empty count, distinct, and distinct after upper()

```sql
SELECT count(*) entries, count(*) FILTER (WHERE short IS NULL OR short='') null_or_empty_short, count(DISTINCT short) distinct_short, count(DISTINCT upper(short)) distinct_upper_short, count(DISTINCT fname) distinct_name,
 count(*) FILTER (WHERE upper(short) <> upper(coalesce(nullif(l1,''),short))) differs_from_l1_caseinsens FROM pfl
```

| entries | null_or_empty_short | distinct_short | distinct_upper_short | distinct_name | differs_from_l1_caseinsens |
|---|---|---|---|---|---|
| 4,042,458 | 0 | 103 | 103 | 104 | 1891 |

_0 s_

### A2g funder key = shortName: top 30 with name, jurisdiction, streams

```sql
SELECT short, any_value(fname) AS funder_name, any_value(jur) AS jurisdiction, count(DISTINCT sid) AS streams, count(DISTINCT project_id) projects FROM pfl GROUP BY 1 ORDER BY 5 DESC LIMIT 30
```

| short | funder_name | jurisdiction | streams | projects |
|---|---|---|---|---|
| NIH | National Institutes of Health | US | 132 | 2,343,782 |
| NSF | National Science Foundation | US | 100 | 601,434 |
| UKRI | UK Research and Innovation | GB | 30 | 175,092 |
| EC | European Commission | EU | 188 | 128,574 |
| SNSF | Swiss National Science Foundation | CH | 233 | 92,757 |
| FCT | Fundação para a Ciência e a Tecnologia, I.P. | PT | 481 | 89,867 |
| NWO | Netherlands Organisation for Scientific Research (NWO) | NL | 2438 | 47,080 |
| NHMRC | National Health and Medical Research Council (NHMRC) | AU | 43 | 33,217 |
| AKA | Research Council of Finland | FI | 0 | 32,292 |
| ARC | Australian Research Council (ARC) | AU | 52 | 32,238 |
| DFG | Deutsche Forschungsgemeinschaft | DE | 34 | 32,198 |
| ANR | French National Research Agency (ANR) | FR | 0 | 27,760 |
| RCN | The Research Council of Norway | NO | 1003 | 25,208 |
| VINNOVA | Swedish Governmental Agency for Innovation Systems | SE | 1 | 23,922 |
| VR | Swedish Research Council | SE | 5 | 22,095 |
| GA0 | Czech Science Foundation | CZ | 13 | 21,619 |
| WT | Wellcome Trust |  | 37 | 21,291 |
| FWF | Austrian Science Fund (FWF) | AT | 91 | 19,983 |
| TUBITAK | Türkiye Bilimsel ve Teknolojik Araştırma Kurumu | TR | 22 | 16,609 |
| MSM | Ministry of Education, Youth and Sports | CZ | 80 | 11,357 |
| FORMAS | Swedish Research Council for Environment, Agricultural Sciences and Spatial Planning | SE | 3 | 7284 |
| SFI | Science Foundation Ireland | IE | 376 | 7252 |
| MZ0 | Ministry of Health | CZ | 25 | 6248 |
| MPO | Ministry of Industry and Trade | CZ | 22 | 5803 |
| TA0 | Technology Agency of the Czech Republic | CZ | 24 | 5496 |
| STEM | The Swedish Energy Agency | SE | 1 | 5386 |
| FCF | The Finnish Cultural Foundation | FI | 0 | 4441 |
| IRFD | Independent Research Fund Denmark | DK | 53 | 4018 |
| AV0 | Czech Academy of Sciences | CZ | 9 | 3666 |
| GSRI | General Secretariat of Research and Innovation (GSRI) | GR | 0 | 3244 |

_0 s_

### A2e streams with HTML entities (&amp;) that duplicate a clean stream

```sql
SELECT count(DISTINCT sid) streams_with_amp, count(DISTINCT replace(sid,'&amp;','&')) after_unescape FROM pfl WHERE sid LIKE '%&amp;%'
```

| streams_with_amp | after_unescape |
|---|---|
| 156 | 156 |

_0 s_

### A5 projects without funding / number of funding entries

```sql
SELECT coalesce(len(fundings),-1) AS n_fundings_entries, count(*) projects FROM project GROUP BY 1 ORDER BY 1
```

| n_fundings_entries | projects |
|---|---|
| 0 | 2056 |
| 1 | 3,747,613 |
| 2 | 136,084 |
| 3 | 6753 |
| 4 | 456 |
| 5 | 37 |
| 6 | 53 |
| 7 | 13 |

_0 s_

-1 = fundings IS NULL

### A5b distinct funders (level1) per project

```sql
SELECT n_funders, count(*) projects FROM (SELECT project_id, count(DISTINCT funder) n_funders FROM pfl GROUP BY project_id) GROUP BY 1 ORDER BY 1
```

| n_funders | projects |
|---|---|
| 1 | 3,891,006 |
| 2 | 3 |

_0 s_

### A5c projects with several distinct streams

```sql
SELECT n_streams, count(*) projects FROM (SELECT project_id, count(DISTINCT sid) n_streams FROM pfl GROUP BY project_id) GROUP BY 1 ORDER BY 1
```

| n_streams | projects |
|---|---|
| 0 | 292,993 |
| 1 | 3,479,691 |
| 2 | 111,228 |
| 3 | 6660 |
| 4 | 334 |
| 5 | 37 |
| 6 | 53 |
| 7 | 13 |

_0 s_

### A6 grantId / frameworkProgrammes

```sql
SELECT count(*) projects, count(grantId) with_grantId, count(DISTINCT grantId) distinct_grantId, count(*) FILTER (WHERE frameworkProgrammes IS NOT NULL AND len(frameworkProgrammes)>0) with_fp,
 (SELECT count(DISTINCT x) FROM (SELECT unnest(frameworkProgrammes) x FROM project)) distinct_fp,
 (SELECT count(DISTINCT frameworkProgrammes) FROM project) distinct_fp_lists FROM project
```

| projects | with_grantId | distinct_grantId | with_fp | distinct_fp | distinct_fp_lists |
|---|---|---|---|---|---|
| 3,893,065 | 3,893,065 | 3,849,410 | 3,598,016 | 4552 | 4776 |

_0 s_

### A6b top 30 frameworkProgrammes values

```sql
SELECT x AS frameworkProgramme, count(*) projects FROM (SELECT unnest(frameworkProgrammes) x FROM project) GROUP BY 1 ORDER BY 2 DESC LIMIT 30
```

| frameworkProgramme | projects |
|---|---|
| NATIONAL_CANCER_INSTITUTE | 288,456 |
| NATIONAL_INSTITUTE_OF_GENERAL_MEDICAL_SCIENCES | 231,417 |
| NATIONAL_HEART,_LUNG,_AND_BLOOD_INSTITUTE | 217,261 |
| NATIONAL_INSTITUTE_OF_ALLERGY_AND_INFECTIOUS_DISEASES | 214,742 |
| NATIONAL_INSTITUTE_OF_DIABETES_AND_DIGESTIVE_AND_KIDNEY_DISEASES | 168,044 |
| NATIONAL_INSTITUTE_OF_NEUROLOGICAL_DISORDERS_AND_STROKE | 155,841 |
| NATIONAL_INSTITUTE_OF_MENTAL_HEALTH | 129,131 |
| MPS/OAD | 101,751 |
| NATIONAL_INSTITUTE_ON_AGING | 101,012 |
| NATIONAL_INSTITUTE_ON_DRUG_ABUSE | 82,242 |
| ENG/OAD | 77,521 |
| EUNICE_KENNEDY_SHRIVER_NATIONAL_INSTITUTE_OF_CHILD_HEALTH_&amp;HUMAN_DEVELOPMENT | 71,571 |
| GEO/OAD | 68,448 |
| NATIONAL_EYE_INSTITUTE | 67,583 |
| BIO/OAD | 65,693 |
| CISE/OAD | 61,958 |
| Project grant | 54,829 |
| EUNICE_KENNEDY_SHRIVER_NATIONAL_INSTITUTE_OF_CHILD_HEALTH_&amp;_HUMAN_DEVELOPMENT | 53,938 |
| NATIONAL_INSTITUTE_OF_ARTHRITIS_AND_MUSCULOSKELETAL_AND_SKIN_DISEASES | 52,921 |
| NATIONAL_INSTITUTE_OF_ENVIRONMENTAL_HEALTH_SCIENCES | 47,710 |
| Projects | 45,603 |
| ERASMUS+ | 44,461 |
| EPSRC | 44,100 |
| NATIONAL_INSTITUTE_ON_ALCOHOL_ABUSE_AND_ALCOHOLISM | 39,999 |
| NATIONAL_INSTITUTE_ON_DEAFNESS_AND_OTHER_COMMUNICATION_DISORDERS | 39,771 |
| SBE/OAD | 39,283 |
| EHR/OAD | 38,917 |
| H2020 | 35,434 |
| Innovate UK | 35,028 |
| NATIONAL_CENTER_FOR_RESEARCH_RESOURCES | 34,569 |

_0 s_

### A7 top-20 funders (funder = level1 of stream id, fallback fundings.shortName when the stream id is NULL): projects, with fundedAmount>0, sum fundedAmount (raw, mixed currencies; a project with 2 funders counts for both)

```sql
SELECT p.funder, count(*) projects, count(*) FILTER (WHERE pr.granted.fundedAmount>0) with_funded_amount, round(sum(pr.granted.fundedAmount)) sum_fundedAmount_raw, string_agg(DISTINCT pr.granted.currency,',') currencies
FROM (SELECT DISTINCT project_id, funder FROM pfl) p JOIN project pr ON pr.id=p.project_id
GROUP BY p.funder ORDER BY projects DESC LIMIT 20
```

| funder | projects | with_funded_amount | sum_fundedAmount_raw | currencies |
|---|---|---|---|---|
| NIH | 2,343,782 | 1,583,417 | 763,227,967,428 | USD |
| NSF | 601,434 | 81,160 | 39,635,040,590 | USD |
| UKRI | 175,092 | 126,972 | 75,217,739,675 | GBP |
| EC | 128,574 | 102,460 | 153,408,866,540 | EUR |
| SNSF | 92,757 | 92,424 | 25,685,895,526 | None |
| FCT | 89,867 | 32,695 | 4,832,449,535 | EUR |
| NWO | 47,080 | 0 | 0 | None |
| NHMRC | 33,217 | 33,156 | 19,764,174,788 | $,AUD |
| AKA | 32,292 | 32,222 | 7,407,633,459 | EUR |
| ARC | 32,238 | 32,237 | 15,853,302,744 | AUD |
| DFG | 32,198 | 0 | 0 | None |
| ANR | 27,760 | 26,983 | 10,692,618,217 | EUR |
| RCN | 25,208 | 0 | 0 | None |
| VINNOVA | 23,922 | 23,629 | 51,198,291,810 | SEK |
| VR | 22,095 | 21,939 | 88,093,720,305 | SEK |
| GA0 | 21,619 | 0 | 0 | None |
| WT | 21,291 | 15,478 | 18,526,119,192 | INR,NOK,GBP,SGD,USD,AUD,ZAR,CHF,EUR,CAD |
| FWF | 19,983 | 19,955 | 5,441,774,074 | EUR |
| tubitak | 16,609 | 0 | 0 | None |
| MSM | 11,357 | 0 | 0 | None |

_0 s_


_section runtime total: 2 s, job 8981166_



---

# B. Currency and budget

**Design meaning (B).**
- Only **2.27M of 3.89M projects (58%)** have `fundedAmount > 0`; `totalCost` is 0 for 98.77% (useless, do not index/sort on it). `fundedAmount` = 0 for NIH 32%, NSF 86.5%, and 100% for NWO, DFG, RCN, GA0, TUBITAK. A budget sort / funding map is therefore mostly EC, NIH, UKRI, SNSF, VR, VINNOVA, NHMRC, ARC.
- `currency` is NULL for 42.7%; among those, **92,424 projects have a positive amount and NO currency: all SNSF (Swiss, presumably CHF)**. Currencies with positive money: USD, EUR, GBP, SEK, AUD, `$` (6,366), HRK, CHF, INR, ZAR, SGD, IDR, NOK, CAD (+NULL).
- Conversion table needed (D14): **USD, EUR (=1), GBP, SEK, AUD, CHF (for NULL/SNSF)** covers 99.2% of the nominal money and 99.6% of projects; add `$` (map to USD, but NHMRC uses `$` for AUD: 33,156 NHMRC projects carry `$`/`AUD`, decide per funder), HRK. INR/ZAR/NOK/SGD/CAD/IDR are 1-7 projects each. NB nominal sums mix currencies: SEK looks like 14.9% of the money but is roughly 1/10 of that in EUR.
- **Outliers to cap/flag**: four EC projects with fundedAmount = totalCost = 2,500,000,000 EUR (obviously 2.5M, EIC pilot rows), Wellcome INR 3.64 bn, MAX IV SEK 1.55 bn. Clip funded_eur or exclude `> 1e9 EUR` from rollups, otherwise `total_funding_eur` of an org is dominated by them.

slurm job `8981166`

### B1 currency distribution

```sql
SELECT coalesce(granted.currency,'<NULL>') currency, count(*) projects, count(*) FILTER (WHERE granted.fundedAmount>0) with_funded, round(sum(granted.fundedAmount)) sum_funded,
 min(granted.fundedAmount) FILTER (WHERE granted.fundedAmount>0) min_pos, quantile_cont(granted.fundedAmount,0.5) FILTER (WHERE granted.fundedAmount>0) median_pos, max(granted.fundedAmount) max_funded
FROM project GROUP BY 1 ORDER BY 2 DESC
```

| currency | projects | with_funded | sum_funded | min_pos | median_pos | max_funded |
|---|---|---|---|---|---|---|
| USD | 1,669,155 | 1,666,747 | 804,181,497,206 | 1 | 313,850 | 448,283,328 |
| <NULL> | 1,661,421 | 92,424 | 25,685,895,526 | 100 | 131,362 | 32,800,000 |
| EUR | 239,133 | 236,189 | 195,898,331,792 | 0.1 | 199,888 | 2,500,000,000 |
| GBP | 190,321 | 142,266 | 89,510,658,781 | 1 | 206,085 | 729,097,980 |
| SEK | 65,177 | 65,119 | 202,064,575,534 | 1 | 2,000,000 | 1,550,000,000 |
| AUD | 59,061 | 59,033 | 30,013,862,693 | 992 | 345,190 | 300,987,008 |
| $ | 6367 | 6366 | 5,617,528,543 | 4,133 | 642,267 | 25,000,000 |
| HRK | 2413 | 2290 | 2,157,001,441 | 12 | 585,650 | 232,602,000 |
| CHF | 7 | 7 | 3,907,231 | 207,358 | 400,000 | 1,240,900 |
| INR | 4 | 4 | 3,643,023,761 | 23,760.9 | 1,500,000 | 3,640,000,000 |
| ZAR | 2 | 2 | 12,310,860 | 117,360 | 6,155,430 | 12,193,500 |
| SGD | 1 | 1 | 1,467,900 | 1,467,900 | 1,467,900 | 1,467,900 |
| IDR | 1 | 1 | 100,000 | 100,000 | 100,000 | 100,000 |
| CAD | 1 | 1 | 870,800 | 870,800 | 870,800 | 870,800 |
| NOK | 1 | 1 | 11,938,800 | 11,938,800 | 11,938,800 | 11,938,800 |

_0 s_

### B2 shares of NULL / zero

```sql
SELECT count(*) projects, round(100.0*count(*) FILTER (WHERE granted IS NULL)/count(*),2) pct_granted_null, round(100.0*count(*) FILTER (WHERE granted.currency IS NULL)/count(*),2) pct_currency_null,
 round(100.0*count(*) FILTER (WHERE granted.fundedAmount IS NULL)/count(*),2) pct_funded_null, round(100.0*count(*) FILTER (WHERE granted.fundedAmount=0)/count(*),2) pct_funded_zero,
 round(100.0*count(*) FILTER (WHERE granted.fundedAmount IS NULL OR granted.fundedAmount=0)/count(*),2) pct_funded_null_or_zero,
 round(100.0*count(*) FILTER (WHERE granted.totalCost IS NULL)/count(*),2) pct_total_null, round(100.0*count(*) FILTER (WHERE granted.totalCost=0)/count(*),2) pct_total_zero,
 round(100.0*count(*) FILTER (WHERE granted.fundedAmount>0 AND granted.currency IS NULL)/count(*),4) pct_funded_pos_but_no_currency FROM project
```

| projects | pct_granted_null | pct_currency_null | pct_funded_null | pct_funded_zero | pct_funded_null_or_zero | pct_total_null | pct_total_zero | pct_funded_pos_but_no_currency |
|---|---|---|---|---|---|---|---|---|
| 3,893,065 | 0 | 42.68 | 0 | 41.68 | 41.68 | 0 | 98.77 | 2.3741 |

_0 s_

### B3 per top-10 funder: shares

```sql
WITH top AS (SELECT funder FROM (SELECT DISTINCT project_id,funder FROM pfl) GROUP BY funder ORDER BY count(*) DESC LIMIT 10)
SELECT p.funder, count(*) projects, round(100.0*count(*) FILTER (WHERE pr.granted.currency IS NULL)/count(*),1) pct_currency_null, round(100.0*count(*) FILTER (WHERE pr.granted.fundedAmount IS NULL)/count(*),1) pct_funded_null,
 round(100.0*count(*) FILTER (WHERE pr.granted.fundedAmount=0)/count(*),1) pct_funded_zero, round(100.0*count(*) FILTER (WHERE pr.granted.totalCost=0)/count(*),1) pct_total_zero,
 round(100.0*count(*) FILTER (WHERE pr.granted.totalCost IS NULL)/count(*),1) pct_total_null,
 string_agg(DISTINCT coalesce(pr.granted.currency,'NULL'),',') currencies
FROM (SELECT DISTINCT project_id,funder FROM pfl WHERE funder IN (SELECT funder FROM top)) p JOIN project pr ON pr.id=p.project_id GROUP BY 1 ORDER BY 2 DESC
```

| funder | projects | pct_currency_null | pct_funded_null | pct_funded_zero | pct_total_zero | pct_total_null | currencies |
|---|---|---|---|---|---|---|---|
| NIH | 2,343,782 | 32.3 | 0 | 32.4 | 100 | 0 | NULL,USD |
| NSF | 601,434 | 86.5 | 0 | 86.5 | 100 | 0 | NULL,USD |
| UKRI | 175,092 | 0 | 0 | 27.5 | 100 | 0 | NULL,GBP |
| EC | 128,574 | 20.1 | 0 | 20.3 | 63.6 | 0 | EUR,NULL |
| SNSF | 92,757 | 100 | 0 | 0.4 | 100 | 0 | NULL |
| FCT | 89,867 | 63 | 0 | 63.6 | 100 | 0 | NULL,EUR |
| NWO | 47,080 | 100 | 0 | 100 | 100 | 0 | NULL |
| NHMRC | 33,217 | 0.1 | 0 | 0.2 | 100 | 0 | NULL,$,AUD |
| AKA | 32,292 | 0 | 0 | 0.2 | 100 | 0 | EUR |
| ARC | 32,238 | 0 | 0 | 0 | 100 | 0 | NULL,AUD |

_0 s_

### B4 20 largest fundedAmount

```sql
SELECT p.id, p.grantId, left(p.title,60) title, p.granted.currency, p.granted.fundedAmount, p.granted.totalCost, (SELECT string_agg(DISTINCT split_part(x.fundingStream.id,'::',1),',') FROM (SELECT unnest(p.fundings) x)) funders
FROM project p WHERE p.granted.fundedAmount IS NOT NULL ORDER BY p.granted.fundedAmount DESC LIMIT 20
```

| id | grantId | title | currency | fundedAmount | totalCost | funders |
|---|---|---|---|---|---|---|
| 6,062,159,526,990,492,744 | 218696 | DBT Wellcome Trust India Alliance - continuation of jointly- | INR | 3,640,000,000 | 0 | WT |
| 14,932,148,542,716,154,855 | 101254405 | SWEELIN®: REDEFINING SWEETNESS WITH A FOCUS ON TASTE, HEALTH | EUR | 2,500,000,000 | 2,500,000,000 | EC |
| 9,033,498,197,691,690,700 | 101223724 | PARty Headphones: The Future of Immersive Sound by Brandenbu | EUR | 2,500,000,000 | 2,500,000,000 | EC |
| 367,507,073,904,102,154 | 101227975 | Breaking BArriers using Disruptive innate immunotherapy in c | EUR | 2,500,000,000 | 2,500,000,000 | EC |
| 3,128,478,171,967,488,935 | 101314767 | Safe Surgical Control at Depth, Reaching Previously Inaccess | EUR | 2,500,000,000 | 2,500,000,000 | EC |
| 666,917,512,850,592,308 | 2022-06690_VR | Contribution to MAX IV operation 2023-2026 | SEK | 1,550,000,000 | 0 | VR |
| 11,403,246,923,824,564,575 | 2013-02235_VR | MAX IV Operations 2014 - 2018 | SEK | 1,184,999,940 | 0 | VR |
| 8,417,590,340,166,615,087 | 160080 | HIGH VALUE MANUFACTURING CATAPULT CORE DELIVERY PROGRAMME | GBP | 729,097,980 | 0 | UKRI |
| 9,896,036,264,427,665,105 | 160109 | HIGH VALUE MANUFACTURING CATAPULT CORE DELIVERY PROGRAMME | GBP | 728,214,980 | 0 | UKRI |
| 17,240,250,806,148,583,817 | 2009-06551_VR | Memorandum of Understanding on the establishment of MAX IV.  | SEK | 726,691,970 | 0 | VR |
| 17,161,145,885,095,076,075 | CTIN-2021-001 | Cancer Trials Ireland Network | EUR | 694,104,000 | 0 | 100010414 |
| 15,798,256,007,575,993,805 | 633053 | Implementation of activities described in the Roadmap to Fus | EUR | 678,800,000 | 1,329,689,980 | EC |
| 17,292,725,644,459,692,558 | 800000000 | EIT KIC Climate Change Mitigation and Adaptation | EUR | 559,904,000 | 668,486,980 | EC |
| 11,515,012,870,872,919,354 | 800000000 | EIT KIC Climate Change Mitigation and Adaptation | EUR | 559,904,000 | 668,486,980 | EC |
| 11,230,465,580,294,638,076 | 101052200 | Implementation of activities described in the Roadmap to Fus | EUR | 549,441,980 | 999,809,980 | EC |
| 378,673,782,422,211,314 | CRFC-2021-007 | Wellcome-HRB Clinical Research Facility at St James’ Hospita | EUR | 536,483,008 | 0 | 100010414 |
| 16,201,966,350,079,010,352 | CRFC-2021-005 | HRB Clinical Research Facility Cork | EUR | 533,211,008 | 0 | 100010414 |
| 13,111,058,281,098,141,584 | CRFC-2021-002 | UCD Clinical Research Centre | EUR | 532,143,008 | 0 | 100010414 |
| 4,139,346,828,472,371,966 | 800000003 | EIT KIC Sustainable Energy | EUR | 531,028,992 | 627,113,020 | EC |
| 12,909,316,799,501,406,689 | 800000003 | EIT KIC Sustainable Energy | EUR | 531,028,992 | 627,113,020 | EC |

_3 s_

### B4b 10 largest totalCost

```sql
SELECT p.id, left(p.title,60) title, p.granted.currency, p.granted.fundedAmount, p.granted.totalCost FROM project p WHERE p.granted.totalCost IS NOT NULL ORDER BY p.granted.totalCost DESC LIMIT 10
```

| id | title | currency | fundedAmount | totalCost |
|---|---|---|---|---|
| 367,507,073,904,102,154 | Breaking BArriers using Disruptive innate immunotherapy in c | EUR | 2,500,000,000 | 2,500,000,000 |
| 9,033,498,197,691,690,700 | PARty Headphones: The Future of Immersive Sound by Brandenbu | EUR | 2,500,000,000 | 2,500,000,000 |
| 3,128,478,171,967,488,935 | Safe Surgical Control at Depth, Reaching Previously Inaccess | EUR | 2,500,000,000 | 2,500,000,000 |
| 14,932,148,542,716,154,855 | SWEELIN®: REDEFINING SWEETNESS WITH A FOCUS ON TASTE, HEALTH | EUR | 2,500,000,000 | 2,500,000,000 |
| 15,798,256,007,575,993,805 | Implementation of activities described in the Roadmap to Fus | EUR | 678,800,000 | 1,329,689,980 |
| 11,230,465,580,294,638,076 | Implementation of activities described in the Roadmap to Fus | EUR | 549,441,980 | 999,809,980 |
| 6,200,054,431,367,357,056 | European Partnership on Innovative SMEs | EUR | 250,112,992 | 967,441,020 |
| 11,515,012,870,872,919,354 | EIT KIC Climate Change Mitigation and Adaptation | EUR | 559,904,000 | 668,486,980 |
| 17,292,725,644,459,692,558 | EIT KIC Climate Change Mitigation and Adaptation | EUR | 559,904,000 | 668,486,980 |
| 4,139,346,828,472,371,966 | EIT KIC Sustainable Energy | EUR | 531,028,992 | 627,113,020 |

_0 s_

### B5 currencies covering 99% of the money (nominal sum of fundedAmount; sums of mixed currencies are NOT comparable, so also by project count)

```sql
WITH c AS (SELECT coalesce(granted.currency,'<NULL>') cur, sum(granted.fundedAmount) amt, count(*) FILTER (WHERE granted.fundedAmount>0) n FROM project WHERE granted.fundedAmount>0 GROUP BY 1)
SELECT cur, n projects_with_funded, round(amt) nominal_sum, round(100*amt/sum(amt) OVER (),3) pct_nominal, round(100*sum(amt) OVER (ORDER BY amt DESC)/sum(amt) OVER (),3) cum_pct_nominal,
 round(100*n/sum(n) OVER (),3) pct_count, round(100*sum(n) OVER (ORDER BY n DESC)/sum(n) OVER (),3) cum_pct_count_ordered_by_count FROM c ORDER BY amt DESC
```

| cur | projects_with_funded | nominal_sum | pct_nominal | cum_pct_nominal | pct_count | cum_pct_count_ordered_by_count |
|---|---|---|---|---|---|---|
| USD | 1,666,747 | 804,181,497,206 | 59.181 | 59.181 | 73.41 | 73.41 |
| SEK | 65,119 | 202,064,575,534 | 14.87 | 74.051 | 2.868 | 97.018 |
| EUR | 236,189 | 195,942,592,709 | 14.42 | 88.471 | 10.403 | 83.813 |
| GBP | 142,266 | 89,510,658,781 | 6.587 | 95.058 | 6.266 | 90.079 |
| AUD | 59,033 | 30,013,862,693 | 2.209 | 97.267 | 2.6 | 99.618 |
| <NULL> | 92,424 | 25,685,895,526 | 1.89 | 99.158 | 4.071 | 94.15 |
| $ | 6366 | 5,617,528,543 | 0.413 | 99.571 | 0.28 | 99.898 |
| INR | 4 | 3,643,023,761 | 0.268 | 99.839 | 0 | 100 |
| HRK | 2290 | 2,157,001,441 | 0.159 | 99.998 | 0.101 | 99.999 |
| ZAR | 2 | 12,310,860 | 0.001 | 99.999 | 0 | 100 |
| NOK | 1 | 11,938,800 | 0.001 | 100 | 0 | 100 |
| CHF | 7 | 3,907,231 | 0 | 100 | 0 | 100 |
| SGD | 1 | 1,467,900 | 0 | 100 | 0 | 100 |
| CAD | 1 | 870,800 | 0 | 100 | 0 | 100 |
| IDR | 1 | 100,000 | 0 | 100 | 0 | 100 |

_0 s_


_section runtime total: 4 s, job 8981166_



---

# C. Geolocation and regions

**Design meaning (C).**
- Only **63,885 project-connected orgs are geolocated (18.3%)**; 26.5% of the 5.5M project->org relations, **26.4% of all projects (28.7% of projects that have orgs)** have >= 1 geolocated org; for `is_ch` projects 49.2% (60.0% of those with orgs). Weighted by money: in EUR projects 87.9% of the money sits in projects with >= 1 geolocated org (82.9% equal-split), for `is_ch` EUR 89.8% / 81.2%. Unweighted raw sum says 30.0% because the non-EUR (NIH/NSF/SNSF) projects are almost never geolocated. **The funding map is effectively an EU map**; say so in the UI.
- **Region is NULL for 43.3% of project-connected orgs and countryCode NULL for 43.3%** (all orgs: 31.8% / 31.85%): the missing ones are the NIH/NSF-style US institutions (e.g. JOHNS HOPKINS UNIVERSITY, `countryCode` NULL). A `region` filter on organisations silently drops ~150k project-connected orgs; give the filter a `Unknown` bucket or fall back to `address_country`.
- `geolocation_source` split confirms READ_CORE: Cordis 44,740 + ROR 17,941 + core_v2 1,204 = 63,885. All arrays have length 2.

slurm job `8981152`

### C1 region distribution: all orgs vs project-connected orgs

```sql
SELECT coalesce(o.region,'<NULL>') region, count(*) all_orgs, count(*) FILTER (WHERE o.id IN (SELECT org_id FROM pc_orgs)) project_connected,
 round(100.0*count(*) FILTER (WHERE o.id IN (SELECT org_id FROM pc_orgs))/(SELECT count(*) FROM pc_orgs),2) pct_of_project_connected FROM organization o GROUP BY 1 ORDER BY 2 DESC
```

| region | all_orgs | project_connected | pct_of_project_connected |
|---|---|---|---|
| <NULL> | 157,268 | 150,970 | 43.31 |
| Outside Europe | 128,681 | 43,073 | 12.36 |
| Central Europe | 51,578 | 36,316 | 10.42 |
| Southern Europe | 50,683 | 41,817 | 12 |
| Western Europe | 47,509 | 36,507 | 10.47 |
| Northern Europe | 46,643 | 32,651 | 9.37 |
| Eastern Europe | 11,737 | 7244 | 2.08 |

_0 s_

### C2 NULL shares (all orgs / project-connected)

```sql
SELECT grp, n, round(100.0*null_region/n,2) pct_region_null, round(100.0*null_cc/n,2) pct_countryCode_null, round(100.0*null_geo/n,2) pct_geolocation_null FROM (
 SELECT 'all' grp, count(*) n, count(*) FILTER (WHERE region IS NULL) null_region, count(*) FILTER (WHERE countryCode IS NULL) null_cc, count(*) FILTER (WHERE geolocation IS NULL) null_geo FROM organization
 UNION ALL SELECT 'project-connected', count(*), count(*) FILTER (WHERE region IS NULL), count(*) FILTER (WHERE countryCode IS NULL), count(*) FILTER (WHERE geolocation IS NULL) FROM organization WHERE id IN (SELECT org_id FROM pc_orgs))
```

| grp | n | pct_region_null | pct_countryCode_null | pct_geolocation_null |
|---|---|---|---|---|
| all | 494,099 | 31.83 | 31.85 | 65.04 |
| project-connected | 348,578 | 43.31 | 43.33 | 81.67 |

_0 s_

### C3 geolocation_source split for project-connected orgs (check total 63,885)

```sql
SELECT coalesce(geolocation_source,'<NULL>') src, count(*) orgs FROM organization WHERE id IN (SELECT org_id FROM pc_orgs) AND geolocation IS NOT NULL GROUP BY 1
UNION ALL SELECT 'TOTAL geolocated project-connected', count(*) FROM organization WHERE id IN (SELECT org_id FROM pc_orgs) AND geolocation IS NOT NULL
```

| src | orgs |
|---|---|
| ror | 17,941 |
| cordis | 44,740 |
| core_v2 | 1204 |
| TOTAL geolocated project-connected | 63,885 |

_0 s_

### C3b geolocation array length sanity (should be 2)

```sql
SELECT len(geolocation) l, count(*) FROM organization WHERE geolocation IS NOT NULL GROUP BY 1
```

| l | count_star() |
|---|---|
| 2 | 172,739 |

_0 s_

### C4 relation-level and project-level coverage of geolocated orgs

```sql
WITH po AS (SELECT r.project_id, r.org_id, o.geolocation IS NOT NULL AS geo FROM proj_org r JOIN organization o ON o.id=r.org_id),
 pp AS (SELECT project_id, bool_or(geo) any_geo, count(*) n FROM po GROUP BY 1)
SELECT 'all projects' grp, (SELECT count(*) FROM po) relations, round(100.0*(SELECT count(*) FILTER (WHERE geo) FROM po)/(SELECT count(*) FROM po),2) pct_relations_geo,
 (SELECT count(*) FROM project) projects, (SELECT count(*) FROM pp) projects_with_orgs, round(100.0*count(*) FILTER (WHERE any_geo)/(SELECT count(*) FROM project),2) pct_projects_ge1_geo_of_all,
 round(100.0*count(*) FILTER (WHERE any_geo)/count(*),2) pct_projects_ge1_geo_of_with_orgs FROM pp
UNION ALL
SELECT 'is_ch projects', count(*), round(100.0*count(*) FILTER (WHERE geo)/count(*),2), (SELECT count(*) FROM project WHERE is_ch), count(DISTINCT po.project_id), round(100.0*count(DISTINCT po.project_id) FILTER (WHERE geo)/(SELECT count(*) FROM project WHERE is_ch),2),
 round(100.0*count(DISTINCT po.project_id) FILTER (WHERE geo)/count(DISTINCT po.project_id),2) FROM po JOIN project p ON p.id=po.project_id WHERE p.is_ch
```

| grp | relations | pct_relations_geo | projects | projects_with_orgs | pct_projects_ge1_geo_of_all | pct_projects_ge1_geo_of_with_orgs |
|---|---|---|---|---|---|---|
| all projects | 5,500,329 | 26.48 | 3,893,065 | 3,576,835 | 26.36 | 28.69 |
| is_ch projects | 52,246 | 36.55 | 21,280 | 17,463 | 49.21 | 59.97 |

_0 s_

### C5 weighted by granted.fundedAmount (raw, and EUR-only subset)

```sql
WITH pp AS (SELECT r.project_id, bool_or(o.geolocation IS NOT NULL) any_geo, count(*) n, count(*) FILTER (WHERE o.geolocation IS NOT NULL) n_geo FROM proj_org r JOIN organization o ON o.id=r.org_id GROUP BY 1)
SELECT grp, round(sum(amt)) total_funded, round(100*sum(amt) FILTER (WHERE any_geo)/sum(amt),2) pct_money_in_projects_ge1_geo, round(100*sum(amt*n_geo/n)/sum(amt),2) pct_money_geo_equal_split
FROM (SELECT 'all, raw' grp, p.granted.fundedAmount amt, pp.any_geo, pp.n, pp.n_geo FROM project p JOIN pp ON pp.project_id=p.id WHERE p.granted.fundedAmount>0
      UNION ALL SELECT 'EUR only', p.granted.fundedAmount, pp.any_geo, pp.n, pp.n_geo FROM project p JOIN pp ON pp.project_id=p.id WHERE p.granted.fundedAmount>0 AND p.granted.currency='EUR'
      UNION ALL SELECT 'is_ch, EUR only', p.granted.fundedAmount, pp.any_geo, pp.n, pp.n_geo FROM project p JOIN pp ON pp.project_id=p.id WHERE p.granted.fundedAmount>0 AND p.granted.currency='EUR' AND p.is_ch) GROUP BY grp
```

| grp | total_funded | pct_money_in_projects_ge1_geo | pct_money_geo_equal_split |
|---|---|---|---|
| is_ch, EUR only | 3,213,580,797 | 89.82 | 81.2 |
| all, raw | 1,286,120,155,116 | 29.99 | 27.05 |
| EUR only | 190,829,983,434 | 87.93 | 82.89 |

_0 s_


_section runtime total: 1 s, job 8981152_



---

# D. Cardinalities and tails

**Design meaning (D).**
- **Arrays are small**: orgs per project avg 1.41 (all) / 1.54 (with orgs), p50 1, p99 8, p99.9 24, max 380; only 436 projects > 50 orgs and 2 > 200. `org_ids[]` on projects needs no cap. 316,230 projects (8.1%) have **no** org.
- **Works arrays need a cap**: orgs per work tier 0 avg 5.81, p99 41, p99.9 127, **max 2,660** (32,167 tier-0 works > 50, 2,524 > 200); tier 1 avg 2.49, p99 15, max 1,142. Projects per work: avg 1.78 over linked works, p99 9, max 358. Suggest `organisation_ids` cap 100 (keep `org_count`), loses < 0.1% of works.
- Works per project (738,722 projects have >= 1): avg 9.85, p50 3, p90 19, p99 106, max 72,132 (a Deep Drug Discovery project) -> the project's works tab is a filtered query, never an embedded array (D2 confirmed). 3.15M projects have 0 works.
- Projects per org: avg 15.8, p99 100, max 48,337; works per org: only 175,797 orgs have any work, avg 802, p50 12, p99 14.7k, **max 840,577 (CNRS)**. Org rollups are heavy-tailed: use `rank_feature`/log scaling, not raw values.
- **Collaboration size (D11)**: exact `sum(n(n-1)/2)` = **7,348,351 (project, org-pair) edges** (5,954,451 without the 436 projects > 50 orgs; `is_ch`: 169,864); **distinct unordered org pairs = 4,406,144** for projects with <= 200 orgs (+ at most 94,376 from the 2 excluded projects) and **153,616 for `is_ch`** (exact). Only 682,514 pairs share >= 2 projects, 115,654 share >= 5. So a global pair index would be ~4.4-4.5M docs (not "tens of millions" as D11 feared) - still cannot be query-aware; the `is_ch` corpus (154k pairs) is trivially small. The busiest pair (23,469 shared projects) is the same institution stored under two org ids (see Surprises).
- (project, org) relation kinds: only `hasParticipant` (5,460,580 Harvested, 39,749 Inferred by OpenAIRE); no other relType.

slurm job `8981155`

### D1 orgs per project (projects WITH >=1 org; and all 3.89M incl. zeros)

```sql
WITH c AS (SELECT project_id, count(*) n FROM proj_org GROUP BY 1), a AS (SELECT p.id, coalesce(c.n,0) n FROM project p LEFT JOIN c ON c.project_id=p.id)
SELECT 'with >=1 org' grp, count(*) projects, min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx, count(*) FILTER (WHERE n>50) gt50, count(*) FILTER (WHERE n>200) gt200 FROM c
UNION ALL SELECT 'all projects', count(*), min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx, count(*) FILTER (WHERE n>50), count(*) FILTER (WHERE n>200) FROM a
```

| grp | projects | mn | avg | p50 | p90 | p99 | p999 | mx | gt50 | gt200 |
|---|---|---|---|---|---|---|---|---|---|---|
| with >=1 org | 3,576,835 | 1 | 1.538 | 1 | 2 | 8 | 24 | 380 | 436 | 2 |
| all projects | 3,893,065 | 0 | 1.413 | 1 | 2 | 7 | 23 | 380 | 436 | 2 |

_0 s_

### D1b projects with zero orgs

```sql
SELECT count(*) FROM project WHERE id NOT IN (SELECT project_id FROM proj_org)
```

| count_star() |
|---|
| 316,230 |

_0 s_

### D1c 10 projects with most orgs

```sql
SELECT c.project_id, c.n orgs, left(p.title,80) title, p.is_ch FROM (SELECT project_id, count(*) n FROM proj_org GROUP BY 1 ORDER BY 2 DESC LIMIT 10) c JOIN project p ON p.id=c.project_id ORDER BY 2 DESC
```

| project_id | orgs | title | is_ch |
|---|---|---|---|
| 8,303,088,955,748,938,288 | 380 | A Centre for Innovative Manufacturing and Construction | False |
| 1,455,035,746,565,831,797 | 212 | Engineering Innovative Manufacturing Research Centre Renewal | False |
| 5,621,599,005,235,425,352 | 194 | Graphene Flagship Core Project 2 | False |
| 12,608,569,376,282,173,379 | 191 | Graphene Flagship Core Project 3 | False |
| 11,770,413,856,183,930,028 | 188 | Graphene-based disruptive technologies | False |
| 7,279,022,588,314,932,555 | 187 | Graphene Flagship 2D Experimental Pilot Line | False |
| 1,950,747,105,777,585,330 | 180 | Innovative Construction Research Centre (ICRC) | False |
| 4,533,130,943,600,900,689 | 174 | The Digital Creativity Hub | False |
| 16,049,506,796,381,845,148 | 173 | Industrial Decarbonisation Research and Innovation Centre (IDRIC) | False |
| 18,074,802,261,504,481,501 | 163 | Centre for Creativity, Regulation, Enterprise &amp; Technology (CREATe) | True |

_0 s_

### D2 works per project (project->product)

```sql
WITH c AS (SELECT project_id, count(*) n FROM proj_work GROUP BY 1), a AS (SELECT p.id, coalesce(c.n,0) n FROM project p LEFT JOIN c ON c.project_id=p.id)
SELECT 'with >=1 work' grp, count(*) projects, min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx FROM c UNION ALL SELECT 'all projects', count(*), min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx FROM a
```

| grp | projects | mn | avg | p50 | p90 | p99 | p999 | mx |
|---|---|---|---|---|---|---|---|---|
| with >=1 work | 738,722 | 1 | 9.853 | 3 | 19 | 106 | 442.558 | 72,132 |
| all projects | 3,893,065 | 0 | 1.87 | 0 | 3 | 32 | 155 | 72,132 |

_0 s_

### D2b top 20 projects by works

```sql
SELECT c.project_id, c.n works, left(p.title,70) title, p.granted.fundedAmount funded FROM (SELECT project_id, count(*) n FROM proj_work GROUP BY 1 ORDER BY 2 DESC LIMIT 20) c JOIN project p ON p.id=c.project_id ORDER BY 2 DESC
```

| project_id | works | title | funded |
|---|---|---|---|
| 1,310,287,006,550,409,184 | 72,132 | Deep Drug Discovery and Deployment | 237,876 |
| 14,837,555,135,748,487,481 | 25,320 | Incentive - LA 1 - 2013 | 14,500 |
| 14,490,549,588,302,915,675 | 16,212 | MS3: New foundations for micro-services and serverless systems | 245,535 |
| 15,798,256,007,575,993,805 | 9787 | Implementation of activities described in the Roadmap to Fusion during | 678,800,000 |
| 9,355,434,136,741,739,331 | 8676 | MOUSE GENETICS | 0 |
| 3,056,152,542,409,342,692 | 6788 | Incentive - LA 2 - 2013 | 48,628 |
| 10,804,060,711,421,320,240 | 5497 | Cancer Center Support (CORE) Grant | 200,000 |
| 14,198,665,381,584,943,381 | 5141 | Caenorhabditis Genetics Center | 433,613 |
| 11,404,397,613,419,998,595 | 4425 | Horizon21: Early language development in Down Syndrome | 166,360 |
| 13,121,500,838,571,204,011 | 4391 | Incentive - LA 3 - 2013 | 18,681 |
| 7,544,250,119,780,077,109 | 4294 | Alzheimers Disease Neuroimaging Initiative | 8,750,000 |
| 11,230,465,580,294,638,076 | 4148 | Implementation of activities described in the Roadmap to Fusion during | 549,441,980 |
| 17,497,561,568,045,613,693 | 4125 | Coordination reactions of macrocyclic ligands in solution | 0 |
| 12,785,403,361,783,632,430 | 4050 | Advanced Multi-Variate Analysis for New Physics Searches at the LHC | 2,393,360 |
| 8,439,839,083,650,483,063 | 3966 | The strong interaction at the frontier of knowledge: fundamental resea | 10,000,000 |
| 12,286,170,279,808,851,460 | 3082 | Kavli Institute for Theoretical Physics | 0 |
| 3,424,887,628,765,168,781 | 2669 | Incentive - LA 4 - 2013 | 318,975 |
| 2,155,956,861,846,910,776 | 2550 | International Training Network for Statistics in High Energy Physics a | 3,014,500 |
| 120,897,417,736,412,765 | 2542 | XSEDE: eXtreme Science and Engineering Discovery Environment | 0 |
| 12,749,314,003,607,602,587 | 2362 | Incentive - LA 5 - 2013 | 103,406 |

_0 s_

### D3 projects per work (over works linked to >=1 project)

```sql
WITH c AS (SELECT work_id, count(*) n FROM proj_work GROUP BY 1) SELECT count(*) works, min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx, count(*) FILTER (WHERE n>10) gt10, count(*) FILTER (WHERE n>100) gt100 FROM c
```

| works | mn | avg | p50 | p90 | p99 | p999 | mx | gt10 | gt100 |
|---|---|---|---|---|---|---|---|---|---|
| 4,079,072 | 1 | 1.784 | 1 | 3 | 9 | 25 | 358 | 24,091 | 116 |

_0 s_

### D3b tier check: distinct works in proj_work vs link_tier=0

```sql
SELECT (SELECT count(DISTINCT work_id) FROM proj_work) distinct_works_in_proj_work, (SELECT count(*) FROM work WHERE link_tier=0) tier0, (SELECT count(*) FROM work WHERE link_tier=0 AND id NOT IN (SELECT work_id FROM proj_work)) tier0_not_in_proj_work, (SELECT count(DISTINCT work_id) FROM proj_work WHERE work_id NOT IN (SELECT id FROM work)) proj_work_missing_work
```

| distinct_works_in_proj_work | tier0 | tier0_not_in_proj_work | proj_work_missing_work |
|---|---|---|---|
| 4,079,072 | 5,001,873 | 922,801 | 0 |

_1 s_

### D4 orgs per work, per tier (all 50M works, zeros included)

```sql
WITH c AS (SELECT work_id, count(*) n FROM work_org GROUP BY 1), a AS (SELECT w.link_tier t, coalesce(c.n,0) n FROM work w LEFT JOIN c ON c.work_id=w.id)
SELECT t link_tier, count(*) works, count(*) FILTER (WHERE n=0) with_zero_orgs, min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx, count(*) FILTER (WHERE n>50) gt50, count(*) FILTER (WHERE n>200) gt200 FROM a GROUP BY ROLLUP(t) ORDER BY t
```

| link_tier | works | with_zero_orgs | mn | avg | p50 | p90 | p99 | p999 | mx | gt50 | gt200 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 443,648 | 0 | 5.808 | 3 | 12 | 41 | 127 | 2660 | 32,167 | 2524 |
| 1 | 44,998,127 | 0 | 1 | 2.487 | 1 | 5 | 15 | 37 | 1142 | 19,485 | 261 |
| None | 50,000,000 | 443,648 | 0 | 2.819 | 2 | 6 | 19 | 51 | 2660 | 51,652 | 2785 |

_4 s_

### D4b top 10 works by orgs

```sql
SELECT c.work_id, c.n orgs, left(w.title,80) title, w.link_tier FROM (SELECT work_id, count(*) n FROM work_org GROUP BY 1 ORDER BY 2 DESC LIMIT 10) c JOIN work w ON w.id=c.work_id ORDER BY 2 DESC
```

| work_id | orgs | title | link_tier |
|---|---|---|---|
| 12,802,795,879,151,640,705 | 2660 | Characterising acute and chronic care needs: insights from the Global Burden of  | 0 |
| 11,127,208,317,711,914,465 | 1677 | Pan-cancer analysis of whole genomes | 0 |
| 12,821,080,187,089,861,644 | 1590 | Author Correction: The landscape of viral associations in human cancers | 0 |
| 11,987,671,283,217,489,275 | 1548 | Rising rural body-mass index is the main driver of the global obesity epidemic i | 0 |
| 11,963,941,922,442,647,438 | 1346 | Burden of 375 diseases and injuries, risk-attributable burden of 88 risk factors | 0 |
| 2,949,178,106,162,348,901 | 1285 | Guidelines for the use and interpretation of assays for monitoring autophagy (4t | 0 |
| 1,150,561,681,521,106,102 | 1276 | Global burden of 292 causes of death in 204 countries and territories and 660 su | 0 |
| 799,297,444,318,672,033 | 1142 | A second update on mapping the human genetic architecture of COVID-19 | 1 |
| 12,022,861,719,768,304,142 | 1074 | Mapping the human genetic architecture of COVID-19 | 0 |
| 5,654,267,924,297,882,785 | 1046 | Guidelines for the use and interpretation of assays for monitoring autophagy (3r | 0 |

_1 s_

### D5 projects per org / works per org (orgs with >=1)

```sql
WITH pc AS (SELECT org_id, count(*) n FROM proj_org GROUP BY 1), wc AS (SELECT org_id, count(*) n FROM work_org GROUP BY 1)
SELECT 'projects per org' m, count(*) orgs, min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx FROM pc UNION ALL SELECT 'works per org', count(*), min(n) mn, round(avg(n),3) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, quantile_cont(n,0.999) p999, max(n) mx FROM wc
```

| m | orgs | mn | avg | p50 | p90 | p99 | p999 | mx |
|---|---|---|---|---|---|---|---|---|
| projects per org | 348,578 | 1 | 15.779 | 1 | 7 | 100 | 2,814.115 | 48,337 |
| works per org | 175,797 | 1 | 801.896 | 12 | 896 | 14,703 | 85,646.672 | 840,577 |

_0 s_

### D5b top 20 orgs by projects (with works)

```sql
WITH pc AS (SELECT org_id, count(*) n FROM proj_org GROUP BY 1), wc AS (SELECT org_id, count(*) n FROM work_org GROUP BY 1)
SELECT o.id, left(o.legalName,60) legalName, o.countryCode, pc.n projects, coalesce(wc.n,0) works FROM pc JOIN organization o ON o.id=pc.org_id LEFT JOIN wc ON wc.org_id=pc.org_id ORDER BY pc.n DESC LIMIT 20
```

| id | legalName | countryCode | projects | works |
|---|---|---|---|---|
| 8,451,065,414,052,464,375 | JOHNS HOPKINS UNIVERSITY | None | 48,337 | 15,991 |
| 7,829,102,945,856,278,639 | UNIVERSITY OF PENNSYLVANIA | None | 40,382 | 11,986 |
| 6,609,372,035,559,517,646 | Stanford University | US | 37,451 | 198,741 |
| 4,857,126,525,531,888,087 | UNIVERSITY OF WASHINGTON | None | 36,893 | 12,182 |
| 6,047,922,915,305,188,880 | UNIVERSITY OF PITTSBURGH AT PITTSBURGH | None | 34,089 | 10,285 |
| 3,981,465,908,373,139,430 | YALE UNIVERSITY | None | 33,831 | 10,764 |
| 15,488,786,585,350,802,359 | UNIVERSITY OF WISCONSIN-MADISON | US | 33,743 | 28,880 |
| 12,580,808,939,514,225,574 | University of California, Los Angeles | None | 33,502 | 8615 |
| 3,529,758,830,023,099,581 | UNIVERSITY OF MICHIGAN AT ANN ARBOR | None | 31,944 | 8467 |
| 2,174,235,729,250,116,800 | WASHINGTON UNIVERSITY | None | 31,386 | 9596 |
| 15,027,101,037,484,212,415 | DUKE UNIVERSITY | None | 28,302 | 11,533 |
| 12,486,844,375,982,165,280 | MASSACHUSETTS GENERAL HOSPITAL | None | 25,723 | 9147 |
| 223,546,705,627,001,315 | JOHNS HOPKINS UNIVERSITY | None | 24,956 | 6745 |
| 17,371,786,910,681,134,521 | COLUMBIA UNIVERSITY HEALTH SCIENCES | None | 23,927 | 6876 |
| 18,042,036,425,579,194,190 | UNIVERSITY OF CALIFORNIA, SAN FRANCISCO | None | 22,949 | 2576 |
| 13,338,316,640,383,574,009 | EMORY UNIVERSITY | None | 22,406 | 9375 |
| 5,712,306,351,699,168,088 | UNIVERSITY OF MICHIGAN AT ANN ARBOR | None | 21,981 | 2093 |
| 17,000,393,588,288,294,581 | UNIVERSITY OF PENNSYLVANIA | None | 21,397 | 3003 |
| 18,201,541,583,144,479,165 | University of Colorado Denver | US | 20,937 | 28,024 |
| 10,803,472,946,464,314,865 | UNIVERSITY OF CALIFORNIA, SAN FRANCISCO | None | 20,809 | 11,339 |

_0 s_

### D5c top 20 orgs by works

```sql
WITH pc AS (SELECT org_id, count(*) n FROM proj_org GROUP BY 1), wc AS (SELECT org_id, count(*) n FROM work_org GROUP BY 1)
SELECT o.id, left(o.legalName,60) legalName, o.countryCode, coalesce(pc.n,0) projects, wc.n works FROM wc JOIN organization o ON o.id=wc.org_id LEFT JOIN pc ON pc.org_id=wc.org_id ORDER BY wc.n DESC LIMIT 20
```

| id | legalName | countryCode | projects | works |
|---|---|---|---|---|
| 7,649,459,455,343,105,576 | French National Centre for Scientific Research | FR | 98 | 840,577 |
| 13,174,849,542,392,989,230 | Chinese Academy of Sciences | CN | 130 | 677,697 |
| 12,535,759,026,515,651,181 | Harvard University | US | 5537 | 363,657 |
| 4,896,023,081,145,280,515 | University of California | US | 0 | 321,169 |
| 13,634,867,663,095,623,650 | University of Mary | US | 0 | 297,211 |
| 4,151,066,238,124,917,458 | Universidade de São Paulo | BR | 0 | 272,397 |
| 5,000,724,265,733,196,394 | University College London | GB | 11,771 | 271,790 |
| 11,226,745,934,011,585,717 | UNIVERSIDADE DE SAO PAULO | BR | 92 | 270,928 |
| 15,017,975,248,457,206,235 | University of California, San Francisco | US | 5301 | 270,124 |
| 1,107,240,349,866,947,128 | University of Chinese Academy of Sciences | CN | 0 | 268,658 |
| 12,566,782,248,100,638,402 | University of Oxford | GB | 10,758 | 266,313 |
| 15,605,841,012,081,852,702 | University of Toronto | CA | 777 | 252,142 |
| 10,149,679,324,396,748,619 | Zhejiang Ocean University | CN | 40 | 233,792 |
| 15,446,823,789,863,328,996 | Shanghai Jiao Tong University | CN | 48 | 222,497 |
| 345,906,752,459,341,229 | University System of Ohio | US | 5407 | 206,142 |
| 16,173,704,674,414,494,525 | University of Cambridge | GB | 5961 | 205,499 |
| 11,675,671,102,500,733,752 | Sorbonne Paris Cité | FR | 0 | 201,510 |
| 13,610,665,767,831,435,338 | University of Paris | FR | 121 | 200,055 |
| 6,609,372,035,559,517,646 | Stanford University | US | 37,451 | 198,741 |
| 442,570,941,600,113,840 | Sapienza University of Rome | IT | 635 | 197,182 |

_0 s_

### D5d orgs connected to projects / works / both / neither

```sql
SELECT count(*) FILTER (WHERE p AND w) both_, count(*) FILTER (WHERE p AND NOT w) projects_only, count(*) FILTER (WHERE w AND NOT p) works_only, count(*) FILTER (WHERE NOT p AND NOT w) neither
FROM (SELECT o.id, o.id IN (SELECT org_id FROM proj_org) p, o.id IN (SELECT org_id FROM work_org) w FROM organization o)
```

| both_ | projects_only | works_only | neither |
|---|---|---|---|
| 85,764 | 262,814 | 90,033 | 55,488 |

_0 s_

### D6 collaboration size: exact sum(n*(n-1)/2) over projects

```sql
WITH c AS (SELECT project_id, count(*) n FROM proj_org GROUP BY 1)
SELECT 'all' grp, count(*) projects, sum(n) relations, sum(n*(n-1)/2)::BIGINT project_org_pair_edges, sum(n*(n-1)/2) FILTER (WHERE n<=50)::BIGINT edges_if_projects_gt50_dropped, sum(n*(n-1)/2) FILTER (WHERE n>50)::BIGINT edges_from_gt50_projects FROM c
UNION ALL SELECT 'is_ch', count(*), sum(n), sum(n*(n-1)/2)::BIGINT, sum(n*(n-1)/2) FILTER (WHERE n<=50)::BIGINT, sum(n*(n-1)/2) FILTER (WHERE n>50)::BIGINT FROM c JOIN project p ON p.id=c.project_id WHERE p.is_ch
```

| grp | projects | relations | project_org_pair_edges | edges_if_projects_gt50_dropped | edges_from_gt50_projects |
|---|---|---|---|---|---|
| all | 3,576,835 | 5,500,329 | 7,348,351 | 5,954,451 | 1,393,900 |
| is_ch | 17,463 | 52,246 | 169,864 | 146,349 | 23,515 |

_0 s_

### D7 (project, org) relations per relType / provenance

```sql
SELECT rel_name, prov, count(*) relations FROM proj_org GROUP BY ALL ORDER BY 3 DESC
```

| rel_name | prov | relations |
|---|---|---|
| hasParticipant | Harvested | 5,460,580 |
| hasParticipant | Inferred by OpenAIRE | 39,749 |

_0 s_

### D7b all relation kinds

```sql
SELECT sourceType, targetType, relType.name rel, provenance.provenance prov, validated, count(*) n FROM relation GROUP BY ALL ORDER BY n DESC
```

| sourceType | targetType | rel | prov | validated | n |
|---|---|---|---|---|---|
| product | organization | hasAuthorInstitution | Inferred by OpenAIRE | False | 131,367,534 |
| product | organization | hasAuthorInstitution | Harvested | False | 9,603,312 |
| project | organization | hasParticipant | Harvested | False | 5,460,580 |
| project | product | produces | Inferred by OpenAIRE | False | 5,166,980 |
| project | product | produces | Harvested | False | 1,164,075 |
| project | product | produces | Harvested | True | 941,335 |
| project | organization | hasParticipant | Inferred by OpenAIRE | False | 39,749 |
| project | product | produces | Linked by user | False | 4919 |
| project | product | produces | Linked by user | True | 1581 |

_1 s_


_section runtime total: 8 s, job 8981155_


slurm job `8981157`

### D8a distinct unordered org pairs sharing >=1 project, is_ch projects only (exact)

```sql
SELECT count(*) distinct_pairs, sum(k) pair_project_edges, max(k) max_shared_projects FROM (
 SELECT least(a.org_id,b.org_id) x, greatest(a.org_id,b.org_id) y, count(*) k FROM proj_org a JOIN proj_org b ON a.project_id=b.project_id AND a.org_id<b.org_id
 WHERE a.project_id IN (SELECT id FROM project WHERE is_ch) GROUP BY 1,2)
```

| distinct_pairs | pair_project_edges | max_shared_projects |
|---|---|---|
| 153,616 | 169,864 | 151 |

_0 s_

### D8b distinct unordered pairs, all projects with <=200 orgs (exact); projects >200 orgs excluded, count given

```sql
WITH big AS (SELECT project_id FROM proj_org GROUP BY 1 HAVING count(*)>200)
SELECT count(*) distinct_pairs, sum(k) pair_project_edges, max(k) max_shared_projects, count(*) FILTER (WHERE k>=2) pairs_ge2, count(*) FILTER (WHERE k>=5) pairs_ge5, (SELECT count(*) FROM big) excluded_projects_gt200 FROM (
 SELECT a.org_id x, b.org_id y, count(*) k FROM proj_org a JOIN proj_org b ON a.project_id=b.project_id AND a.org_id<b.org_id
 WHERE a.project_id NOT IN (SELECT project_id FROM big) GROUP BY 1,2)
```

| distinct_pairs | pair_project_edges | max_shared_projects | pairs_ge2 | pairs_ge5 | excluded_projects_gt200 |
|---|---|---|---|---|---|
| 4,406,144 | 7,253,975 | 23,469 | 682,514 | 115,654 | 2 |

_0 s_

### D8c the busiest org pair (shares 23k+ projects?) and the two >200-org projects

```sql
WITH pr AS (SELECT least(a.org_id,b.org_id) x, greatest(a.org_id,b.org_id) y, count(*) k FROM proj_org a JOIN proj_org b ON a.project_id=b.project_id AND a.org_id<b.org_id GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5)
SELECT pr.k shared_projects, ox.legalName org_x, ox.countryCode cx, oy.legalName org_y, oy.countryCode cy FROM pr JOIN organization ox ON ox.id=pr.x JOIN organization oy ON oy.id=pr.y ORDER BY 1 DESC
```

| shared_projects | org_x | cx | org_y | cy |
|---|---|---|---|---|
| 23,469 | JOHNS HOPKINS UNIVERSITY | None | JOHNS HOPKINS UNIVERSITY | None |
| 20,391 | UNIVERSITY OF CALIFORNIA, SAN FRANCISCO | None | UNIVERSITY OF CALIFORNIA, SAN FRANCISCO | None |
| 20,122 | UNIVERSITY OF PENNSYLVANIA | None | UNIVERSITY OF PENNSYLVANIA | None |
| 18,947 | UNIVERSITY OF PITTSBURGH AT PITTSBURGH | None | UNIVERSITY OF PITTSBURGH AT PITTSBURGH | None |
| 17,002 | UNIVERSITY OF WASHINGTON | None | UNIVERSITY OF WASHINGTON | None |

_0 s_

### D8d the projects with >200 orgs

```sql
SELECT c.project_id, c.n orgs, left(p.title,80) title FROM (SELECT project_id, count(*) n FROM proj_org GROUP BY 1 HAVING count(*)>200 ORDER BY 2 DESC) c JOIN project p ON p.id=c.project_id
```

| project_id | orgs | title |
|---|---|---|
| 8,303,088,955,748,938,288 | 380 | A Centre for Innovative Manufacturing and Construction |
| 1,455,035,746,565,831,797 | 212 | Engineering Innovative Manufacturing Research Centre Renewal |

_0 s_


_section runtime total: 1 s, job 8981157_



---

# E. Coordinators

**Design meaning (E).**
- `cordis_type`: 392,682 of 5.5M relations (7.1%): participant 311,314, coordinator 81,100, associatedPartner 155, thirdParty 88, internationalPartner 21, partner 4. **81,043 projects (2.1%) have a coordinator**, 57 have two (max 2). `is_ch`: 1,800 of 21,280 (8.5%).
- Among the top-10 funders **only EC has coordinators: 81,035 of 128,574 (63.0%)**; SNSF 1, AKA 6, all others 0. `coordinator_id` is an EC-only field (D15 confirmed): the UI must not show a coordinator column for other funders; `org_ids` "coordinator first" is a no-op for 97.9% of projects.

slurm job `8981152`

### E1 cordis_type distribution on project->organization relations

```sql
SELECT coalesce(cordis_type,'<NULL>') cordis_type, count(*) relations, count(DISTINCT project_id) projects, round(sum(cordis_ec_contribution)) sum_ec_contribution FROM proj_org GROUP BY 1 ORDER BY 2 DESC
```

| cordis_type | relations | projects | sum_ec_contribution |
|---|---|---|---|
| <NULL> | 5,107,647 | 3,503,813 | None |
| participant | 311,314 | 33,572 | 97,172,396,541 |
| coordinator | 81,100 | 81,043 | 68,303,947,832 |
| associatedPartner | 155 | 117 | None |
| thirdParty | 88 | 84 | 89,563 |
| internationalPartner | 21 | 12 | 0 |
| partner | 4 | 4 | None |

_0 s_

### E2 projects with a coordinator; more than one coordinator

```sql
WITH c AS (SELECT project_id, count(*) n FROM proj_org WHERE lower(cordis_type)='coordinator' GROUP BY 1)
SELECT (SELECT count(*) FROM c) projects_with_coordinator, (SELECT count(*) FROM c WHERE n>1) projects_with_gt1_coordinator, (SELECT max(n) FROM c) max_coordinators,
 (SELECT count(*) FROM c JOIN project p ON p.id=c.project_id WHERE p.is_ch) is_ch_projects_with_coordinator, (SELECT count(*) FROM project WHERE is_ch) is_ch_projects,
 (SELECT count(*) FROM project) all_projects
```

| projects_with_coordinator | projects_with_gt1_coordinator | max_coordinators | is_ch_projects_with_coordinator | is_ch_projects | all_projects |
|---|---|---|---|---|---|
| 81,043 | 57 | 2 | 1800 | 21,280 | 3,893,065 |

_0 s_

### E2b projects with >1 coordinator: examples

```sql
SELECT project_id, count(*) n_coord FROM proj_org WHERE lower(cordis_type)='coordinator' GROUP BY 1 HAVING count(*)>1 ORDER BY 2 DESC LIMIT 10
```

| project_id | n_coord |
|---|---|
| 2,729,202,830,111,355,794 | 2 |
| 11,427,481,097,486,236,502 | 2 |
| 2,874,132,940,422,821,455 | 2 |
| 5,635,629,954,989,520,060 | 2 |
| 16,417,323,228,216,827,700 | 2 |
| 18,226,703,349,393,867,371 | 2 |
| 4,628,621,882,223,585,131 | 2 |
| 8,815,529,836,824,248,594 | 2 |
| 6,748,055,226,582,810,999 | 2 |
| 7,001,108,709,657,678,376 | 2 |

_0 s_

### E3 coordinator coverage per top-10 funder (level1 of funding stream id)

```sql
WITH pf AS (SELECT DISTINCT id project_id, coalesce(nullif(split_part(f.fundingStream.id,'::',1),''), f.shortName) l1 FROM (SELECT id, unnest(fundings) f FROM project)),
 top AS (SELECT l1 FROM pf GROUP BY 1 ORDER BY count(*) DESC LIMIT 10),
 co AS (SELECT DISTINCT project_id FROM proj_org WHERE lower(cordis_type)='coordinator')
SELECT pf.l1 funder, count(*) projects, count(co.project_id) with_coordinator, round(100.0*count(co.project_id)/count(*),2) pct FROM pf LEFT JOIN co USING(project_id) WHERE pf.l1 IN (SELECT l1 FROM top) GROUP BY 1 ORDER BY 2 DESC
```

| funder | projects | with_coordinator | pct |
|---|---|---|---|
| NIH | 2,343,782 | 0 | 0 |
| NSF | 601,434 | 0 | 0 |
| UKRI | 175,092 | 0 | 0 |
| EC | 128,574 | 81,035 | 63.03 |
| SNSF | 92,757 | 1 | 0 |
| FCT | 89,867 | 0 | 0 |
| NWO | 47,080 | 0 | 0 |
| NHMRC | 33,217 | 0 | 0 |
| AKA | 32,292 | 6 | 0.02 |
| ARC | 32,238 | 0 | 0 |

_0 s_

### E4 relations with cordis_ec_contribution / cordis_type

```sql
SELECT count(*) relations, count(cordis_type) with_type, count(cordis_ec_contribution) with_contribution, count(*) FILTER (WHERE cordis_type IS NULL AND cordis_ec_contribution IS NOT NULL) contribution_without_type FROM proj_org
```

| relations | with_type | with_contribution | contribution_without_type |
|---|---|---|---|
| 5,500,329 | 392,682 | 384,988 | 0 |

_0 s_


_section runtime total: 0 s, job 8981152_



---

# F. Works: sizing of a trimmed document

**Design meaning (F).**
- **Trimmed doc, measured**: tier 0 avg 813 B JSON (p99 1,875), tier 1 avg 670 B (p99 1,145), all 684 B -> **~34.2 GB raw JSON for 50M** (formula estimate 35.5 GB; tier 0: 4.1 GB, tier 1: 30.1 GB). Biggest contributors: title 98 B, first-20 authors 61 B (+4 B each), ids 23 B each (avg 0.15 project + 2.8 org per work = ~90 B), urls ~51 B, fixed keys ~300 B. In Parquet zstd this is 7.6 GB (section M).
- 50M works: 8.0% publisher NULL, 25.1% `container.name` NULL, 3.9% no authors, 0.011% title NULL, publicationDate NULL 5.15% in tier 0 (0 in tier 1), `citationCount` never NULL (50.1% are 0; integer valued; tier 0: 23.6% zero, positive median 15, p99 370, max 131,561), OA colour NULL 58.2%, `bestAccessRight` NULL 16.0%.
- **Year**: tier 1 is 2018-05..2026 only (cutoff 2018-04-30), 5-7M works per year; tier 0 reaches back (10,451 works < 1990, 54,751 in the 1990s). Year filter = range filter on `year`/date, fine.
- **`language.code` is a 3-letter code, not 2-letter**: `eng` 53.7%, **`und` 36.35%**, then rus 1.2%, fra 1.2%, ... 606 distinct codes of which **30 are "T/B" pairs such as `fra/fre`, `esl/spa`, `dut/nld` (1.84M works)** and `spa` co-exists with `esl/spa`. Normalise at export (first part / map to ISO 639-1) before it becomes a filter. Among works with a known language 84.4% are English (tier 0: 98.3%, tier 1: 82.6%); 9.9% of all works have a known non-English language, 36.4% unknown -> titles of ~10-46% of the works may be non-English; no translation was done.
- **Publisher**: 310,891 distinct values (178,765 occur once); top 100 cover 67.2% of works with publisher, top 1,000 cover 81.7%, the 2,722 publishers with >= 1,000 works cover 88.0%. A free `terms` facet is not reasonable; a **typeahead over the top ~1,000-3,000 publishers** (or exact-match keyword filter with autocomplete from a small side list) is. Values are also unnormalised (`Springer Science and Business Media LLC` / `Springer International Publishing` / `Springer Nature Switzerland`; `IEEE` / `Institute of Electrical and Electronics Engineers (IEEE)`).

slurm job `8981162`

### F0 works per tier

```sql
SELECT link_tier, count(*) works FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | works |
|---|---|
| 0 | 5,001,873 |
| 1 | 44,998,127 |
| None | 50,000,000 |

_0 s_

### F1 length(title) per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(title_len) non_null, round(avg(title_len),2) avg, quantile_cont(title_len,0.5) p50, quantile_cont(title_len,0.9) p90, quantile_cont(title_len,0.99) p99, max(title_len) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 5,001,410 | 95.9 | 92 | 144 | 212 | 10,146 |
| 1 | 44,993,156 | 98.44 | 96 | 148 | 211 | 119,750 |
| ALL | 49,994,566 | 98.19 | 95 | 148 | 211 | 119,750 |

_2 s_

### F1 len(authors) per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(n_authors) non_null, round(avg(n_authors),2) avg, quantile_cont(n_authors,0.5) p50, quantile_cont(n_authors,0.9) p90, quantile_cont(n_authors,0.99) p99, max(n_authors) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 4,736,553 | 11.07 | 5 | 12 | 45 | 13,555 |
| 1 | 43,294,603 | 4.41 | 3 | 8 | 18 | 32,294 |
| ALL | 48,031,156 | 5.06 | 3 | 9 | 20 | 32,294 |

_2 s_

### F1 sum of length of first 20 authors[].fullName per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(a20_len) non_null, round(avg(a20_len),2) avg, quantile_cont(a20_len,0.5) p50, quantile_cont(a20_len,0.9) p90, quantile_cont(a20_len,0.99) p99, max(a20_len) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 84.03 | 65 | 178 | 316 | 35,966 |
| 1 | 44,998,127 | 58.61 | 46 | 118 | 258 | 7,832 |
| ALL | 50,000,000 | 61.15 | 48 | 124 | 274 | 35,966 |

_4 s_

### F1 length(publisher) per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(publisher_len) non_null, round(avg(publisher_len),2) avg, quantile_cont(publisher_len,0.5) p50, quantile_cont(publisher_len,0.9) p90, quantile_cont(publisher_len,0.99) p99, max(publisher_len) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 4,464,022 | 23.54 | 22 | 39 | 59 | 2615 |
| 1 | 41,537,320 | 24.11 | 20 | 44 | 78 | 3526 |
| ALL | 46,001,342 | 24.05 | 20 | 43 | 77 | 3526 |

_1 s_

### F1 length(container.name) per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(container_len) non_null, round(avg(container_len),2) avg, quantile_cont(container_len,0.5) p50, quantile_cont(container_len,0.9) p90, quantile_cont(container_len,0.99) p99, max(container_len) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 4,063,340 | 27.96 | 25 | 48 | 94 | 593 |
| 1 | 33,370,002 | 34.91 | 31 | 62 | 106 | 499 |
| ALL | 37,433,342 | 34.15 | 30 | 61 | 105 | 593 |

_1 s_

### F1 len(instances) per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(n_instances) non_null, round(avg(n_instances),2) avg, quantile_cont(n_instances,0.5) p50, quantile_cont(n_instances,0.9) p90, quantile_cont(n_instances,0.99) p99, max(n_instances) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 4.95 | 4 | 9 | 15 | 167 |
| 1 | 44,998,127 | 2.42 | 2 | 5 | 8 | 275 |
| ALL | 50,000,000 | 2.67 | 2 | 5 | 10 | 275 |

_1 s_

### F1 orgs per work per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(n_orgs) non_null, round(avg(n_orgs),2) avg, quantile_cont(n_orgs,0.5) p50, quantile_cont(n_orgs,0.9) p90, quantile_cont(n_orgs,0.99) p99, max(n_orgs) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 5.81 | 3 | 12 | 41 | 2660 |
| 1 | 44,998,127 | 2.49 | 1 | 5 | 15 | 1142 |
| ALL | 50,000,000 | 2.82 | 2 | 6 | 19 | 2660 |

_1 s_

### F1 projects per work per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(n_projects) non_null, round(avg(n_projects),2) avg, quantile_cont(n_projects,0.5) p50, quantile_cont(n_projects,0.9) p90, quantile_cont(n_projects,0.99) p99, max(n_projects) mx FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | non_null | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 1.46 | 1 | 3 | 8 | 358 |
| 1 | 44,998,127 | 0 | 0 | 0 | 0 | 0 |
| ALL | 50,000,000 | 0.15 | 0 | 0 | 3 | 358 |

_1 s_

### F2a estimated raw JSON bytes per trimmed doc, FORMULA (per work, then per tier). Assumptions: 300 B keys/punctuation, ids 20-char strings + 3 B quoting = 23 B each (id, project_ids[], organisation_ids[]), authors +4 B each (quotes, comma, space), doi/pdf/landing = actual lengths + 6

```sql
WITH d AS (SELECT link_tier, 300 + 23 + coalesce(title_len,0) + a20_len + 4*least(n_authors,20) + coalesce(publisher_len,0) + coalesce(container_len,0) + 23*(n_projects+n_orgs)
   + coalesce(length(doi_any),0)+6 + coalesce(length(pdf_url),0)+6 + coalesce(length(landing_url),0)+6 AS bytes FROM wfx)
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) works, round(avg(bytes),2) avg, quantile_cont(bytes,0.5) p50, quantile_cont(bytes,0.9) p90, quantile_cont(bytes,0.99) p99, max(bytes) mx, round(sum(bytes)/1e9,2) total_GB_json FROM d GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | works | avg | p50 | p90 | p99 | mx | total_GB_json |
|---|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 848.09 | 781 | 1,130 | 1,932 | 63,249 | 4.24 |
| 1 | 44,998,127 | 693.78 | 674 | 843 | 1,178 | 120,461 | 31.22 |
| ALL | 50,000,000 | 709.22 | 681 | 874 | 1,308 | 120,461 | 35.46 |

_7 s_

### F2b MEASURED raw JSON bytes per trimmed doc on a 0.5% hash sample (to_json of the real doc, incl. urls by the G rules), per tier and extrapolated to all works

```sql
WITH ids AS (SELECT id FROM wf WHERE hash(id) % 200 = 0),
 po AS (SELECT work_id, list(project_id::VARCHAR) AS pids FROM (SELECT work_id, project_id FROM proj_work WHERE work_id IN (SELECT id FROM ids)) GROUP BY 1),
 oo AS (SELECT work_id, list(org_id::VARCHAR) AS oids FROM (SELECT work_id, org_id FROM work_org WHERE work_id IN (SELECT id FROM ids)) GROUP BY 1),
 doc AS (SELECT w.link_tier, to_json({'id': w.id::VARCHAR, 'title': w.title, 'authors': list_transform(w.authors[1:20], a -> a.fullName), 'author_count': len(w.authors),
    'publication_date': w.publicationDate, 'publisher': w.publisher, 'container_name': w.container.name, 'open_access_color': w.openAccessColor, 'best_access_right': w.bestAccessRight.label,
    'language': w.language.code, 'citation_count': w.citationCount, 'doi': wk_doi_any(w.pids, w.instances), 'pdf_url': wk_pdf_url(w.instances), 'landing_url': wk_landing_url(w.pids, w.instances),
    'project_ids': coalesce(po.pids, []), 'organisation_ids': coalesce(oo.oids, []), 'link_tier': w.link_tier}) j
   FROM work w JOIN ids ON ids.id = w.id LEFT JOIN po ON po.work_id = w.id LEFT JOIN oo ON oo.work_id = w.id),
 b AS (SELECT link_tier, strlen(j::VARCHAR) bytes FROM doc)
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) sampled, round(avg(bytes),2) avg, quantile_cont(bytes,0.5) p50, quantile_cont(bytes,0.9) p90, quantile_cont(bytes,0.99) p99, max(bytes) mx, round(avg(bytes) * CASE link_tier WHEN 0 THEN 5001873 WHEN 1 THEN 44998127 ELSE 50000000 END / 1e9, 2) extrapolated_GB_json FROM b GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | sampled | avg | p50 | p90 | p99 | mx | extrapolated_GB_json |
|---|---|---|---|---|---|---|---|
| 0 | 25,156 | 812.9 | 753 | 1,096 | 1,875.35 | 10,874 | 4.07 |
| 1 | 225,208 | 669.59 | 651 | 816 | 1,145 | 5151 | 30.13 |
| ALL | 250,364 | 683.99 | 658 | 845 | 1,272 | 10,874 | 34.2 |

_10 s_

### F3 NULL shares per tier

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) works, round(100.0*count(*) FILTER (WHERE publicationDate IS NULL)/count(*),2) pct_pubDate_null, round(100.0*count(*) FILTER (WHERE title_null)/count(*),3) pct_title_null,
 round(100.0*count(*) FILTER (WHERE citationCount IS NULL)/count(*),2) pct_citation_null, round(100.0*count(*) FILTER (WHERE lang_code IS NULL)/count(*),2) pct_language_null, round(100.0*count(*) FILTER (WHERE lang_code='und')/count(*),2) pct_language_und,
 round(100.0*count(*) FILTER (WHERE publisher IS NULL)/count(*),2) pct_publisher_null, round(100.0*count(*) FILTER (WHERE container_len IS NULL)/count(*),2) pct_container_null, round(100.0*count(*) FILTER (WHERE n_authors IS NULL OR n_authors=0)/count(*),2) pct_no_authors,
 round(100.0*count(*) FILTER (WHERE oa_color IS NULL)/count(*),2) pct_oaColor_null, round(100.0*count(*) FILTER (WHERE best_access IS NULL)/count(*),2) pct_bestAccess_null FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | works | pct_pubDate_null | pct_title_null | pct_citation_null | pct_language_null | pct_language_und | pct_publisher_null | pct_container_null | pct_no_authors | pct_oaColor_null | pct_bestAccess_null |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 5.15 | 0.009 | 0 | 0 | 28.74 | 10.75 | 18.76 | 5.3 | 40.13 | 3.55 |
| 1 | 44,998,127 | 0 | 0.011 | 0 | 0 | 37.2 | 7.69 | 25.84 | 3.79 | 60.23 | 17.33 |
| ALL | 50,000,000 | 0.51 | 0.011 | 0 | 0 | 36.35 | 8 | 25.13 | 3.94 | 58.22 | 15.96 |

_1 s_

### F4 year histogram (tier split)

```sql
SELECT coalesce(year::VARCHAR,'<NULL>') AS yr, count(*) works, count(*) FILTER (WHERE link_tier=0) tier0, count(*) FILTER (WHERE link_tier=1) tier1 FROM wf GROUP BY 1 ORDER BY 1
```

| yr | works | tier0 | tier1 |
|---|---|---|---|
| 1012 | 1 | 1 | 0 |
| 1483 | 1 | 1 | 0 |
| 1486 | 1 | 1 | 0 |
| 1533 | 1 | 1 | 0 |
| 1588 | 1 | 1 | 0 |
| 1595 | 1 | 1 | 0 |
| 1596 | 1 | 1 | 0 |
| 1601 | 1 | 1 | 0 |
| 1606 | 1 | 1 | 0 |
| 1628 | 1 | 1 | 0 |
| 1636 | 1 | 1 | 0 |
| 1637 | 1 | 1 | 0 |
| 1657 | 1 | 1 | 0 |
| 1669 | 1 | 1 | 0 |
| 1670 | 1 | 1 | 0 |
| 1701 | 2 | 2 | 0 |
| 1703 | 1 | 1 | 0 |
| 1714 | 1 | 1 | 0 |
| 1719 | 1 | 1 | 0 |
| 1726 | 1 | 1 | 0 |
| 1727 | 1 | 1 | 0 |
| 1732 | 1 | 1 | 0 |
| 1748 | 1 | 1 | 0 |
| 1758 | 1 | 1 | 0 |
| 1769 | 1 | 1 | 0 |
| 1770 | 1 | 1 | 0 |
| 1772 | 1 | 1 | 0 |
| 1786 | 2 | 2 | 0 |
| 1799 | 1 | 1 | 0 |
| 1801 | 1 | 1 | 0 |
| 1804 | 1 | 1 | 0 |
| 1806 | 3 | 3 | 0 |
| 1819 | 1 | 1 | 0 |
| 1826 | 1 | 1 | 0 |
| 1830 | 1 | 1 | 0 |
| 1832 | 2 | 2 | 0 |
| 1834 | 4 | 4 | 0 |
| 1836 | 4 | 4 | 0 |
| 1837 | 5 | 5 | 0 |
| 1838 | 2 | 2 | 0 |
| 1840 | 2 | 2 | 0 |
| 1842 | 1 | 1 | 0 |
| 1844 | 2 | 2 | 0 |
| 1846 | 1 | 1 | 0 |
| 1847 | 2 | 2 | 0 |
| 1848 | 1 | 1 | 0 |
| 1850 | 3 | 3 | 0 |
| 1855 | 1 | 1 | 0 |
| 1856 | 1 | 1 | 0 |
| 1857 | 1 | 1 | 0 |
| 1858 | 1 | 1 | 0 |
| 1860 | 1 | 1 | 0 |
| 1862 | 2 | 2 | 0 |
| 1863 | 2 | 2 | 0 |
| 1865 | 3 | 3 | 0 |
| 1867 | 1 | 1 | 0 |
| 1869 | 2 | 2 | 0 |
| 1870 | 2 | 2 | 0 |
| 1871 | 2 | 2 | 0 |
| 1872 | 2 | 2 | 0 |
| 1874 | 5 | 5 | 0 |
| 1875 | 4 | 4 | 0 |
| 1876 | 4 | 4 | 0 |
| 1877 | 3 | 3 | 0 |
| 1878 | 4 | 4 | 0 |
| 1879 | 4 | 4 | 0 |
| 1880 | 4 | 4 | 0 |
| 1881 | 4 | 4 | 0 |
| 1882 | 2 | 2 | 0 |
| 1883 | 5 | 5 | 0 |
| 1884 | 3 | 3 | 0 |
| 1885 | 6 | 6 | 0 |
| 1886 | 2 | 2 | 0 |
| 1887 | 3 | 3 | 0 |
| 1888 | 6 | 6 | 0 |
| 1889 | 3 | 3 | 0 |
| 1890 | 2 | 2 | 0 |
| 1891 | 6 | 6 | 0 |
| 1892 | 5 | 5 | 0 |
| 1893 | 2 | 2 | 0 |
| 1894 | 8 | 8 | 0 |
| 1895 | 4 | 4 | 0 |
| 1896 | 4 | 4 | 0 |
| 1897 | 6 | 6 | 0 |
| 1898 | 6 | 6 | 0 |
| 1899 | 4 | 4 | 0 |
| 1900 | 9 | 9 | 0 |
| 1901 | 3 | 3 | 0 |
| 1902 | 7 | 7 | 0 |
| 1903 | 6 | 6 | 0 |
| 1904 | 4 | 4 | 0 |
| 1905 | 1 | 1 | 0 |
| 1906 | 6 | 6 | 0 |
| 1907 | 7 | 7 | 0 |
| 1908 | 10 | 10 | 0 |
| 1909 | 6 | 6 | 0 |
| 1910 | 5 | 5 | 0 |
| 1911 | 8 | 8 | 0 |
| 1912 | 6 | 6 | 0 |
| 1913 | 6 | 6 | 0 |
| 1914 | 8 | 8 | 0 |
| 1915 | 4 | 4 | 0 |
| 1916 | 2 | 2 | 0 |
| 1917 | 6 | 6 | 0 |
| 1918 | 5 | 5 | 0 |
| 1919 | 5 | 5 | 0 |
| 1920 | 11 | 11 | 0 |
| 1921 | 9 | 9 | 0 |
| 1922 | 2 | 2 | 0 |
| 1923 | 2 | 2 | 0 |
| 1924 | 7 | 7 | 0 |
| 1925 | 9 | 9 | 0 |
| 1926 | 9 | 9 | 0 |
| 1927 | 3 | 3 | 0 |
| 1928 | 10 | 10 | 0 |
| 1929 | 4 | 4 | 0 |
| 1930 | 7 | 7 | 0 |
| 1931 | 7 | 7 | 0 |
| 1932 | 6 | 6 | 0 |
| 1933 | 8 | 8 | 0 |
| ... 94 more rows |  |  | 

_0 s_

### F4b year buckets

```sql
SELECT CASE WHEN year IS NULL THEN 'NULL' WHEN year<1990 THEN '<1990' WHEN year<2000 THEN '1990s' WHEN year<2010 THEN '2000s' WHEN year<2018 THEN '2010-2017' ELSE year::VARCHAR END b, count(*) works, count(*) FILTER (WHERE link_tier=0) tier0, count(*) FILTER (WHERE link_tier=1) tier1 FROM wf GROUP BY 1 ORDER BY 1
```

| b | works | tier0 | tier1 |
|---|---|---|---|
| 1990s | 54,751 | 54,751 | 0 |
| 2000s | 296,620 | 296,620 | 0 |
| 2010-2017 | 1,372,472 | 1,372,472 | 0 |
| 2018 | 2,756,014 | 300,566 | 2,455,448 |
| 2019 | 5,267,130 | 337,548 | 4,929,582 |
| 2020 | 5,853,506 | 408,708 | 5,444,798 |
| 2021 | 6,202,665 | 443,690 | 5,758,975 |
| 2022 | 6,364,310 | 425,694 | 5,938,616 |
| 2023 | 6,567,660 | 389,917 | 6,177,743 |
| 2024 | 6,772,382 | 370,321 | 6,402,061 |
| 2025 | 6,634,422 | 287,984 | 6,346,438 |
| 2026 | 1,590,164 | 45,698 | 1,544,466 |
| <1990 | 10,451 | 10,451 | 0 |
| NULL | 257,453 | 257,453 | 0 |

_0 s_

### F5 openAccessColor / bestAccessRight.label

```sql
SELECT coalesce(oa_color,'<NULL>') oa_color, coalesce(best_access,'<NULL>') best_access, count(*) works, count(*) FILTER (WHERE link_tier=0) tier0 FROM wf GROUP BY ALL ORDER BY 3 DESC
```

| oa_color | best_access | works | tier0 |
|---|---|---|---|
| gold | OPEN | 14,142,767 | 1,400,014 |
| <NULL> | CLOSED | 12,715,563 | 562,124 |
| <NULL> | <NULL> | 7,947,052 | 176,519 |
| <NULL> | OPEN | 7,691,684 | 1,201,423 |
| hybrid | OPEN | 4,535,555 | 955,835 |
| bronze | OPEN | 2,172,392 | 637,542 |
| <NULL> | RESTRICTED | 700,273 | 64,166 |
| <NULL> | EMBARGO | 56,256 | 3056 |
| gold | <NULL> | 26,577 | 657 |
| hybrid | EMBARGO | 6902 | 107 |
| bronze | <NULL> | 2763 | 312 |
| hybrid | <NULL> | 1354 | 18 |
| hybrid | CLOSED | 260 | 20 |
| bronze | CLOSED | 201 | 31 |
| bronze | EMBARGO | 157 | 14 |
| gold | CLOSED | 122 | 22 |
| bronze | RESTRICTED | 79 | 7 |
| gold | RESTRICTED | 21 | 3 |
| hybrid | RESTRICTED | 20 | 3 |
| gold | EMBARGO | 2 | 0 |

_0 s_

### F6 language.code top 20 (share, tier split)

```sql
SELECT coalesce(lang_code,'<NULL>') lang, count(*) works, round(100.0*count(*)/50000000,2) pct, count(*) FILTER (WHERE link_tier=0) tier0, count(*) FILTER (WHERE link_tier=1) tier1 FROM wf GROUP BY 1 ORDER BY 2 DESC LIMIT 20
```

| lang | works | pct | tier0 | tier1 |
|---|---|---|---|---|
| eng | 26,846,644 | 53.69 | 3,504,618 | 23,342,026 |
| und | 18,175,691 | 36.35 | 1,437,684 | 16,738,007 |
| rus | 608,967 | 1.22 | 839 | 608,128 |
| fra/fre | 580,903 | 1.16 | 9234 | 571,669 |
| deu/ger | 553,622 | 1.11 | 6999 | 546,623 |
| tur | 412,152 | 0.82 | 2381 | 409,771 |
| ita | 395,637 | 0.79 | 3295 | 392,342 |
| esl/spa | 385,772 | 0.77 | 5086 | 380,686 |
| spa | 347,292 | 0.69 | 1350 | 345,942 |
| por | 253,564 | 0.51 | 6926 | 246,638 |
| pol | 148,176 | 0.3 | 444 | 147,732 |
| ukr | 144,569 | 0.29 | 349 | 144,220 |
| fin | 133,872 | 0.27 | 400 | 133,472 |
| hrv | 100,785 | 0.2 | 3898 | 96,887 |
| swe | 82,049 | 0.16 | 673 | 81,376 |
| dut/nld | 74,447 | 0.15 | 403 | 74,044 |
| ces/cze | 66,794 | 0.13 | 157 | 66,637 |
| ind | 57,567 | 0.12 | 30 | 57,537 |
| ron/rum | 51,518 | 0.1 | 66 | 51,452 |
| nor | 44,413 | 0.09 | 255 | 44,158 |

_0 s_

### F6b non-English titles: share by known language

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) works, count(*) FILTER (WHERE lang_code IS NOT NULL AND lang_code NOT IN ('und','mul','zxx')) known_lang, round(100.0*count(*) FILTER (WHERE lang_code='eng')/nullif(count(*) FILTER (WHERE lang_code IS NOT NULL AND lang_code NOT IN ('und','mul','zxx')),0),2) pct_en_of_known,
 round(100.0*count(*) FILTER (WHERE lang_code NOT IN ('eng','und','mul','zxx'))/count(*),2) pct_non_en_of_all, round(100.0*count(*) FILTER (WHERE lang_code IS NULL OR lang_code IN ('und','mul','zxx'))/count(*),2) pct_unknown_of_all FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | works | known_lang | pct_en_of_known | pct_non_en_of_all | pct_unknown_of_all |
|---|---|---|---|---|---|
| 0 | 5,001,873 | 3,564,067 | 98.33 | 1.19 | 28.75 |
| 1 | 44,998,127 | 28,249,330 | 82.63 | 10.91 | 37.22 |
| ALL | 50,000,000 | 31,813,397 | 84.39 | 9.93 | 36.37 |

_0 s_

### F6c language code shapes (3-letter ISO 639-2 T/B pairs like 'fra/fre'; distinct codes, how many contain '/')

```sql
SELECT count(DISTINCT lang_code) distinct_codes, count(DISTINCT lang_code) FILTER (WHERE lang_code LIKE '%/%') slash_codes, sum(1) FILTER (WHERE lang_code LIKE '%/%') works_with_slash_code, count(DISTINCT lang_code) FILTER (WHERE length(lang_code)=2) two_letter, count(DISTINCT split_part(lang_code,'/',1)) distinct_after_first_part FROM wf
```

| distinct_codes | slash_codes | works_with_slash_code | two_letter | distinct_after_first_part |
|---|---|---|---|---|
| 606 | 30 | 1,844,376 | 33 | 597 |

_0 s_

### F7 citationCount

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, round(100.0*count(*) FILTER (WHERE citationCount IS NULL)/count(*),2) pct_null, round(100.0*count(*) FILTER (WHERE citationCount=0)/count(*),2) pct_zero, round(100.0*count(*) FILTER (WHERE citationCount>0)/count(*),2) pct_pos,
 quantile_cont(citationCount,0.5) FILTER (WHERE citationCount>0) p50_pos, quantile_cont(citationCount,0.9) p90, quantile_cont(citationCount,0.99) p99, quantile_cont(citationCount,0.999) p999, max(citationCount) mx, count(*) FILTER (WHERE citationCount<>floor(citationCount)) fractional
FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | pct_null | pct_zero | pct_pos | p50_pos | p90 | p99 | p999 | mx | fractional |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 23.64 | 76.36 | 15 | 71 | 370 | 1,459 | 131,561 | 0 |
| 1 | 0 | 53.06 | 46.94 | 5 | 16 | 84 | 306 | 93,704 | 0 |
| ALL | 0 | 50.11 | 49.89 | 6 | 21 | 120 | 506 | 131,561 | 0 |

_4 s_

### F8 publisher: distinct values and concentration

```sql
WITH c AS (SELECT publisher, count(*) n FROM wf WHERE publisher IS NOT NULL GROUP BY 1)
SELECT count(*) distinct_publishers, sum(n) works_with_publisher, count(*) FILTER (WHERE n>=1000) publishers_ge_1000_works, count(*) FILTER (WHERE n>=100) publishers_ge_100, count(*) FILTER (WHERE n=1) publishers_with_1_work,
 round(100.0*sum(n) FILTER (WHERE n>=1000)/sum(n),2) pct_works_covered_by_ge_1000_publishers, (SELECT round(100.0*sum(n)/(SELECT sum(n) FROM c),2) FROM (SELECT n FROM c ORDER BY n DESC LIMIT 100)) pct_covered_by_top_100, (SELECT round(100.0*sum(n)/(SELECT sum(n) FROM c),2) FROM (SELECT n FROM c ORDER BY n DESC LIMIT 1000)) pct_covered_by_top_1000 FROM c
```

| distinct_publishers | works_with_publisher | publishers_ge_1000_works | publishers_ge_100 | publishers_with_1_work | pct_works_covered_by_ge_1000_publishers | pct_covered_by_top_100 | pct_covered_by_top_1000 |
|---|---|---|---|---|---|---|---|
| 310,891 | 46,001,342 | 2722 | 15,796 | 178,765 | 87.98 | 67.17 | 81.72 |

_0 s_

### F8b top 20 publishers

```sql
SELECT publisher, count(*) works FROM wf WHERE publisher IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 20
```

| publisher | works |
|---|---|
| Elsevier BV | 6,921,394 |
| Springer Science and Business Media LLC | 3,751,165 |
| Wiley | 2,119,592 |
| MDPI AG | 1,776,969 |
| IEEE | 1,646,342 |
| Informa UK Limited | 1,152,179 |
| Oxford University Press (OUP) | 837,704 |
| Springer International Publishing | 682,299 |
| SAGE Publications | 670,833 |
| Ovid Technologies (Wolters Kluwer Health) | 638,216 |
| Institute of Electrical and Electronics Engineers (IEEE) | 635,240 |
| Zenodo | 623,785 |
| Frontiers Media SA | 606,186 |
| American Chemical Society (ACS) | 591,678 |
| IOP Publishing | 492,656 |
| Cambridge University Press (CUP) | 345,363 |
| Springer Nature Switzerland | 332,012 |
| openRxiv | 305,275 |
| Copernicus GmbH | 297,078 |
| Royal Society of Chemistry (RSC) | 296,581 |

_0 s_

### F8c distinct publishers by tier

```sql
SELECT link_tier, count(DISTINCT publisher) distinct_publishers FROM wf GROUP BY 1 ORDER BY 1
```

| link_tier | distinct_publishers |
|---|---|
| 0 | 39,065 |
| 1 | 295,563 |

_0 s_


_section runtime total: 37 s, job 8981162_



---

# G. Works: PDF / landing URL

**Design meaning (G).**
- **The PDF button will be hidden for most works**: `pdf_url` (rule below) exists for **36.8% of tier 0, 10.8% of tier 1, 13.4% of all works**; 77.1% of works whose `bestAccessRight` is OPEN still have no pdf_url (56.6% in tier 0). `landing_url` exists for **99.65%** (86.5% via DOI, 6.9% first OPEN url, 6.3% first url); only 0.35% have neither.
- Stages of the pdf rule (tier 0 / tier 1): open `.pdf` 22.3% / 5.8%; open `/pdf` (e.g. `mdpi.com/.../pdf`, wiley `pdfdirect`) 13.4% / 4.4%; any-instance `.pdf` 1.0% / 0.5%; any `/pdf` 0.04% / 0.08%.
- Size added per work: pdf_url avg 68 B (when present), landing avg 42 B; **2.57 GB raw for 50M** (0.34 GB tier 0). Hosts: pdf_url dominated by mdpi.com 8.4%, arxiv.org 5.5%, link.springer.com 4.1%, wiley 3.5%, nature.com 3.1%, frontiersin 3.1%, oup 3.0% (top 20 in G3); landing: doi.org 86.9%, hdl.handle.net 3.9%, pubmed 0.7%. **No sci-hub/libgen/z-lib style hosts** in a 735k-url sample; 63 IP-address urls; 11.4% of urls are plain `http://`; 1,205 of 735,162 urls contain an HTML-escaped `&amp;` -> the macros unescape them.
- Rule is verified: the reusable macros equal the materialised features on 50,101 sampled works (0 mismatches), see G4. Macro SQL below.

### G6 the extraction rules as reusable DuckDB SQL (temp macros, defined once per connection; also used by the M export)

```sql
CREATE OR REPLACE TEMP MACRO wk_is_pdf_strict(u) AS regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$');
CREATE OR REPLACE TEMP MACRO wk_is_pdf_loose(u) AS (regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$') OR contains(lower(u), '/pdf'));
CREATE OR REPLACE TEMP MACRO wk_all_urls(ins) AS flatten(list_transform(ins, i -> coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_open_urls(ins) AS flatten(list_transform(list_filter(ins, i -> i.accessRight.label = 'OPEN'), i -> coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_doi_work(pids) AS regexp_replace(list_filter(pids, p -> lower(p.scheme) = 'doi')[1].value, '^https?://(dx\.)?doi\.org/', '');
CREATE OR REPLACE TEMP MACRO wk_doi_any(pids, ins) AS coalesce(wk_doi_work(pids),
    regexp_replace(list_filter(flatten(list_transform(ins, i -> coalesce(i.pids, []))), p -> lower(p.scheme) = 'doi')[1].value, '^https?://(dx\.)?doi\.org/', ''));


CREATE OR REPLACE TEMP MACRO wk_pdf_url(ins) AS replace(coalesce(
    list_filter(wk_open_urls(ins), u -> wk_is_pdf_strict(u))[1],
    list_filter(wk_open_urls(ins), u -> wk_is_pdf_loose(u))[1],
    list_filter(wk_all_urls(ins), u -> wk_is_pdf_strict(u))[1],
    list_filter(wk_all_urls(ins), u -> wk_is_pdf_loose(u))[1]), '&amp;', '&');
CREATE OR REPLACE TEMP MACRO wk_landing_url(pids, ins) AS replace(coalesce('https://doi.org/' || wk_doi_any(pids, ins), wk_open_urls(ins)[1], wk_all_urls(ins)[1]), '&amp;', '&');
```

Use: `wk_pdf_url(w.instances) AS pdf_url`, `wk_landing_url(w.pids, w.instances) AS landing_url`, `wk_doi_any(w.pids, w.instances) AS doi`.
Rule text: **pdf_url** = first url of an OPEN instance ending in `.pdf` (query string ignored, case-insensitive), else first OPEN url containing `/pdf`, else first `.pdf` url of any instance, else first url containing `/pdf`, else NULL; `&amp;` unescaped. **landing_url** = `https://doi.org/<doi>` (doi from the work pids, else from instance pids; scheme prefix stripped), else first url of an OPEN instance, else first url of any instance.

slurm job `8981162`

### G1 coverage per tier (share of works)

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) works,
 round(100.0*count(*) FILTER (WHERE n_urls>0)/count(*),2) pct_any_url, round(100.0*count(*) FILTER (WHERE pdf_any_strict IS NOT NULL)/count(*),2) pct_any_pdf_strict, round(100.0*count(*) FILTER (WHERE pdf_any_loose IS NOT NULL)/count(*),2) pct_any_pdf_or_slashpdf,
 round(100.0*count(*) FILTER (WHERE n_open_urls>0)/count(*),2) pct_open_url, round(100.0*count(*) FILTER (WHERE pdf_open_strict IS NOT NULL)/count(*),2) pct_open_pdf_strict, round(100.0*count(*) FILTER (WHERE pdf_open_loose IS NOT NULL)/count(*),2) pct_open_pdf_or_slashpdf,
 round(100.0*count(*) FILTER (WHERE doi_work IS NOT NULL)/count(*),2) pct_doi_work_pid, round(100.0*count(*) FILTER (WHERE doi_any IS NOT NULL)/count(*),2) pct_doi_any_pid FROM wf GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | works | pct_any_url | pct_any_pdf_strict | pct_any_pdf_or_slashpdf | pct_open_url | pct_open_pdf_strict | pct_open_pdf_or_slashpdf | pct_doi_work_pid | pct_doi_any_pid |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 5,001,873 | 99.55 | 23.57 | 36.78 | 83.53 | 22.33 | 35.71 | 89.03 | 89.03 |
| 1 | 44,998,127 | 99.66 | 6.34 | 10.82 | 53.71 | 5.84 | 10.27 | 86.16 | 86.16 |
| ALL | 50,000,000 | 99.65 | 8.07 | 13.42 | 56.7 | 7.49 | 12.81 | 86.45 | 86.45 |

_1 s_

### G2 coverage of the proposed rules (pdf_url, landing_url), per tier; bytes added per work

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, round(100.0*count(pdf_url)/count(*),2) pct_pdf_url, round(100.0*count(*) FILTER (WHERE pdf_open_strict IS NOT NULL OR pdf_open_loose IS NOT NULL)/count(*),2) pct_pdf_from_open_instance,
 round(100.0*count(landing_url)/count(*),2) pct_landing_url, round(100.0*count(*) FILTER (WHERE doi_any IS NOT NULL)/count(*),2) pct_landing_is_doi, round(100.0*count(*) FILTER (WHERE doi_any IS NULL AND first_open_url IS NOT NULL)/count(*),2) pct_landing_open_url,
 round(100.0*count(*) FILTER (WHERE doi_any IS NULL AND first_open_url IS NULL AND first_url IS NOT NULL)/count(*),2) pct_landing_any_url, round(100.0*count(*) FILTER (WHERE landing_url IS NULL AND pdf_url IS NULL)/count(*),2) pct_neither,
 round(avg(length(pdf_url)),1) avg_len_pdf, round(avg(length(landing_url)),1) avg_len_landing, round(avg(coalesce(length(pdf_url),0)+coalesce(length(landing_url),0)),1) avg_bytes_added_per_work,
 round(sum(coalesce(length(pdf_url),0)+coalesce(length(landing_url),0))/1e9,2) total_GB FROM wfx GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | pct_pdf_url | pct_pdf_from_open_instance | pct_landing_url | pct_landing_is_doi | pct_landing_open_url | pct_landing_any_url | pct_neither | avg_len_pdf | avg_len_landing | avg_bytes_added_per_work | total_GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 36.78 | 35.71 | 99.55 | 89.03 | 8.93 | 1.59 | 0.45 | 65.4 | 43.9 | 67.8 | 0.34 |
| 1 | 10.82 | 10.27 | 99.66 | 86.16 | 6.62 | 6.87 | 0.34 | 69.4 | 42.1 | 49.5 | 2.23 |
| ALL | 13.42 | 12.81 | 99.65 | 86.45 | 6.86 | 6.34 | 0.35 | 68.3 | 42.3 | 51.3 | 2.57 |

_3 s_

### G2b which stage of the pdf rule fires (tier split)

```sql
SELECT CASE WHEN pdf_open_strict IS NOT NULL THEN '1 open .pdf' WHEN pdf_open_loose IS NOT NULL THEN '2 open /pdf' WHEN pdf_any_strict IS NOT NULL THEN '3 any .pdf' WHEN pdf_any_loose IS NOT NULL THEN '4 any /pdf' ELSE '5 none' END stage,
 count(*) works, count(*) FILTER (WHERE link_tier=0) tier0, count(*) FILTER (WHERE link_tier=1) tier1, round(100.0*count(*) FILTER (WHERE link_tier=0)/(SELECT count(*) FROM wf WHERE link_tier=0),2) pct_tier0, round(100.0*count(*) FILTER (WHERE link_tier=1)/(SELECT count(*) FROM wf WHERE link_tier=1),2) pct_tier1 FROM wf GROUP BY 1 ORDER BY 1
```

| stage | works | tier0 | tier1 | pct_tier0 | pct_tier1 |
|---|---|---|---|---|---|
| 1 open .pdf | 3,746,343 | 1,117,111 | 2,629,232 | 22.33 | 5.84 |
| 2 open /pdf | 2,659,743 | 668,839 | 1,990,904 | 13.37 | 4.42 |
| 3 any .pdf | 267,223 | 51,463 | 215,760 | 1.03 | 0.48 |
| 4 any /pdf | 36,943 | 2195 | 34,748 | 0.04 | 0.08 |
| 5 none | 43,289,748 | 3,162,265 | 40,127,483 | 63.22 | 89.18 |

_0 s_

### G2c OA works (bestAccessRight OPEN) without any pdf_url

```sql
SELECT coalesce(link_tier::VARCHAR,'ALL') link_tier, count(*) open_works, round(100.0*count(*) FILTER (WHERE pdf_url IS NULL)/count(*),2) pct_without_pdf_url FROM wfx WHERE best_access='OPEN' GROUP BY ROLLUP(link_tier) ORDER BY 1
```

| link_tier | open_works | pct_without_pdf_url |
|---|---|---|
| 0 | 4,194,814 | 56.56 |
| 1 | 24,347,584 | 80.68 |
| ALL | 28,542,398 | 77.13 |

_1 s_

### G3 top-20 hosts of the chosen pdf_url

```sql
SELECT regexp_extract(pdf_url,'^https?://([^/:]+)',1) host, count(*) works, round(100.0*count(*)/(SELECT count(pdf_url) FROM wfx),2) pct FROM wfx WHERE pdf_url IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 20
```

| host | works | pct |
|---|---|---|
| www.mdpi.com | 566,354 | 8.44 |
| arxiv.org | 370,810 | 5.53 |
| link.springer.com | 275,549 | 4.11 |
| onlinelibrary.wiley.com | 236,850 | 3.53 |
| www.nature.com | 210,563 | 3.14 |
| www.frontiersin.org | 207,368 | 3.09 |
| academic.oup.com | 199,536 | 2.97 |
| escholarship.org | 114,526 | 1.71 |
| www.tandfonline.com | 99,134 | 1.48 |
| www.researchsquare.com | 91,672 | 1.37 |
| www.biorxiv.org | 90,663 | 1.35 |
| journals.sagepub.com | 81,094 | 1.21 |
| www.cell.com | 75,676 | 1.13 |
| ieeexplore.ieee.org | 75,646 | 1.13 |
| downloads.hindawi.com | 75,614 | 1.13 |
| curis.ku.dk | 47,783 | 0.71 |
| www.cambridge.org | 46,716 | 0.7 |
| repository.ubn.ru.nl | 45,888 | 0.68 |
| www.pure.ed.ac.uk | 41,883 | 0.62 |
| hal.archives-ouvertes.fr | 39,772 | 0.59 |

_1 s_

### G3b top-20 hosts of the chosen landing_url

```sql
SELECT regexp_extract(landing_url,'^https?://([^/:]+)',1) host, count(*) works, round(100.0*count(*)/50000000,2) pct FROM wfx WHERE landing_url IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 20
```

| host | works | pct |
|---|---|---|
| doi.org | 43,461,071 | 86.92 |
| hdl.handle.net | 1,934,421 | 3.87 |
| pubmed.ncbi.nlm.nih.gov | 332,529 | 0.67 |
| hal.science | 298,174 | 0.6 |
| ec.europa.eu | 187,248 | 0.37 |
| urn.kb.se | 118,007 | 0.24 |
| ibn.idsi.md | 80,991 | 0.16 |
| elib.bsu.by | 75,874 | 0.15 |
| dx.doi.org | 73,500 | 0.15 |
| www.nusl.cz | 70,731 | 0.14 |
| urn.fi | 70,264 | 0.14 |
| elar.urfu.ru | 69,065 | 0.14 |
| shs.hal.science | 63,671 | 0.13 |
| acikbilim.yok.gov.tr | 62,719 | 0.13 |
| rep.bntu.by | 62,386 | 0.12 |
| urn.nsk.hr | 61,360 | 0.12 |
| dspace.susu.ru | 45,945 | 0.09 |
| www.ec.europa.eu | 45,754 | 0.09 |
| elib.belstu.by | 45,599 | 0.09 |
| repository.kpi.kharkov.ua | 45,320 | 0.09 |

_3 s_

### G3c top-30 hosts over ALL instance urls (0.5% hash sample of works), and suspicious hosts

```sql
WITH u AS (SELECT unnest(wk_all_urls(instances)) url FROM work WHERE hash(id) % 200 = 0), h AS (SELECT regexp_extract(url,'^https?://([^/:]+)',1) host, url FROM u)
SELECT host, count(*) urls, round(100.0*count(*)/(SELECT count(*) FROM h),2) pct, count(*) FILTER (WHERE wk_is_pdf_loose(url)) pdf_like, any_value(url) example FROM h GROUP BY 1 ORDER BY 2 DESC LIMIT 30
```

| host | urls | pct | pdf_like | example |
|---|---|---|---|---|
| doi.org | 294,226 | 40.02 | 20 | https://doi.org/10.1016/j.apsusc.2021.149281 |
| dx.doi.org | 160,044 | 21.77 | 8 | https://dx.doi.org/10.1016/j.apsusc.2021.149281 |
| pubmed.ncbi.nlm.nih.gov | 61,846 | 8.41 | 0 | https://pubmed.ncbi.nlm.nih.gov/32897538 |
| hdl.handle.net | 40,345 | 5.49 | 0 | https://hdl.handle.net/11365/1106082 |
| doaj.org | 30,294 | 4.12 | 0 | https://doaj.org/article/3e6192eec2294efd8aa033f3ce692dff |
| arxiv.org | 11,043 | 1.5 | 1965 | http://arxiv.org/abs/2003.12334 |
| hal.science | 8456 | 1.15 | 1 | https://hal.science/hal-03123760v1 |
| zenodo.org | 5507 | 0.75 | 78 | https://zenodo.org/records/15184318 |
| pmc.ncbi.nlm.nih.gov | 3608 | 0.49 | 6 | https://pmc.ncbi.nlm.nih.gov/articles/PMC12705277/ |
| www.mdpi.com | 3350 | 0.46 | 2963 | https://www.mdpi.com/2227-9067/8/5/392/pdf |
| zbmath.org | 3208 | 0.44 | 0 | https://zbmath.org/7184547 |
| escholarship.org | 1910 | 0.26 | 719 | https://escholarship.org/uc/item/8b81r54h |
| dergipark.org.tr | 1749 | 0.24 | 0 | https://dergipark.org.tr/tr/pub/jrespharm/issue/91554/1688699 |
| link.springer.com | 1743 | 0.24 | 1440 | https://link.springer.com/content/pdf/10.1007/s11418-021-01489-y.pdf |
| www.scopus.com | 1568 | 0.21 | 0 | https://www.scopus.com/inward/record.url?partnerID=HzOxMe3b&origin=inward&scp=85098859661 |
| onlinelibrary.wiley.com | 1510 | 0.21 | 1352 | https://onlinelibrary.wiley.com/doi/pdfdirect/10.1111/jdi.13099 |
| urn.kb.se | 1485 | 0.2 | 0 | http://urn.kb.se/resolve?urn=urn:nbn:se:uu:diva-498547 |
| www.frontiersin.org | 1212 | 0.16 | 1143 | https://www.frontiersin.org/articles/10.3389/fpls.2019.00560/pdf |
| www.scielo.br | 1171 | 0.16 | 190 | http://www.scielo.br/scielo.php?script=sci_arttext&pid=S1807-59322026000100629&lng=en&tlng=en |
| www.nature.com | 1142 | 0.16 | 1074 | https://www.nature.com/articles/s40494-025-02003-3 |
| europepmc.org | 1078 | 0.15 | 0 | https://europepmc.org/articles/pmc2225991?pdf=render |
| academic.oup.com | 1023 | 0.14 | 960 | https://academic.oup.com/ndt/article-pdf/33/suppl_1/i415/24823982/gfy104.sp214.pdf |
| api.library.uq.edu.au | 995 | 0.14 | 129 | https://api.library.uq.edu.au/view/UQ:f39e003 |
| ec.europa.eu | 948 | 0.13 | 0 | https://ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5c2ddf58b&amp;appId=PPGMS |
| shs.hal.science | 936 | 0.13 | 0 | https://shs.hal.science/halshs-03842341v1 |
| vbn.aau.dk | 845 | 0.11 | 263 | https://vbn.aau.dk/da/publications/e572c271-5690-43a7-a8a8-9afbf3df656a |
| research.manchester.ac.uk | 821 | 0.11 | 0 | https://research.manchester.ac.uk/en/publications/3fe48148-18e6-4f4a-b75c-6af4b37add0a |
| hal.inrae.fr | 809 | 0.11 | 7 | https://hal.inrae.fr/hal-05490087v1/document |
| treatment.plazi.org | 794 | 0.11 | 0 | http://treatment.plazi.org/id/03EB87E0136C7A36B592457AB68BFF40 |
| journals.plos.org | 763 | 0.1 | 0 | https://journals.plos.org/plosbiology/article/file?id=10.1371/journal.pbio.3000692&type=printable |

_5 s_

### G3d suspicious url hosts among ALL urls in the sample (sci-hub/libgen/anna/z-lib style, ip hosts, localhost, http only)

```sql
WITH u AS (SELECT unnest(wk_all_urls(instances)) url FROM work WHERE hash(id) % 200 = 0)
SELECT count(*) urls, count(*) FILTER (WHERE lower(url) SIMILAR TO '.*(sci-hub|scihub|libgen|z-lib|annas-archive|sci-net).*') scihub_like, count(*) FILTER (WHERE regexp_matches(url,'^https?://[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')) ip_hosts,
 count(*) FILTER (WHERE url LIKE 'http://%') http_only, count(*) FILTER (WHERE url NOT LIKE 'http%') non_http, count(*) FILTER (WHERE length(url)>300) longer_300, max(length(url)) max_len FROM u
```

| urls | scihub_like | ip_hosts | http_only | non_http | longer_300 | max_len |
|---|---|---|---|---|---|---|
| 735,162 | 0 | 63 | 83,471 | 0 | 34 | 2047 |

_3 s_

### G3e urls containing an HTML entity (&amp;) in the raw data (0.5% sample) -> the rule unescapes them

```sql
SELECT count(*) urls, count(*) FILTER (WHERE url LIKE '%&amp;%') with_amp FROM (SELECT unnest(wk_all_urls(instances)) url FROM work WHERE hash(id) % 200 = 0)
```

| urls | with_amp |
|---|---|
| 735,162 | 1205 |

_3 s_

### G4 rule sanity: the reusable macros give the same result as the materialized features (0.1% sample, expect 0 mismatches)

```sql
SELECT count(*) sampled, count(*) FILTER (WHERE wk_pdf_url(w.instances) IS DISTINCT FROM f.pdf_url) pdf_mismatch, count(*) FILTER (WHERE wk_landing_url(w.pids, w.instances) IS DISTINCT FROM f.landing_url) landing_mismatch
FROM work w JOIN wfx f ON f.id=w.id WHERE hash(w.id) % 1000 = 0
```

| sampled | pdf_mismatch | landing_mismatch |
|---|---|---|
| 50,101 | 0 | 0 |

_4 s_

### G5 15 example rows across tiers and stages

```sql
WITH x AS (SELECT f.id, f.link_tier, f.pdf_url, f.landing_url, CASE WHEN pdf_open_strict IS NOT NULL THEN 'open .pdf' WHEN pdf_open_loose IS NOT NULL THEN 'open /pdf' WHEN pdf_any_strict IS NOT NULL THEN 'any .pdf' WHEN pdf_any_loose IS NOT NULL THEN 'any /pdf' ELSE 'none' END stage,
   row_number() OVER (PARTITION BY f.link_tier, CASE WHEN pdf_open_strict IS NOT NULL THEN 1 WHEN pdf_open_loose IS NOT NULL THEN 2 WHEN pdf_any_strict IS NOT NULL THEN 3 WHEN pdf_any_loose IS NOT NULL THEN 4 ELSE 5 END ORDER BY hash(f.id)) rn
   FROM wfx f WHERE hash(f.id) % 5000 = 0)
SELECT x.link_tier, x.stage, left(w.title,70) title, x.pdf_url, x.landing_url FROM x JOIN work w ON w.id=x.id WHERE rn<=2 ORDER BY x.link_tier, x.stage LIMIT 15
```

| link_tier | stage | title | pdf_url | landing_url |
|---|---|---|---|---|
| 0 | any .pdf | A Critique of Modern SQL And A Proposal Towards A Simple and Expressiv | https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf | https://doi.org/10.5281/zenodo.14843459 |
| 0 | any .pdf | Phase transitions of the typical algorithmic complexity of the random  | http://oops.uni-oldenburg.de/4466/1/2018-104_schawe_article_journal.pone.0215309.pdf | https://doi.org/10.1371/journal.pone.0215309 |
| 0 | none | Patient and facility characteristics of an NDM-producing <i>Acinetobac | None | https://doi.org/10.1017/ash.2023.240 |
| 0 | none | Final Planning and Activity Reports for WP2 | None | https://www.ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5b939f128&appId=PPGMS |
| 0 | open .pdf | Clinical and social outcomes of adolescent self harm: population based | https://www.bmj.com/content/349/bmj.g5954.full.pdf | https://doi.org/10.1136/bmj.g5954 |
| 0 | open .pdf | Modular invariant models of lepton masses at levels 4 and 5 | https://link.springer.com/content/pdf/10.1007/JHEP02(2020)001.pdf | https://doi.org/10.1007/jhep02(2020)001 |
| 0 | open /pdf | Cross‐Metathesis of Biosourced Fatty Acid Derivatives: A Step Further  | https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/cssc.201403366 | https://doi.org/10.1002/cssc.201403170 |
| 0 | open /pdf | Numerical Comparison of a Combined Hydrothermal Carbonization and Anae | https://www.mdpi.com/2227-9717/8/1/43/pdf | https://doi.org/10.3390/pr8010043 |
| 1 | any .pdf | Self-efficacy of vision-impaired students: impacts of technology-enhan | https://eprints.soton.ac.uk/444060/2/Permission_to_deposit_thesis_form_Elkhereiji_1_.pdf | https://eprints.soton.ac.uk/444060/2/Permission_to_deposit_thesis_form_Elkhereiji_1_.pdf |
| 1 | any .pdf | Dimensionality reduction techniques for simulations of the spectral ra | https://elib.dlr.de/130133/1/EGU2019-507.pdf | https://elib.dlr.de/130133/ |
| 1 | any /pdf | Implementacija arhitektonskog uzorka entitetsko- komponentnog sustava  | https://repozitorij.unipu.hr/islandora/object/unipu:5830/datastream/PDF | https://www.bib.irb.hr/1143376 |
| 1 | any /pdf | Iz klavirskog opusa Sergeja Bortkijeviča: izvedba skladbi | https://repozitorij.unipu.hr/islandora/object/unipu:8522/datastream/PDF | https://urn.nsk.hr/urn:nbn:hr:137:751222 |
| 1 | none | Ash formation and trace elements associations with fine particles in a | None | https://doi.org/10.1016/j.fuel.2020.119718 |
| 1 | none | Short-Term Dynamic Exchange Rate Model: IFEER Concept Development | None | https://doi.org/10.25046/aj050455 |
| 1 | open .pdf | Context Defined Aspects of Gamification for Factory Floor | http://jultika.oulu.fi/files/nbnfi-fe2020062645834.pdf | https://doi.org/10.1109/vs-games.2019.8864527 |

_3 s_


_section runtime total: 26 s, job 8981162_



---

# H. Projects: text and classifier

**Design meaning (H).**
- **`summary` is NULL for 86.3% of projects** (only 533,148 have one; avg 1,716 chars, p50 1,438, p99 5,960); `keywords` NULL 87.0%; `acronym` NULL 96.9% (120,629 have one). Total text: summary 0.92 GB, titles 0.26 GB, keywords 0.04 GB -> the projects index is small (~1.2 GB text). Do not promise abstract search on most projects.
- `pred`: >0.3: 27,903; >0.4: 24,237; >0.5: 21,262; >0.55: 20,110; >0.6: 19,027; >0.7: 16,840; >0.8: 14,498; >0.9: 11,023. `is_ch` = 21,280, i.e. **not exactly `pred > 0.55`** (1,170 `is_ch` projects have pred <= 0.55). `is_translated` = 136,473.
- `startDate`: NULL 223,875 (5.75%); 1900-2125 with junk (612,045 before 1990, 2,500+ in 2027-2037, one in 2125, 589 projects end before they start). Facet range should clamp to e.g. 1980..currentYear+3.
- Theme: NULL 3,831,465, Economy 57,248, Tourism 4,352. Pillars (projects with any 228,592): inclusive 29,944, sustainable 71,808, resilient 20,273, innovative 117,250, global 71,981; all 31 non-zero bit combos occur (top: innovative only 68,816, global only 43,373, sustainable only 31,597). OA mandate: publications 102,603, dataset 35,111 (dataset is always a subset of publications).

slurm job `8981156`

### H1 text length percentiles + NULL shares

```sql
SELECT 'length(summary)' m, count(summary) non_null, round(100.0*(count(*)-count(summary))/count(*),2) pct_null, min(length(summary)) mn, round(avg(length(summary)),1) avg, quantile_cont(length(summary),0.5) p50, quantile_cont(length(summary),0.9) p90, quantile_cont(length(summary),0.99) p99, max(length(summary)) mx FROM project
UNION ALL SELECT 'length(title)', count(title), round(100.0*(count(*)-count(title))/count(*),2), min(length(title)) mn, round(avg(length(title)),1) avg, quantile_cont(length(title),0.5) p50, quantile_cont(length(title),0.9) p90, quantile_cont(length(title),0.99) p99, max(length(title)) mx FROM project
UNION ALL SELECT 'length(keywords)', count(keywords), round(100.0*(count(*)-count(keywords))/count(*),2), min(length(keywords)) mn, round(avg(length(keywords)),1) avg, quantile_cont(length(keywords),0.5) p50, quantile_cont(length(keywords),0.9) p90, quantile_cont(length(keywords),0.99) p99, max(length(keywords)) mx FROM project
UNION ALL SELECT 'length(acronym)', count(acronym), round(100.0*(count(*)-count(acronym))/count(*),2), min(length(acronym)) mn, round(avg(length(acronym)),1) avg, quantile_cont(length(acronym),0.5) p50, quantile_cont(length(acronym),0.9) p90, quantile_cont(length(acronym),0.99) p99, max(length(acronym)) mx FROM project
```

| m | non_null | pct_null | mn | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|---|---|
| length(summary) | 533,148 | 86.31 | 1 | 1,716.5 | 1,438 | 3,824 | 5,959.53 | 32,777 |
| length(title) | 3,893,065 | 0 | 1 | 65.7 | 56 | 108 | 166 | 661 |
| length(keywords) | 507,115 | 86.97 | 1 | 81 | 62 | 157 | 480 | 2225 |
| length(acronym) | 120,629 | 96.9 | 1 | 8.4 | 8 | 13 | 20 | 157 |

_1 s_

### H1b total text bytes (summary+title+keywords) for the index-size estimate

```sql
SELECT round(sum(length(summary))/1e9,3) summary_GB_chars, round(sum(strlen(summary))/1e9,3) summary_GB_bytes, round(sum(strlen(title))/1e9,3) title_GB, round(sum(strlen(keywords))/1e9,3) keywords_GB FROM project
```

| summary_GB_chars | summary_GB_bytes | title_GB | keywords_GB |
|---|---|---|---|
| 0.915 | 0.917 | 0.256 | 0.041 |

_0 s_

### H2 pred histogram (pred >= threshold) and flags

```sql
SELECT count(*) projects, count(pred) with_pred, count(*) FILTER (WHERE pred>0.3) gt_0_3, count(*) FILTER (WHERE pred>0.4) gt_0_4, count(*) FILTER (WHERE pred>0.5) gt_0_5, count(*) FILTER (WHERE pred>0.55) gt_0_55,
 count(*) FILTER (WHERE pred>0.6) gt_0_6, count(*) FILTER (WHERE pred>0.7) gt_0_7, count(*) FILTER (WHERE pred>0.8) gt_0_8, count(*) FILTER (WHERE pred>0.9) gt_0_9,
 count(*) FILTER (WHERE is_ch) is_ch, count(*) FILTER (WHERE is_ch IS NULL) is_ch_null, count(*) FILTER (WHERE is_translated) is_translated,
 count(*) FILTER (WHERE is_ch AND pred<=0.55) is_ch_with_pred_le_0_55 FROM project
```

| projects | with_pred | gt_0_3 | gt_0_4 | gt_0_5 | gt_0_55 | gt_0_6 | gt_0_7 | gt_0_8 | gt_0_9 | is_ch | is_ch_null | is_translated | is_ch_with_pred_le_0_55 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3,893,065 | 3,893,065 | 27,903 | 24,237 | 21,262 | 20,110 | 19,027 | 16,840 | 14,498 | 11,023 | 21,280 | 0 | 136,473 | 1170 |

_0 s_

### H3 startDate year histogram

```sql
SELECT coalesce(year(startDate)::VARCHAR,'<NULL>') start_year, count(*) projects, count(*) FILTER (WHERE is_ch) is_ch FROM project GROUP BY 1 ORDER BY 1
```

| start_year | projects | is_ch |
|---|---|---|
| 1900 | 31 | 0 |
| 1917 | 4 | 0 |
| 1918 | 1 | 0 |
| 1926 | 1 | 0 |
| 1950 | 2 | 0 |
| 1952 | 97 | 0 |
| 1953 | 179 | 0 |
| 1954 | 388 | 0 |
| 1955 | 615 | 0 |
| 1956 | 741 | 0 |
| 1957 | 1152 | 0 |
| 1958 | 1363 | 0 |
| 1959 | 3010 | 0 |
| 1960 | 3754 | 1 |
| 1961 | 3966 | 5 |
| 1962 | 3036 | 3 |
| 1963 | 5759 | 9 |
| 1964 | 4796 | 1 |
| 1965 | 4785 | 4 |
| 1966 | 5409 | 5 |
| 1967 | 5580 | 7 |
| 1968 | 5826 | 9 |
| 1969 | 5804 | 7 |
| 1970 | 6105 | 7 |
| 1971 | 6815 | 4 |
| 1972 | 8678 | 4 |
| 1973 | 8437 | 6 |
| 1974 | 11,377 | 5 |
| 1975 | 10,363 | 4 |
| 1976 | 17,574 | 11 |
| 1977 | 26,064 | 12 |
| 1978 | 30,042 | 32 |
| 1979 | 27,422 | 15 |
| 1980 | 22,570 | 21 |
| 1981 | 21,559 | 11 |
| 1982 | 23,227 | 16 |
| 1983 | 34,086 | 18 |
| 1984 | 39,936 | 17 |
| 1985 | 51,388 | 27 |
| 1986 | 50,737 | 17 |
| 1987 | 51,914 | 21 |
| 1988 | 55,760 | 29 |
| 1989 | 51,692 | 54 |
| 1990 | 51,182 | 20 |
| 1991 | 55,392 | 14 |
| 1992 | 56,788 | 15 |
| 1993 | 50,549 | 30 |
| 1994 | 59,081 | 27 |
| 1995 | 53,518 | 40 |
| 1996 | 55,510 | 42 |
| 1997 | 65,287 | 59 |
| 1998 | 62,480 | 50 |
| 1999 | 66,931 | 45 |
| 2000 | 70,569 | 57 |
| 2001 | 73,691 | 79 |
| 2002 | 80,124 | 145 |
| 2003 | 81,057 | 153 |
| 2004 | 79,926 | 166 |
| 2005 | 82,134 | 247 |
| 2006 | 82,476 | 494 |
| 2007 | 93,226 | 543 |
| 2008 | 93,490 | 481 |
| 2009 | 115,357 | 473 |
| 2010 | 107,058 | 483 |
| 2011 | 97,969 | 553 |
| 2012 | 100,289 | 588 |
| 2013 | 96,250 | 537 |
| 2014 | 102,050 | 574 |
| 2015 | 106,107 | 608 |
| 2016 | 101,729 | 719 |
| 2017 | 110,568 | 693 |
| 2018 | 118,658 | 959 |
| 2019 | 115,486 | 920 |
| 2020 | 118,048 | 1100 |
| 2021 | 114,534 | 1035 |
| 2022 | 107,032 | 974 |
| 2023 | 97,734 | 1076 |
| 2024 | 77,227 | 880 |
| 2025 | 45,644 | 553 |
| 2026 | 9508 | 257 |
| 2027 | 1510 | 36 |
| 2028 | 666 | 8 |
| 2029 | 221 | 6 |
| 2030 | 63 | 1 |
| 2031 | 12 | 1 |
| 2032 | 4 | 0 |
| 2033 | 1 | 0 |
| 2034 | 3 | 0 |
| 2035 | 2 | 0 |
| 2036 | 1 | 0 |
| 2037 | 2 | 0 |
| 2125 | 1 | 0 |
| <NULL> | 223,875 | 5157 |

_0 s_

### H3b startDate/endDate oddities

```sql
SELECT min(startDate) mn, max(startDate) mx, count(*) FILTER (WHERE startDate IS NULL) null_start, count(*) FILTER (WHERE endDate IS NULL) null_end, count(*) FILTER (WHERE endDate<startDate) end_before_start, count(*) FILTER (WHERE year(startDate)<1990) before_1990 FROM project
```

| mn | mx | null_start | null_end | end_before_start | before_1990 |
|---|---|---|---|---|---|
| 1900-01-01 | 2125-05-30 | 223,875 | 376,985 | 589 | 612,045 |

_0 s_

### H4 theme counts

```sql
SELECT coalesce(theme,'<NULL>') theme, count(*) projects, count(*) FILTER (WHERE is_ch) is_ch FROM project GROUP BY 1 ORDER BY 2 DESC
```

| theme | projects | is_ch |
|---|---|---|
| <NULL> | 3,831,465 | 20,872 |
| Economy | 57,248 | 179 |
| Tourism | 4352 | 229 |

_0 s_

### H5 pillars per bit (inclusive=1, sustainable=2, resilient=4, innovative=8, global=16)

```sql
SELECT count(*) FILTER (WHERE pillars>0) any_pillar, count(*) FILTER (WHERE pillars&1>0) inclusive, count(*) FILTER (WHERE pillars&2>0) sustainable, count(*) FILTER (WHERE pillars&4>0) resilient,
 count(*) FILTER (WHERE pillars&8>0) innovative, count(*) FILTER (WHERE pillars&16>0) global_, max(pillars) max_value FROM project
```

| any_pillar | inclusive | sustainable | resilient | innovative | global_ | max_value |
|---|---|---|---|---|---|---|
| 228,592 | 29,944 | 71,808 | 20,273 | 117,250 | 71,981 | 31 |

_0 s_

### H5b pillars combos

```sql
SELECT pillars, bin(pillars) bits, count(*) projects FROM project WHERE pillars>0 GROUP BY 1 ORDER BY 3 DESC
```

| pillars | bits | projects |
|---|---|---|
| 8 | 1000 | 68,816 |
| 16 | 10000 | 43,373 |
| 2 | 10 | 31,597 |
| 10 | 1010 | 15,480 |
| 1 | 1 | 11,089 |
| 24 | 11000 | 10,183 |
| 4 | 100 | 10,051 |
| 9 | 1001 | 6576 |
| 18 | 10010 | 6214 |
| 26 | 11010 | 4856 |
| 11 | 1011 | 3763 |
| 3 | 11 | 2505 |
| 6 | 110 | 1847 |
| 12 | 1100 | 1803 |
| 14 | 1110 | 1409 |
| 20 | 10100 | 1286 |
| 17 | 10001 | 1198 |
| 25 | 11001 | 1018 |
| 27 | 11011 | 1011 |
| 22 | 10110 | 680 |
| 19 | 10011 | 640 |
| 30 | 11110 | 629 |
| 15 | 1111 | 564 |
| 28 | 11100 | 424 |
| 13 | 1101 | 416 |
| 5 | 101 | 391 |
| 7 | 111 | 304 |
| 31 | 11111 | 217 |
| 23 | 10111 | 92 |
| 29 | 11101 | 85 |
| 21 | 10101 | 75 |

_0 s_

### H6 open access mandates

```sql
SELECT count(*) FILTER (WHERE openAccessMandateForPublications) mandate_publications, count(*) FILTER (WHERE openAccessMandateForDataset) mandate_dataset,
 count(*) FILTER (WHERE openAccessMandateForPublications AND openAccessMandateForDataset) both_, count(*) FILTER (WHERE openAccessMandateForPublications IS NULL) pub_null, count(*) FILTER (WHERE openAccessMandateForDataset IS NULL) data_null FROM project
```

| mandate_publications | mandate_dataset | both_ | pub_null | data_null |
|---|---|---|---|---|
| 102,603 | 35,111 | 35,111 | 0 | 0 |

_0 s_


_section runtime total: 1 s, job 8981156_



---

# I. Topics

**Design meaning (I).**
- **Exactly one topic per project holds (max = 1)**, but **8,795 projects (0.23%) have none** -> `topic_id` must be nullable, the topic agg needs a `missing` bucket. 4,513 of the 4,516 topics are used; taxonomy is 4 domains, 26 fields, 252 subfields, all used (197 subfields, 26 fields, 1,715 topics by `is_ch` projects). 2,798 used topics have no DCH project -> the corpus-aware topic modal (D3) shows 1,715 topics instead of 4,513 in DCH.
- Projects per topic: avg 861, p50 390, p90 2,130, p99 6,651, max 21,120 (Geological and Geochemical Analysis). DCH top topics: Archaeology and Cultural Heritage 1,767, Cultural Heritage Management and Preservation 726, Libraries and Information Services 659. Score p50 0.277 (max 0.875, min 0): topic quality is mediocre, keep it a soft facet.

slurm job `8981156`

### I1 exactly one topic per project?

```sql
SELECT (SELECT max(c) FROM (SELECT source_id, count(*) c FROM relation_topic GROUP BY 1)) max_topics_per_project, (SELECT count(DISTINCT source_id) FROM relation_topic) projects_with_topic,
 (SELECT count(*) FROM relation_topic) rows_, (SELECT count(*) FROM project WHERE id NOT IN (SELECT source_id FROM relation_topic)) projects_without_topic,
 (SELECT count(DISTINCT type) FROM relation_topic) distinct_types, (SELECT count(*) FROM relation_topic WHERE topic_id NOT IN (SELECT id FROM topic)) rows_with_unknown_topic
```

| max_topics_per_project | projects_with_topic | rows_ | projects_without_topic | distinct_types | rows_with_unknown_topic |
|---|---|---|---|---|---|
| 1 | 3,884,270 | 3,884,270 | 8795 | 1 | 0 |

_0 s_

### I2 projects per topic: percentiles (over topics with >=1 project), topics with 0 projects

```sql
WITH c AS (SELECT topic_id, count(*) n, count(*) FILTER (WHERE p.is_ch) n_ch FROM relation_topic rt JOIN project p ON p.id=rt.source_id GROUP BY 1)
SELECT (SELECT count(*) FROM topic) topics_total, count(*) topics_used, (SELECT count(*) FROM topic WHERE id NOT IN (SELECT topic_id FROM c)) topics_with_0_projects, min(n) mn, round(avg(n),1) avg, quantile_cont(n,0.5) p50, quantile_cont(n,0.9) p90, quantile_cont(n,0.99) p99, max(n) mx,
 (SELECT count(*) FROM c WHERE n_ch=0) used_topics_with_0_ch_projects, (SELECT count(*) FROM c WHERE n_ch>0) topics_with_ch_projects FROM c
```

| topics_total | topics_used | topics_with_0_projects | mn | avg | p50 | p90 | p99 | mx | used_topics_with_0_ch_projects | topics_with_ch_projects |
|---|---|---|---|---|---|---|---|---|---|---|
| 4516 | 4513 | 3 | 1 | 860.7 | 390 | 2,129.8 | 6,650.92 | 21,120 | 2798 | 1715 |

_0 s_

### I2b top 20 topics by projects, with is_ch split

```sql
SELECT t.id, t.topic_name, t.subfield_name, t.field_name, t.domain_name, count(*) projects, count(*) FILTER (WHERE p.is_ch) is_ch_projects
FROM relation_topic rt JOIN project p ON p.id=rt.source_id JOIN topic t ON t.id=rt.topic_id GROUP BY ALL ORDER BY projects DESC LIMIT 20
```

| id | topic_name | subfield_name | field_name | domain_name | projects | is_ch_projects |
|---|---|---|---|---|---|---|
| 10,001 | Geological and Geochemical Analysis | Geophysics | Earth and Planetary Sciences | Physical Sciences | 21,120 | 19 |
| 14,446 | Civil and Geotechnical Engineering Research | General Engineering | Engineering | Physical Sciences | 17,748 | 11 |
| 10,345 | Hematopoietic Stem Cell Transplantation | Hematology | Medicine | Health Sciences | 17,732 | 0 |
| 11,020 | Immune Cell Function and Interaction | Immunology | Immunology and Microbiology | Life Sciences | 15,790 | 0 |
| 10,046 | Stability and Control of Uncertain Systems | Control and Systems Engineering | Engineering | Physical Sciences | 15,440 | 3 |
| 12,168 | Health and Medical Research Impacts | Public Health, Environmental and Occupational Health | Medicine | Health Sciences | 15,247 | 2 |
| 10,493 | Ion channel regulation and function | Molecular Biology | Biochemistry, Genetics and Molecular Biology | Life Sciences | 13,225 | 1 |
| 10,885 | Gene expression and cancer classification | Molecular Biology | Biochemistry, Genetics and Molecular Biology | Life Sciences | 12,929 | 1 |
| 10,543 | Prostate Cancer Treatment and Research | Pulmonary and Respiratory Medicine | Medicine | Health Sciences | 12,080 | 0 |
| 11,135 | Virology and Viral Diseases | Epidemiology | Medicine | Health Sciences | 11,871 | 0 |
| 12,041 | Public Health Policies and Education | General Health Professions | Health Professions | Health Sciences | 11,338 | 4 |
| 12,725 | transportation and logistics systems | Transportation | Social Sciences | Social Sciences | 10,948 | 5 |
| 11,178 | Receptor Mechanisms and Signaling | Molecular Biology | Biochemistry, Genetics and Molecular Biology | Life Sciences | 10,944 | 0 |
| 10,679 | Service-Oriented Architecture and Web Services | Information Systems | Computer Science | Physical Sciences | 10,769 | 14 |
| 12,324 | History of Medicine Studies | History | Arts and Humanities | Social Sciences | 10,692 | 6 |
| 13,765 | Social Science and Policy Research | General Social Sciences | Social Sciences | Social Sciences | 10,563 | 19 |
| 10,441 | Muscle Physiology and Disorders | Molecular Biology | Biochemistry, Genetics and Molecular Biology | Life Sciences | 10,467 | 0 |
| 10,157 | Sports Performance and Training | Orthopedics and Sports Medicine | Medicine | Health Sciences | 10,438 | 10 |
| 11,134 | Breast Lesions and Carcinomas | Pathology and Forensic Medicine | Medicine | Health Sciences | 10,114 | 0 |
| 10,494 | Plant Virus Research Studies | Plant Science | Agricultural and Biological Sciences | Life Sciences | 9858 | 0 |

_0 s_

### I2c top 20 topics among is_ch projects

```sql
SELECT t.id, t.topic_name, t.field_name, count(*) is_ch_projects FROM relation_topic rt JOIN project p ON p.id=rt.source_id JOIN topic t ON t.id=rt.topic_id WHERE p.is_ch GROUP BY ALL ORDER BY 4 DESC LIMIT 20
```

| id | topic_name | field_name | is_ch_projects |
|---|---|---|---|
| 13,595 | Archaeology and Cultural Heritage | Social Sciences | 1767 |
| 11,846 | Cultural Heritage Management and Preservation | Arts and Humanities | 726 |
| 14,380 | Libraries and Information Services | Arts and Humanities | 659 |
| 14,170 | Classical Studies and Philology | Arts and Humanities | 595 |
| 12,444 | Art, Politics, and Modernism | Arts and Humanities | 586 |
| 14,191 | Historical Art and Architecture Studies | Arts and Humanities | 401 |
| 11,657 | Digital and Traditional Archives Management | Arts and Humanities | 381 |
| 14,386 | Libraries, Manuscripts, and Books | Arts and Humanities | 372 |
| 10,941 | Musicology and Musical Analysis | Arts and Humanities | 268 |
| 13,621 | Ancient and Medieval Archaeology Studies | Arts and Humanities | 224 |
| 11,391 | Theatre and Performance Studies | Arts and Humanities | 222 |
| 12,950 | Educational Environments and Student Outcomes | Social Sciences | 212 |
| 14,207 | Higher Education Practises and Engagement | Social Sciences | 205 |
| 10,595 | Medieval Literature and History | Arts and Humanities | 188 |
| 12,777 | Philosophy, History, and Historiography | Arts and Humanities | 187 |
| 14,021 | Medieval European Literature and History | Arts and Humanities | 167 |
| 12,409 | Reformation and Early Modern Christianity | Arts and Humanities | 163 |
| 13,831 | Archaeological and Geological Studies | Arts and Humanities | 148 |
| 11,113 | Music History and Culture | Arts and Humanities | 146 |
| 13,302 | Amazonian Archaeology and Ethnohistory | Arts and Humanities | 145 |

_0 s_

### I3 topic hierarchy: levels in the taxonomy vs actually used

```sql
SELECT 'taxonomy' src, count(DISTINCT id) topics, count(DISTINCT subfield_id) subfields, count(DISTINCT field_id) fields, count(DISTINCT domain_id) domains FROM topic
UNION ALL SELECT 'used by any project', count(DISTINCT t.id), count(DISTINCT t.subfield_id), count(DISTINCT t.field_id), count(DISTINCT t.domain_id) FROM topic t WHERE t.id IN (SELECT topic_id FROM relation_topic)
UNION ALL SELECT 'used by is_ch project', count(DISTINCT t.id), count(DISTINCT t.subfield_id), count(DISTINCT t.field_id), count(DISTINCT t.domain_id) FROM topic t WHERE t.id IN (SELECT topic_id FROM relation_topic rt JOIN project p ON p.id=rt.source_id WHERE p.is_ch)
```

| src | topics | subfields | fields | domains |
|---|---|---|---|---|
| taxonomy | 4516 | 252 | 26 | 4 |
| used by any project | 4513 | 252 | 26 | 4 |
| used by is_ch project | 1715 | 197 | 26 | 4 |

_0 s_

### I3b domain / field counts of projects

```sql
SELECT t.domain_name, count(DISTINCT t.field_id) fields, count(DISTINCT t.subfield_id) subfields, count(DISTINCT t.id) topics, count(*) projects FROM relation_topic rt JOIN topic t ON t.id=rt.topic_id GROUP BY 1 ORDER BY 5 DESC
```

| domain_name | fields | subfields | topics | projects |
|---|---|---|---|---|
| Health Sciences | 5 | 63 | 843 | 1,147,460 |
| Physical Sciences | 10 | 89 | 1571 | 1,113,139 |
| Life Sciences | 5 | 42 | 614 | 885,957 |
| Social Sciences | 6 | 58 | 1485 | 737,714 |

_0 s_

### I4 relation_topic score

```sql
SELECT min(score) mn, quantile_cont(score,0.5) p50, max(score) mx, count(*) FILTER (WHERE score IS NULL) null_scores FROM relation_topic
```

| mn | p50 | mx | null_scores |
|---|---|---|---|
| 0 | 0.2774 | 0.8747 | 0 |

_0 s_


_section runtime total: 1 s, job 8981156_



---

# J. Minorities

**Design meaning (J).**
- 96 minority groups are used by 9,293 projects (9,528 project-group pairs); 182 of the 278 rows have no project at all. 211 projects carry 2 groups, 12 carry 3. Largest: Russians 2,242, Jewish 1,870, **Manx 1,787, Turkish 1,676**, Sámi 590, Dom 218, Silesians 200; 25 groups have exactly 1 project (39 have <= 2).
- **Title blob is tiny**: all titles of all groups = 753,476 chars in total, the largest group (Russians) 187k chars, so **no cap is needed** (or cap at 20k chars per group and stay well under 1 MB overall). Orgs per group: up to 4,202 (Turkish), geolocated typically 20-45% of them; groups whose projects have no org (Galaicos, Circassians, Flemish, Lemkos, Ostrobothnians) show NULL orgs -> the map is empty for them.
- Precision caution: Manx (1,787) and Russians/Turkish look too large to be genuine; keyword matching was not reviewed (READ_CORE says so). `merged_qids` not needed: every qid used by projects exists as primary `minority.qid` (J4b = 0).

slurm job `8981156`

### J1 projects per minority qid (all groups used), + text size, organizations

```sql
WITH pm AS (SELECT id project_id, unnest(minority_qid) qid, title FROM project WHERE len(minority_qid)>0),
 g AS (SELECT qid, count(*) projects, sum(length(title)) title_chars FROM pm GROUP BY 1),
 o AS (SELECT pm.qid, count(DISTINCT r.org_id) orgs, count(DISTINCT r.org_id) FILTER (WHERE org.geolocation IS NOT NULL) orgs_geolocated FROM pm JOIN proj_org r ON r.project_id=pm.project_id JOIN organization org ON org.id=r.org_id GROUP BY 1)
SELECT g.qid, m.group_name_en, g.projects, g.title_chars, o.orgs, o.orgs_geolocated FROM g LEFT JOIN minority m ON m.qid=g.qid LEFT JOIN o ON o.qid=g.qid ORDER BY g.projects DESC
```

| qid | group_name_en | projects | title_chars | orgs | orgs_geolocated |
|---|---|---|---|---|---|
| Q49542 | Russians | 2242 | 187,453 | 2358 | 1027 |
| Q7325 | Jewish people | 1870 | 147,911 | 2160 | 573 |
| Q125564 | Manx people | 1787 | 132,011 | 3911 | 709 |
| Q84072 | Turkish people | 1676 | 118,925 | 4202 | 804 |
| Q48199 | Sámi people | 590 | 49,901 | 626 | 189 |
| Q2656122 | Dom people | 218 | 17,873 | 270 | 80 |
| Q140472 | Silesians | 200 | 18,969 | 302 | 126 |
| Q170284 | Finns | 141 | 12,840 | 111 | 32 |
| Q179248 | Albanians | 121 | 10,044 | 398 | 154 |
| Q208551 | Laz people | 99 | 8537 | 166 | 54 |
| Q115175925 | Azoreans | 72 | 6918 | 13 | 3 |
| Q1061510 | Egyptians | 52 | 4452 | 94 | 42 |
| Q483047 | Uzbeks | 30 | 2851 | 135 | 97 |
| Q616803 | Walser | 28 | 1765 | 14 | 6 |
| Q383256 | Chams | 24 | 1868 | 87 | 30 |
| Q43482 | Franks | 23 | 1685 | 62 | 36 |
| Q2140968 | Sardinians | 21 | 1767 | 7 | 0 |
| Q1799968 | Ladins | 19 | 1630 | 90 | 31 |
| Q486316 | Yakuts | 17 | 1890 | 20 | 13 |
| Q140420 | Rusyns | 14 | 1191 | 13 | 3 |
| Q15474747 | Franconians | 13 | 1093 | 11 | 4 |
| Q2706746 | Aragonese people | 13 | 1293 | 34 | 14 |
| Q17161 | Etruscans | 13 | 773 | 25 | 12 |
| Q377085 | Assyrians | 12 | 968 | 12 | 8 |
| Q1970302 | Swabians | 11 | 856 | 28 | 10 |
| Q115944305 | Mirandese people | 9 | 469 | 11 | 5 |
| Q2556103 | Pashtuns | 9 | 955 | 18 | 5 |
| Q431164 | Arab-Christians | 8 | 867 | 9 | 5 |
| Q13048871 | Dalmatian people | 8 | 718 | 9 | 7 |
| Q846578 | Svan people | 8 | 670 | 24 | 10 |
| Q273824 | South Slavs | 7 | 526 | 11 | 4 |
| Q415693 | Akan people | 7 | 571 | 6 | 0 |
| Q144964 | Thracians | 7 | 685 | 3 | 0 |
| Q147540 | Székelys | 7 | 443 | 22 | 3 |
| Q855178 | Csángós | 7 | 576 | 15 | 4 |
| Q752526 | Galicians | 6 | 235 | 5 | 2 |
| Q172717 | Avars | 6 | 351 | 20 | 16 |
| Q5645960 | Irish Catholic | 6 | 460 | 8 | 5 |
| Q11326181 | Highlander | 5 | 531 | 20 | 16 |
| Q12632053 | Haci | 5 | 405 | 26 | 8 |
| Q2602788 | Groningers | 5 | 370 | 12 | 3 |
| Q2840535 | Aluku | 5 | 441 | 8 | 4 |
| Q97216241 | Galaicos | 5 | 355 | None | None |
| Q61362977 | Limburgish people | 4 | 365 | 11 | 0 |
| Q309197 | Bretons | 4 | 257 | 13 | 3 |
| Q106416 | Frisians | 4 | 294 | 6 | 1 |
| Q126756 | Basques | 4 | 286 | 10 | 3 |
| Q720982 | Crimean Karaites | 4 | 382 | 6 | 4 |
| Q115473 | Hazaras | 4 | 360 | 3 | 2 |
| Q185461 | Arameans | 4 | 302 | 7 | 4 |
| Q147239 | Kashubians | 3 | 218 | 12 | 2 |
| Q55598995 | Picard people | 3 | 183 | 8 | 2 |
| Q157139 | Baltic Germans | 3 | 439 | 8 | 3 |
| Q31230 | Chechens | 3 | 337 | 6 | 1 |
| Q2299931 | Wayana people | 3 | 147 | 6 | 3 |
| Q201111 | Aromanians | 3 | 205 | 22 | 9 |
| Q716937 | Arvanites | 3 | 170 | 9 | 5 |
| Q1060538 | Latgalians | 2 | 84 | 8 | 5 |
| Q216151 | Vietnamese people | 2 | 179 | 1 | 0 |
| Q38915 | Pomaks | 2 | 77 | 7 | 2 |
| Q1064662 | Serbs of Bosnia and Herzegovina | 2 | 153 | 4 | 3 |
| Q56975 | Arbëreshë | 2 | 87 | 3 | 2 |
| Q214361 | Mari people | 2 | 150 | 5 | 2 |
| Q427036 | Tigray people | 2 | 216 | 3 | 2 |
| Q273854 | Gauls | 2 | 225 | 3 | 2 |
| Q63884107 | Asia Minor Greeks | 2 | 120 | 2 | 0 |
| Q689831 | Oirats | 2 | 172 | 4 | 3 |
| Q115298569 | Eurytanes | 2 | 225 | 2 | 1 |
| Q25352442 | Amantes | 2 | 133 | 2 | 2 |
| Q842323 | Khalkhas | 2 | 188 | 1 | 1 |
| Q65202642 | Fani | 2 | 205 | 3 | 2 |
| Q16089590 | Romands | 1 | 82 | 2 | 1 |
| Q15763 | Circassians | 1 | 43 | None | None |
| Q960850 | Molise Croats | 1 | 10 | 4 | 0 |
| Q192616 | Nogais | 1 | 82 | 1 | 1 |
| Q7654710 | Swedish Indians | 1 | 38 | 1 | 0 |
| Q171795 | Abkhazians | 1 | 82 | 2 | 1 |
| Q1316922 | Tarján | 1 | 36 | 2 | 1 |
| Q18690619 | Kildin Saami | 1 | 96 | 1 | 0 |
| Q1341522 | Võros | 1 | 57 | 1 | 1 |
| Q610315 | Kven people | 1 | 92 | 1 | 1 |
| Q257528 | Suits | 1 | 148 | 1 | 1 |
| Q930242 | Hernici | 1 | 96 | 2 | 1 |
| Q3303204 | Hoti | 1 | 102 | 1 | 1 |
| Q86735 | Afro-Germans | 1 | 148 | 2 | 0 |
| Q1262476 | Torbeši | 1 | 180 | 1 | 1 |
| Q12631792 | Građani | 1 | 43 | 2 | 0 |
| Q146521 | Sorbs | 1 | 121 | 4 | 0 |
| Q13048507 | Bavarians | 1 | 101 | 1 | 0 |
| Q3651629 | camminanti | 1 | 43 | 2 | 0 |
| Q891187 | Boykos | 1 | 14 | 2 | 0 |
| Q15991880 | Arpitan people | 1 | 92 | 2 | 0 |
| Q242485 | Flemish people | 1 | 94 | None | None |
| Q837881 | Lemkos | 1 | 96 | None | None |
| Q107175958 | Ostrobothnians | 1 | 149 | None | None |
| Q851126 | Gorani people | 1 | 180 | 1 | 1 |

_0 s_

### J2 totals

```sql
SELECT count(DISTINCT qid) groups_used, count(*) project_group_pairs, count(DISTINCT project_id) projects, sum(length(title)) total_title_chars_with_duplicates,
 (SELECT count(*) FROM minority) minority_rows, (SELECT count(*) FROM minority WHERE qid NOT IN (SELECT DISTINCT unnest(minority_qid) FROM project)) minority_rows_without_project FROM (SELECT id project_id, unnest(minority_qid) qid, title FROM project WHERE len(minority_qid)>0)
```

| groups_used | project_group_pairs | projects | total_title_chars_with_duplicates | minority_rows | minority_rows_without_project |
|---|---|---|---|---|---|
| 96 | 9528 | 9293 | 753,476 | 278 | 182 |

_0 s_

### J3 projects carrying more than one minority (distribution)

```sql
SELECT len(minority_qid) n_minorities, count(*) projects FROM project GROUP BY 1 ORDER BY 1
```

| n_minorities | projects |
|---|---|
| 0 | 3,883,772 |
| 1 | 9070 |
| 2 | 211 |
| 3 | 12 |

_0 s_

### J3b top qid pairs on the same project

```sql
SELECT a, b, count(*) projects FROM (SELECT list_sort(minority_qid)[1] a, list_sort(minority_qid)[2] b FROM project WHERE len(minority_qid)>1) GROUP BY 1,2 ORDER BY 3 DESC LIMIT 15
```

| a | b | projects |
|---|---|---|
| Q7325 | Q84072 | 21 |
| Q49542 | Q84072 | 21 |
| Q49542 | Q7325 | 21 |
| Q125564 | Q84072 | 15 |
| Q125564 | Q7325 | 13 |
| Q170284 | Q49542 | 12 |
| Q48199 | Q49542 | 11 |
| Q48199 | Q7325 | 9 |
| Q125564 | Q48199 | 7 |
| Q125564 | Q49542 | 6 |
| Q125564 | Q2656122 | 6 |
| Q486316 | Q49542 | 4 |
| Q125564 | Q208551 | 4 |
| Q179248 | Q84072 | 3 |
| Q1799968 | Q7325 | 3 |

_0 s_

### J4 minority table: source_class values, merged_qids usage

```sql
SELECT source_class, count(*) n_groups, count(*) FILTER (WHERE len(merged_qids)>0) with_merged FROM minority GROUP BY 1 ORDER BY 2 DESC
```

| source_class | n_groups | with_merged |
|---|---|---|
| ['ethnic group'] | 224 | 224 |
| ['tribe'] | 29 | 29 |
| ['indigenous_to_europe'] | 12 | 12 |
| ['ethnoreligious group'] | 5 | 5 |
| ['ethnic group', 'ethnoreligious group'] | 3 | 3 |
| ['manual_seed'] | 2 | 2 |
| ['ethnic group', 'indigenous_to_europe'] | 1 | 1 |
| ['indigenous_to_europe', 'manual_seed'] | 1 | 1 |
| ['ethnic group', 'tribe'] | 1 | 1 |

_0 s_

### J4b project minority_qid values that are only in merged_qids (not a primary qid)

```sql
SELECT count(*) FROM (SELECT DISTINCT unnest(minority_qid) q FROM project) WHERE q NOT IN (SELECT qid FROM minority)
```

| count_star() |
|---|
| 0 |

_0 s_


_section runtime total: 0 s, job 8981156_



---

# K. Organizations

**Design meaning (K).**
- `legalName` never NULL (avg 32 chars, p99 110, max 376), `legalShortName` NULL 3.78%, `alternativeNames` present on 181,580 orgs (36.8%, avg 1.5, p99 5, max 24) -> autocomplete corpus is ~0.8-1.0M strings (legalName + shortName + alternativeNames). `rorId` on 25.58% (126,401, only 121,300 distinct: duplicates), wikiId 11.3%, website 38.2%, `nuts3` 8.75%, city 14.1%.
- `rorTypes` NULL/empty for **367,699 (74.4%)**, then company 33,939, education 24,744, funder 19,584, nonprofit 17,239, healthcare 14,949, facility 14,189, other 9,833, government 8,373, archive 3,328: a `ror_types` filter only applies to 25% of orgs.
- **Duplicates**: 46,065 `legalName` values occur > 1 time (100,325 rows, 54,260 surplus); (legalName, countryCode) duplicated for 37,083 pairs (78,042 rows). Top: Ministry of Health x55 (55 countries - legitimately different), but Karolinska Institutet x20, Lund University x19, University of Copenhagen x18, Chalmers x16 (same country/no country, same institution). Expect duplicate hits in org search and split rollups for experts.
- Project-connected 348,578, works-only 90,033, both 85,764, neither 55,488 (unlinked). Region NULL: 157,268 orgs (150,970 project-connected).

slurm job `8981156`

### K1 rorTypes distribution

```sql
SELECT coalesce(t,'<no rorTypes>') rorType, count(*) orgs FROM (SELECT unnest(CASE WHEN rorTypes IS NULL OR len(rorTypes)=0 THEN [NULL] ELSE rorTypes END) t FROM organization) GROUP BY 1 ORDER BY 2 DESC
```

| rorType | orgs |
|---|---|
| <no rorTypes> | 367,699 |
| company | 33,939 |
| education | 24,744 |
| funder | 19,584 |
| nonprofit | 17,239 |
| healthcare | 14,949 |
| facility | 14,189 |
| other | 9833 |
| government | 8373 |
| archive | 3328 |

_0 s_

### K2 lengths + NULL shares

```sql
SELECT 'len(alternativeNames)' m, count(alternativeNames) non_null, min(len(alternativeNames)) mn, round(avg(len(alternativeNames)),1) avg, quantile_cont(len(alternativeNames),0.5) p50, quantile_cont(len(alternativeNames),0.9) p90, quantile_cont(len(alternativeNames),0.99) p99, max(len(alternativeNames)) mx FROM organization
UNION ALL SELECT 'length(legalName)', count(legalName), min(length(legalName)) mn, round(avg(length(legalName)),1) avg, quantile_cont(length(legalName),0.5) p50, quantile_cont(length(legalName),0.9) p90, quantile_cont(length(legalName),0.99) p99, max(length(legalName)) mx FROM organization
UNION ALL SELECT 'length(legalShortName)', count(legalShortName), min(length(legalShortName)) mn, round(avg(length(legalShortName)),1) avg, quantile_cont(length(legalShortName),0.5) p50, quantile_cont(length(legalShortName),0.9) p90, quantile_cont(length(legalShortName),0.99) p99, max(length(legalShortName)) mx FROM organization
UNION ALL SELECT 'sum length(alternativeNames)', count(alternativeNames), min(list_sum(list_transform(alternativeNames, x -> length(x)))) mn, round(avg(list_sum(list_transform(alternativeNames, x -> length(x)))),1) avg, quantile_cont(list_sum(list_transform(alternativeNames, x -> length(x))),0.5) p50, quantile_cont(list_sum(list_transform(alternativeNames, x -> length(x))),0.9) p90, quantile_cont(list_sum(list_transform(alternativeNames, x -> length(x))),0.99) p99, max(list_sum(list_transform(alternativeNames, x -> length(x)))) mx FROM organization
```

| m | non_null | mn | avg | p50 | p90 | p99 | mx |
|---|---|---|---|---|---|---|---|
| len(alternativeNames) | 181,580 | 1 | 1.5 | 1 | 3 | 5 | 24 |
| length(legalName) | 494,099 | 1 | 32.4 | 27 | 59 | 110 | 376 |
| length(legalShortName) | 475,410 | 1 | 27.2 | 22 | 55 | 108 | 376 |
| sum length(alternativeNames) | 181,580 | 1 | 35.6 | 28 | 72 | 161 | 1281 |

_0 s_

### K3 NULL shares

```sql
SELECT count(*) orgs, round(100.0*count(*) FILTER (WHERE legalName IS NULL)/count(*),3) pct_legalName_null, round(100.0*count(*) FILTER (WHERE legalShortName IS NULL)/count(*),2) pct_shortName_null,
 round(100.0*count(*) FILTER (WHERE rorId IS NOT NULL)/count(*),2) pct_with_rorId, round(100.0*count(*) FILTER (WHERE wikiId IS NOT NULL)/count(*),2) pct_with_wikiId, round(100.0*count(*) FILTER (WHERE websiteUrl IS NOT NULL)/count(*),2) pct_with_website,
 round(100.0*count(*) FILTER (WHERE nuts3 IS NOT NULL)/count(*),2) pct_nuts3, round(100.0*count(*) FILTER (WHERE address_city IS NOT NULL)/count(*),2) pct_city FROM organization
```

| orgs | pct_legalName_null | pct_shortName_null | pct_with_rorId | pct_with_wikiId | pct_with_website | pct_nuts3 | pct_city |
|---|---|---|---|---|---|---|---|
| 494,099 | 0 | 3.78 | 25.58 | 11.3 | 38.17 | 8.75 | 14.12 |

_0 s_

### K4 duplicated legalName: distinct duplicated names, rows involved, top 20

```sql
SELECT legalName, count(*) orgs, count(DISTINCT countryCode) countries, count(*) FILTER (WHERE id IN (SELECT org_id FROM proj_org)) project_connected FROM organization WHERE legalName IS NOT NULL GROUP BY 1 HAVING count(*)>1 ORDER BY 2 DESC LIMIT 20
```

| legalName | orgs | countries | project_connected |
|---|---|---|---|
| Ministry of Health | 55 | 54 | 14 |
| Ministry of Education | 23 | 22 | 2 |
| Karolinska Institutet | 20 | 0 | 20 |
| Ministry of Justice | 20 | 18 | 5 |
| Lund University | 19 | 1 | 19 |
| University of Copenhagen | 18 | 1 | 12 |
| Argosy University | 17 | 1 | 0 |
| Ministry of Foreign Affairs | 17 | 16 | 1 |
| Other Companies | 17 | 1 | 17 |
| Ministry of Culture | 16 | 14 | 3 |
| University of Manchester | 16 | 1 | 10 |
| Chalmers University of Technology | 16 | 1 | 16 |
| Ministry of Agriculture | 16 | 15 | 5 |
| Swedish University of Agricultural Sciences | 15 | 1 | 14 |
| České vysoké učení technické v Praze / Fakulta stavební | 15 | 0 | 15 |
| Linnaeus University | 14 | 1 | 14 |
| Ministry of Science and Technology | 14 | 10 | 10 |
| Mid Sweden University | 13 | 1 | 12 |
| Ministry of Finance | 13 | 11 | 4 |
| Institute of Physics | 13 | 11 | 6 |

_0 s_

### K4b duplicate summary

```sql
SELECT count(*) duplicated_names, sum(n) rows_involved, sum(n-1) surplus_rows FROM (SELECT count(*) n FROM organization WHERE legalName IS NOT NULL GROUP BY legalName HAVING count(*)>1)
```

| duplicated_names | rows_involved | surplus_rows |
|---|---|---|
| 46,065 | 100,325 | 54,260 |

_0 s_

### K4c duplicated (legalName, countryCode)

```sql
SELECT count(*) duplicated_pairs, sum(n) rows_involved FROM (SELECT count(*) n FROM organization WHERE legalName IS NOT NULL GROUP BY legalName, countryCode HAVING count(*)>1)
```

| duplicated_pairs | rows_involved |
|---|---|
| 37,083 | 78,042 |

_0 s_

### K5 countryCode top 20

```sql
SELECT coalesce(countryCode,'<NULL>') cc, count(*) orgs, count(*) FILTER (WHERE id IN (SELECT org_id FROM proj_org)) project_connected FROM organization GROUP BY 1 ORDER BY 2 DESC LIMIT 20
```

| cc | orgs | project_connected |
|---|---|---|
| <NULL> | 157,392 | 151,032 |
| US | 61,119 | 25,209 |
| FR | 31,567 | 24,484 |
| GB | 21,730 | 13,769 |
| DE | 20,700 | 14,534 |
| IT | 15,531 | 13,107 |
| ES | 15,140 | 12,772 |
| NL | 8622 | 6253 |
| JP | 7382 | 410 |
| GR | 7263 | 6479 |
| CH | 7141 | 5319 |
| CN | 6925 | 637 |
| PL | 6701 | 4741 |
| BE | 6695 | 5248 |
| TR | 6656 | 5580 |
| PT | 5756 | 3442 |
| IN | 5754 | 529 |
| CA | 5599 | 1145 |
| CZ | 5259 | 2410 |
| SE | 5197 | 3731 |

_0 s_

### K6 project-connected vs works-only orgs, by region

```sql
SELECT coalesce(region,'<NULL>') region, count(*) orgs, count(*) FILTER (WHERE p) project_connected, count(*) FILTER (WHERE NOT p AND w) works_only, count(*) FILTER (WHERE NOT p AND NOT w) neither
FROM (SELECT region, id IN (SELECT org_id FROM proj_org) p, id IN (SELECT org_id FROM work_org) w FROM organization) GROUP BY 1 ORDER BY 2 DESC
```

| region | orgs | project_connected | works_only | neither |
|---|---|---|---|---|
| <NULL> | 157,268 | 150,970 | 2880 | 3418 |
| Outside Europe | 128,681 | 43,073 | 57,585 | 28,023 |
| Central Europe | 51,578 | 36,316 | 7692 | 7570 |
| Southern Europe | 50,683 | 41,817 | 5049 | 3817 |
| Western Europe | 47,509 | 36,507 | 6218 | 4784 |
| Northern Europe | 46,643 | 32,651 | 7429 | 6563 |
| Eastern Europe | 11,737 | 7244 | 3180 | 1313 |

_1 s_

### K7 rorId / other id shapes

```sql
SELECT count(*) FILTER (WHERE rorId LIKE 'https://ror.org/%') ror_url_form, count(*) FILTER (WHERE rorId IS NOT NULL AND rorId NOT LIKE 'https://ror.org/%') ror_other_form, any_value(rorId) ex, count(DISTINCT rorId) distinct_ror, count(rorId) non_null_ror FROM organization
```

| ror_url_form | ror_other_form | ex | distinct_ror | non_null_ror |
|---|---|---|---|---|
| 126,401 | 0 | https://ror.org/023s2h905 | 121,300 | 126,401 |

_0 s_


_section runtime total: 2 s, job 8981156_



---

# L. Sanity

**Design meaning (L).**
- Ids are unique in all three tables, no NULL, no collision across tables, no duplicated `openaireId`, 1% relation sample (1.54M rows) has **0 dangling endpoints**, topic/minority links intact.
- All ids are 20-digit `UBIGINT` (max 18,446,744,035,794,761,783). **Above signed long: 1,946,332 projects, 247,204 orgs, 24,999,485 works (50%); above 2^53 (JavaScript-unsafe): 3,891,190 projects, 493,851 orgs, 49,975,496 works** -> strings everywhere incl. the API JSON (already the plan, D2), use `keyword` mapping; `unsigned_long` would still break JS clients.

slurm job `8981156`

### L1 id uniqueness

```sql
SELECT 'project' t, count(*) n, count(DISTINCT id) distinct_id, count(*) FILTER (WHERE id IS NULL) null_id, min(id) min_id, max(id) max_id, max(id) > 9223372036854775807 exceeds_signed_long, max(length(id::VARCHAR)) max_digits FROM project
UNION ALL SELECT 'organization', count(*), count(DISTINCT id), count(*) FILTER (WHERE id IS NULL), min(id), max(id), max(id) > 9223372036854775807, max(length(id::VARCHAR)) FROM organization
UNION ALL SELECT 'work', count(*), count(DISTINCT id), count(*) FILTER (WHERE id IS NULL), min(id), max(id), max(id) > 9223372036854775807, max(length(id::VARCHAR)) FROM work
```

| t | n | distinct_id | null_id | min_id | max_id | exceeds_signed_long | max_digits |
|---|---|---|---|---|---|---|---|
| project | 3,893,065 | 3,893,065 | 0 | 4,066,918,968,945 | 18,446,739,136,689,828,062 | True | 20 |
| organization | 494,099 | 494,099 | 0 | 134,545,224,674,195 | 18,446,725,206,911,490,895 | True | 20 |
| work | 50,000,000 | 50,000,000 | 0 | 1,285,962,078,103 | 18,446,744,035,794,761,783 | True | 20 |

_1 s_

### L1b share of ids above signed long, per table

```sql
SELECT (SELECT count(*) FROM project WHERE id > 9223372036854775807) projects_gt_signed, (SELECT count(*) FROM organization WHERE id > 9223372036854775807) orgs_gt_signed, (SELECT count(*) FROM work WHERE id > 9223372036854775807) works_gt_signed,
 (SELECT count(*) FROM work WHERE id > 9007199254740991) works_gt_2p53, (SELECT count(*) FROM project WHERE id > 9007199254740991) projects_gt_2p53, (SELECT count(*) FROM organization WHERE id > 9007199254740991) orgs_gt_2p53
```

| projects_gt_signed | orgs_gt_signed | works_gt_signed | works_gt_2p53 | projects_gt_2p53 | orgs_gt_2p53 |
|---|---|---|---|---|---|
| 1,946,332 | 247,204 | 24,999,485 | 49,975,496 | 3,891,190 | 493,851 |

_0 s_

### L1c id collisions across tables (same id in project/org/work)

```sql
SELECT (SELECT count(*) FROM project p JOIN organization o USING(id)) proj_org_collisions, (SELECT count(*) FROM project p JOIN work w USING(id)) proj_work_collisions
```

| proj_org_collisions | proj_work_collisions |
|---|---|
| 0 | 0 |

_0 s_

### L2 relation endpoint existence on a 1% hash sample

```sql
WITH r AS (SELECT * FROM relation WHERE hash(source, target) % 100 = 0)
SELECT count(*) sampled,
 count(*) FILTER (WHERE sourceType='project' AND source NOT IN (SELECT id FROM project)) missing_project_source,
 count(*) FILTER (WHERE sourceType='product' AND source NOT IN (SELECT id FROM work)) missing_work_source,
 count(*) FILTER (WHERE targetType='organization' AND target NOT IN (SELECT id FROM organization)) missing_org_target,
 count(*) FILTER (WHERE targetType='product' AND target NOT IN (SELECT id FROM work)) missing_work_target FROM r
```

| sampled | missing_project_source | missing_work_source | missing_org_target | missing_work_target |
|---|---|---|---|---|
| 1,536,722 | 0 | 0 | 0 | 0 |

_2 s_

### L2b topic / minority integrity

```sql
SELECT (SELECT count(*) FROM relation_topic WHERE source_id NOT IN (SELECT id FROM project)) rt_missing_project, (SELECT count(*) FROM relation_topic WHERE topic_id NOT IN (SELECT id FROM topic)) rt_missing_topic
```

| rt_missing_project | rt_missing_topic |
|---|---|
| 0 | 0 |

_0 s_

### L3 duplicate openaireId

```sql
SELECT (SELECT count(*)-count(DISTINCT openaireId) FROM project) dup_project_openaireId, (SELECT count(*)-count(DISTINCT openaireId) FROM organization) dup_org_openaireId, (SELECT count(*)-count(DISTINCT openaireId) FROM work) dup_work_openaireId
```

| dup_project_openaireId | dup_org_openaireId | dup_work_openaireId |
|---|---|---|
| 0 | 0 | 0 |

_2 s_


_section runtime total: 5 s, job 8981156_



---

# M. Export dry run

**Design meaning (M).**
- Parquet zstd sizes (all runs a few seconds to 21 s): **works tier 0 (5.0M) 910 MB (182 B/row)**, tier-1 1M-row sample 149 MB (149 B/row) -> **all 50M works = ~7.6 GB** (0.91 + 6.70). At the stated 6 MB/s that is ~0.4 h (the "20 GB/h" figure gives 0.4 h too): **works can go up from home, no university-network detour needed**.
- Projects: 3% sample 20 MB (172 B/row) -> 669 MB extrapolated; **the full 3,893,065 projects export was also run: 656 MB (exact)**. Organizations 494,099 rows: **44.8 MB**. Grants 6,075 rows: 0.2 MB. Everything except works = ~0.7 GB.
- Biggest compressed columns: works: authors 263 MB, title 198 MB, organisation_ids 138 MB, project_ids 72 MB, landing_url 55 MB, id 53 MB; projects: summary 304 MB, title 102 MB, openaireId 76 MB.
- Export views below use the macros of section G. **`pdf_url`/`landing_url` are HTML-unescaped** in the export, `id`/`project_ids`/`organisation_ids` are strings.

slurm job `8981163`

### M1a works tier 0 (all 5.0M project-linked works)

```sql
COPY (

SELECT w.id::VARCHAR AS id, w.title,
       list_transform(w.authors[1:20], a -> a.fullName) AS authors, len(w.authors) AS author_count,
       w.publicationDate AS publication_date, year(w.publicationDate) AS year,
       w.publisher, w.container.name AS container_name,
       w.openAccessColor AS open_access_color, w.bestAccessRight.label AS best_access_right,
       w.language.code AS language, w.citationCount AS citation_count,
       wk_doi_any(w.pids, w.instances) AS doi, wk_pdf_url(w.instances) AS pdf_url, wk_landing_url(w.pids, w.instances) AS landing_url,
       coalesce(pr.project_ids, []) AS project_ids, coalesce(og.organisation_ids, []) AS organisation_ids,
       w.link_tier, coalesce(pr.is_ch_via_project, false) AS is_ch_via_project
FROM work w
LEFT JOIN (SELECT r.target AS work_id, list(DISTINCT r.source::VARCHAR) AS project_ids, bool_or(coalesce(p.is_ch, false)) AS is_ch_via_project
           FROM relation r JOIN project p ON p.id = r.source WHERE r.sourceType = 'project' AND r.targetType = 'product' GROUP BY r.target) pr ON pr.work_id = w.id
LEFT JOIN (SELECT r.source AS work_id, list(DISTINCT r.target::VARCHAR) AS organisation_ids
           FROM relation r WHERE r.sourceType = 'product' AND r.targetType = 'organization' AND r.source IN (SELECT id FROM work WHERE link_tier = 0) GROUP BY r.source) og ON og.work_id = w.id
WHERE w.link_tier = 0
) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier0.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 5,001,873 | 20 | 910.5 | 182.0 |



### M1b works tier 1, 1M row sample (hash(id) % 45 = 0)

```sql
COPY (

SELECT w.id::VARCHAR AS id, w.title,
       list_transform(w.authors[1:20], a -> a.fullName) AS authors, len(w.authors) AS author_count,
       w.publicationDate AS publication_date, year(w.publicationDate) AS year,
       w.publisher, w.container.name AS container_name,
       w.openAccessColor AS open_access_color, w.bestAccessRight.label AS best_access_right,
       w.language.code AS language, w.citationCount AS citation_count,
       wk_doi_any(w.pids, w.instances) AS doi, wk_pdf_url(w.instances) AS pdf_url, wk_landing_url(w.pids, w.instances) AS landing_url,
       coalesce(pr.project_ids, []) AS project_ids, coalesce(og.organisation_ids, []) AS organisation_ids,
       w.link_tier, coalesce(pr.is_ch_via_project, false) AS is_ch_via_project
FROM work w
LEFT JOIN (SELECT r.target AS work_id, list(DISTINCT r.source::VARCHAR) AS project_ids, bool_or(coalesce(p.is_ch, false)) AS is_ch_via_project
           FROM relation r JOIN project p ON p.id = r.source WHERE r.sourceType = 'project' AND r.targetType = 'product' GROUP BY r.target) pr ON pr.work_id = w.id
LEFT JOIN (SELECT r.source AS work_id, list(DISTINCT r.target::VARCHAR) AS organisation_ids
           FROM relation r WHERE r.sourceType = 'product' AND r.targetType = 'organization' AND hash(r.source) % 45 = 0 GROUP BY r.source) og ON og.work_id = w.id
WHERE w.link_tier = 1 AND hash(w.id) % 45 = 0
) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier1_sample.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 999,844 | 9 | 148.8 | 148.8 |

(tier-1 works have no project ids, so the project join is empty; org filter samples with the same hash as the work filter)

### M1 extrapolation to all 50M works
| part | rows | bytes/row | est. GB |
|---|---|---|---|
| tier 0 (exact) | 5,001,873 | 182.0 | 0.91 |
| tier 1 (from 999,844 sample) | 44,998,127 | 148.8 | 6.70 |
| **total 50M** | 50,000,000 | 152.1 | **7.6** |

Upload time at 6 MB/s (50 Mbps): 0.4 h; at 20 GB/h: 0.4 h.

### M2a projects 3% random sample (hash(id) % 100 < 3), all columns + org_ids[] (coordinator first) + coordinator_id + topic ids + work_count

```sql
COPY (

SELECT p.* REPLACE (p.id::VARCHAR AS id),
       coalesce(o.org_ids, []) AS org_ids, len(coalesce(o.org_ids, [])) AS org_count, o.coordinator_id,
       t.topic_id, tp.subfield_id, tp.field_id, tp.domain_id,
       coalesce(w.work_count, 0) AS work_count
FROM project p
LEFT JOIN (SELECT source AS project_id,
                  list(target::VARCHAR ORDER BY (lower(cordis_type) = 'coordinator') DESC, target) AS org_ids,
                  min(target::VARCHAR) FILTER (WHERE lower(cordis_type) = 'coordinator') AS coordinator_id
           FROM relation WHERE sourceType = 'project' AND targetType = 'organization' GROUP BY source) o ON o.project_id = p.id
LEFT JOIN (SELECT source_id, min(topic_id) AS topic_id FROM relation_topic WHERE type = 'project' GROUP BY source_id) t ON t.source_id = p.id
LEFT JOIN topic tp ON tp.id = t.topic_id
LEFT JOIN (SELECT source AS project_id, count(DISTINCT target) AS work_count FROM relation WHERE sourceType = 'project' AND targetType = 'product' GROUP BY source) w ON w.project_id = p.id
WHERE hash(p.id) % 100 < 3
) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/projects_sample3pct.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 116,562 | 3 | 20.0 | 171.8 |



Extrapolated to 3,893,065 projects: **669 MB** (172 bytes/row).

### M2b projects FULL (bonus, exact size)

```sql
COPY (

SELECT p.* REPLACE (p.id::VARCHAR AS id),
       coalesce(o.org_ids, []) AS org_ids, len(coalesce(o.org_ids, [])) AS org_count, o.coordinator_id,
       t.topic_id, tp.subfield_id, tp.field_id, tp.domain_id,
       coalesce(w.work_count, 0) AS work_count
FROM project p
LEFT JOIN (SELECT source AS project_id,
                  list(target::VARCHAR ORDER BY (lower(cordis_type) = 'coordinator') DESC, target) AS org_ids,
                  min(target::VARCHAR) FILTER (WHERE lower(cordis_type) = 'coordinator') AS coordinator_id
           FROM relation WHERE sourceType = 'project' AND targetType = 'organization' GROUP BY source) o ON o.project_id = p.id
LEFT JOIN (SELECT source_id, min(topic_id) AS topic_id FROM relation_topic WHERE type = 'project' GROUP BY source_id) t ON t.source_id = p.id
LEFT JOIN topic tp ON tp.id = t.topic_id
LEFT JOIN (SELECT source AS project_id, count(DISTINCT target) AS work_count FROM relation WHERE sourceType = 'project' AND targetType = 'product' GROUP BY source) w ON w.project_id = p.id

) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/projects_full.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 3,893,065 | 8 | 655.7 | 168.4 |



### M3 organizations, all columns + project_count, work_count

```sql
COPY (

SELECT o.* REPLACE (o.id::VARCHAR AS id),
       coalesce(pc.project_count, 0) AS project_count, coalesce(wc.work_count, 0) AS work_count
FROM organization o
LEFT JOIN (SELECT target AS org_id, count(DISTINCT source) AS project_count FROM relation WHERE sourceType = 'project' AND targetType = 'organization' GROUP BY target) pc ON pc.org_id = o.id
LEFT JOIN (SELECT target AS org_id, count(DISTINCT source) AS work_count FROM relation WHERE sourceType = 'product' AND targetType = 'organization' GROUP BY target) wc ON wc.org_id = o.id
) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/organizations.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 494,099 | 2 | 44.8 | 90.8 |



### M4 (bonus) grants: derived funding streams

```sql
COPY (

SELECT sid AS id, any_value(descr) AS description, any_value(funder) AS funder, any_value(jur) AS jurisdiction, count(DISTINCT project_id) AS project_count
FROM (SELECT id AS project_id, f.fundingStream.id AS sid, f.fundingStream.description AS descr, f.jurisdiction AS jur,
             coalesce(nullif(split_part(f.fundingStream.id,'::',1),''), f.shortName) AS funder FROM (SELECT id, unnest(fundings) f FROM project)) GROUP BY sid
) TO '/vast/lu72hip/hm_pipeline/serving/agent_job/out/grants.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| rows | runtime s | file size MB | bytes/row |
|---|---|---|---|
| 6,075 | 0 | 0.2 | 31.7 |




_section runtime total: 43 s, job 8981163_


files:
- `grants.parquet` 0.2 MB
- `organizations.parquet` 44.8 MB
- `projects_full.parquet` 655.7 MB
- `projects_sample3pct.parquet` 20.0 MB
- `works_tier0.parquet` 910.5 MB
- `works_tier1_sample.parquet` 148.8 MB

slurm job `8981167`

### schema of works_tier0.parquet

```sql
DESCRIBE SELECT * FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier0.parquet'
```

| column_name | column_type | null | key | default | extra |
|---|---|---|---|---|---|
| id | VARCHAR | YES | None | None | None |
| title | VARCHAR | YES | None | None | None |
| authors | VARCHAR[] | YES | None | None | None |
| author_count | BIGINT | YES | None | None | None |
| publication_date | DATE | YES | None | None | None |
| year | BIGINT | YES | None | None | None |
| publisher | VARCHAR | YES | None | None | None |
| container_name | VARCHAR | YES | None | None | None |
| open_access_color | VARCHAR | YES | None | None | None |
| best_access_right | VARCHAR | YES | None | None | None |
| language | VARCHAR | YES | None | None | None |
| citation_count | DOUBLE | YES | None | None | None |
| doi | VARCHAR | YES | None | None | None |
| pdf_url | VARCHAR | YES | None | None | None |
| landing_url | VARCHAR | YES | None | None | None |
| project_ids | VARCHAR[] | YES | None | None | None |
| organisation_ids | VARCHAR[] | YES | None | None | None |
| link_tier | SMALLINT | YES | None | None | None |
| is_ch_via_project | BOOLEAN | YES | None | None | None |

_0 s_

### works_tier0 content check

```sql
SELECT count(*) n, round(avg(len(project_ids)),3) avg_project_ids, count(*) FILTER (WHERE len(project_ids)=0) no_project, round(avg(len(organisation_ids)),3) avg_org_ids, count(*) FILTER (WHERE is_ch_via_project) is_ch_via_project,
 count(pdf_url) pdf, count(landing_url) landing, count(doi) doi, count(*) FILTER (WHERE pdf_url LIKE '%&amp;%' OR landing_url LIKE '%&amp;%') amp_left, max(len(authors)) max_authors FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier0.parquet'
```

| n | avg_project_ids | no_project | avg_org_ids | is_ch_via_project | pdf | landing | doi | amp_left | max_authors |
|---|---|---|---|---|---|---|---|---|---|
| 5,001,873 | 1.455 | 922,801 | 5.808 | 35,975 | 1,839,608 | 4,979,459 | 4,453,128 | 0 | 20 |

_1 s_

### works_tier1_sample content check

```sql
SELECT count(*) n, round(avg(len(project_ids)),3) avg_project_ids, round(avg(len(organisation_ids)),3) avg_org_ids, count(pdf_url) pdf, count(landing_url) landing, count(doi) doi FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier1_sample.parquet'
```

| n | avg_project_ids | avg_org_ids | pdf | landing | doi |
|---|---|---|---|---|---|
| 999,844 | 0 | 2.485 | 107,825 | 996,358 | 861,929 |

_0 s_

### works sample rows

```sql
SELECT id, left(title,50) title, authors[1:2] authors, author_count, publication_date, publisher, language, citation_count, len(project_ids) AS n_proj, len(organisation_ids) AS n_orgs, is_ch_via_project FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier0.parquet' USING SAMPLE 4
```

| id | title | authors | author_count | publication_date | publisher | language | citation_count | n_proj | n_orgs | is_ch_via_project |
|---|---|---|---|---|---|---|---|---|---|---|
| 12031236489487822624 | Narrative analysis in individuals with Parkinson’s | ['Amy E. Ramage', 'Amy E. Ramage'] | 9 | 2024-05-22 | Frontiers Media SA | und | 3 | 1 | 15 | False |
| 6100472801588924160 | ACTRIS dissemination and outreach strategy | None | None | None | None | und | 0 | 1 | 0 | False |
| 17704656744812973152 | Validating scores from the short form of the Music | ['Daniel Fiedler', 'Johannes Hasselhorn'] | 5 | 2024-10-18 | SAGE Publications | eng | 3 | 0 | 5 | False |
| 895300940795053504 | PML-Nuclear Bodies Regulate the Stability of the F | ['Andrea\xa0Flores Burroughs', 'Sylvia Eluhu'] | 6 | 2018-01-01 | S. Karger AG | eng | 8 | 8 | 2 | False |

_0 s_

### column size share in works_tier0 (uncompressed vs compressed bytes per column)

```sql
SELECT path_in_schema col, round(sum(total_compressed_size)/1e6,1) compressed_MB, round(sum(total_uncompressed_size)/1e6,1) uncompressed_MB FROM parquet_metadata('/vast/lu72hip/hm_pipeline/serving/agent_job/out/works_tier0.parquet') GROUP BY 1 ORDER BY 2 DESC LIMIT 25
```

| col | compressed_MB | uncompressed_MB |
|---|---|---|
| authors, list, element | 263.1 | 543.8 |
| title | 198.3 | 501.5 |
| organisation_ids, list, element | 137.8 | 684.8 |
| project_ids, list, element | 71.7 | 171.5 |
| landing_url | 54.5 | 239 |
| id | 52.5 | 117 |
| doi | 44.3 | 126.8 |
| pdf_url | 36.9 | 128.4 |
| container_name | 17.2 | 38.8 |
| publication_date | 8.6 | 9.7 |
| publisher | 7.3 | 14.2 |
| citation_count | 4.8 | 6.8 |
| author_count | 3.8 | 6.3 |
| year | 3.6 | 4.7 |
| open_access_color | 1.3 | 1.4 |
| language | 1 | 3.8 |
| best_access_right | 0.9 | 2.4 |
| is_ch_via_project | 0.1 | 0.6 |
| link_tier | 0 | 0 |

_0 s_

### column size share in projects_full

```sql
SELECT path_in_schema col, round(sum(total_compressed_size)/1e6,1) compressed_MB FROM parquet_metadata('/vast/lu72hip/hm_pipeline/serving/agent_job/out/projects_full.parquet') GROUP BY 1 ORDER BY 2 DESC LIMIT 12
```

| col | compressed_MB |
|---|---|
| summary | 304 |
| title | 101.7 |
| openaireId | 76.3 |
| id | 42 |
| grantId | 25.6 |
| org_ids, list, element | 25.3 |
| granted, fundedAmount | 9.3 |
| keywords | 7.9 |
| startDate | 6.6 |
| topic_id | 6.5 |
| endDate | 6 |
| fundings, list, element, fundingStream, id | 5.6 |

_0 s_

### projects_full content check

```sql
SELECT count(*) n, count(*) FILTER (WHERE org_count=0) no_org, count(coordinator_id) coord, count(topic_id) topic, count(*) FILTER (WHERE work_count>0) with_work, max(work_count) max_work_count, count(DISTINCT id) distinct_id FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/projects_full.parquet'
```

| n | no_org | coord | topic | with_work | max_work_count | distinct_id |
|---|---|---|---|---|---|---|
| 3,893,065 | 316,230 | 81,043 | 3,884,270 | 738,722 | 72,132 | 3,893,065 |

_0 s_

### organizations schema

```sql
DESCRIBE SELECT * FROM '/vast/lu72hip/hm_pipeline/serving/agent_job/out/organizations.parquet'
```

| column_name | column_type | null | key | default | extra |
|---|---|---|---|---|---|
| id | VARCHAR | YES | None | None | None |
| openaireId | VARCHAR | YES | None | None | None |
| legalName | VARCHAR | YES | None | None | None |
| legalShortName | VARCHAR | YES | None | None | None |
| websiteUrl | VARCHAR | YES | None | None | None |
| alternativeNames | VARCHAR[] | YES | None | None | None |
| countryCode | VARCHAR | YES | None | None | None |
| rorId | VARCHAR | YES | None | None | None |
| wikiId | VARCHAR | YES | None | None | None |
| pids | STRUCT(scheme VARCHAR, "value" VARCHAR)[] | YES | None | None | None |
| rorStatus | VARCHAR | YES | None | None | None |
| rorEstablished | INTEGER | YES | None | None | None |
| rorTypes | VARCHAR[] | YES | None | None | None |
| rorLocations | JSON | YES | None | None | None |
| geolocation | DOUBLE[] | YES | None | None | None |
| geolocation_source | VARCHAR | YES | None | None | None |
| rorRelationships | JSON | YES | None | None | None |
| address_street | VARCHAR | YES | None | None | None |
| address_postalcode | VARCHAR | YES | None | None | None |
| address_city | VARCHAR | YES | None | None | None |
| address_country | VARCHAR | YES | None | None | None |
| nuts3 | VARCHAR | YES | None | None | None |
| region | VARCHAR | YES | None | None | None |
| project_count | BIGINT | YES | None | None | None |
| work_count | BIGINT | YES | None | None | None |

_0 s_

### organizations column sizes

```sql
SELECT path_in_schema col, round(sum(total_compressed_size)/1e6,2) compressed_MB FROM parquet_metadata('/vast/lu72hip/hm_pipeline/serving/agent_job/out/organizations.parquet') GROUP BY 1 ORDER BY 2 DESC LIMIT 8
```

| col | compressed_MB |
|---|---|
| openaireId | 9.32 |
| legalName | 7.41 |
| legalShortName | 6.39 |
| id | 5.34 |
| pids, list, element, value | 3.46 |
| alternativeNames, list, element | 3.31 |
| websiteUrl | 2.07 |
| rorRelationships | 1.09 |

_0 s_


_section runtime total: 1 s, job 8981167_



---

## Surprises (things that contradict READ_CORE_V4_FINISHED.md / SERVING_DESIGN.md or break a design assumption)

1. **`link_tier = 0` does not mean "has a project link".** Only 4,079,072 distinct works appear in the 7,278,890 project->work relations; **922,801 of the 5,001,873 tier-0 works (18.4%) have no project relation** (883,920 of them have orgs, 38,881 have neither). READ_CORE says all 5.0M are project-linked. Consequences: `project_ids[]` is empty for 18.4% of tier-0 works, `is_ch_via_project` (D4) is false for them, and "DCH corpus on works = tier-0 works of DCH projects" contains only 35,975 works. Tier 1 is clean (0 project relations). See X1-X3.
2. **316,230 projects (8.1%) have no organisation** (229,789 have neither an org nor a work): `org_ids` empty, `org_count` 0, so **D13 `funded_amount_eur / org_count` divides by zero** for them; also 443,648 tier-0 works have 0 orgs.
3. **Collaboration (D11)**: assumption "tens of millions of pair docs" is wrong, it is 4.4M distinct pairs / 7.35M project-pair edges (153,616 pairs for DCH). But the top pairs are one institution under two org ids (JOHNS HOPKINS UNIVERSITY x2 with 23,469 shared projects, UCSF 20,391, UPenn 20,122, Pittsburgh 18,947, Washington 17,002): **duplicate organisation entities** (46,065 duplicated names, 54,260 surplus rows) create fake self-collaborations and split expert/org rollups. Worth a dedupe key (normalised name + country) before ranking experts.
4. **Funder derivation (D17)**: level 1 of `fundingStream.id` is NULL for 293k projects (NSF/FCT/NWO/AKA/ANR/NIH entries with no stream) and has variants (`tubitak`, `INCA`, `100010414`). Use `fundings[].shortName` (103 values, never NULL). Programme (level 2) is NULL for those 293k projects. 156 stream ids contain `&amp;`.
5. **Money (D14)**: 42.7% of projects have NULL currency; 92,424 SNSF projects have an amount but NO currency; currency `$` (6,367) is ambiguous (USD vs AUD for NHMRC); `totalCost` is 0 for 98.8%; `fundedAmount` is 0 for 41.7% of projects (NWO/DFG/RCN/GA0/TUBITAK: 100%); four EC projects have 2.5 billion EUR and one Wellcome project 3.64 bn INR (outliers). An org "total funding" rollup and the funding map are only meaningful for EUR/USD/GBP/SEK/AUD funders.
6. **Coordinators (D15)**: only EC projects have any (63% of EC), other top funders 0%. 57 projects have two coordinators (`min` picks one deterministically in the export).
7. **Geo**: only 26% of projects have a geolocated org, and countryCode/region are NULL for 43% of project-connected orgs (NIH-style US institutions); the `region` facet will be empty for them.
8. **Topics (D3)**: one topic per project holds, but 8,795 projects have none (and only 1,715 of the 4,516 topics have a DCH project).
9. **Works language**: 3-letter codes, 30 "pairs" like `fra/fre`, `esl/spa`, 36% `und`; not usable as a filter without normalisation. Title language is unknown for 36% and non-English for ~10% of works (tier 1: 10.9%).
10. **Works pdf coverage (D5)**: only 13.4% of all works get a `pdf_url` (36.8% tier 0); 77% of OPEN works have none. UI must fall back to `landing_url` (99.65%) and label it "DOI/landing page".
11. **Projects text**: `summary` NULL for 86.3%, `keywords` 87.0%, `acronym` 96.9%; 17,622 summaries and 6,437 titles contain `&amp;`, 2,026 summaries contain HTML tags; 2,161 org names contain `&amp;`. `startDate` has 1900..2125 junk.
12. **Minority precision**: Manx 1,787 and Russians 2,242 projects look like keyword false positives (unreviewed, per READ_CORE); the title blob is only 0.75M chars in total, so no cap problem.
13. **Ids**: 20-digit, half of all works exceed signed long and 99.95% exceed 2^53; string ids are mandatory, also for the api/JS layer (already planned).
14. `rorId` has 126,401 non-NULL values but only 121,300 distinct: several org ids share a ROR id (another face of point 3).
15. Data check that held: work/project/org ids unique, no dangling relations (1% sample), `geolocation` always `[lat,lng]` length 2, 63,885 geolocated project-connected orgs, 5,500,329 project->org relations without duplicates, 392,682 relations with `cordis_type`, 3,884,270 topic links, 96 minority groups used, mini-DB densities (12 orgs/project) are indeed much higher than prod (1.41 orgs/project, 2.82 orgs/work).


### Evidence for the surprises
slurm job `8981161`

### X1 tier 0 works: with / without a project relation, with / without orgs

```sql
SELECT link_tier, n_projects>0 has_project, n_orgs>0 has_orgs, count(*) works FROM wf GROUP BY ALL ORDER BY 1,2,3
```

| link_tier | has_project | has_orgs | works |
|---|---|---|---|
| 0 | False | False | 38,881 |
| 0 | False | True | 883,920 |
| 0 | True | False | 404,767 |
| 0 | True | True | 3,674,305 |
| 1 | False | True | 44,998,127 |

_0 s_

### X2 tier-0 works without project link: year histogram bucket and provenance of their org relations

```sql
SELECT CASE WHEN year IS NULL THEN 'NULL' WHEN year<2018 THEN '<2018' ELSE '>=2018' END y, count(*) works FROM wf WHERE link_tier=0 AND n_projects=0 GROUP BY 1
```

| y | works |
|---|---|
| NULL | 1978 |
| >=2018 | 596,135 |
| <2018 | 324,688 |

_0 s_

### X3 project->work relations whose target work is tier 1 (should be 0)

```sql
SELECT count(*) FROM proj_work pw JOIN wf ON wf.id=pw.work_id WHERE wf.link_tier=1
```

| count_star() |
|---|
| 0 |

_0 s_

### X4 projects->work relations by provenance/validated for distinct works

```sql
SELECT prov, count(*) rels, count(DISTINCT work_id) works FROM proj_work GROUP BY 1
```

| prov | rels | works |
|---|---|---|
| Harvested | 2,105,410 | 1,678,410 |
| Inferred by OpenAIRE | 5,166,980 | 2,601,526 |
| Linked by user | 6500 | 5897 |

_0 s_

### X5 zero-org tier-0 works: have a project?

```sql
SELECT n_projects>0 has_project, count(*) FROM wf WHERE link_tier=0 AND n_orgs=0 GROUP BY 1
```

| has_project | count_star() |
|---|---|
| True | 404,767 |
| False | 38,881 |

_0 s_

### X6 organizations connected to nothing (neither project nor work)

```sql
SELECT count(*) FROM organization WHERE id NOT IN (SELECT org_id FROM proj_org) AND id NOT IN (SELECT org_id FROM work_org)
```

| count_star() |
|---|
| 55,488 |

_1 s_

### X7 projects with no org and no work

```sql
SELECT count(*) FROM project WHERE id NOT IN (SELECT project_id FROM proj_org) AND id NOT IN (SELECT project_id FROM proj_work)
```

| count_star() |
|---|
| 229,789 |

_0 s_

### X8 project text: translated flags and HTML entities in title/summary/orgs (&amp;)

```sql
SELECT count(*) FILTER (WHERE title LIKE '%&amp;%') title_amp, count(*) FILTER (WHERE summary LIKE '%&amp;%') summary_amp, count(*) FILTER (WHERE summary LIKE '%<%>%') summary_has_tags FROM project
```

| title_amp | summary_amp | summary_has_tags |
|---|---|---|
| 6437 | 17,622 | 2026 |

_0 s_

### X9 org legalName with &amp; and uppercase-only names

```sql
SELECT count(*) FILTER (WHERE legalName LIKE '%&amp;%') name_amp, count(*) FILTER (WHERE legalName = upper(legalName) AND legalName ~ '[A-Z]') all_caps FROM organization
```

| name_amp | all_caps |
|---|---|
| 2161 | 1 |

_0 s_


_section runtime total: 2 s, job 8981161_



---

# Appendix

## Job list
| job | what | slurm id (final run) |
|---|---|---|
| prep | relation slices to Parquet | 8981137 |
| wfeat | per-work feature table (50M rows, 46 s) | 8981148 |
| AB | sections A, B | 8981166 |
| CE | sections C, E | 8981152 |
| D / Dpairs | section D / D8 (exact distinct org pairs) | 8981155 / 8981157 |
| HL | sections H-L | 8981156 |
| FG | sections F, G | 8981162 |
| M / mv | section M export / verification | 8981163 / 8981167 |
| X | surprise checks | 8981161 |
Partition `fat`, 16 cpus, 120-180 GB, like the merge job 8981036 (200 GB). Earlier submissions failed on trivial SQL alias errors (`scope`, `year`, `nulls`, ...) and were rerun; only the final runs are in this file.


## Prep SQL
slurm job `8981137`

### materialize proj_org

```sql
COPY (SELECT source AS project_id, target AS org_id, relType.name AS rel_name, provenance.provenance AS prov, cordis_type, cordis_ec_contribution FROM relation WHERE sourceType='project' AND targetType='organization') TO '/vast/lu72hip/hm_pipeline/tmp_session/prep/proj_org.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| Count |
|---|
| 5,500,329 |

_0 s_

### materialize proj_work

```sql
COPY (SELECT source AS project_id, target AS work_id, provenance.provenance AS prov FROM relation WHERE sourceType='project' AND targetType='product') TO '/vast/lu72hip/hm_pipeline/tmp_session/prep/proj_work.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| Count |
|---|
| 7,278,890 |

_0 s_

### materialize work_org

```sql
COPY (SELECT source AS work_id, target AS org_id, provenance.provenance AS prov FROM relation WHERE sourceType='product' AND targetType='organization') TO '/vast/lu72hip/hm_pipeline/tmp_session/prep/work_org.parquet' (FORMAT parquet, COMPRESSION zstd)
```

| Count |
|---|
| 140,970,846 |

_3 s_

### prep rows

```sql
SELECT 'proj_org' t,count(*) n,count(DISTINCT (project_id,org_id)) nd FROM '/vast/lu72hip/hm_pipeline/tmp_session/prep/proj_org.parquet'
 UNION ALL SELECT 'proj_work',count(*),count(DISTINCT (project_id,work_id)) FROM '/vast/lu72hip/hm_pipeline/tmp_session/prep/proj_work.parquet'
 UNION ALL SELECT 'work_org',count(*),count(DISTINCT (work_id,org_id)) FROM '/vast/lu72hip/hm_pipeline/tmp_session/prep/work_org.parquet'
```

| t | n | nd |
|---|---|---|
| proj_org | 5,500,329 | 5,500,329 |
| proj_work | 7,278,890 | 7,278,890 |
| work_org | 140,970,846 | 140,970,846 |

_2 s_


_section runtime total: 5 s, job 8981137_


slurm job `8981148`

### quick macro test on 1000 works

```sql
SELECT count(*) n, count(wk_doi_work(pids)) doi_w, count(wk_doi_any(pids,instances)) doi_a, count(list_filter(wk_all_urls(instances), u->wk_is_pdf_loose(u))[1]) pdf FROM (SELECT * FROM work LIMIT 1000)
```

| n | doi_w | doi_a | pdf |
|---|---|---|---|
| 1000 | 878 | 878 | 136 |

_0 s_

### materialize per-work features (all 50M)

```sql
COPY (
 SELECT w.id, w.link_tier,
  length(w.title) AS title_len, w.title IS NULL AS title_null,
  len(w.authors) AS n_authors,
  coalesce(list_sum(list_transform(w.authors[1:20], a -> coalesce(length(a.fullName), 0))), 0) AS a20_len,
  length(w.publisher) AS publisher_len, w.publisher, length(w.container.name) AS container_len,
  len(w.instances) AS n_instances,
  w.publicationDate, year(w.publicationDate) AS year, w.openAccessColor AS oa_color, w.bestAccessRight.label AS best_access, w.language.code AS lang_code,
  w.citationCount,
  coalesce(oc.n, 0) AS n_orgs, coalesce(pc.n, 0) AS n_projects,
  wk_doi_work(w.pids) AS doi_work, wk_doi_any(w.pids, w.instances) AS doi_any,
  len(wk_all_urls(w.instances)) AS n_urls, len(wk_open_urls(w.instances)) AS n_open_urls,
  list_filter(wk_open_urls(w.instances), u -> wk_is_pdf_strict(u))[1] AS pdf_open_strict,
  list_filter(wk_open_urls(w.instances), u -> wk_is_pdf_loose(u))[1] AS pdf_open_loose,
  list_filter(wk_all_urls(w.instances), u -> wk_is_pdf_strict(u))[1] AS pdf_any_strict,
  list_filter(wk_all_urls(w.instances), u -> wk_is_pdf_loose(u))[1] AS pdf_any_loose,
  wk_open_urls(w.instances)[1] AS first_open_url, wk_all_urls(w.instances)[1] AS first_url
 FROM work w
 LEFT JOIN (SELECT work_id, count(*) n FROM work_org GROUP BY 1) oc ON oc.work_id = w.id
 LEFT JOIN (SELECT work_id, count(*) n FROM proj_work GROUP BY 1) pc ON pc.work_id = w.id
) TO '/vast/lu72hip/hm_pipeline/tmp_session/prep/work_feat.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)
```

| Count |
|---|
| 50,000,000 |

_46 s_

### rows

```sql
SELECT count(*), count(DISTINCT id) FROM '/vast/lu72hip/hm_pipeline/tmp_session/prep/work_feat.parquet'
```

| count_star() | count(DISTINCT id) |
|---|---|
| 50,000,000 | 50,000,000 |

_1 s_


_section runtime total: 47 s, job 8981148_

