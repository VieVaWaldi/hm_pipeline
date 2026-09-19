# Core_v4 Strategy

! Not a target yet! WIP !

---

This document specifies core_v4.
See the [ADR](...) for more.

Core_v4 builds on core_v3, because we probably made decisions there that were import for the project, but we didnt document them.
OpenAire is reinvestigated, @UPDATE and maybe we update the import to it?
Cordis is merged deeper and enrichment expanded. OpenSearch is served from now instead of Postgres.

## Sources:

* OpenAire: `reports/sources/dumps/openaire_dump_2026_06_05.md`
* ROR: `../../../reports/sources/dumps/ror_dump_2026_08_03.md`
* Cordis: `../../../reports/sources/apis/cordis/full_projects_no_pdfs.md`
* Minorities: `reports/sources/apis/cordis/minorities.md` 
* OATopics: `reports/sources/apis/cordis/oatopics.md`

## General

**Idempotence**:
* ... how?
* Just make a new duckdb for each big step, that u always tear down and rebuild from scratch? @Claude if you know better tell me lol

**Reports**:
* For each duckdb created make a report

**Limit runs**:
* parameter --limit -n default 1000 per entity (if thats not snakemake namespace) to make one real but fast test run

Open:
* Can we measure/ note (here in this readme or where? maybe we can add that to reports but reports is static rn/ depends on no state, which is great, not sure how we d add process run time to that)

## ... core_v4 process? ...

* we may just copy the high level base process from core_v3 verbatim
* target 1 single duckdb file for core_v4? how big can duckdb files get? 

@Claude argue with me here:
1. Add ROR to OpenAIRE -> core_v4_staging.duckdb
2. Merge Cordis
- Needs to be analysed in core_v3 first to see what we already have
- I feel like we only found matching project -> relation -> organization triplets and added 

* Merging Cordis 

    * From Cordis only institution level funding information for projects is needed from `j_project_institution`
    * From ROR organization information like geolocation and others are needed.
2. Enrichment
    * TFIdf Topic Classification using `../../enrichment/topic_modelling` -> I did this before step 5 so the intermediate core file is called path_duck_staging_2 in the config.
    * CH Classification
3. And finally a deployment from duckdb to postgresdb

--- This is new and not done ---

...

## Drop Works

-> Because our VM sucks and cant handle 200M

* Create a new core_v3 duckdb that removes works and respective relations
* Hard Limit by 50 Million works
* Keep all works connected to minorities
* Keep as many works connected to projects, works as possible
* Drop works with many nulls or that are older

## Enrichment - WIP

- Parallelize what can be safely and easily parallelized!
- Target projects for all enrichments before doing works, as works takes very long and i present this in 3 days

1. Geolocations 
-> for organisations

2. NLLB
-> for projects and works
-> Must happen before all text based enrichment, so we got english for all texts.

3. Topics TF-IDF
-> for projects and works

4. Keyword Matching
-> for projects and works
* Theme & Pillars (Needs Topics)
* Regions Mapping
* Minorities? (thats a merge later with projects right?)

5. DCH Classification (broke last time after 2 mil projects)
-> for projects and works

## Serving - WIP

* Each index served gets a version number prefix: /corev3/{version}/index