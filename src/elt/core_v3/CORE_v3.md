# Core_v3 Strategy

This document describes how to create datamodel core_v3 in which OpenAire shall be merged with Cordis and ROR.

Core_v4 will be the harmonization between OpenAlex, OpenAire and ROR. Later we will be able to reuse ELT procedures we wrote for OpenAire.
But for now we want the surgical minimal subset of data from OpenAire, which enables HeritageMonitor to give a real picture of CH research.

Core_v3 is saved as a duckdb file in `/work/lu72hip/data/duckdb/core/core_v3.duckdb`.

## High Level Plan

1. Understand source schemas, see more here:
    * OpenAire: `src/sources/openaire_dump/documentation/dump_v10.6/1_SCHEMA.md`
    * ROR: `src/sources/ror_dump/documentation/dump_v2.3/1_SCHEMA.md`
    * Cordis: `src/sources/cordis/documentation`
2. EDA on the sources to understand the data. 
3. Ingest raw sources into duckdb. Only load what ll be used downstream. Paths are here: `config/config_queries.json`
    * OpenAire: `src/sources/openaire_dump/documentation/dump_v10.6/2_SOURECE_MODEL.md` describes what to import and how, using `src/sources/openaire_dump/loader.py`
    * ROR: Is loaded fully, using: `src/sources/ror_dump/loader.py`
    * Cordis: Is loaded fully using the old `src/elt/loading/run_loader.py` pattern.
4. Prepare OpenAire for staging `src/sources/openaire_dump/documentation/dump_v10.6/3_STAGING.md`, running `src/sources/openaire_dump/staging.py`
5. Design and create core_v3 as one duckdb file using `src/elt/core_v3/TRANSFORMATION.md` and `src/elt/core_v3/transformation.py` by merging OpenAire staging with relevant columns from ROR and Cordis
    * From Cordis only institution level funding information for projects is needed from `j_project_institution`
    * From ROR organization information like geolocation and others are needed.
6. Enrichment
    * TFIdf Topic Classification using `src/enrichment/topic_modelling` -> I did this before step 5 so the intermediate core file is called path_topics_duck in the config.
    * CH Classification
7. And finally a deployment from duckdb to postgresdb