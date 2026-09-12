# Transformation with DBT

DBT is used for the entire Transformation Process.

Go [Here](../../sources/README.md) for Source Extraction & Loading.

Go [Here](../../../old_README_DB.md) for the database documentation.

Never worked with dbt? Start [here](https://www.blef.fr/get-started-dbt/), the docs are
[here](https://docs.getdbt.com/docs/introduction).

Connect to db_digicher_v2 database using: `ssh -N -L 5433:localhost:5433 draco`.

Install `dbt deps` !

ToDo:

* Do or remove this
    * Important: dbt-postgres installs psycopg2-binary by default, @Claude:"which is fine for development but not
      optimal for production PyPIdbt. For your 2-day sprint, stick with the default, but note for production you'd want
      to compile psycopg2 from source for better performance."

## Import

* Doc this or something:
* For openaire we ignore measure for project

## Process Overview

* `src/elt/transformation` is the dbt working directory
* [dbt_project.yml](dbt_project.yml) is dbt main config, defines main models and schemas
* {}/{source}/_sources.yml for source and core table and column definition
* Set db connection in `~/.dbt/profiles.yml` use target to chose PROD or DEV
* There are reusable dbr sql macros under `macros`, however its more copypaste code.

1. Loaders ingest raw data into sql DDL source tables in `sources/{source}/data_model.sql`, these have to be created manually
2. DBT **staging** layer creates a view on those tables as a dbt buffer
   * Do we actually need that?
3. **Intermediate** creates physical tables for the deduplication of the staging tables
   * `ref()` in e.g. junctions will make them run last due to DBT's dependency graph 
4. We transform the intermediate tables with **marts** to the unified core data model
   * DBT creates these tables (like for int) automatically based on the DBT SELECT statements
   * The core.* tables have their constraints at the top
5. Post: Create Constraints, Indexes, triggers etc -> in `macros/post_core_indexes.sql`
6. Which we then clean and deduplicate again

## Important commands

* dbt run -> run everything
* dbt debug -> test connection, version & configs
* dbt run --select "type.schema.table" # e.g. "staging.arxiv.*" -> to select/ run specific tables
* dbt ls --resource-type model --output name --output name -> Show all active models
* dbt clean -> removes targets and stuff

## Core Data Model

* All core tables except junctions have a source_id and source_system to indicate which record in the source they refer to.
* core models (ro, person, topic, link, publisher, journal) (later project, institution ...)
* core enrichment models (open alex) (more later) ???

## Deduplication

-> We do not do fuzzy matching yet, as that takes a shit ton of time. This probably has to be done in python as well
according to Claude
-> We do not need to manage junctions of duplicate records as we only use non-duplicate records for the intermediate tables. 
That means we currently ignore junctions from duplicate entries.
-> Always keep the latest record using 
   * **publiaction date DESC** or something similar
-> Junctions will only be filtered by the primary table they are pointing to
-> In the next unification step they will be recreated using the source_id

* We can use DISTINCT or ROW_NUMBER() for DOI
* We can use pg_trgm or levenshtein() or fuzzy similarity() for titles

## Deduplication with changing Junction Pointers

## Marts Unification
1. For normal tables: Select the deduplicated table and all relevant columns, include source_id!
2. For junctions: Join with both marts tables using source_id's
-> Each table in core must know its source_id for the junctions to be picked up correctly
3. Calculate the new id's for each record using `{{ dbt_utils.generate_surrogate_key(['source_system', 'source_id']) }} as id,`, to get deterministic same keys

## Post Core Indexes 
* Can be found under `macros/post_core_indexes.sql`
* Rerun with `dbt run-operation post_core_indexes`, BUT only when u actually changed something there.

## Enrichment

* Open Alex
    * Publication Information like refs etc
    * Adding missing geolocations to institutions
* Crossref
    * Information for each paper with a DOI
* Mapbox
    * Adding missing geolocations to institutions
* LLM Extraction - CURRENTLY ONLY CORDIS
    * get title, pub_date if not there
    * get language code
    * research output type given index for n characters
    * find funding number, maybe normal search better

## Notes

* We use custom schemas to clearly separate sources. This is defined in `macros/generate_schema_name.sql` and overrides
  DBT's default schema naming.

---

# Key dbt Components

* Models: SQL files that define a transformation or query that produces a table or view in your data warehouse
* Sources: References to raw data tables already present in your warehouse
* Snapshots: Tools for tracking historical changes to your data
* Tests: Assertions about your data to ensure quality
* Documentation: Automatic generation of docs for your dbt project
* Macros: Reusable SQL snippets (similar to functions)

## Materialization Types

Dbt supports several ways to persist your transformed data:

* Table: Creates a table for each model
* View: Creates a view for each model
* Incremental: Updates only new/changed records rather than rebuilding entire tables
* Ephemeral: Does not persist in the database but can be referenced by other models