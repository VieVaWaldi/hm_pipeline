# Heritage Monitor Pipeline

A data pipeline, data model and warehouse for the digital humanities.

The pipeline follows  
Medallion Architecture ...
& CQRS Architecture ... for facing the webapp

## Installation

...

### Installing new packages

...

## Orchestration

The [Orchestration Documentation](orchestration/README.md) explains how orchestration with ...

## Project Structure

```
orchestration/                     # DAG only, no pipeline logic lives here
├── Snakefile
├── rules/
│   ├── extract.smk / load.smk     # sources/ → duckdb bronze tables
│   ├── merge.smk / analysis.smk   # → silver
│   ├── enrichment.smk / model.smk # → gold = canon core_v4
│   └── serve.smk                  # → opensearch
├── envs/                          # per-rule container/conda refs (docker:// images)
└── profiles/slurm/                # --sdm apptainer, resources, monthly-poll trigger

config/
└── config.yaml                    # Snakemake-native config; parsed into typed pydantic-settings models in common/

sources/            # bronze: extract+load, per source, canon-independent (openaire, cordis, ror, minorities, ...)
pipelines/
├── core_v4/
│   ├── merge/       # silver
│   ├── analysis/    # silver — dedup projects/orgs, feeds merge iteratively
│   ├── enrichment/  # gold — orchestrates ordered calls into enrichment_lib/
│   ├── model/       # gold — derived entities (collaboration, etc.) → canon core_v4
│   └── serve/       # → denormalized OpenSearch indices
└── core_v3/         # frozen reference docs only, not executed

common/             # pydantic-settings config, db clients, file handling, requests, sanitizers, IExtractor/ILoader + generic runner
enrichment_lib/     # reusable ML capabilities (llm, ocr, geolocation, science_classification, topic_modelling, crossref)
analysis_lib/       # foci.sql and other reusable dedup/profiling SQL
data/               # pile/, checkpoints/ (cordis only), logs/, models/
tests/
```

## Sources

All sources can be used independently, meaning if you just want to get data from one of the available sources, it is as
simple as (optionally) configuring the queries, entering the api keys as needed and running the extractor.
All sources have ORM models in SQL-Alchemy, meaning they can be loaded into any database.

Find more information about the specific sources in the [Query Documentation](src/sources/README.md).

## Database

The [Database Documentation](old_README_DB.md) explains how ...

### Provenance

...

## Development Guidelines

...