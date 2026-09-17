# Heritage Monitor Pipeline

A data pipeline, data model and warehouse for the [Heritage Monitor](https://heritagemonitor.org/).

## Orchestration

WIP Singularity/ Docker...

```bash
./orchestration/run_all_sources.sh extract       # extraction only
./orchestration/run_all_sources.sh load --report # load, then per-source reports
# Optional add -p/--parallel <N>

# TO just get reports for all duckdb files
uv run python -m common.report.generate_reports
```

Go to [Orchestration Documentation](orchestration/README.md) for more snakemake commands.

## Installation

Default Python version is 3.14 (pinned in `.python-version`; uv downloads it automatically if
it's not already on your machine).

```bash
# install uv, if you don't already have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# creates .venv/ and installs the project, including src/common, src/sources, src/enrichment, src/pipelines
uv sync

# fill in API keys, keep ENV=dev for local work
cp .env.example .env
```

**Installing new packages**

```bash
uv add <package>
```

## The Pipeline Way

**ELT**: Load before you transform. Each stage below reads the previous stage's duckdb file
and writes its own; nothing mutates in place.

**Idempotence**: Every step of the pipeline has to ensure **idempotence** as to allow failures without cost,
and free backfills. Enrichment steps may violate that.

We follow the Medallion & CQRS Architectures:

**Medallion**, [learn More](https://www.databricks.com/blog/what-is-medallion-architecture):

1. Bronze: raw data loading/ingestion
2. Silver: Clean/ Merge/ Analyse/ Dedup
3. Gold: Aggregate & Enrich

**CQRS**, [learn More](https://martinfowler.com/bliki/CQRS.html):

1. Meaning core is in duckdb as a source of truth.
2. And we create versioned Meilisearch indices as task-based UIs, that are ready to be deployed

### High Level Overview

1. **Extract**: 
   * Download raw files (incremental sources) or bulk dumps (`sources/`).
2. **Load**:
   * Into duckdb, one file per source, no cross-source logic at all yet.
   * → **bronze**: clean, documented, per-source duckdb tables.
3. **Merge**: 
   * Source duckdbs → `core_vN_raw.duckdb`. 
   * Structural union only: every source's fields mapped into the unified core schema.
   * Duplicate entities from different sources still exist as separate rows at this point.
4. **Analysis**
   * Dedup/entity-resolution, applied on top of merge's output.
   * → `core_vN_processed.duckdb`. See below for how this actually works.
   * → **silver**: one row per real-world entity, structurally unified.
5. **Enrichment** (the ML steps) + **Model** (derived entities, e.g. `collaboration`:
   * what used to be hand-written postgres mat views) → `core_vN_gold.duckdb`.
   * → **gold**: this is canon core_v4.
6. **Serve**: query gold core_v4 to build denormalized tables in Meilisearch,
   shipped to the webapp.

### Analysis stage, in detail

The hard part. General process:

1. Preprocess: Normalize strings (org names, titles) so comparisons aren't tripped up by casing/punctuation.
2. Block: Cheap grouping so you're not comparing every org against every org (e.g. same country + first 3 chars of
   normalized name). This is what makes it tractable at your scale.
3. Compare: Fuzzy similarity within each block (Jaro-Winkler/Levenshtein on names, geo distance for orgs, date overlap
   for projects).
4. Match: Threshold or classifier decides which pairs are "the same real-world thing."
5. Cluster: Transitively group matched pairs into one canonical entity.

The analysis stage's output is not deduplicated data itself — it's a crosswalk table. Something like:

```
analysis.org_entity_map(source, source_id, canonical_org_id)
analysis.project_entity_map(source, source_id, canonical_project_id)
```

Applying that crosswalk — joining `core_vN_raw`'s rows against these maps to fold them onto the canonical id — is what
actually produces `core_vN_processed.duckdb`. The matching logic itself never lives in the merge step.

### Limit Runs

For faster testing the sources and core's are planned with **Limit Runs** (also known as Sampled Runs).
* Meaning only n (eg 500) rows per entity are loaded and processed in each step
* Issue
  * There probably wont be relations between entities (because there are too few rows)
  * Time-window sampling that respects foreign keys fixes this, but thats complicated to implement

→ But wee can still use **Limit Runs** to test the pipeline, because all tables will still be filled and processed. 
Even relations is an entity that has samples.
→ But specific small sources should always be loaded in full: Topic, minorities, ROR ...

@Question: Pillars and ... are just keyword lists right? 

## Project Structure

```
infra/                              # standalone services the pipeline talks to (not Snakemake jobs)
└── meilisearch/                    # docker-compose (dev) / Singularity (HPC) — see infra/meilisearch/README.md

orchestration/                     # DAG only, no pipeline logic lives here
├── Snakefile
├── rules/
│   ├── extract.smk / load.smk     # sources/ → duckdb bronze tables
│   ├── merge.smk / analysis.smk   # → silver
│   ├── enrichment.smk / model.smk # → gold = canon core_v4
│   └── serve.smk                  # → meilisearch
├── envs/                          # per-rule container/conda refs (docker:// images)
└── profiles/slurm/                # --sdm apptainer, resources, monthly-poll trigger

config/
└── config.yaml                    # Snakemake-native config; parsed into typed pydantic-settings models in common/

src/
├── sources/          # bronze: extract+load, per source, canon-independent, split by access pattern
│   ├── apis/         # incremental, checkpointed, query-driven (arxiv, cordis, coreac)
│   ├── dumps/        # periodic bulk snapshots, no checkpointing (openaire, ror, openalex)
│   └── external/     # not core_v4 scope, kept for something else (meta_heritage)
├── pipelines/
│   ├── core_v4/
│   │   ├── merge/       # silver
│   │   ├── analysis/    # silver — dedup projects/orgs, feeds merge iteratively
│   │   ├── enrichment/  # gold — orchestrates ordered calls into enrichment/
│   │   ├── model/       # gold — derived entities (collaboration, etc.) → canon core_v4
│   │   └── serve/       # → denormalized Meilisearch indices
│   └── core_v3/         # frozen reference docs only, not executed
├── common/           # pydantic-settings config, db clients, file handling, requests, sanitizers
│   ├── api_runner/   # run_extractor.py/run_loader.py (IExtractor/ILoader defined in-file) + checkpoint_manager — only for sources/apis/
│   └── report/       # per-duckdb markdown data-profile reports, mirrors sources/pipelines under reports/
└── enrichment/       # reusable ML capabilities (llm, ocr, geolocation, science_classification, topic_modelling, crossref)
data/                 # pile/, checkpoints/ (cordis only), logs/, models/
reports/              # generated markdown data-profile reports (gitignored) — see src/common/report/
tests/
```

## Sources

All sources can be used independently, meaning if you just want to get data from one of the available sources, it is as
simple as (optionally) configuring the queries, entering the api keys as needed and running the extractor.
All sources have ORM models in SQL-Alchemy, meaning they can be loaded into any database.

Find more information about the specific sources in the [Source Documentation](sources/README.md).

## Database

@Walter rework this section

* Duck DB (version): Source of truth DB for the core models (since core_v3)
* Meilisearch: For the webapp, takes denormalized tables from core data model — see [infra/meilisearch/README.md](infra/meilisearch/README.md). Talk to Meilisearch with  http://localhost:7700
* Postgres: Not really used anymore
## Serve

