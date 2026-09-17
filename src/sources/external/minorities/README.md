# Minorities (Wikidata minority-group discovery)

Harvests a candidate list of European minority groups from Wikidata via SPARQL,
then narrows it to a working list in DuckDB. Feeds the platform's planned
minority filter dimension (see [`6_1_minorities.md`](6_1_minorities.md) and
[`planning/Plan.md`](planning/Plan.md) for the full research rationale).

Structurally like `sources/dumps/`: a one-shot harvest against a live external
endpoint (not checkpointed like `sources/apis/`), staged into DuckDB in
several steps rather than loaded straight into Postgres.

## Pipeline

1. `extract.py` — Phase 1: queries the Wikidata Query Service for ethnic
   groups / tribes / indigenous peoples tied to an explicit European country
   allowlist, plus manual seeds for the three pilot groups (Ladin, Sámi,
   Jewish). Writes `data/pile/minorities/minorities.csv` (path from
   `config/dumps.yaml` -> `minorities.path_raw`).
2. `loader.py` — loads that CSV unmodified into `minorities_raw` in
   `minorities_raw.duckdb` (`path_duck`).
3. `staging.py` — Phase 1b: applies three documented, reproducible filters
   (unresolved labels, diaspora typing, non-European-only) and merges
   duplicate Wikidata entries in SQL, writing `minorities_staging` into
   `minorities_staging.duckdb` (`path_duck_staging`).
4. `staging_2.py` — Phase 1c: two more passes over the staged table —
   - drops groups that are the titular/majority population of one of their
     own listed countries (Austrians -> Austria, Poles -> Poland, ...) per
     `titular_majority_overrides.csv` (hand-classified against the actual
     data, keyed by qid; a few ambiguous cases like Bosniaks or Flemish
     people are recorded there but deliberately kept in, with a note on why);
   - rolls up any remaining row whose `part_of` matches another surviving
     row (e.g. the Sámi subgroups) into a `known_subgroups` column on the
     parent, rather than leaving them as separate top-level entries.

   Writes `minorities_staging_2` into `minorities_staging_2.duckdb`
   (`path_duck_staging_2`) — this is the table intended for actual use.

Run individually:

```bash
uv run python src/sources/external/minorities/extract.py
uv run python src/sources/external/minorities/loader.py
uv run python src/sources/external/minorities/staging.py
uv run python src/sources/external/minorities/staging_2.py
```

Or via Snakemake — see `orchestration/rules/external/minorities.smk`. Unlike
`meta_heritage`, this source is wired into `rule all` / `sources_local`, so
`./orchestration/run_all_sources.sh` covers it too, and `generate_reports.py`
picks up `minorities.md`, `minorities_staging.md`, and `minorities_staging_2.md`
automatically since it's a `dumps.yaml` entry.
