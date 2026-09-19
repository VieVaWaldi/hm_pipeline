# core_v4 enrichment

Enrichments do not copy or write the staging duckdb. Each one reads the trimmed
staging file **read-only** and writes its results to **side parquet files keyed
by id**. That makes them independent of each other, shardable across nodes
(`--shard I/N`) and resumable. Phase 3's assemble step builds the final tables
from the side outputs and applies the "overwrite the original text" rule.

## Dependency graph

```
staging ─▶ nllb ─▶ topics ─▶ theme
                └▶ minorities
                └▶ pillars
                └▶ dch
staging ─▶ regions
staging ─▶ geolocation   (runs last: it spends the Mapbox budget)
```

Text enrichments read `COALESCE(text_en, original)` through `text_sources.py`,
which refuses to run before NLLB is complete (`--allow-untranslated` overrides).

## Side outputs

`<enrichment_dir>/<name>/<entity>/part-<shard>-<n>.parquet`, plus `_SUCCESS`
when the output is complete. Paths come from `config/pipelines.yaml`
(`core_v4` / `core_v4_limit`). Column contract (`side_outputs.SCHEMAS`):

| name | entity | columns |
|---|---|---|
| nllb | project, work | id, field, text_en, src_lang (translated rows only) |
| nllb/seen | project, work | id, field, src_lang, translated |
| topics | project, work | id, topic_id, score |
| theme | project, work | id, theme (sparse) |
| dch | project, work | id, is_ch, pred |
| minorities | project, work | id, minority_qid VARCHAR[] (sparse) |
| pillars | project, work | id, pillars UTINYINT (sparse) |
| geolocation | organization | id, lat, lon, geolocation_source (`mapbox` \| `mapbox_temporary`), confidence (`exact` \| `high` \| `medium` \| `street`) |
| regions | organization | id, region |

`id` is the UBIGINT hash (values above int64 max; keep `uint64` in Arrow).
Pillars bitmask, lowest bit first: inclusive, sustainable, resilient, innovative, global.

### Writing

`side_outputs.SideOutput`: `begin()` (removes torn tmp files and a stale `_SUCCESS`), `write(table)`
(tmp file, then atomic rename; one part per call), `finish()` (this shard's marker; `_SUCCESS` once
all N shards have finished). Reading: `read_all(con)` / `read_all_sql()` (empty relation with the
schema when there are no parts), `done_ids_sql()` for resume anti-joins.

**Sparse outputs** (theme, minorities, pillars) only contain matched rows, so their own ids do not
say what was processed. Recompute them wholesale (pure SQL/CPU, cheap) or write a companion
`<name>/seen` output and resume from that.

## Reading rules

- One streaming Arrow cursor per run (`text_sources.text_batches`), no `OFFSET`, no `fetchall()`.
- Resume by anti-joining against the ids already in the side parquet (`exclude_ids_sql=out.done_ids_sql()`).
- There is no `ORDER BY` (too expensive on 50M rows): never rely on row order or offsets.
- Every CLI uses `cli.add_common_args` / `cli.resolve`: `--db`, `--variant`, `--enrichment-dir`,
  `--limit N`, `--test N` (dry run, no writes), `--shard I/N`, `--entity`, `--allow-untranslated`.

## Text fields

| entity | fields (the `field` value in `nllb`) |
|---|---|
| project | title, summary, acronym, keywords, subjects |
| work | title, description (`descriptions[1]`), subjects, container (`container.name`) |

## Country codes

Every step normalises country codes through `common.countries` (`normalize_country`,
SQL: `register_country_macro(con)` gives `norm_cc(c)`), built from Phase 1's `COUNTRY_MAPPING`
(EL→GR, UK→GB, YU/CS→RS, ..., EU/ZZ→NULL; XK kept).

## Modules

- `side_outputs.py`: `SideOutput`, `Shard`, `SCHEMAS`, `PILLARS`
- `text_sources.py`: `text_sql`, `text_batches`, `open_staging`, NLLB gate
- `cli.py`: shared flags and path resolution
- `fixtures.py`: tiny synthetic staging fixture for tests (no real staging sample exists locally)

Tests: `ENV=dev uv run python -m pytest src/pipelines/core_v4/enrichment tests/test_countries.py`
