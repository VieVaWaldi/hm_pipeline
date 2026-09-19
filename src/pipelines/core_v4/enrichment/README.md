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
when the output is complete (`_SUCCESS.tier0` / `.tier1` per tier for works, holding the staging fingerprint: see below). Paths come from `config/pipelines.yaml`
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

(The staging `organization.geolocation_source` column itself carries `ror`, `cordis` or `core_v2`, set by the
transformation; the geolocation side output only ever adds the two Mapbox values, and only where staging has none.)

`id` is the UBIGINT hash (values above int64 max; keep `uint64` in Arrow).
Pillars bitmask, lowest bit first: inclusive, sustainable, resilient, innovative, global.

### Writing

`side_outputs.SideOutput`: `begin()` (removes torn tmp files and a stale `_SUCCESS`), `write(table)`
(tmp file, then atomic rename; one part per call), `finish(stamp)` (this shard's marker; `_SUCCESS` once
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
  `--limit N`, `--test N` (dry run, no writes), `--shard I/N`, `--entity`, `--tier`, `--allow-untranslated`.

## Tiers (works)

Staging `work.link_tier` is 0 for project-linked works (about 5M of the 50M), 1 for org-only ones. Every entity CLI takes
`--tier 0|1|all` (default all; needs `--entity work`), applied where the rows are read (`text_sources.tier_sql`, so
`text_sql` / `text_batches` / nllb's `field_batches` take `tier=`; theme filters its topic rows through staging). This lets
tier 0 be enriched, assembled and served long before tier 1 has run.

Both tiers write into the same `<name>/work/` directory, so every reader (resume anti-joins, `text_sources`, assemble) is
unchanged (ids are disjoint between tiers). What is tier-aware are the file names and the markers:

| file | meaning |
|---|---|
| `part-<shard>-<n>.parquet` | untiered run (`--tier all`) |
| `part-t0-<shard>-<n>.parquet`, `part-t1-...` | tier runs. A reset (`begin(reset=True)`, pillars/theme recompute) or the part numbering only touches the run's own tier and shard, so tiers may use different shard counts |
| `_SUCCESS.tier0`, `_SUCCESS.tier1` | that tier is complete for this output |
| `_SUCCESS` | everything complete: written by an untiered run, or when both tier markers exist (then with the combined fingerprint) |
| `_SUCCESS.t0.<i>-of-<n>` | shard markers of a tier run (removed once the tier is complete) |

`begin()` of a tier run removes only its own tier marker and `_SUCCESS`; an untiered run removes both tier markers. So tier 0
stays usable while tier 1 runs, and any rerun invalidates the "everything" marker. `SideOutput(..., tier=t)`,
`is_complete(tier)` (true for `_SUCCESS` or that tier's marker), `completion(tier)`. The NLLB gate of the text enrichments
(`text_sources._use_nllb`) needs NLLB complete for the tier being read, and `theme` needs `topics` complete for the tier.
Per-shard side files (`_keyword_counts-`, `_match_rates-`, `review-`) carry the tier tag too (`_match_rates-t0-0.json`).

Assemble: `--entity work --tier 0` builds a works file from tier 0 only (READ_ASSEMBLE.md); without `--tier` it needs the full
`_SUCCESS`.

## Staging fingerprint

Side outputs are never cleared when staging is rebuilt (a rebuild with another `--limit` / `--work-cap` / dump leaves the old
rows behind), so every `_SUCCESS` (and tier marker) records what it was computed against: `fingerprint.staging_stamp(con,
entity, tier)` = `{n: row count, sum: sum of the ids, xor: xor of the ids}`, one scan of the `id` column (cheap for 50M works),
order independent, and combinable for disjoint id sets (the full `_SUCCESS` written from two tier markers holds the sum of
both). Each enrichment computes it at the start of its run and passes it to `SideOutput.finish(stamp)`; the shards of one output
must agree or the output never completes (`ShardStampMismatch`); the Snakemake `core_v4_success` rules stamp too. Assemble
compares it with the current staging and fails with a clear message on a mismatch unless `--allow-stale name1,name2` is passed
(`--allow-stale nllb` keeps cached translations of a rebuilt staging for rows whose id still exists: READ_ASSEMBLE.md
explains the risk). An empty marker (older run) counts as stale. Not covered: same ids, changed text.

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

- `side_outputs.py`: `SideOutput`, `Shard`, `SCHEMAS`, `PILLARS`, tier markers and stamps
- `fingerprint.py`: `staging_stamp`, `combine`, `same`
- `text_sources.py`: `text_sql`, `text_batches`, `open_staging`, NLLB gate
- `cli.py`: shared flags and path resolution
- `fixtures.py`: tiny synthetic staging fixture for tests (no real staging sample exists locally)

Tests: `ENV=dev uv run python -m pytest src/pipelines/core_v4/enrichment tests/test_countries.py`
