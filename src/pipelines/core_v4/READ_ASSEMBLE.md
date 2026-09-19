# core_v4 assemble (Phase 3B)

`assemble.py` builds the final gold duckdb files from the trimmed staging duckdb plus the side parquet
outputs of the enrichments (`enrichment/README.md` is the side-output contract). Staging and side outputs are
read-only; nothing is copied into staging.

## Two files

Projects are enriched first and are ~10x smaller than works, so they get their own file and can be served first.
Serve ATTACHes both.

| file (config key) | tables |
|---|---|
| projects (`path_duck_projects`, limit: `path_duck_projects_limit`) | `project`, `organization`, `relation` (rows without a work/product side, i.e. project hasParticipant organization), `topic`, `relation_topic` (type = 'project') |
| works (`path_duck_works`, limit: `path_duck_works_limit`) | `work`, `relation` (rows where a `product`/`work` is source or target: product hasAuthorInstitution organization, project produces product), `relation_topic` (type = 'work') |
| works, tier 0 (`path_duck_works_linked`, limit: `path_duck_works_linked_limit`), `--entity work --tier 0` | the same tables, only for the project-linked works (`work.link_tier = 0`): tier-1 works, their relations and their relation_topic rows are absent. A subset of the full works file, built as soon as the tier-0 enrichments are done |

`topic` and `organization` exist only in the projects file; the works files refer to them through the attach.

## Run

```
uv run python -m pipelines.core_v4.assemble --entity project [--variant full|limit] [--staging-db P] [--enrichment-dir P]
         [--out P] [--skip name1,name2] [--shards N] [--oa-topics-db P] [--tmp-dir P] [--mem-mb M] [--threads T]
uv run python -m pipelines.core_v4.assemble --entity work --mem-mb 240000 --threads 16
uv run python -m pipelines.core_v4.assemble --entity work --tier 0      # project-linked works only -> path_duck_works_linked
         [--allow-stale name1,name2]
```

- Paths default to the variant's config block (`core_v4` / `core_v4_limit`): `path_duck_staging`, `path_enrichment_dir`,
  `path_duck_projects` / `path_duck_works`; `--oa-topics-db` defaults to `dumps.yaml` `oa_topics.path_duck`.
- The output is rebuilt from scratch every run: written to `<out>.tmp`, renamed at the end. A failure removes the tmp file and
  the spill directory and leaves any previous `<out>` untouched.
- **Tiers.** `--tier 0` (works only; tier 1 alone is not built) reads the tier-0 markers (`_SUCCESS.tier0`, or a full `_SUCCESS`),
  builds `work` from `stg.work WHERE link_tier = 0` and keeps only the relation rows whose work endpoint is in it, so nothing
  points at an absent work; `relation_topic` is limited the same way. Without `--tier` every work is built and the full `_SUCCESS`
  (both tiers complete) is required. `link_tier` is a staging column and passes through to the gold work table (SMALLINT, after
  `container`, before the new enrichment columns). The default `--out` of a tier-0 run is `path_duck_works_linked`.
- **Stale side outputs.** Each `_SUCCESS` holds a fingerprint (row count, sum and xor of the ids; `enrichment/fingerprint.py`) of
  the staging the enrichment ran against. Assemble computes the current one (per tier for the tier markers, so tier-0 outputs
  stay valid when only tier 1 changed) and stops with a message listing every side output that differs, or that has an empty
  marker from before fingerprints existed, unless it is named in `--allow-stale` (same names as `--skip`). Allowed stale
  outputs are still applied by id, with a WARNING: ids no longer in staging are ignored, new ids get no result. `--allow-stale
  nllb` is the one to know: it reuses the expensive cached translations of a staging that was rebuilt (e.g. another `--limit`
  or `--work-cap`) for every row whose id is still there; the risk is a row whose text changed under the same id keeping its
  old translation. Not covered by the fingerprint: same ids, changed text.
- Side outputs whose dir has no `_SUCCESS` (nllb also needs `nllb/seen`) stop the run and are listed in the error. `--skip nllb,dch`
  (names: nllb, topics, theme, dch, minorities, pillars, geolocation, regions) builds without them: a skipped output is not read at
  all, even if parts exist, and its columns keep their defaults, so the schema is identical to a full run.
- Memory: `--mem-mb` follows `pipelines.core_v3.resources` (DuckDB `memory_limit` = mem_mb - 40,000, but never below half of it),
  `--threads`, spill to `<out>.spill` (`--tmp-dir`), `preserve_insertion_order=false`. The work table and its `relation_topic` are
  built in id-hash shards (`--shards`, default 16 for work, 1 for project): the first shard is a CTAS, the others INSERT ... SELECT, and every
  side output is filtered to the shard before it is joined. `relation` is a streaming copy. Nothing large goes through Python.
- Log: row count of every table, and how many rows each enrichment touched (translated, dch, is_ch, minorities, pillars != 0, theme,
  region, geolocation overlay). The run fails if `project`/`work`/`organization` do not have exactly the staging row count (a
  duplicated side-output key would show up there).

## Column contract

Staging columns keep their name and position; the columns below are replaced in place or appended.

| table | column | type | source / rule | default when missing or skipped |
|---|---|---|---|---|
| project | `title`, `summary` | as staging | `COALESCE(nllb.text_en, original)` per field | original |
| work | `link_tier` | SMALLINT | staging (0 = project-linked, 1 = org-only), passed through | |
| work | `title` | as staging | `COALESCE(nllb.text_en, original)` | original |
| work | `descriptions` | `VARCHAR[]` | element 1 replaced by nllb field `description`; elements 2.. untouched | original |
| project, work | `is_translated` | BOOLEAN | id has any field (also acronym, keywords, subjects, container, ...) in the nllb side output | false |
| project, work | `is_ch` | BOOLEAN | dch | NULL (not classified) |
| project, work | `pred` | FLOAT | dch | NULL |
| project, work | `minority_qid` | VARCHAR[] | minorities | `[]` |
| project, work | `pillars` | UTINYINT | pillars bitmask, lowest bit first: inclusive, sustainable, resilient, innovative, global | 0 |
| project, work | `theme` | VARCHAR | theme | NULL |
| organization | `region` | VARCHAR | regions (falls back to a staging `region` value if the side output has none) | NULL |
| organization | `geolocation` | staging type | geolocation side output (lat, lon), only where staging `geolocation IS NULL` | staging value |
| organization | `geolocation_source` | VARCHAR | the geocoder's source string verbatim (e.g. `mapbox`, `mapbox_temporary`), only for the overlaid rows | staging value |
| topic | `id, subfield_id, field_id, domain_id, topic_name, subfield_name, field_name, domain_name, keywords, summary, wikipedia_url, created_at, updated_at` | as `enrichment/topic_modelling/schema.py` | `oa_topics_raw` (`topic_id` -> `id`, ids cast to VARCHAR, keywords JSON as text); `created_at` = assemble time, `updated_at` NULL | |
| relation_topic | `type, source_id, topic_id, score, created_at` | as schema.py | topics side output of the entity; one row per (id, topic_id); ids not in staging dropped | empty |

Notes:

- `geolocation`: the staging column type is kept. `STRUCT(lat DOUBLE, lon DOUBLE)` (dev fixture / expected core_v4) and core_v3-style
  `DOUBLE[]` = `[lat, lon]` both work. If staging has no `geolocation` (or `geolocation_source`, `region`) column, assemble appends it
  (struct for geolocation).
- `relation_topic` has no primary key (schema.py has one on `(type, source_id, topic_id)`): the INSERT already guarantees uniqueness and an
  index over ~50M work rows would dominate memory. Add the index in the serving database if needed.
- Side outputs are deduplicated per id (per `(id, topic_id)` for topics; nllb per `(id, field)` via `max`), so a rerun that left an extra
  part cannot multiply rows.
- Rows are unordered.

## Tests

`ENV=dev uv run python -m pytest src/pipelines/core_v4/test_assemble.py` (tiny tmp-dir staging + fake side outputs; no GPU, no network).
