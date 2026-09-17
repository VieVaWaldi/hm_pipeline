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
   (`path_duck_staging_2`).
5. `enrich_terms.py` — Phase 2 (per `planning/Plan.md`): harvests
   self-designation terms for every surviving group — native label (P1705),
   demonym (P1549), and English aliases (`skos:altLabel`, filtered to
   `lang="en"`). These are literal-valued properties, unlike Phase 1's
   DIMENSION_PROPS (all entity-valued, resolved via `wikibase:label`), so
   they need their own query shape. A term in a non-Latin script can't match
   English project text, so it's dropped via a documented, reproducible
   Unicode-block check rather than a per-group judgement call.

   Also merges in `manual_term_overrides.csv` — verified against Kat's
   original hand-curated keyword lists for the 3 pilot groups (the OR-lists
   embedded in
   `src/pipelines/core_v2/analyses/minorities/full_text_minoritiy_search_*.sql`),
   the automated harvest reliably finds direct name variants but structurally
   can't recover two categories those lists also carry: topically-related
   terms that aren't aliases of the group at all ("Hebrew"/"Yiddish"/
   "Judaism" for Jewish people — separate Wikidata entities, not names for
   the people), and domain/cultural knowledge with no Wikidata property to
   harvest from ("joik"/"yoik", a Sámi singing style; "Gardenese"/"Badiese"/
   "Fascian"/"Marebbano"/"Ampezzan", named Dolomite valleys where Ladin is
   spoken). `manual_term_overrides.csv` (qid, group_name_en, term) preserves
   those verbatim rather than pretending Wikidata alone can reproduce a
   domain expert's list — same override pattern as
   `titular_majority_overrides.csv`, just for search terms instead of
   majority-status classification.

   Adds a `search_keywords` column (union of group_name_en + harvested terms
   + manual overrides, deduped) to every row from `minorities_staging_2`.

   Writes `minorities_terms` into `minorities_terms.duckdb`
   (`path_duck_terms`) — this is the table intended for actual use.

Run individually:

```bash
uv run python src/sources/external/minorities/extract.py
uv run python src/sources/external/minorities/loader.py
uv run python src/sources/external/minorities/staging.py
uv run python src/sources/external/minorities/staging_2.py
uv run python src/sources/external/minorities/enrich_terms.py
```

Or via Snakemake — see `orchestration/rules/external/minorities.smk`. Unlike
`meta_heritage`, this source is wired into `rule all` / `sources_local`, so
`./orchestration/run_all_sources.sh` covers it too, and `generate_reports.py`
picks up `minorities.md`, `minorities_staging.md`, `minorities_staging_2.md`,
and `minorities_terms.md` automatically since it's a `dumps.yaml` entry.

## Serving

`index_meilisearch.py` — experimental, not wired into Snakemake: loads
`minorities_terms` into a Meilisearch `minorities` index (`search_keywords`
ranked right behind `group_name_en` in `searchableAttributes`), for testing
autocomplete/facet behaviour ahead of the real core_v4 serve pipeline (see
`infra/meilisearch/README.md`).
