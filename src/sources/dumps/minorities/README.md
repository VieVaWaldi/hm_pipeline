# Minorities (Wikidata minority-group discovery)

Harvests a candidate list of European minority groups from Wikidata via SPARQL,
then narrows it to a working list in DuckDB. Feeds the platform's planned
minority filter dimension (see [`6_1_minorities.md`](6_1_minorities.md) and
[`Plan.md`](Plan.md) for the full research rationale).

Structurally like `sources/dumps/`: a one-shot harvest against a live external
endpoint (not checkpointed like `sources/apis/`), staged into DuckDB rather
than loaded straight into Postgres.

## Pipeline

1. `extract.py` — queries the Wikidata Query Service for ethnic groups /
   tribes / indigenous peoples tied to an explicit European country
   allowlist, plus manual seeds for the three pilot groups (Ladin, Sámi,
   Jewish). Writes `data/pile/minorities/minorities.csv` (path from
   `config/dumps.yaml` -> `minorities.path_raw`). Kept as its own step: it's
   the one genuinely expensive, rate-limited part of this pipeline (a live
   external SPARQL discovery query), and it already writes its own real,
   inspectable artifact — same treatment as any other `sources/dumps` source.

2. `loader.py` — everything downstream of the raw harvest, run against one
   DuckDB connection, writing only the final result to disk:
   - **load** the CSV as-is.
   - **filter + dedup** — three documented, reproducible filters (none of
     them a judgement about minority status): unresolved labels, diaspora
     typing (Wikidata's own `P3833` is set), and non-European-only (an
     artifact of the "indigenous to" discovery query). Merges duplicate
     Wikidata entries for the same group.
   - **titular-majority filter + subgroup rollup** — drops groups that are
     the titular/majority population of one of their own listed countries
     (Austrians -> Austria, ...) per `titular_majority_overrides.csv`
     (hand-classified against the actual data; a few ambiguous cases like
     Bosniaks or Flemish people are recorded there but deliberately kept in,
     with a note on why); rolls up any remaining row whose `part_of` matches
     another surviving row (e.g. the Sámi subgroups) into a
     `known_subgroups` column on the parent.
   - **term enrichment** (Phase 2, per `Plan.md`) — harvests
     self-designation terms for every surviving group from Wikidata: native
     label (`P1705`), demonym (`P1549`), English aliases (`skos:altLabel`,
     `lang="en"`). A term in a non-Latin script can't match English project
     text, so it's dropped via a documented, reproducible Unicode-block
     check rather than a per-group judgement call. Also merges in
     `manual_term_overrides.csv` — verified against Kat's original
     hand-curated keyword lists for the 3 pilot groups (the OR-lists
     embedded in
     `src/pipelines/core_v2/analyses/minorities/full_text_minoritiy_search_*.sql`):
     the automated harvest reliably finds direct name variants but
     structurally can't recover two categories those lists also carry —
     topically-related terms that aren't aliases of the group at all
     ("Hebrew"/"Yiddish"/"Judaism" for Jewish people), and domain/cultural
     knowledge with no Wikidata property to harvest from ("joik"/"yoik", a
     Sámi singing style; "Gardenese"/"Badiese"/"Fascian"/"Marebbano"/
     "Ampezzan", named Dolomite valleys where Ladin is spoken).
     `manual_term_overrides.csv` (qid, group_name_en, term) preserves those
     verbatim rather than pretending Wikidata alone can reproduce a domain
     expert's list — same override pattern as
     `titular_majority_overrides.csv`, just for search terms instead of
     majority-status classification. Adds a `search_keywords` column (union
     of `group_name_en` + every harvested/manual term, deduped).

   Writes `minorities_raw` into `minorities_raw.duckdb` (`path_duck`) — this
   is the table intended for actual use. (Used to be four separate duckdb
   files — raw / staging / staging_2 / terms — one per phase; collapsed once
   the pipeline stabilized at 304 groups, since that granularity stopped
   earning its keep. The phases above are still separate *functions* in
   `loader.py`, just no longer separate on-disk artifacts.)

Run individually:

```bash
uv run python src/sources/dumps/minorities/extract.py
uv run python src/sources/dumps/minorities/loader.py
```

Or via Snakemake — see `orchestration/rules/dumps.smk`. Unlike
`meta_heritage`, this source is wired into `rule all` / `sources_dev`, and
`generate_reports.py` picks up `reports/sources/dumps/minorities.md`
automatically since it's a `config/dumps.yaml` entry.

## Serving

`index_opensearch.py` — experimental, not wired into Snakemake: loads
`minorities_raw` into an OpenSearch `minorities` index (`search_keywords`
alongside `group_name_en` as full-text searchable fields), for testing
search/facet behaviour ahead of the real core_v4 serve pipeline.
