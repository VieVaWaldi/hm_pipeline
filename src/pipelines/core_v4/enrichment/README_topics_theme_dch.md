# Phase 2c: topics, theme, DCH

Depends on NLLB (text) for real runs; all three refuse to run before `nllb/<entity>/_SUCCESS`
unless `--allow-untranslated`. Common flags: see `cli.py`.

| step | module | output | notes |
|---|---|---|---|
| topics | `topic_modelling.py` | `topics/<entity>` (id, topic_id, score) | CPU; `--build-model` once, then any number of `--shard I/N` |
| theme | `theme.py` + `themes.yaml` | `theme/<entity>` (id, theme), sparse | pure SQL, recomputed wholesale; needs `topics/<entity>` complete |
| dch | `dch_classification.py` | `dch/<entity>` (id, is_ch, pred) | GPU node; chunked, resumable per chunk |

## Topics

- Model: `<enrichment_dir>/topics/tfidf_model.pkl`, built by `--build-model` from the oa_topics taxonomy plus a
  seeded (42) reservoir sample of 50k translated project texts (IDF weighting only). Built once so every shard
  scores with the same model; the run refuses to start without it.
- Hot path (`classifier.py`, additive: `normalise_texts`, `classify_batch`, `enrich_batch`; `enrich` is unchanged
  and is the "before" baseline): worker processes load the model once through a pool initializer, `nlp.pipe`
  replaces one `nlp()` per text, and one sparse product `X @ T.T` replaces a `cosine_similarity` call per text
  (both sides are already L2-normalised). Predictions are identical to `enrich` (tested).
- Rows without usable text get no row. Rows sharing no vocabulary with any topic get score 0.0; the theme step drops them.
- `benchmark_topics.py` measures docs/s before/after on locally available English text (synthetic, see its docstring).

## Theme

`themes.yaml`: Economy = field 20 + topics 12033, 14219, 12312, 14352, 14433; Tourism = subfield 1409 + topics
10055, 11793, 11925, 12399, 12456, 12584, 12402, 11474, 11410 (from `core_v3/materialized/foci.sql`).
Best topic per row (highest score), then `score >= --min-score`, then the first matching theme in file order.
`--sweep` prints rows per theme at thresholds 0 to 0.5 to choose `min_score` on the representative sample;
the value in `themes.yaml` (0.1) is provisional.

## DCH

`DchClassifier` unchanged. One parquet part per chunk of 40,960 rows; kill-safe. `--test N` reads and infers N
rows, writes nothing. Tests use a mocked classifier (no GPU).
