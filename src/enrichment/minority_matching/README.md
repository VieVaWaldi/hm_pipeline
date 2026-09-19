# Minority matching

Tags project/work text with the minority groups in `minorities_raw` (Wikidata harvest, see
`sources/dumps/minorities/`). `matcher.py` is pure (no duckdb, no pipeline knowledge); the glue is
`pipelines/core_v4/enrichment/minorities.py`, which writes `minorities/<entity>` and
`minorities/seen/<entity>` side parquet files.

## Matching

- Aho-Corasick (`pyahocorasick`) over normalised text: accent-stripped (NFKD, plus ł ø đ ß æ),
  lower-cased, apostrophes unified, whitespace collapsed. Whole-word boundaries (the neighbours of a hit
  must not be alphanumeric).
- Keywords per group: `group_name_en` + `search_keywords`, unioned with `manual_term_overrides.csv`
  (through `qid` or any `merged_qids`). Groups whose qid/merged qid is `is_titular_majority=true` in
  `titular_majority_overrides.csv` are dropped (the loader already does this; guards a stale table).
- The emitted id is the group's canonical `qid`. A keyword shared by several groups emits all of them.
- Typo tolerance: keywords of >= 6 chars also match every edit-distance-1 variant (delete, transpose,
  substitute, insert; first letter fixed). Variants equal to an exact keyword, or shared by two groups, are
  dropped. Variants are flagged (`Match.typo`) and reviewed separately.
- Ambiguity (`rules.yaml`): keywords of <= 4 chars and `common_words` match only as an exact,
  capitalised, accent-stripped word ("Sami", not "sami"/"SAMI"/"same"), no typo variants. `always_match`
  exempts short distinctive keywords (joik, yoik, manx); `blocklist` drops keywords entirely. All three lists
  are judgement calls: check them against the hit counts and precision sample.
- Multi-process over Arrow batches (`--workers`); each worker builds its own automaton
  (about 350k patterns with typos, ~2 s and ~470 MB per worker on the 278-group table). Use `--no-typos`
  for an exact-only run (666 patterns).

## Outputs and the precision check

- `_keyword_counts-<shard>.json`: rows hit per keyword (additive across resumed runs).
- `--review N`: writes `review-<shard>.csv` with up to N snippets for each of the 20 most frequent keywords
  (`keyword,id,typo,capitalised_only,snippet,correct`, match marked `[[...]]`); fill `correct` by hand.
  Run it on a representative sample: `--limit 200000 --review 200`.
- `--test N`: dry run, prints rows/hits/top keywords, writes nothing.

## Status of measurements

Not yet measured on real text (no local project/work text sample exists): per-keyword hit counts and the
~200-match precision review per top-20 keyword. Run on prod after NLLB is complete (or with
`--allow-untranslated`).
