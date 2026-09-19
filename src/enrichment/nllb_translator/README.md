# nllb_translator

Language identification and translation to English for core_v4 (NLLB-200 + fastText LID-218).
Pure modules, no duckdb: the pipeline glue is `src/pipelines/core_v4/enrichment/nllb_translation.py`.

| file | role |
|---|---|
| `language_id.py` | fastText LID-218 wrapper (FLORES codes), all-caps and script guards, threshold helper |
| `translator.py` | truncate, sentence-split, chunk to ~200 tokens, batch, translate (CTranslate2 or Transformers) |
| `download.py` | fetches LID + models into `data/models/nllb/` (**login node only**), path guards |
| `benchmark.py`, `sample_works.py`, `score_outputs.py`, `run_measurements.sbatch` | the measurements below |

## Setup (login node, once)

Compute nodes have no internet. `download.py` raises `ModelNotDownloadedError` with this command when
something is missing:

```bash
uv run python -m enrichment.nllb_translator.download                       # LID + 600M + 1.3B, CT2 float16
uv run python -m enrichment.nllb_translator.download --quantization int8_float16
```

Layout: `data/models/nllb/lid218e.bin`, `<600M|1.3B>/hf/` (Transformers weights, tokenizer), `<key>/ct2-<quant>/`.

## Behaviour

- **Language id**: LID-218 on every field of every row; OpenAire's `language` label is ignored (36.9% of the
  sample is `und`/null, and it disagrees with LID in both directions, see below). Empty or < 20 character
  texts are skipped (`seen.translated = false`, `src_lang` NULL).
- **Translate when** the label is not `eng_Latn`, P(label) >= `--min-prob` (default 0.5), and NLLB supports
  the language (LID-218 knows a few labels NLLB does not).
- **Two guards LID-218 needs** (both measured on the sample, both in `language_id.py`):
  1. ALL-CAPS text is lower-cased before LID *and* before translation. Upper-case English titles come out as
     `yue_Hant`/`kor_Hang` with probability ~1.0 (3,039 of 60,000 titles), and NLLB copies all-caps input through untranslated.
  2. A label whose script is not Latin must be backed by non-Latin letters in the text (and vice versa).
     Without the guards, non-English share of titles at P>=0.5 was 23.6%; with them 18.8%.
- **Translation**: truncate (`--max-chars`, default 1500; p95 of descriptions is 2,441 chars, max 5.18M), split
  into sentences, pack into chunks of <= 200 sentencepiece tokens (a longer run is cut on token boundaries), sort by
  length, batch by token budget, re-assemble in order. Batches mix source languages by default (the language is only a
  prefix token; `--group-by-language` groups them as originally briefed; +7% tokens/s ungrouped at 20k texts, more at
  small call sizes).
- **Runaway output guard** (glue): a translation longer than 3x its source + 40 characters is dropped and the
  field stays untranslated; NLLB sometimes appends invented sentences to short input
  ("... This is the first time I've seen this.").
- **Output**: `nllb/seen` for every processed field, `nllb` for translated ones (see `../../pipelines/core_v4/enrichment/README.md`).

## Threshold (`--min-prob`, default 0.5)

Measured on 200,000 works dated >= 2018, org-linked and without project (`sample_works.py`, the tier the 50M cap is
filled with; ids verified: 2,000/2,000 have an org link, 0 have a project link). Non-English share of *works*:

| min P | title | any field (title or description) |
|---|---|---|
| 0.3 | 19.2% | 20.6% |
| 0.5 | 18.8% | 20.2% |
| 0.7 | 17.9% | 19.6% |
| 0.9 | 16.0% | 18.6% |

Only ~1% of works sit in the 0.3-0.7 band, so the choice moves little. I read 14 random titles per band
(a small, manual check, not a benchmark): 0.3-0.5 about 5/14 genuinely non-English (taxon names, references,
author lists); 0.5-0.7 about 11/14 (false positives are taxon names and English proper nouns, which NLLB
mostly copies); 0.7-0.9 14/14. 0.5 is the default: it keeps most of the recoverable text and the false positives
cost little GPU time. Raise it to 0.7 to cut those.

Titles identifiable (>= 20 chars): 94.8%; works with a >= 20 character description: 71.5%.
OpenAire's own label vs LID (title, P >= 0.8): 15,202 works labelled `und`/null are non-English by LID,
1,077 labelled `eng` are non-English, 1,934 labelled non-English are English.

## Measurements (gpu-test, 1x A100 80GB, driver 530.30.02 / CUDA 12.1)

**CTranslate2 4.8.2 works on driver 530** (torch cu126 wheels supply cuBLAS/cuDNN; `import torch` first). No fallback to
Transformers is needed; it stays available (`--backend transformers`) and is 3-4x slower.

Model size (disk): 600M HF 2.48 GB, CT2 float16 1.24 GB, CT2 int8_float16 0.60 GB. 1.3B HF 5.51 GB, CT2 float16 2.75 GB,
CT2 int8_float16 1.3 GB. "1.3B" is `facebook/nllb-200-distilled-1.3B`; the non-distilled 1.3B was not compared.

Speed on the same 20,000 non-English texts (titles and descriptions mixed, 26k chunks, 2.3M source tokens), float16, mixed batches:

| model | backend | beam | batch tokens | src tokens/s | chunks/s | peak VRAM (incl. model) |
|---|---|---|---|---|---|---|
| 600M | CTranslate2 | 2 | 8,192 | 9,499 | 110 | 4.1 GB |
| 600M | CTranslate2 | 2 | 32,768 | 14,366 | 167 | 8.6 GB |
| 600M | CTranslate2 | 1 | 32,768 | 25,264 | 294 | 6.3 GB |
| 600M | CTranslate2 | 4 | 32,768 | 8,342 | 97 | 12.4 GB |
| 1.3B | CTranslate2 | 2 | 8,192 (grouped) | 4,865 | 55 | 6.4 GB |
| 1.3B | CTranslate2 | 2 | 32,768 | 7,935 | 92 | 14.8 GB |
| 1.3B | CTranslate2 | 4 | 32,768 | 4,722 | 55 | 20.9 GB |
| 600M | Transformers | 2 | 32,768 | 4,078 | 47 | 73 GB* |
| 1.3B | Transformers | 2 | 16,384 | 2,876 | 33 | 56 GB* |

Follow-up on 10,000 texts: 600M CT2 beam 1: 30,029 tok/s at 65,536 batch tokens; 2 inter-threads change nothing at beam 1
(30.5k) and give +12% at beam 2 (15.9k vs 14.1k); int8_float16 is slower than float16 (23.2k vs 25.3k) with no quality gain
worth the smaller model. 1.3B CT2 beam 1: 13,262 tok/s / 155 chunks/s / 10.1 GB (int8_float16: 11,825).
\* Transformers VRAM is the allocator's reserved memory, not a requirement.

Quality proxy: there are no human references, so chrF against the strongest configuration (1.3B, float16, beam 4)
on 10,000 texts, `score_outputs.py`:

| configuration | chrF vs 1.3B beam 4 |
|---|---|
| 1.3B beam 1 | 86.8 |
| 1.3B beam 1, int8_float16 | 85.9 |
| 600M beam 4 | 77.6 |
| 600M beam 2 | 77.2 |
| 600M beam 1 | 75.4 |

The model matters far more than the beam (+9-11 chrF for the 1.3B, +2 for beam 2 over 1). Reading the side-by-side
outputs, the 600M drops content (an Indonesian abstract came out without its first clause), the 1.3B does not.
This proxy favours the 1.3B by construction; treat it as "how far the cheaper setting drifts".

**Choice: 1.3B distilled, CTranslate2 float16, beam 1, 32,768 batch tokens** (defaults). Same throughput as the 600M at
beam 2 (13.3k vs 14.1k tok/s) and clearly better output. The 600M at beam 1 is 2.3x faster if the budget needs it.

### GPU-hour estimate, full 50M works

Sample: mean 35.8 source tokens per work (title and 1,500-char description, P >= 0.5, all works counted, English
ones as zero) -> **1.79 billion source tokens** for 50M works. Pure translation time at the measured rates:

| setting | tok/s | GPU-hours |
|---|---|---|
| 600M CT2 beam 1 | 30,000 | 17 |
| **1.3B CT2 beam 1 (default)** | 13,300 | **37** |
| 1.3B CT2 beam 2 | 7,900 | 63 |
| 1.3B CT2 beam 4 | 4,700 | 105 |

Add LID: ~2.8-6.8k texts/s per CPU core (100M texts -> 4-10 core-hours, spread over the shards) and the database read.
The estimate uses the org-only 2018+ tier; the real 50M set also holds the project-linked works (5M) of any date. Projects
(a few million rows) add a few percent. The `--shard I/N` flag splits it over gpu-test nodes (3 GPUs there, up to 12 h each).
### End to end (glue, gpu-test, defaults)

`nllb_translation.py` on 60,000 works (a `--limit` run, so no `_SUCCESS`; output in `data/enrichment/nllb_sample/e2e_scale/`,
not from the org-only 2018+ tier): 93,757 fields seen (`nllb/seen`), 17,739 translated fields on 12,702 works (`nllb`),
21,705 chunks. 12,420 source tokens/s, 173 chunks/s, 421 works/s. Time split: LID 16 s, translation 125 s, of 143 s total
(the rest is reading and writing). Extrapolated linearly, 50M works take 33 GPU-hours, in line with the 37 h above (21% of
these works had a translated field, vs 20.2% in the org-only sample, so the mix is comparable; treat it as a cross-check).
Resume, shard partitioning and `_SUCCESS` are covered by `test_nllb_translation.py`.

## Licence

The NLLB-200 models and the fastText LID-218 model are **CC-BY-NC 4.0** (non-commercial). Our CTranslate2 conversions
are derivatives of the NLLB weights and carry the same terms. Whether the licence reaches the *translations* is not
settled; check before any commercial use of the enriched data.

## Reproduce

```bash
uv run python -m enrichment.nllb_translator.download                                   # login node
sbatch --wrap 'ENV=prod uv run python -m enrichment.nllb_translator.sample_works'     # fat partition, ~7 min
sbatch src/enrichment/nllb_translator/run_measurements.sbatch                          # gpu-test
```
