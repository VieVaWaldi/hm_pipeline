# Enrichment

Reusable ML/lookup capabilities. Nothing in here knows about duckdb, tables, or any
pipeline's schema — each capability implements `interface.Enricher[InputT, OutputT]`:
plain records in, plain records out.

| Capability | Enricher | Input → Output |
|---|---|---|
| `topic_modelling/` | `TfidfTopicClassifier` | `str` → `TopicPrediction` |
| `dch_classification/` | `DchClassifier` | `str` → `float` (P(is DCH)) |
| `geolocation/` | `GeolocationEnricher` | `InstitutionQuery` → `GeolocationResult` |

The duckdb-aware glue (which rows to read, where results get written) is pipeline-specific and
lives with the pipeline: `src/pipelines/core_v3/enrichment/` today, a parallel
`src/pipelines/core_v4/enrichment/` once core_v4 has tables. `llm/` and `ocr/` predate the
interface and haven't been ported to it yet.
