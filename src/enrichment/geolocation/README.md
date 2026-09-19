# Geolocation (Mapbox batch geocoding)

`geocoder.py` is a pure Enricher; the duckdb glue is `pipelines/core_v4/enrichment/geolocation.py`.
It geocodes only organizations that still have no coordinates after the ROR and Cordis tiers (both
arrive in staging with `geolocation_source` set) and that have a real address (street + city +
country). A name alone is never queried.

## Endpoint and request

`POST https://api.mapbox.com/search/geocode/v6/batch?access_token=...&permanent=false` (default; `--permanent` sends `true`), JSON body =
array of up to **1,000** structured queries:
`{"street", "address_number"?, "postcode"?, "place": city, "country": ISO alpha-2, "limit": 1}`.
The response is `{"batch": [<FeatureCollection>, ...]}` in request order; an unmatched query is an
empty `features` list. Coordinates are `[lon, lat]`. The street string is split into
`address_number` + `street` heuristically (`Hauptstr. 1`, `10 Downing Street`); if there is no
clear number the whole string is sent as `street`.

`permanent` is an option, default **false** (temporary geocoding, free tier). `--permanent` /
`permanent=True` sends `permanent=true` (billed as permanent, needs a credit card on the account).
Temporary results are subject to Mapbox's storage restrictions; the user has deferred that decision.

**No `fuzzyMatch`**: the parameter existed in Geocoding v5 and was removed in v6. Approximate
matching is always on; its quality is `features[0].properties.match_code.confidence`
(`exact` > `high` > `medium` > `low`). Only medium-or-better is accepted (stored as `confidence`
in the side output); anything else is cached as a negative.

## Cost and limits (checked against docs.mapbox.com / mapbox.com/pricing, Sep 2026)

- Temporary geocoding (default): **first 100,000 requests / month free**, then $0.75 / 1,000 (to 500k), $0.60 (to 1M), $0.45 above. `--dry-run` applies this but cannot see usage already spent this month.
- Permanent geocoding: **$5.00 / 1,000 requests** for the first 500,000 a month, **$4.00 / 1,000**
  above (contact sales beyond 1M). 
- Every query inside a batch is billed as one request; batching saves HTTP calls, not money.
- Default rate limit: **1,000 requests / minute** across the Geocoding API. The docs do not say
  whether one batch call counts as 1 or as N; the geocoder assumes N and paces itself to
  1,000 queries/min (`rate_limit_per_min`), so 16k queries take about 16 minutes.
  Raise it only after confirming the account limit.
- Phase 1 estimate: about 16,050 orgs need Mapbox: **$0** temporary (inside the free 100k), roughly **$80** permanent (distinct addresses are sent once).
  `--dry-run` prints the exact count and cost for the current data.

## Budget and safety

- `--max-requests N` (default **0 = spend nothing**): hard cap on billed queries per run. With 0,
  cached answers are still written, everything else is deferred and no `_SUCCESS` is written.
- `--dry-run`: counts eligible orgs, distinct addresses, cached, and to-be-billed, and prints the
  estimated cost (free tier unless `--permanent`). Sends and writes nothing. `--test N` does the same computation on N rows through
  the normal path with a zero budget (cache only).
- Retry with exponential backoff (2s, 4s, 8s ... honouring `Retry-After`) on 429, 5xx and
  connection errors; a 401/403 stops immediately; a 422 bisects the batch down to the bad address,
  which is cached as unanswerable.

## Cache

`data/cache/mapbox.duckdb` (`path_cache_dir` in `config/pipelines.yaml`), table `mapbox_cache`,
keyed by the normalised (street, postcode, city, country). It stores **negative** results too
(no match, or low confidence) and is checked before every request. It is written after every HTTP
call, so a crash never loses paid answers. It lives outside the enrichment directory precisely so
that no teardown deletes it: every entry cost money. It is a single-writer duckdb file, so run the
geolocation step in one process (the CLI refuses `--shard`).

## Tests

HTTP is mocked everywhere (`test_geocoder.py`, `pipelines/core_v4/enrichment/test_geolocation.py`).
`test_live_smoke` (at most 20 queries) only runs with `MAPBOX_LIVE_OK=1` and `API_KEY_MAPBOX` set;
it spends real money, so get the owner's OK first.
