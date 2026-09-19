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
Temporary results are subject to Mapbox's storage restrictions; the decision (Sep 2026): **store
the free-tier results for now and pay for permanent geocoding later**. To make that backfill
trivial every cache row records the mode it was fetched in (see "Temporary now, permanent later").

**No `fuzzyMatch`**: the parameter existed in Geocoding v5 and was removed in v6. Approximate
matching is always on; its quality is `features[0].properties.match_code.confidence`
(`exact` > `high` > `medium` > `low`). Only medium-or-better is accepted (stored as `confidence`
in the side output); anything else is cached as a negative.

**Street-only addresses**: for a street without house number Mapbox returns a `street` feature
(`properties.feature_type = "street"`) that carries **no `match_code`** (per the v6 docs `match_code`
exists only on address features). Such a result (feature_type `street` or `address`, no confidence) is
accepted, with `confidence = 'street'`, only if the feature's `properties.context.place.name` equals
**any part** of the queried city (case- and diacritic-insensitive: `Zürich` = `zurich`), or if the
queried postcode equals `properties.context.postcode.name` (spaces ignored; place names differ across
languages). Cordis cities can be bilingual (`LOUVAIN / LEUVEN`): the city is split on `/`, `;`, `,`
and spaced dashes (`Aix-en-Provence` stays whole), and only the **first part** is sent as `place`
(one query = one billed request, so alternatives cannot be tried; the cache key still uses the raw
string). A different place and postcode is cached as a negative with `confidence = 'street_place_mismatch'`; a result that states `low` stays
rejected. Side-output `confidence` is therefore one of `exact | high | medium | street`.

## Side output

`id, lat, lon, geolocation_source, confidence` (parquet, see the core_v4 enrichment README).
`geolocation_source` is `'mapbox'` for a row fetched with permanent geocoding and
`'mapbox_temporary'` for one fetched without. It follows the cache row, not the current run: a
temporary row read back by a permanent run is still `'mapbox_temporary'`.

## Temporary now, permanent later

The cache table has a `permanent BOOLEAN` column (rows from databases created before it existed
are migrated by `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and count as `permanent = FALSE`).

`--permanent --refresh-temporary` (`GeolocationEnricher(permanent=True, refresh_temporary=True)`)
treats every cached **temporary positive** row as not cached, so exactly those are re-requested
(and overwritten with `permanent = TRUE`). Temporary negatives are not refreshed: they carry no
coordinates to store, so paying again would only buy the same "no result". `--refresh-temporary`
without `--permanent` is an error. The side output is **rewritten from scratch** in this mode (orgs
already written as `mapbox_temporary` must be redone); a row that cannot be re-requested because the
budget ran out keeps its cached temporary answer (`mapbox_temporary`), and no `_SUCCESS` is written until
none are left. Nothing is lost by stopping halfway: refreshed rows are permanent in the cache.

Cost estimate (`--dry-run --permanent --refresh-temporary`): `to request` counts only distinct
addresses that will really be sent, i.e. not cached at all, plus cached temporary hits;
permanent rows and temporary negatives are free. Cost = that count at $5/1,000 (first 500k a
month, $4 above). Example: 16k distinct addresses, 12k of them temporary hits already cached, 4k
negatives: the refresh requests 12k, about $60.

## Cost and limits (checked against docs.mapbox.com / mapbox.com/pricing, Sep 2026)

- Temporary geocoding (default): **first 100,000 requests / month free**, then $0.75 / 1,000 (to 500k), $0.60 (to 1M), $0.45 above. `--dry-run` applies this but cannot see usage already spent this month.
- Permanent geocoding: **$5.00 / 1,000 requests** for the first 500,000 a month, **$4.00 / 1,000**
  above (contact sales beyond 1M). 
- Every query inside a batch is billed as one request; batching saves HTTP calls, not money.
- Default rate limit: **1,000 requests / minute** across the Geocoding API. The docs do not say
  whether one batch call counts as 1 or as N; the geocoder assumes N and paces itself to
  1,000 queries/min (`rate_limit_per_min`), so 16k queries take about 16 minutes.
  Raise it only after confirming the account limit.
- Phase 1 estimate: about 16,050 orgs need Mapbox: **$0** temporary (inside the free 100k), roughly **$80** permanent (distinct addresses are sent once; a later `--refresh-temporary` costs $5/1,000 x the cached temporary hits only).
  `--dry-run` prints the exact count and cost for the current data.

## Budget and safety

- `--max-requests N` (default **0 = spend nothing**): hard cap on billed queries per run. With 0,
  cached answers are still written, everything else is deferred and no `_SUCCESS` is written.
- `--dry-run`: counts eligible orgs, distinct addresses, cached, and to-be-billed, and prints the
  estimated cost (free tier unless `--permanent`; with `--refresh-temporary` see above). Sends and writes nothing. `--test N` does the same computation on N rows through
  the normal path with a zero budget (cache only).
- Retry with exponential backoff (2s, 4s, 8s ... honouring `Retry-After`) on 429, 5xx and
  connection errors; a 401/403 stops immediately; a 422 bisects the batch down to the bad address,
  which is cached as unanswerable.

## Cache

`data/cache/mapbox.duckdb` (`path_cache_dir` in `config/pipelines.yaml`), table `mapbox_cache`,
keyed by the normalised (street, postcode, city, country); columns `found, lat, lon, confidence, queried_at, permanent`. It stores **negative** results too
(no match, low confidence, street in another city) and is checked before every request. It is written after every HTTP
call, so a crash never loses paid answers. It lives outside the enrichment directory precisely so
that no teardown deletes it: every entry cost money. It is a single-writer duckdb file, so run the
geolocation step in one process (the CLI refuses `--shard`).

## Tests

HTTP is mocked everywhere (`test_geocoder.py`, `pipelines/core_v4/enrichment/test_geolocation.py`).
`test_live_smoke` (at most 20 queries) only runs with `MAPBOX_LIVE_OK=1` and `API_KEY_MAPBOX` set;
it spends real money, so get the owner's OK first.
