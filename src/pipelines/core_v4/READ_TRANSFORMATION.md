# Core_v4 Transformation

Script: `src/pipelines/core_v4/transformation.py`, tests: `src/pipelines/core_v4/test_transformation.py`.
Numbers marked "Phase 1" come from `investigation/phase1_measurements.py` (see `README.md` sections 3 and 4).

## General

`transformation.py` builds core_v4's staging duckdb (`config/pipelines.yaml`: `core_v4.path_duck_staging`, or
`core_v4_limit.path_duck_staging_limit` for `--limit`) from OpenAire staging v4 (`openaire_dump.path_duck_staging_v4`),
ROR (`ror_dump.path_duck`) and Cordis (`full_projects_no_pdfs`). It follows core_v3
(`../core_v3/READ_TRANSFORMATION.md`): the target is deleted (with a leftover `.wal`) and rebuilt from scratch on every run;
sources are attached read-only. Persistent caches (`path_cache_dir`) and enrichment side outputs are never touched.
Tables are seeded first, then columns are added (no new tables). It differs from core_v3 in:

| | core_v3 | core_v4 |
|---|---|---|
| works | all 218M copied | trimmed **at seed time** to `work_cap` (default 50M), unlinked works never copied |
| relation | all | only product-side relations of kept works; all project -> organization |
| countries | as found | `common.countries` (organization.countryCode, work.countries) |
| ROR | `geolocation = [lat, lng]` (a `[NULL, NULL]` array when ROR has none) | same order, but NULL when ROR has no coordinates, plus `geolocation_source` |
| Cordis project match | `grantId = id_original` | same, plus a DOI fallback |
| Cordis org match | `lower(trim(name))` only (2.5 pairs per triplet) | PIC first, name + normalised country as strict fallback, never name only |
| Cordis output | 2 relation columns | 2 relation columns + address columns and coordinates on organization |

Usage:

```
python -m pipelines.core_v4.transformation [--variant full|limit] [--staging-db PATH] [--limit N] [--work-cap N]
                                           [--mem-mb M] [--threads T]
```

`--mem-mb` / `--threads` are the shared `core_v3.resources` flags. `--limit N` implies `--variant limit`; `--variant full`
together with `--limit` is rejected. `--variant limit` without `--limit` uses N = 1000.

## 1. Seed and work trim

`organization`, `project` and `relation` come from staging v4. `work` is never copied whole:

1. `_work_link`: one row per work that a relation points at. Tier 0 = `project -produces-> product`,
   tier 1 = `product -hasAuthorInstitution-> organization` (tier 0 wins when both). Works without a relation are not in it.
2. `_work_rank` (narrow: id, tier, has_desc, d): joined to staging `work` for `len(descriptions) > 0` and
   `publicationDate`. `d` is the date, or NULL when the date is after `current_date` (bad dates: 16,879 works were dated after 2026,
   up to 2999; without this they would sort as "newest").
3. `_work_keep`: `ORDER BY tier, d DESC NULLS LAST, has_desc DESC, id LIMIT work_cap`. `id` (a hash, unique) makes the order total, so
   the works on the cutoff date are chosen the same way on every run. Preferring works with a description matters for NLLB and the
   text enrichments.
4. `work` = staging `work` joined to `_work_keep` (no sort of the wide table). `publicationDate` is set to NULL for future dates,
   `countries` normalised (`list_sort(list_distinct(norm_cc(...)))`, invalid codes dropped).
5. `relation` = all project -> organization rows (hasParticipant), plus product-side rows only for kept works. The final
   verification logs the number of relation rows that point at a dropped work (must be 0).

The log prints works in staging, linked, dropped as unlinked, future-dated, total/kept per tier, and where the cutoff falls
(tier, date, kept of the works on that date).

`work_cap` is `core_v4.work_cap` in `config/pipelines.yaml` (a quoted string, because `SourcePaths` only takes string values under
keys not starting with `path_`); `--work-cap` overrides it.

### Expected yield at 50M (Phase 1)

| | works |
|---|---|
| in staging | 218,421,450 |
| tier 0 (project-linked): project + org 4,558,270, project only 443,603 | 5,001,873, all kept |
| tier 1 (org only) | 115,456,657 in total, 44,998,127 kept |
| unlinked (`neither`) | 97,962,920, dropped |
| cutoff | tier 1, 2018-05-01: 106,004 works on that date, 103,254 fit (2,750 dropped, the ones without description and with the highest ids go first) |
| dated in the future, set to NULL | at most 16,879 (only matters for those linked to a project or organization) |

Kept works with a title 99.99%, with a description about 74%, with a valid date 99.5%. Re-measure the cutoff on prod: NULLing
future dates and the description tie-break change which works are on the boundary, not how many are kept.

## 2. Country codes

`common.countries` (`register_country_macro` defines `norm_cc`): EL->GR, UK->GB, YU/CS->RS, ..., EU/ZZ->NULL, XK kept.
Applied to `organization.countryCode`, `work.countries` and (inside the merge) the Cordis institution country, so the name
fallback compares `GB` with `GB`, not `UK` with `GB`. ROR country codes are clean (Phase 1) and kept as they are in `rorLocations`.

## 3. ROR -> organization

Join `organization.rorId = ror.organizations.id`. Columns as core_v3: `rorStatus`, `rorEstablished`, `rorTypes`, `rorLocations` (JSON),
`rorRelationships` (JSON), `geolocation DOUBLE[]` = **`[lat, lng]`** of `locations[1]`. New: `geolocation_source VARCHAR` = `'ror'`
where coordinates were set (NULL otherwise). Phase 1: 126,401 orgs have a rorId, 126,400 are found in ROR and have coordinates.

## 4. Cordis -> relation + organization

Cordis tables: `project`, `institution`, `j_project_institution`. A *triplet* is a (Cordis project, Cordis institution) pair
inside a matched project, the unit Phase 1 counted (480,133 of them).

**(a) Project match.** `cordis.project.id_original = project.grantId`; for Cordis projects that found no OpenAire project that way,
`norm_doi(cordis.project.doi) = norm_doi(project.doi)` (lower-cased, `https://doi.org/` prefix removed; `project.doi` exists in
staging v4 only, the step is skipped with a warning when it is missing). A Cordis project can match several OpenAire projects
(9,250 do in Phase 1); the relation join in (c) picks the right pair.

**(b) Org match inside matched projects.**
1. PIC: `j_project_institution.organization_id` = an OpenAire org pid with scheme `PIC` (an org can carry several, a PIC can sit on
   several orgs).
2. Only for triplets without a PIC match (no PIC in Cordis, or a PIC OpenAire does not know): `lower(trim(legal_name))` equal and
   the normalised country equal and not NULL on both sides. Never name only.

**(c) Relation columns** `cordis_ec_contribution` (from `ec_contribution`), `cordis_type` (`coordinator` / `participant`), on
`project -> organization` rows **that already exist** (join on `source`, `target`), not on the product direction. One value per
(project, org): PIC before name+country, then a known contribution, then the lowest institution id.

**(d) Address columns** for matched orgs only, from **one** institution per org: PIC-matched first, then real coordinates, then
street + city both present, then the highest institution id. `address_street`, `address_postalcode`, `address_city`,
`address_country` (normalised ISO2), `nuts3` (`nuts_level_3`). Empty strings become NULL. Unmatched orgs stay NULL. An org counts as
matched when it matched in (b), whether or not it has a `hasParticipant` relation to that project.

**(e) Coordinates.** If `organization.geolocation IS NULL` (ROR had none) and the chosen institution has real coordinates:
`geolocation = [lat, lon]`, `geolocation_source = 'cordis'`. ROR is never overwritten.

**Coordinate order.** Cordis stores `institution.geolocation` as a JSON array **`[lon, lat]`** (checked against the data: e.g. NL rows
`[5.04, 51.56]`, UK `[-4.13, 53.22]`, and for every NL/DE/BE/DK/PL/SE/FI/EL row the first value is smaller than the second). The
literal `null` (or any array that is not two numbers in the lat/lon range) means missing. core_v4 flips it to `[lat, lng]`, the
order core_v3 uses for ROR.

### Expected yield (Phase 1, to compare against the log lines of a prod run)

| metric | Phase 1 | what the log prints |
|---|---|---|
| Cordis projects matched on grantId | 87,439 / 142,773 (61.2%) | `Cordis projects matched: ... grantId ... DOI fallback` (grantId must reproduce 87,439; the DOI fallback is on top, not measured) |
| triplets matched by PIC | 430,591 (89.7% of 480,133) | `pic:` |
| triplets matched by name + country (Phase 1: for all triplets, 249,755) | 52.0% | `name_country fallback:` (smaller: only PIC-less triplets are tried) |
| triplets matched in total | at least 89.7% | `matched:` |
| triplets also in OpenAire `hasParticipant` (PIC) | 393,063 (81.9%) | `relation rows enriched with Cordis` (about one row per confirmed (project, org) pair; core_v3: ~193K) |
| orgs matched, org level PIC (not limited to matched projects) | 68,125 | `organizations matched` (lower: limited to matched projects) |
| orgs with no ROR coordinates, matched, with address | 59,644 (PIC, org level) | `with street + city` |
| Cordis coordinates available for those | 43,557 of the 59,644 | `geolocation adopted from Cordis` (only those not already in ROR) |

Orgs left with an address but without coordinates are the Mapbox candidates for `enrichment/geolocation.py` (Phase 1: about 16k).

## 5. `--limit N` sample

Writes only to `core_v4_limit.path_duck_staging_limit`. Steps:
1. N projects, ordered by: grantId exists in Cordis, produces at least one work, id (hashes, so a fixed pseudo-random order).
2. Their relations, the works they produce (if the work exists), plus org-only works of the orgs they touch, in id order, at most
   5*N of them, so the trim has something to drop.
3. Relations of the sampled projects and the product-side relations of the sampled works. Orgs = every organization those relations
   point at. So no relation points at a missing project, work or org.
4. Cap = `min(work_cap, max(3*N, produced + N))`: all produced works stay and about N org-only ones (both tiers are exercised).

ROR and Cordis are joined in full (attached, never copied). The full-run tests cover everything else.

## Tests

`ENV=dev uv run python -m pytest src/pipelines/core_v4/test_transformation.py`. They build tiny OpenAire staging, ROR and Cordis
duckdb files in a tmp dir. Covered: tiers, unlinked works dropped, cap ordering, undated last, deterministic tie-break at the cutoff
date (also with reversed insertion order), future dates nulled, relation cascade, country normalisation, rebuild from scratch, ROR
`[lat, lng]`, PIC before name+country, name-only never matches, no match without country, relation columns only on existing
relations, DOI fallback, addresses only on matched orgs, Cordis coordinates only when geolocation was NULL, institution choice, the
limit sample (FK check) and that a limit run never writes to the full path.

## Not done / open

- Address and coordinates come from matches inside matched projects only. An org-level PIC match (any Cordis institution with the
  same PIC, whatever the project) would give more addresses (Phase 1: 68,125 orgs vs. whatever the project-restricted count is).
- Project `pids` other than DOI, and the dropped raw columns (README section 6), are not brought back.
- The "keep all works connected to minorities" rule is not implemented (needs the minorities dump joined to works).
