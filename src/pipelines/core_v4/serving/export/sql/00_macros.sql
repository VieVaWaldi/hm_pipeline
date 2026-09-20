-- Shared macros + small tables for the final export. Everything here is TEMP (the source DB is opened read-only).
-- Decisions: see ../../SERVING_DESIGN.md (D-numbers in the comments).

-- ---- D14: ECB euro reference rates of 2026-09-18, units of currency per 1 EUR (spot-checked against the raw ECB xml on 2026-09-20).
-- HRK (fixed 7.53450, Croatia joined the euro 2023) and BGN (fixed peg 1.95583) are not in the ECB list.
-- A currency not in this table -> funded_amount_eur NULL (never guess).
CREATE OR REPLACE TEMP TABLE fx AS
SELECT * FROM (VALUES
  ('EUR', 1.0), ('USD', 1.1460), ('JPY', 180.94), ('CZK', 24.339), ('DKK', 7.4754), ('GBP', 0.85880), ('HUF', 364.28),
  ('PLN', 4.3635), ('RON', 5.2647), ('SEK', 11.2915), ('CHF', 0.9462), ('ISK', 139.40), ('NOK', 10.8095), ('TRY', 55.9077),
  ('AUD', 1.6095), ('BRL', 5.8857), ('CAD', 1.6056), ('CNY', 7.6755), ('HKD', 8.9903), ('IDR', 20424.81), ('ILS', 3.4812),
  ('INR', 109.8755), ('KRW', 1590.76), ('MXN', 19.6855), ('MYR', 4.6763), ('NZD', 2.0068), ('PHP', 71.972), ('SGD', 1.4651),
  ('THB', 38.225), ('ZAR', 18.6482), ('HRK', 7.53450), ('BGN', 1.95583)) AS t(currency, units_per_eur);

-- D21: currency normalisation. NULL currency of SNSF (Swiss, 92k projects with an amount but no currency) -> CHF;
-- '$' is USD except for NHMRC (Australian) -> AUD; everything else as is.
CREATE OR REPLACE TEMP MACRO hm_cur(cur, funders) AS
  CASE WHEN cur IS NULL AND list_contains(coalesce(funders, []), 'SNSF') THEN 'CHF'
       WHEN cur = '$' AND list_contains(coalesce(funders, []), 'NHMRC') THEN 'AUD'
       WHEN cur = '$' THEN 'USD'
       ELSE cur END;

-- D27: text cleaning. Strip HTML tags (real tags only), unescape the common entities, collapse whitespace, empty -> NULL.
CREATE OR REPLACE TEMP MACRO hm_clean(s) AS
  nullif(trim(regexp_replace(
    replace(replace(replace(replace(replace(replace(
      regexp_replace(s, '</?[a-zA-Z][^>]*>', ' ', 'g'),
      '&amp;', '&'), '&lt;', '<'), '&gt;', '>'), '&quot;', '"'), '&#39;', ''''), '&apos;', ''''),
    '\s+', ' ', 'g')), '');

-- D19: name_key = normalised legalName + '|' + countryCode; used by the api to drop self-pairs / group duplicate institutions.
CREATE OR REPLACE TEMP MACRO hm_name_key(name, cc) AS
  trim(regexp_replace(lower(strip_accents(replace(coalesce(name, ''), '&amp;', '&'))), '[^\p{L}\p{N}]+', ' ', 'g'))
  || '|' || lower(coalesce(cc, ''));

-- D28: year only inside 1950..2040 (the data has 1900..2125 junk); startDate/endDate stay raw for the overview.
CREATE OR REPLACE TEMP MACRO hm_year(d) AS CASE WHEN year(d) BETWEEN 1950 AND 2040 THEN year(d)::INTEGER END;

-- D23: works language. 3-letter ISO 639-2 codes, 30 "pairs" like fra/fre, esl/spa. Take the first part, map the ISO B-codes and
-- the non-ISO 'esl' to the T-code, 'und' -> NULL (36% of works: unknown, not a language).
CREATE OR REPLACE TEMP MACRO hm_lang(code) AS
  CASE WHEN code IS NULL OR lower(split_part(code, '/', 1)) IN ('und', '') THEN NULL
       ELSE coalesce(MAP {'alb': 'sqi', 'arm': 'hye', 'baq': 'eus', 'bur': 'mya', 'chi': 'zho', 'cze': 'ces', 'dut': 'nld', 'fre': 'fra',
                          'geo': 'kat', 'ger': 'deu', 'gre': 'ell', 'ice': 'isl', 'mac': 'mkd', 'mao': 'mri', 'may': 'msa', 'per': 'fas',
                          'rum': 'ron', 'slo': 'slk', 'tib': 'bod', 'wel': 'cym', 'esl': 'spa'}[lower(split_part(code, '/', 1))],
                     lower(split_part(code, '/', 1))) END;

-- ---- works: url extraction rules (D5/D25, verified on the cluster with 0 mismatches, results section G6). `&amp;` unescaped.
CREATE OR REPLACE TEMP MACRO wk_is_pdf_strict(u) AS regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$');
CREATE OR REPLACE TEMP MACRO wk_is_pdf_loose(u) AS (regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$') OR contains(lower(u), '/pdf'));
CREATE OR REPLACE TEMP MACRO wk_all_urls(ins) AS flatten(list_transform(ins, lambda i: coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_open_urls(ins) AS
  flatten(list_transform(list_filter(ins, lambda i: i.accessRight.label = 'OPEN'), lambda i: coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_doi_work(pids) AS
  regexp_replace(list_filter(pids, lambda p: lower(p.scheme) = 'doi')[1].value, '^https?://(dx\.)?doi\.org/', '');
CREATE OR REPLACE TEMP MACRO wk_doi_any(pids, ins) AS coalesce(wk_doi_work(pids),
  regexp_replace(list_filter(flatten(list_transform(ins, lambda i: coalesce(i.pids, []))), lambda p: lower(p.scheme) = 'doi')[1].value,
                 '^https?://(dx\.)?doi\.org/', ''));
-- pdf_url = first url of an OPEN instance ending in .pdf, else first OPEN url containing /pdf, else first .pdf of any instance, else first /pdf url
CREATE OR REPLACE TEMP MACRO wk_pdf_url(ins) AS replace(coalesce(
  list_filter(wk_open_urls(ins), lambda u: wk_is_pdf_strict(u))[1],
  list_filter(wk_open_urls(ins), lambda u: wk_is_pdf_loose(u))[1],
  list_filter(wk_all_urls(ins),  lambda u: wk_is_pdf_strict(u))[1],
  list_filter(wk_all_urls(ins),  lambda u: wk_is_pdf_loose(u))[1]), '&amp;', '&');
-- landing_url = https://doi.org/<doi>, else first OPEN url, else first url
CREATE OR REPLACE TEMP MACRO wk_landing_url(pids, ins) AS
  replace(coalesce('https://doi.org/' || wk_doi_any(pids, ins), wk_open_urls(ins)[1], wk_all_urls(ins)[1]), '&amp;', '&');
