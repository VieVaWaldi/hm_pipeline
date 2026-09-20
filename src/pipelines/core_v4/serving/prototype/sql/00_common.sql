-- Shared temp tables, built once per export run. Read-only source DB, everything here is TEMP.
-- D13 (decided): equal split of the project amount over its organizations. D14 (decided): ECB table below, see SERVING_DESIGN.md 3b.

-- D14 (decided): ECB euro reference rates of 2026-09-18, units of currency per 1 EUR (SERVING_DESIGN.md section 3b).
-- HRK (fixed 7.53450) and BGN (fixed 1.95583) are not in the ECB list; the design doc says to verify these two and spot-check 2-3 rates
-- before the real export. Currency not in this table -> funded_amount_eur NULL (never guess).
CREATE TEMP TABLE fx AS
SELECT * FROM (VALUES ('EUR', 1.0), ('USD', 1.1460), ('GBP', 0.85880), ('CHF', 0.9462), ('SEK', 11.2915), ('NOK', 10.8095),
                      ('DKK', 7.4754), ('PLN', 4.3635), ('CZK', 24.339), ('HUF', 364.28), ('RON', 5.2647), ('ISK', 139.40),
                      ('TRY', 55.9077), ('HRK', 7.53450), ('BGN', 1.95583))
       AS t(currency, units_per_eur);

-- project -> organization, one row per pair; coordinator flag from the Cordis type (known for ~7% of relations only).
CREATE TEMP TABLE po AS
SELECT r.source AS pid, r.target AS oid,
       bool_or(r.cordis_type = 'coordinator') AS is_coord,
       any_value(o.legalName) AS legalName, any_value(o.region) AS region, any_value(o.countryCode) AS countryCode
FROM relation r JOIN organization o ON o.id = r.target
WHERE r.sourceType = 'project' AND r.targetType = 'organization'
GROUP BY r.source, r.target;

-- per-project rollup over its organizations (coordinator first, then by id for a stable order)
CREATE TEMP TABLE p_orgs AS
SELECT pid,
       list(oid::VARCHAR ORDER BY is_coord DESC, oid)                           AS org_ids,
       list(legalName ORDER BY is_coord DESC, oid) FILTER (legalName IS NOT NULL) AS org_names,
       list_sort(list_distinct(list(region) FILTER (region IS NOT NULL)))        AS org_regions,
       list_sort(list_distinct(list(countryCode) FILTER (countryCode IS NOT NULL))) AS org_countries,
       any_value(oid::VARCHAR) FILTER (is_coord)                                AS coordinator_id,
       count(*)::INTEGER                                                        AS org_count
FROM po GROUP BY pid;

CREATE TEMP TABLE p_works AS
SELECT source AS pid, count(*)::INTEGER AS work_count
FROM relation WHERE sourceType = 'project' AND targetType = 'product' GROUP BY source;

-- per-project money: raw, EUR (ECB table, D14) and the equal share per participating organization (D13, decided)
CREATE TEMP TABLE p_money AS
SELECT p.id AS pid, p.granted.currency AS currency, p.granted.fundedAmount AS funded_amount,
       p.granted.fundedAmount / fx.units_per_eur AS funded_amount_eur,
       (p.granted.fundedAmount / fx.units_per_eur) / nullif(po.org_count, 0) AS funded_eur_per_org
FROM project p LEFT JOIN fx ON fx.currency = p.granted.currency LEFT JOIN p_orgs po ON po.pid = p.id;
