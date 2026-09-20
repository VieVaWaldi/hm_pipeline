-- Shared temp tables, built once per export run. Read-only source DB, everything here is TEMP.
-- Placeholders (D13/D14): EUR conversion table and equal-split funding attribution. NOT final decisions.

-- PLACEHOLDER (D14): fixed rates, only to make the pipeline testable. Unknown currency -> NULL (not summed).
CREATE TEMP TABLE fx AS
SELECT * FROM (VALUES ('EUR', 1.0), ('GBP', 1.15), ('USD', 0.92), ('HRK', 0.133), ('CHF', 1.05), ('SEK', 0.09),
                      ('DKK', 0.134), ('NOK', 0.085), ('PLN', 0.23), ('CZK', 0.04), ('HUF', 0.0025), ('RON', 0.2))
       AS t(currency, eur_per_unit);

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

-- per-project money: raw, EUR (placeholder rates) and the equal share per participating organization (placeholder D13)
CREATE TEMP TABLE p_money AS
SELECT p.id AS pid, p.granted.currency AS currency, p.granted.fundedAmount AS funded_amount,
       p.granted.fundedAmount * fx.eur_per_unit AS funded_amount_eur,
       (p.granted.fundedAmount * fx.eur_per_unit) / nullif(po.org_count, 0) AS funded_eur_per_org
FROM project p LEFT JOIN fx ON fx.currency = p.granted.currency LEFT JOIN p_orgs po ON po.pid = p.id;
