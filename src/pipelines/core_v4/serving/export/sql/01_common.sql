-- Shared TEMP tables (built once per export run, projects/organisations/minorities/grants read them). Needs 00_macros.sql first.

-- funders per project = distinct fundings[].shortName (D20: 103 values, never NULL; level 1 of the stream id is NOT usable alone)
CREATE OR REPLACE TEMP TABLE p_fund AS
SELECT p.id AS pid,
       list_sort(list_distinct(list_filter(list_transform(coalesce(p.fundings, []), lambda f: f.shortName),
                                           lambda x: x IS NOT NULL AND x <> ''))) AS funders
FROM project p;

-- per-project money (D14/D21): currency normalised, EUR via the ECB table, 0 amounts -> NULL, > 1e9 EUR (junk rows) -> NULL
CREATE OR REPLACE TEMP TABLE p_money AS
WITH c AS (
    SELECT p.id AS pid, hm_cur(p.granted.currency, pf.funders) AS currency, p.granted.fundedAmount AS amt
    FROM project p JOIN p_fund pf ON pf.pid = p.id
)
SELECT c.pid, c.currency, nullif(c.amt, 0) AS funded_amount,
       CASE WHEN c.amt > 0 AND c.amt / fx.units_per_eur <= 1e9 THEN c.amt / fx.units_per_eur END AS funded_amount_eur
FROM c LEFT JOIN fx ON fx.currency = c.currency;

-- project -> organization, one row per pair; coordinator from the Cordis type (EC only, ~7% of relations); region Unknown bucket (D26)
CREATE OR REPLACE TEMP TABLE po AS
SELECT r.source AS pid, r.target AS oid,
       bool_or(lower(r.cordis_type) = 'coordinator') AS is_coord,
       any_value(hm_clean(o.legalName)) AS legalName, coalesce(any_value(o.region), 'Unknown') AS region,
       any_value(o.countryCode) AS countryCode
FROM relation r JOIN organization o ON o.id = r.target
WHERE r.sourceType = 'project' AND r.targetType = 'organization'
GROUP BY r.source, r.target;

-- per-project rollup over its organizations (coordinators first, then by id for a stable order). Projects without an org: no row (8.1%).
CREATE OR REPLACE TEMP TABLE p_orgs AS
SELECT pid,
       list(oid::VARCHAR ORDER BY is_coord DESC, oid)                                AS org_ids,
       list(legalName ORDER BY is_coord DESC, oid) FILTER (legalName IS NOT NULL)     AS org_names,
       list_sort(list_distinct(list(region)))                                        AS org_regions,
       list_sort(list_distinct(list(countryCode) FILTER (countryCode IS NOT NULL)))   AS org_countries,
       list(oid::VARCHAR ORDER BY oid) FILTER (is_coord)                              AS coordinator_ids,   -- D30: EC only, can be 2
       count(*)::INTEGER                                                             AS org_count
FROM po GROUP BY pid;

CREATE OR REPLACE TEMP TABLE p_works AS
SELECT source AS pid, count(DISTINCT target)::INTEGER AS work_count
FROM relation WHERE sourceType = 'project' AND targetType = 'product' GROUP BY source;

-- one topic per project at most (8,795 have none, D29); arg_max keeps this robust if a project ever got two
CREATE OR REPLACE TEMP TABLE p_topic AS
SELECT source_id AS pid, arg_max(topic_id, score) AS topic_id
FROM relation_topic WHERE type = 'project' GROUP BY source_id;

-- D13 (decided): equal split of the EUR amount over the project's organizations; NULL when the project has no org or no amount
CREATE OR REPLACE TEMP TABLE p_share AS
SELECT m.pid, m.funded_amount_eur / nullif(o.org_count, 0) AS funded_eur_per_org
FROM p_money m LEFT JOIN p_orgs o ON o.pid = m.pid;

-- D20: one row per (project, funding entry). Streams without a stream id get a per-funder pseudo id 'NONE::<funder>' so a grant
-- filter/facet works for every project (NSF/NIH/NWO/... have no stream ids: 318k entries). programme = level 2, action = level 3,
-- both NULL for the pseudo streams. Stream ids are text-cleaned (156 ids carry &amp;), the same value goes into grants.id.
CREATE OR REPLACE TEMP TABLE p_fundings AS
SELECT pid, is_ch, n_fund, coalesce(sid, 'NONE::' || shortName) AS stream_id,
       CASE WHEN sid IS NOT NULL THEN nullif(split_part(sid, '::', 2), '') END AS programme,
       CASE WHEN sid IS NOT NULL THEN nullif(split_part(sid, '::', 3), '') END AS action,
       shortName AS funder, funder_name, jurisdiction, description
FROM (SELECT pid, is_ch, n_fund, f.shortName AS shortName, hm_clean(f.fundingStream.id) AS sid, hm_clean(f.name) AS funder_name,
             f.jurisdiction AS jurisdiction, hm_clean(f.fundingStream.description) AS description
      FROM (SELECT p.id AS pid, p.is_ch, len(p.fundings) AS n_fund, unnest(p.fundings) AS f FROM project p));

CREATE OR REPLACE TEMP TABLE p_streams AS
SELECT pid,
       list_sort(list_distinct(list(stream_id)))                                               AS stream_ids,
       list_sort(list_distinct(list(programme) FILTER (programme IS NOT NULL)))                AS programmes,
       list_sort(list_distinct(list(funder_name) FILTER (funder_name IS NOT NULL)))            AS funder_names
FROM p_fundings GROUP BY pid;
