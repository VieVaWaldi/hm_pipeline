-- organisations index docs: every organization column + rollups (from projects / works). JSON columns as text.
COPY (
WITH pc AS (
    SELECT po.oid, count(*)::INTEGER AS project_count,
           count(*) FILTER (p.is_ch)::INTEGER AS dch_project_count,
           sum(m.funded_eur_per_org) AS total_funding_eur          -- D13 equal split (decided); EUR via ECB table (D14)
    FROM po JOIN project p ON p.id = po.pid LEFT JOIN p_money m ON m.pid = po.pid
    GROUP BY po.oid
), wc AS (
    SELECT target AS oid, count(*)::INTEGER AS work_count
    FROM relation WHERE sourceType = 'product' AND targetType = 'organization' GROUP BY target
)
SELECT o.id::VARCHAR AS id, o.openaireId, o.legalName, o.legalShortName, o.websiteUrl, o.alternativeNames,
       o.countryCode, o.rorId, o.wikiId, o.pids, o.rorStatus, o.rorEstablished, o.rorTypes,
       o.rorLocations::VARCHAR AS rorLocations, o.rorRelationships::VARCHAR AS rorRelationships,
       CASE WHEN o.geolocation IS NOT NULL THEN {'lat': o.geolocation[1], 'lon': o.geolocation[2]} END AS geo,
       o.geolocation_source, o.address_street, o.address_postalcode, o.address_city, o.address_country, o.nuts3, o.region,
       coalesce(pc.project_count, 0) AS project_count, coalesce(wc.work_count, 0) AS work_count,
       coalesce(pc.dch_project_count, 0) AS dch_project_count, coalesce(pc.dch_project_count, 0) > 0 AS has_dch_project,
       coalesce(pc.total_funding_eur, 0) AS total_funding_eur
FROM organization o LEFT JOIN pc ON pc.oid = o.id LEFT JOIN wc ON wc.oid = o.id
) TO '__OUT__/organisations.parquet' (FORMAT parquet, COMPRESSION zstd);
