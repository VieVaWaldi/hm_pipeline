-- organisations index docs: every organization column (names text-cleaned) + rollups from projects / works.
-- D19: name_key for duplicate institutions. D26: region 'Unknown', rorTypes ['unknown'] buckets. D33: rank_* fields (rank_feature, must be > 0 -> NULL).
COPY (
WITH pc AS (
    SELECT po.oid, count(*)::INTEGER AS project_count,
           (count(*) FILTER (p.is_ch))::INTEGER AS dch_project_count,
           sum(sh.funded_eur_per_org) AS total_funding_eur         -- D13 equal split; EUR via ECB table; junk rows > 1e9 EUR already NULL
    FROM po JOIN project p ON p.id = po.pid LEFT JOIN p_share sh ON sh.pid = po.pid
    GROUP BY po.oid
), wc AS (
    SELECT target AS oid, count(*)::INTEGER AS work_count
    FROM relation WHERE sourceType = 'product' AND targetType = 'organization' GROUP BY target
)
SELECT o.id::VARCHAR AS id, o.openaireId, hm_clean(o.legalName) AS legalName, hm_clean(o.legalShortName) AS legalShortName, o.websiteUrl,
       hm_clean_list(coalesce(o.alternativeNames, [])) AS alternativeNames,
       o.countryCode, o.rorId, o.wikiId, o.pids, o.rorStatus, o.rorEstablished,
       CASE WHEN len(coalesce(o.rorTypes, [])) = 0 THEN ['unknown'] ELSE o.rorTypes END AS rorTypes,
       o.rorLocations::VARCHAR AS rorLocations, o.rorRelationships::VARCHAR AS rorRelationships,
       CASE WHEN o.geolocation IS NOT NULL THEN {'lat': o.geolocation[1], 'lon': o.geolocation[2]} END AS geo,
       o.geolocation_source, hm_clean(o.address_street) AS address_street, hm_clean(o.address_postalcode) AS address_postalcode,
       hm_clean(o.address_city) AS address_city, o.address_country, o.nuts3,
       coalesce(o.region, 'Unknown') AS region,
       hm_name_key(o.legalName, o.countryCode) AS name_key,
       coalesce(pc.project_count, 0) AS project_count, coalesce(wc.work_count, 0) AS work_count,
       coalesce(pc.dch_project_count, 0) AS dch_project_count, coalesce(pc.dch_project_count, 0) > 0 AS has_dch_project,
       coalesce(pc.total_funding_eur, 0) AS total_funding_eur,
       nullif(coalesce(pc.project_count, 0), 0) AS rank_projects,
       nullif(coalesce(wc.work_count, 0), 0) AS rank_works,
       CASE WHEN coalesce(pc.total_funding_eur, 0) > 0 THEN pc.total_funding_eur END AS rank_funding
FROM organization o LEFT JOIN pc ON pc.oid = o.id LEFT JOIN wc ON wc.oid = o.id
ORDER BY o.id
) TO '{OUT}/organisations/organisations.parquet.tmp' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000);
