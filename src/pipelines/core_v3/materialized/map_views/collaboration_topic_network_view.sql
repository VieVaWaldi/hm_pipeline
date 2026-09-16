-----------------------------------------------
-- Collaboration Network for topics         --
-----------------------------------------------

-- v3 changes vs v2:
--   - j_project_institution replaced by relation WHERE sourceType='project' AND targetType='organization'
--   - institution replaced by organization
--   - i.country (text)    -> o."countryCode" (ISO code) — values are now country codes, not names
--   - sme                 -> dropped
--   - j_project_topicoa   -> relation_topic WHERE type = 'project'
--   - topicoa             -> topic
--   - framework_programmes -> p."frameworkProgrammes" direct array (no junction join needed)

SET maintenance_work_mem = '8GB';
SET max_parallel_maintenance_workers = 8;

CREATE MATERIALIZED VIEW core_mats.collaboration_by_topic AS
SELECT DISTINCT
    r1.target            AS a_institution_id,
    r2.target            AS b_institution_id,
    o1.geolocation       AS a_geolocation,
    o2.geolocation       AS b_geolocation,
    o1."countryCode"     AS a_country,
    o2."countryCode"     AS b_country,
    r1.source            AS project_id,
    p."startDate"        AS start_date,
    p."endDate"          AS end_date,
    rt.topic_id,
    t.subfield_id,
    t.field_id,
    p."frameworkProgrammes" AS framework_programmes
FROM
    relation r1
    JOIN relation r2
        ON  r1.source       = r2.source
        AND r1."sourceType" = 'project'
        AND r2."sourceType" = 'project'
        AND r1."targetType" = 'organization'
        AND r2."targetType" = 'organization'
    JOIN organization o1   ON o1.id = r1.target
    JOIN organization o2   ON o2.id = r2.target
    JOIN relation_topic rt ON rt.type = 'project' AND rt.source_id = r1.source
    JOIN topic t           ON t.id = rt.topic_id
    JOIN project p         ON p.id = r1.source
WHERE
    r1.target < r2.target
    AND o1.geolocation IS NOT NULL
    AND o2.geolocation IS NOT NULL;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.collaboration_by_topic;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

CREATE INDEX idx_collaboration_by_topic_topic_id
    ON core_mats.collaboration_by_topic (topic_id);

CREATE INDEX idx_collaboration_by_topic_subfield_id
    ON core_mats.collaboration_by_topic (subfield_id);

CREATE INDEX idx_collaboration_by_topic_field_id
    ON core_mats.collaboration_by_topic (field_id);

CREATE INDEX idx_collaboration_by_topic_start_date
    ON core_mats.collaboration_by_topic (start_date);

CREATE INDEX idx_collaboration_by_topic_end_date
    ON core_mats.collaboration_by_topic (end_date);

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

SELECT pg_size_pretty(pg_total_relation_size('core_mats.collaboration_by_topic'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.collaboration_by_topic;
