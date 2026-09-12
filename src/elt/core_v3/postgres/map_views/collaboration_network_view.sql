-----------------------------------------------
-- Collaboration Network View               --
-----------------------------------------------

-- Note: Same project can appear multiple times per collaborator pair when an
-- organization has multiple relation rows in the same project (e.g. different
-- cordis_type). This is correct data, not a bug.
-- React keys must account for this by using array index or combined_institution_cost.

-- v3 changes vs v2:
--   - j_project_institution replaced by relation WHERE sourceType='project' AND targetType='organization'
--   - institution replaced by organization
--   - type_title (text)   -> rorTypes (text[])  exposed as array, handled downstream
--   - sme                 -> dropped (not in organization table)
--   - pi.total_cost       -> r.cordis_ec_contribution (nullable — org may have no CORDIS data)
--   - p.total_cost        -> (p.granted->>'totalCost')::double precision
--   - framework_programmes -> p."frameworkProgrammes" direct array (no junction join needed)

-- Query this to get all collaborators with the respective projects from one institution
CREATE MATERIALIZED VIEW core_mats.collaboration_network_view AS
SELECT
    r1.target                                               AS institution_id,
    r2.target                                               AS collaborator_id,
    o2.geolocation                                          AS collaborator_geolocation,
    o2."countryCode"                                        AS collaborator_country_code,
    o2."rorTypes"                                           AS collaborator_type,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'project_id',               r1.source,
            'total_cost',               (p.granted->>'totalCost')::double precision,
            'combined_institution_cost', (COALESCE(r1."cordis_ec_contribution", 0)
                                        + COALESCE(r2."cordis_ec_contribution", 0)),
            'start_date',               p."startDate",
            'end_date',                 p."endDate",
            'framework_programmes',     p."frameworkProgrammes"
        ) ORDER BY r1.source
    ) AS projects
FROM
    relation r1
    JOIN relation r2
        ON  r1.source      = r2.source
        AND r1."sourceType" = 'project'
        AND r2."sourceType" = 'project'
        AND r1."targetType" = 'organization'
        AND r2."targetType" = 'organization'
    JOIN organization o1 ON o1.id = r1.target
    JOIN organization o2 ON o2.id = r2.target
    JOIN project p       ON p.id  = r1.source
WHERE
    r1.target != r2.target
    AND o1.geolocation IS NOT NULL
    AND o2.geolocation IS NOT NULL
GROUP BY
    r1.target, r2.target, o2.geolocation, o2."countryCode", o2."rorTypes";

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.collaboration_network_view;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

CREATE INDEX idx_collaboration_network_institution
    ON core_mats.collaboration_network_view (institution_id);

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.collaboration_network_view;
