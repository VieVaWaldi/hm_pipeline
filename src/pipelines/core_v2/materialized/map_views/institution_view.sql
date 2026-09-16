-----------------------------------------------
-- Institution View
-----------------------------------------------

-- One row per institution with aggregated project participations.
-- Only includes institutions that have at least one project.
-- Geolocation filtering (for map display) should be done downstream.

-- v3 changes vs v2:
--   - institution replaced by organization
--   - j_project_institution replaced by relation WHERE sourceType='project' AND targetType='organization'
--   - i.country (text)    -> o."countryCode" (ISO code)
--   - i.type_title (text) -> o."rorTypes" (text[]) — exposed as array, handled downstream
--   - sme                 -> dropped (not in organization table)
--   - jpi.total_cost      -> r.cordis_ec_contribution (nullable)
--   - p.total_cost        -> (p.granted->>'totalCost')::double precision
--   - p.start_date        -> p."startDate"
--   - p.end_date          -> p."endDate"
--   - core_mats.fundingprogramme join -> replaced by p."frameworkProgrammes" (direct on project row)

CREATE MATERIALIZED VIEW core_mats.institution_view AS
WITH institution_projects AS (
  -- Get all (institution, project) pairs with project details
  SELECT
    o.id                                                        AS institution_id,
    o.geolocation,
    o."countryCode"                                             AS country_code,
    o."rorTypes"                                                AS type,
    p.id                                                        AS project_id,
    p."startDate"                                               AS start_date,
    p."endDate"                                                 AS end_date,
    (p.granted->>'totalCost')::double precision                 AS project_total_cost,
    r."cordis_ec_contribution"                                  AS participation_cost,
    p."frameworkProgrammes"                                     AS framework_programmes
  FROM organization o
  INNER JOIN relation r
      ON  r.target       = o.id
      AND r."sourceType" = 'project'
      AND r."targetType" = 'organization'
  INNER JOIN project p ON p.id = r.source
)
SELECT
  institution_id,
  geolocation,
  country_code,
  type,
  jsonb_agg(
    jsonb_build_object(
      'id',                   project_id,
      'start',                start_date,
      'end',                  end_date,
      'total_cost',           project_total_cost,
      'participation_cost',   participation_cost,
      'framework_programmes', framework_programmes
    )
  ) AS projects
FROM institution_projects
GROUP BY institution_id, geolocation, country_code, type
ORDER BY institution_id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

-- Takes 0.4 seconds in v2; expect similar in v3
SELECT * FROM core_mats.institution_view
WHERE institution_id = 88024035;

SELECT * FROM organization
WHERE "legalName" ILIKE '%Friedrich Schiller%';

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------



-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes 60 MB
SELECT pg_size_pretty(pg_total_relation_size('core_mats.institution_view'));

-- Data size without indexes 49 MB
SELECT pg_size_pretty(pg_relation_size('core_mats.institution_view'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.institution_view;
