-----------------------------------------------
-- Project Data View
-----------------------------------------------

-----------------------------------------------
-- Filters

-- startYear, endYear
-- ft search
-- topic           -> relation_topic
-- FP              -> project.frameworkProgrammes (direct array, no junction needed)

-----------------------------------------------
-- View
-----------------------------------------------

-- Note: "startDate" / "endDate" require quotes because the DuckDB export preserves
-- camelCase column names. PostgreSQL folds unquoted identifiers to lowercase.

CREATE VIEW table_view_project AS
SELECT
    p.id,
    -- p.doi,  -- not in v3 project table
    p.title,
    p.acronym,
    p."startDate"                                       AS start_date,
    p."endDate"                                         AS end_date,
    p.summary,
    (p.granted->>'totalCost')::double precision         AS total_cost,
    (p.granted->>'fundedAmount')::double precision      AS funded_amount,
    p.granted->>'currency'                              AS currency,
    p.keywords,
    p.is_ch,
    p.pred,
    p."frameworkProgrammes"                             AS framework_programmes,
    t.id                                                AS topic_id,
    t.subfield_id,
    t.field_id,
    t.domain_id
FROM project p
LEFT JOIN relation_topic rt ON rt.type = 'project' AND rt.source_id = p.id
LEFT JOIN topic t ON rt.topic_id = t.id;

-----------------------------------------------
-- Select
-----------------------------------------------

SELECT count(*)
FROM table_view_project;
-- WHERE lower(acronym) = 'digicher'

SELECT topic_name, id, subfield_id FROM topic
WHERE id = 14346;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP VIEW table_view_project;
