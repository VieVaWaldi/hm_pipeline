-----------------------------------------------
-- Project Data Materialized View
-----------------------------------------------

-- One row per project. The best-scoring topic (from relation_topic) is chosen
-- via DISTINCT ON so that topic/subfield/field/domain filters work without
-- producing duplicate projects in the list view.

SET maintenance_work_mem = '8GB';
SET max_parallel_maintenance_workers = 8;

-----------------------------------------------
-- Materialized View
-----------------------------------------------

CREATE MATERIALIZED VIEW table_view_project AS
SELECT DISTINCT ON (p.id)
    p.id,
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
LEFT JOIN topic t ON rt.topic_id = t.id
ORDER BY p.id, rt.score DESC NULLS LAST;

-----------------------------------------------
-- Indexes
-----------------------------------------------

-- PK / unique lookup
CREATE UNIQUE INDEX idx_tvp_id
    ON table_view_project (id);

-- Default sort (title ASC, the most common case)
CREATE INDEX idx_tvp_title
    ON table_view_project (title);

-- Date sorts / year-range filters
CREATE INDEX idx_tvp_start_date
    ON table_view_project (start_date);
CREATE INDEX idx_tvp_end_date
    ON table_view_project (end_date);

-- FTS — expression must match exactly what the API query uses:
--   setweight(to_tsvector('english', COALESCE(title,'')),   'A') ||
--   setweight(to_tsvector('english', COALESCE(summary, '')), 'B')
CREATE INDEX idx_tvp_fts
    ON table_view_project USING GIN (
        (  setweight(to_tsvector('english', COALESCE(title,   '')), 'A')
        || setweight(to_tsvector('english', COALESCE(summary, '')), 'B')
        )
    );

-- Topic / taxonomy filters
CREATE INDEX idx_tvp_topic_id
    ON table_view_project (topic_id);
CREATE INDEX idx_tvp_subfield_id
    ON table_view_project (subfield_id);
CREATE INDEX idx_tvp_field_id
    ON table_view_project (field_id);
CREATE INDEX idx_tvp_domain_id
    ON table_view_project (domain_id);

-- Framework programmes array overlap filter
CREATE INDEX idx_tvp_framework_programmes
    ON table_view_project USING GIN (framework_programmes);

-- CH / pred filter
CREATE INDEX idx_tvp_pred
    ON table_view_project (pred);

-- total_cost (used for relevance-sort fallback)
CREATE INDEX idx_tvp_total_cost
    ON table_view_project (total_cost DESC NULLS LAST);

-----------------------------------------------
-- Verify
-----------------------------------------------

SELECT count(*) FROM table_view_project;
-- Expected: one row per project (same as SELECT count(*) FROM project).

-- SELECT topic_name, t.id, t.subfield_id
-- FROM table_view_project tvp
-- JOIN topic t ON tvp.topic_id = t.id
-- WHERE lower(tvp.acronym) = 'digicher';

-----------------------------------------------
-- DROP (run first when rebuilding)          --
-----------------------------------------------

-- DROP MATERIALIZED VIEW IF EXISTS table_view_project;