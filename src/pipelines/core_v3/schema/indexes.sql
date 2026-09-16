-- =============================================================================
-- Core v3 Indexes
-- =============================================================================

-- Note on camelCase columns:
--   Column names exported from DuckDB preserve their case (e.g. "startDate").
--   PostgreSQL folds unquoted identifiers to lowercase, so these columns MUST
--   be referenced with double quotes: p."startDate". The indexes below quote
--   them for the same reason.

SET maintenance_work_mem = '20GB';
SET max_parallel_maintenance_workers = 8;

-- =============================================================================
-- project
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | project — start', clock_timestamp();

RAISE NOTICE '% | idx_project_pk', clock_timestamp();
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_pk
    ON project (id);

RAISE NOTICE '% | idx_project_start_date', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_start_date
    ON project ("startDate");

RAISE NOTICE '% | idx_project_end_date', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_end_date
    ON project ("endDate");

RAISE NOTICE '% | idx_project_start_year (functional)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_start_year
    ON project (EXTRACT(year FROM "startDate"));

RAISE NOTICE '% | idx_project_end_year (functional)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_end_year
    ON project (EXTRACT(year FROM "endDate"));

RAISE NOTICE '% | idx_project_is_ch (partial)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_is_ch
    ON project (is_ch) WHERE is_ch = true;

RAISE NOTICE '% | idx_project_pred', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_pred
    ON project (pred);

RAISE NOTICE '% | idx_project_call_identifier', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_call_identifier
    ON project ("callIdentifier");

RAISE NOTICE '% | idx_project_framework_programmes (GIN array)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_framework_programmes
    ON project USING GIN ("frameworkProgrammes");

RAISE NOTICE '% | idx_project_title_fts (GIN)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_title_fts
    ON project USING GIN (to_tsvector('english', COALESCE(title, '')));

RAISE NOTICE '% | idx_project_summary_fts (GIN)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_summary_fts
    ON project USING GIN (to_tsvector('english', COALESCE(summary, '')));

RAISE NOTICE '% | idx_project_combined_fts (GIN weighted — slowest)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_project_combined_fts
    ON project USING GIN (
        (  setweight(to_tsvector('english', COALESCE(title,   '')), 'A')
        || setweight(to_tsvector('english', COALESCE(summary, '')), 'B')
        )
    );

RAISE NOTICE '% | project — done', clock_timestamp();

END $$;


-- =============================================================================
-- organization
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | organization — start', clock_timestamp();

RAISE NOTICE '% | idx_organization_pk', clock_timestamp();
CREATE UNIQUE INDEX IF NOT EXISTS idx_organization_pk
    ON organization (id);

RAISE NOTICE '% | idx_organization_ror_id', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_ror_id
    ON organization ("rorId");

RAISE NOTICE '% | idx_organization_country_code', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_country_code
    ON organization ("countryCode");

RAISE NOTICE '% | idx_organization_legal_name', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_legal_name
    ON organization ("legalName");

RAISE NOTICE '% | idx_organization_legal_short_name', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_legal_short_name
    ON organization ("legalShortName");

RAISE NOTICE '% | idx_organization_alternative_names (GIN array)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_alternative_names
    ON organization USING GIN ("alternativeNames");

RAISE NOTICE '% | idx_organization_ror_types (GIN array)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_ror_types
    ON organization USING GIN ("rorTypes");

RAISE NOTICE '% | idx_organization_geolocation_notnull (partial)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_geolocation_notnull
    ON organization (id) WHERE geolocation IS NOT NULL;

RAISE NOTICE '% | idx_organization_legal_name_fts (GIN)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_legal_name_fts
    ON organization USING GIN (to_tsvector('english', COALESCE("legalName", '')));

RAISE NOTICE '% | idx_organization_ror_locations (GIN jsonb cast)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_organization_ror_locations
    ON organization USING GIN (("rorLocations"::text::jsonb));

RAISE NOTICE '% | organization — done', clock_timestamp();

END $$;


-- =============================================================================
-- work
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | work — start', clock_timestamp();

RAISE NOTICE '% | idx_work_pk', clock_timestamp();
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_pk
    ON work (id);

RAISE NOTICE '% | idx_work_publication_date', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_publication_date
    ON work ("publicationDate");

RAISE NOTICE '% | idx_work_publication_year (functional)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_publication_year
    ON work (EXTRACT(year FROM "publicationDate"));

RAISE NOTICE '% | idx_work_citation_count', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_citation_count
    ON work ("citationCount");

RAISE NOTICE '% | idx_work_open_access_color', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_open_access_color
    ON work ("openAccessColor");

RAISE NOTICE '% | idx_work_pids (GIN jsonb cast)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_pids
    ON work USING GIN ((pids::jsonb));

RAISE NOTICE '% | idx_work_title_fts (GIN)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_work_title_fts
    ON work USING GIN (to_tsvector('english', COALESCE(title, '')));

RAISE NOTICE '% | idx_work_combined_fts (GIN weighted — slowest)', clock_timestamp();
-- descriptions[1] = first (main) description element; array_to_string is not IMMUTABLE in PG
CREATE INDEX IF NOT EXISTS idx_work_combined_fts
    ON work USING GIN (
        (  setweight(to_tsvector('english', COALESCE(title,            '')), 'A')
        || setweight(to_tsvector('english', COALESCE(descriptions[1], '')), 'B')
        )
    );

RAISE NOTICE '% | work — done', clock_timestamp();

END $$;


-- =============================================================================
-- relation
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | relation — start', clock_timestamp();

RAISE NOTICE '% | idx_relation_source', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_source
    ON relation ("sourceType", source);

RAISE NOTICE '% | idx_relation_target', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_target
    ON relation ("targetType", target);

RAISE NOTICE '% | idx_relation_project_org (partial — critical for map views)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_project_org
    ON relation (source, target)
    WHERE "sourceType" = 'project' AND "targetType" = 'organization';

RAISE NOTICE '% | idx_relation_cordis_type (partial)', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_cordis_type
    ON relation ("cordis_type") WHERE "cordis_type" IS NOT NULL;

RAISE NOTICE '% | relation — done', clock_timestamp();

END $$;


-- =============================================================================
-- relation_topic
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | relation_topic — start', clock_timestamp();

RAISE NOTICE '% | idx_relation_topic_topic_id', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_topic_topic_id
    ON relation_topic (topic_id);

RAISE NOTICE '% | idx_relation_topic_source', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_relation_topic_source
    ON relation_topic (type, source_id);

RAISE NOTICE '% | relation_topic — done', clock_timestamp();

END $$;


-- =============================================================================
-- topic
-- =============================================================================
DO $$
BEGIN

RAISE NOTICE '% | topic — start', clock_timestamp();

RAISE NOTICE '% | idx_topic_pk', clock_timestamp();
CREATE UNIQUE INDEX IF NOT EXISTS idx_topic_pk
    ON topic (id);

RAISE NOTICE '% | idx_topic_subfield_id', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_topic_subfield_id
    ON topic (subfield_id);

RAISE NOTICE '% | idx_topic_field_id', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_topic_field_id
    ON topic (field_id);

RAISE NOTICE '% | idx_topic_domain_id', clock_timestamp();
CREATE INDEX IF NOT EXISTS idx_topic_domain_id
    ON topic (domain_id);

RAISE NOTICE '% | topic — done', clock_timestamp();

END $$;


-- =============================================================================
-- ANALYZE
-- =============================================================================
DO $$
BEGIN
RAISE NOTICE '% | ANALYZE — start', clock_timestamp();
END $$;

ANALYZE project;
ANALYZE organization;
ANALYZE work;
ANALYZE relation;
ANALYZE relation_topic;
ANALYZE topic;

DO $$
BEGIN
RAISE NOTICE '% | ANALYZE — done. All indexes complete.', clock_timestamp();
END $$;
