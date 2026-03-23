-- =============================================================================
-- Institution lol
-- =============================================================================
SELECT * 
FROM core.institution 
WHERE lower(legal_name) ilike lower('%Friedrich schiller%');

-- =============================================================================
-- PROJECT FULL-TEXT SEARCH TESTS
-- =============================================================================

-- Test 1: Simple search in project titles
SELECT
    id,
    title,
    source_system,
    ts_rank(to_tsvector('english', COALESCE(title, '')), to_tsquery('english', 'digital')) as rank
FROM core.project
WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'digital')
ORDER BY rank DESC;

-- 300 mili seconds

-- Test 2: Search in project objectives with AND operator
SELECT
    id,
    title,
    LEFT(objective, 100) as objective_preview,
    ts_rank(to_tsvector('english', COALESCE(objective, '')), to_tsquery('english', 'cultural & heritage')) as rank
FROM core.project
WHERE to_tsvector('english', COALESCE(objective, '')) @@ to_tsquery('english', 'cultural & heritage')
ORDER BY rank DESC;

-- 3 seconds

-- Test 3: Combined weighted search across title and objective
SELECT
    id,
    title,
    LEFT(objective, 100) as objective_preview,
    ts_rank(
        setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(objective, '')), 'B'),
        to_tsquery('english', 'heritage | cultural')
    ) as rank
FROM core.project
WHERE (
    setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(objective, '')), 'B')
) @@ to_tsquery('english', 'heritage | cultural')
ORDER BY rank DESC;

-- 4 seconds

-- =============================================================================
-- RESEARCH OUTPUT FULL-TEXT SEARCH TESTS
-- =============================================================================

-- Test 4: Simple search in research output titles
SELECT
    id,
    title,
    source_system,
    ts_rank(to_tsvector('english', COALESCE(title, '')), to_tsquery('english', 'machine & learning')) as rank
FROM core.researchoutput
WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'machine & learning')
ORDER BY rank DESC;

-- 400 mili seconds

-- Test 5: Search in abstracts with OR operator
SELECT
    id,
    title,
    LEFT(abstract, 100) as abstract_preview,
    ts_rank(to_tsvector('english', COALESCE(abstract, '')), to_tsquery('english', 'artificial | intelligence | AI')) as rank
FROM core.researchoutput
WHERE to_tsvector('english', COALESCE(abstract, '')) @@ to_tsquery('english', 'artificial | intelligence | AI')
AND abstract IS NOT NULL
ORDER BY rank DESC
LIMIT 10;

-- 2 seconds

-- Test 6: Combined title + abstract search (fast alternative to fulltext)
SELECT 
    id,
    title,
    LEFT(abstract, 100) as abstract_preview,
    journal_name,
    ts_rank(
        setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(abstract, '')), 'B'),
        to_tsquery('english', 'machine & learning')
    ) as rank
FROM core.researchoutput 
WHERE (
    setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(abstract, '')), 'B')
) @@ to_tsquery('english', 'machine & learning')
ORDER BY rank DESC
LIMIT 10;

-- 6 seconds

-- Test 7: Search in fulltext content
SELECT
    id,
    title,
    LENGTH(fulltext) as fulltext_length,
    ts_rank(to_tsvector('english', COALESCE(fulltext, '')), to_tsquery('english', 'acknowledgments')) as rank
FROM core.researchoutput
WHERE to_tsvector('english', COALESCE(fulltext, '')) @@ to_tsquery('english', 'acknowledgments')
AND fulltext IS NOT NULL
ORDER BY rank DESC;

-- 11 minutes C:

-- Test 8: Combined weighted search across all research output text fields
SELECT
    id,
    title,
    journal_name,
    publication_date,
    ts_rank(
        setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(abstract, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(fulltext, '')), 'C'),
        to_tsquery('english', 'database | postgresql | sql')
    ) as rank
FROM core.researchoutput
WHERE (
    setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(abstract, '')), 'B') ||
    setweight(to_tsvector('english', COALESCE(fulltext, '')), 'C')
) @@ to_tsquery('english', 'database | postgresql | sql')
ORDER BY rank DESC
LIMIT 10;

-- Havent tested it yet

-- =============================================================================
-- PERFORMANCE VERIFICATION QUERIES
-- =============================================================================

-- Test 9: Verify indexes are being used (run with EXPLAIN ANALYZE)
EXPLAIN ANALYZE
SELECT
    id,
    title,
    ts_rank(to_tsvector('english', COALESCE(title, '')), to_tsquery('english', 'science')) as rank
FROM core.project
WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'science')
ORDER BY rank DESC
LIMIT 5;

-- Test 10: Prefix search (useful for autocomplete)
SELECT
    id,
    title,
    ts_rank(to_tsvector('english', COALESCE(title, '')), to_tsquery('english', 'digit:*')) as rank
FROM core.project
WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'digit:*')
ORDER BY rank DESC
LIMIT 10;

-- Test 11: Complex query with NOT operator
SELECT
    id,
    title,
    ts_rank(to_tsvector('english', COALESCE(title, '')), to_tsquery('english', 'research & !medical')) as rank
FROM core.project
WHERE to_tsvector('english', COALESCE(title, '')) @@ to_tsquery('english', 'research & !medical')
ORDER BY rank DESC
LIMIT 10;

-- =============================================================================
-- STATISTICS AND INDEX USAGE QUERIES
-- =============================================================================

-- These do not work :c

-- Check index sizes
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'core'
AND indexname LIKE '%_fts'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Check index usage statistics
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'core'
AND indexname LIKE '%_fts'
ORDER BY idx_scan DESC;