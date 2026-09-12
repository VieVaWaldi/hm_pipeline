-----------------------------------------------
-- Data Description
-----------------------------------------------

SELECT
	table_name,
	column_name,
	data_type
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;

-----------------------------------------------
-- Data Description with samples and count
-----------------------------------------------

DO $$
DECLARE
    r RECORD;
    sample_val TEXT;
    filled_count BIGINT;
    not_empty_clause TEXT;
BEGIN
    DROP TABLE IF EXISTS schema_samples;
    CREATE TEMP TABLE schema_samples (
        table_name    TEXT,
        column_name   TEXT,
        data_type     TEXT,
        filled_count  BIGINT,
        sample_value  TEXT
    );

    FOR r IN
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    LOOP
        IF r.data_type = 'ARRAY' THEN
            not_empty_clause := format('%I IS NOT NULL AND cardinality(%I) > 0', r.column_name, r.column_name);
        ELSE
            not_empty_clause := format('%I IS NOT NULL', r.column_name);
        END IF;

        BEGIN
            EXECUTE format(
                'SELECT (SELECT %I::text FROM %I.%I WHERE %s LIMIT 1), (SELECT count(*) FROM %I.%I WHERE %s)',
                r.column_name, 'public', r.table_name, not_empty_clause,
                'public', r.table_name, not_empty_clause
            ) INTO sample_val, filled_count;
        EXCEPTION WHEN OTHERS THEN
            sample_val := '(error: ' || SQLERRM || ')';
            filled_count := NULL;
        END;

        INSERT INTO schema_samples
        VALUES (r.table_name, r.column_name, r.data_type, filled_count, sample_val);
    END LOOP;
END $$;

SELECT * FROM schema_samples
ORDER BY table_name, filled_count DESC, column_name;

-----------------------------------------------
-- Advanced Data Description
-----------------------------------------------

SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    CASE
        WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PK'
        WHEN tc.constraint_type = 'UNIQUE' THEN 'UNIQUE'
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN 'FK'
        WHEN tc.constraint_type = 'CHECK' THEN 'CHECK'
        ELSE NULL
    END as constraint_type,
    tc.constraint_name
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage kcu
    ON c.table_schema = kcu.table_schema
    AND c.table_name = kcu.table_name
    AND c.column_name = kcu.column_name
LEFT JOIN information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
    AND kcu.table_schema = tc.table_schema
-- WHERE c.table_schema = 'core'
ORDER BY c.table_name, c.ordinal_position;

--

SELECT 
    tc.constraint_name, 
    tc.table_name, 
    kcu.column_name,
    tc.constraint_type
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'core' 
    AND tc.table_name = 'author'
    AND tc.constraint_type = 'PRIMARY KEY';