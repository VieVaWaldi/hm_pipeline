-- topics: NOT an index. A static table (4,516 rows) the api keeps in memory: id -> names/hierarchy. Written as parquet and json.
COPY (SELECT id::VARCHAR AS id, subfield_id::VARCHAR AS subfield_id, field_id::VARCHAR AS field_id, domain_id::VARCHAR AS domain_id,
             topic_name, subfield_name, field_name, domain_name FROM topic ORDER BY id)
TO '{OUT}/api/topics.json' (FORMAT json, ARRAY true);
