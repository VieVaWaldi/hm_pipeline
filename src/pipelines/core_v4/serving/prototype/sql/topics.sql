-- topics: not an index, a static table the api holds in memory (4.5k rows).
COPY (SELECT id::VARCHAR AS id, subfield_id, field_id, domain_id, topic_name, subfield_name, field_name, domain_name FROM topic)
TO '__OUT__/topics.parquet' (FORMAT parquet, COMPRESSION zstd);
