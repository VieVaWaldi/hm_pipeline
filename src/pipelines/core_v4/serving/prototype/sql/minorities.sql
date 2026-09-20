-- minorities index docs: every column + rollups from their projects + a capped title blob (search only, not in _source).
COPY (
WITH pm AS (
    SELECT p.id AS pid, unnest(p.minority_qid) AS qid, p.title, p.acronym, p.pred, p.is_ch, t.id::VARCHAR AS topic_id
    FROM project p LEFT JOIN relation_topic rt ON rt.source_id = p.id AND rt.type = 'project' LEFT JOIN topic t ON t.id = rt.topic_id
), agg AS (
    SELECT qid, count(*)::INTEGER AS project_count, count(*) FILTER (is_ch)::INTEGER AS dch_project_count,
           array_to_string(list(coalesce(acronym || ': ', '') || title ORDER BY pred DESC NULLS LAST)[1:200], ' | ') AS blob
    FROM pm GROUP BY qid
), tc AS (
    SELECT qid, list({'topic_id': topic_id, 'n': n} ORDER BY n DESC) AS topic_counts
    FROM (SELECT qid, topic_id, count(*)::INTEGER AS n FROM pm WHERE topic_id IS NOT NULL GROUP BY 1, 2) GROUP BY qid
), oc AS (
    SELECT pm.qid, count(DISTINCT po.oid)::INTEGER AS org_count FROM pm JOIN po ON po.pid = pm.pid GROUP BY pm.qid
)
SELECT m.*, len(m.known_subgroups) > 0 AS has_subgroups,
       coalesce(agg.project_count, 0) AS project_count, coalesce(agg.dch_project_count, 0) AS dch_project_count,
       coalesce(oc.org_count, 0) AS org_count,
       list_transform(coalesce(tc.topic_counts, []), lambda x: x.topic_id) AS topic_ids,
       coalesce(tc.topic_counts, []) AS topic_counts,
       left(coalesce(agg.blob, ''), 20000) AS project_title_blob,
       list_contains(m.source_class, 'manual_seed') AS is_seed
FROM minority m LEFT JOIN agg ON agg.qid = m.qid LEFT JOIN tc ON tc.qid = m.qid LEFT JOIN oc ON oc.qid = m.qid
) TO '__OUT__/minorities.parquet' (FORMAT parquet, COMPRESSION zstd);
