-- works index docs (trimmed, D4/D4b/D5/D22/D23/D25): no abstract, no influence/views/subjects/instances/formats/sources/other pids.
-- Run once per chunk: `w.id % {N} = {K}` splits the 50M works into N Parquet files (resumable, bounded memory). {SAMPLE_FILTER} is empty
-- or `AND hash(w.id) % S = 0` for smoke runs. Links live HERE (many side, D2): project_ids / organisation_ids as VARCHAR[].
--   project_ids[]        empty for the 18.4% of tier-0 works without a project relation (link_tier 0 does NOT mean "has a project")
--   is_ch_via_project    PROXY (D4): any linked project has is_ch. minority_qids PROXY (D4b): union over linked projects. Never a property of the work.
--   organisation_ids[]   capped at 100 (p99 41, max 2,660), org_count keeps the true number (D22)
COPY (
WITH wp AS (
    SELECT r.target AS wid, list(r.source::VARCHAR ORDER BY r.source) AS project_ids,
           bool_or(coalesce(p.is_ch, false)) AS is_ch_via_project,
           list_sort(list_distinct(flatten(list(coalesce(p.minority_qid, []))))) AS minority_qids
    FROM relation r JOIN project p ON p.id = r.source
    WHERE r.sourceType = 'project' AND r.targetType = 'product' AND r.target % {N} = {K}
    GROUP BY r.target
), wo AS (
    SELECT source AS wid, list(target::VARCHAR ORDER BY target) AS organisation_ids
    FROM relation WHERE sourceType = 'product' AND targetType = 'organization' AND source % {N} = {K}
    GROUP BY source
)
SELECT w.id::VARCHAR AS id, replace(w.title, '&amp;', '&') AS title,
       list_transform(w.authors[1:20], lambda a: a.fullName) AS authors, len(coalesce(w.authors, []))::INTEGER AS author_count,
       w.publicationDate AS publication_date, hm_year(w.publicationDate) AS year,
       w.publisher, w.container.name AS container_name, w.openAccessColor AS open_access_color,
       w.bestAccessRight.label AS best_access_right, hm_lang(w.language.code) AS language,
       w.citationCount::INTEGER AS citation_count,
       wk_doi_any(w.pids, w.instances) AS doi, wk_pdf_url(w.instances) AS pdf_url, wk_landing_url(w.pids, w.instances) AS landing_url,
       coalesce(wp.project_ids, [])::VARCHAR[] AS project_ids,
       coalesce(wo.organisation_ids[1:100], [])::VARCHAR[] AS organisation_ids,
       len(coalesce(wo.organisation_ids, []))::INTEGER AS org_count,
       coalesce(wp.is_ch_via_project, false) AS is_ch_via_project,
       coalesce(wp.minority_qids, [])::VARCHAR[] AS minority_qids,
       w.link_tier
FROM work w LEFT JOIN wp ON wp.wid = w.id LEFT JOIN wo ON wo.wid = w.id
WHERE w.id % {N} = {K} {SAMPLE_FILTER}
ORDER BY w.id
) TO '{OUT}/works/works_{KK}.parquet.tmp' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000);
