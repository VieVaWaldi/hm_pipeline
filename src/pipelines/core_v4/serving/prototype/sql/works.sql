-- works index docs (trimmed): no abstract, no influence/views/subjects/instances/formats/sources.
-- The links live here (many side): project_ids / organisation_ids as VARCHAR[].
-- pdf_url rule: first .pdf url among OPEN instances, else first .pdf url of any instance, else NULL.
-- landing_url rule: https://doi.org/<doi>, else first url of any instance.
COPY (
WITH wp AS (
    SELECT target AS wid, list(source::VARCHAR ORDER BY source) AS project_ids
    FROM relation WHERE sourceType = 'project' AND targetType = 'product' GROUP BY target
), wo AS (
    SELECT source AS wid, list(target::VARCHAR ORDER BY target) AS organisation_ids
    FROM relation WHERE sourceType = 'product' AND targetType = 'organization' GROUP BY source
), w AS (
    SELECT w.*,
           coalesce(list_filter(w.pids, lambda p: lower(p.scheme) = 'doi')[1].value, NULL) AS doi_v,
           flatten(list_transform(coalesce(w.instances, []), lambda i: coalesce(i.urls, []))) AS all_urls,
           flatten(list_transform(list_filter(coalesce(w.instances, []), lambda i: i.accessRight.label = 'OPEN'),
                                  lambda i: coalesce(i.urls, []))) AS open_urls
    FROM work w
)
SELECT w.id::VARCHAR AS id, w.title,
       list_transform(w.authors[1:20], lambda a: a.fullName) AS authors, len(w.authors)::INTEGER AS author_count,
       w.publicationDate AS publication_date, year(w.publicationDate)::INTEGER AS year,
       w.publisher, w.container.name AS container_name, w.openAccessColor AS open_access_color,
       w.bestAccessRight.label AS best_access_right, w.language.code AS language,
       w.citationCount::INTEGER AS citation_count, w.doi_v AS doi,
       coalesce(list_filter(w.open_urls, lambda u: regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$'))[1],
                list_filter(w.all_urls,  lambda u: regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$'))[1]) AS pdf_url,
       coalesce('https://doi.org/' || w.doi_v, w.all_urls[1]) AS landing_url,
       coalesce(wp.project_ids, [])::VARCHAR[] AS project_ids, coalesce(wo.organisation_ids, [])::VARCHAR[] AS organisation_ids,
       w.link_tier
FROM w LEFT JOIN wp ON wp.wid = w.id LEFT JOIN wo ON wo.wid = w.id
) TO '__OUT__/works.parquet' (FORMAT parquet, COMPRESSION zstd);
