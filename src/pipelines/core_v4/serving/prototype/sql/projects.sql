-- projects index docs: every project column + denormalised org/topic/funding fields. One row per project.
COPY (
SELECT
    p.id::VARCHAR AS id, p.openaireId, p.grantId, p.title, p.acronym, p.websiteUrl,
    p.startDate, p.endDate, year(p.startDate)::INTEGER AS year, p.callIdentifier, p.keywords,
    p.openAccessMandateForPublications, p.openAccessMandateForDataset, p.subjects,
    p.fundings, p.frameworkProgrammes, p.summary, p.doi,
    m.currency, m.funded_amount, p.granted.totalCost AS total_cost,
    m.funded_amount_eur,                       -- ECB table (D14)
    m.funded_eur_per_org,                      -- D13 (decided): funded_amount_eur / org_count
    p.is_translated, p.is_ch, p.pred, p.minority_qid AS minority_qids, p.pillars::INTEGER AS pillars,
    list_filter(['inclusive','sustainable','resilient','innovative','global'],
                lambda x, i: ((p.pillars >> (i - 1)) & 1) = 1) AS pillar_list,
    p.theme,
    -- exactly one topic per project (checked in the prototype test)
    t.id::VARCHAR AS topic_id, t.subfield_id, t.field_id, t.domain_id,
    coalesce(o.org_ids, [])::VARCHAR[] AS org_ids, coalesce(o.org_names, [])::VARCHAR[] AS org_names,
    coalesce(o.org_regions, [])::VARCHAR[] AS org_regions, coalesce(o.org_countries, [])::VARCHAR[] AS org_countries,
    o.coordinator_id, coalesce(o.org_count, 0) AS org_count, coalesce(w.work_count, 0) AS work_count,
    -- D17: funder = level 1 of fundingStream.id (falls back to shortName when a funding has no stream id), programme = level 2
    list_sort(list_distinct(list_filter(list_transform(coalesce(p.fundings, []),
        lambda f: coalesce(nullif(split_part(f.fundingStream.id, '::', 1), ''), f.shortName)), lambda x: x IS NOT NULL))) AS funder,
    list_sort(list_distinct(list_filter(list_transform(coalesce(p.fundings, []),
        lambda f: nullif(split_part(f.fundingStream.id, '::', 2), '')), lambda x: x IS NOT NULL))) AS programme,
    list_distinct(list_transform(coalesce(p.fundings, []), lambda f: f.name)) AS funder_names,
    list_distinct(list_transform(coalesce(p.fundings, []), lambda f: f.fundingStream.id)) AS funding_stream_ids
FROM project p
LEFT JOIN p_orgs o ON o.pid = p.id
LEFT JOIN p_works w ON w.pid = p.id
LEFT JOIN p_money m ON m.pid = p.id
LEFT JOIN relation_topic rt ON rt.source_id = p.id AND rt.type = 'project'
LEFT JOIN topic t ON t.id = rt.topic_id
) TO '__OUT__/projects.parquet' (FORMAT parquet, COMPRESSION zstd);
