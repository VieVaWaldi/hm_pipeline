-- projects index docs: every project column (text-cleaned) + denormalised org / topic / funding fields. One row per project.
-- Dropped on purpose: granted.totalCost (0 for 98.8%), raw granted struct (replaced by currency / funded_amount / funded_amount_eur).
COPY (
SELECT
    p.id::VARCHAR AS id, p.openaireId, p.grantId, hm_clean(p.title) AS title, p.acronym, p.websiteUrl,
    p.startDate, p.endDate, hm_year(p.startDate) AS year, p.callIdentifier, hm_clean(p.keywords) AS keywords,
    p.openAccessMandateForPublications, p.openAccessMandateForDataset, p.subjects,
    -- overview shows the raw fundings; ids/names/descriptions are unescaped (D27)
    list_transform(coalesce(p.fundings, []), lambda f: {'fundingStream': {'description': hm_clean(f.fundingStream.description),
                   'id': hm_clean(f.fundingStream.id)}, 'jurisdiction': f.jurisdiction, 'name': hm_clean(f.name), 'shortName': f.shortName}) AS fundings,
    p.frameworkProgrammes, hm_clean(p.summary) AS summary, p.doi,
    m.currency, m.funded_amount, m.funded_amount_eur, sh.funded_eur_per_org,       -- D13/D14/D21
    p.is_translated, p.is_ch, p.pred,                                              -- pred: display only (D8), is_ch is NOT pred > 0.55 (D31)
    coalesce(pmq.minority_qid, [])::VARCHAR[] AS minority_qids, p.pillars::INTEGER AS pillars,
    list_filter(['inclusive', 'sustainable', 'resilient', 'innovative', 'global'], lambda x, i: ((p.pillars >> (i - 1)) & 1) = 1) AS pillar_list,
    p.theme,
    t.id::VARCHAR AS topic_id, t.subfield_id::VARCHAR AS subfield_id, t.field_id::VARCHAR AS field_id, t.domain_id::VARCHAR AS domain_id,   -- nullable (D29)
    coalesce(o.org_ids, [])::VARCHAR[] AS org_ids, coalesce(o.org_names, [])::VARCHAR[] AS org_names,
    coalesce(o.org_regions, [])::VARCHAR[] AS org_regions, coalesce(o.org_countries, [])::VARCHAR[] AS org_countries,
    coalesce(o.coordinator_ids, [])::VARCHAR[] AS coordinator_ids,
    coalesce(o.org_count, 0)::INTEGER AS org_count, coalesce(w.work_count, 0)::INTEGER AS work_count,
    coalesce(pf.funders, [])::VARCHAR[] AS funder,                                  -- D20 facet 1 (fundings.shortName)
    coalesce(fs.programmes, [])::VARCHAR[] AS programme,                            -- D20 facet 2 (level 2 of the stream id)
    coalesce(fs.funder_names, [])::VARCHAR[] AS funder_names,
    coalesce(fs.stream_ids, [])::VARCHAR[] AS funding_stream_ids
FROM project p
LEFT JOIN p_orgs o ON o.pid = p.id
LEFT JOIN p_works w ON w.pid = p.id
LEFT JOIN p_money m ON m.pid = p.id
LEFT JOIN p_share sh ON sh.pid = p.id
LEFT JOIN p_fund pf ON pf.pid = p.id
LEFT JOIN p_streams fs ON fs.pid = p.id
LEFT JOIN p_topic pt ON pt.pid = p.id
LEFT JOIN p_minority pmq ON pmq.pid = p.id     -- stored project.minority_qid, or the optional override (export.py --minority-override)
LEFT JOIN topic t ON t.id = pt.topic_id
ORDER BY p.id
) TO '{OUT}/projects/projects.parquet.tmp' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000);
