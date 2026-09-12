------------------------------
-- COMPLETE JUNCTION TESTING SCRIPT
------------------------------

-- Test 1: Count all junctions and compare with intermediate
------------------------------
SELECT 'j_researchoutput_author' as table_name,
       (SELECT COUNT(*) FROM core.j_researchoutput_author) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_researchoutput_person_filtered) +
       (SELECT COUNT(*) FROM arxiv_intermediate.int_arxiv_j_entry_author_filtered) as expected_count

UNION ALL

SELECT 'j_researchoutput_link' as table_name,
       (SELECT COUNT(*) FROM core.j_researchoutput_link) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_researchoutput_weblink_filtered) +
       (SELECT COUNT(*) FROM arxiv_intermediate.int_arxiv_j_entry_link_filtered) as expected_count

UNION ALL

SELECT 'j_researchoutput_institution' as table_name,
       (SELECT COUNT(*) FROM core.j_researchoutput_institution) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_researchoutput_institution_filtered) as expected_count

UNION ALL

SELECT 'j_project_institution' as table_name,
       (SELECT COUNT(*) FROM core.j_project_institution) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_project_institution_filtered) as expected_count

UNION ALL

SELECT 'j_project_fundingprogramme' as table_name,
       (SELECT COUNT(*) FROM core.j_project_fundingprogramme) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_project_fundingprogramme_filtered) as expected_count

UNION ALL

SELECT 'j_project_link' as table_name,
       (SELECT COUNT(*) FROM core.j_project_link) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_project_weblink_filtered) as expected_count

UNION ALL

SELECT 'j_project_researchoutput' as table_name,
       (SELECT COUNT(*) FROM core.j_project_researchoutput) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_project_researchoutput_filtered) as expected_count

UNION ALL

SELECT 'j_institution_author' as table_name,
       (SELECT COUNT(*) FROM core.j_institution_author) as core_count,
       (SELECT COUNT(*) FROM cordis_intermediate.int_cordis_j_institution_person_filtered) as expected_count;

------------------------------
-- Test 2: Check for orphaned references
------------------------------
-- Should all return 0 orphaned rows

SELECT 'j_researchoutput_institution' as junction,
       COUNT(*) as total_rows,
       SUM(CASE WHEN ro.id IS NULL THEN 1 ELSE 0 END) as orphaned_ro,
       SUM(CASE WHEN i.id IS NULL THEN 1 ELSE 0 END) as orphaned_institution
FROM core.j_researchoutput_institution j
LEFT JOIN core.researchoutput ro ON j.researchoutput_id = ro.id
LEFT JOIN core.institution i ON j.institution_id = i.id

UNION ALL

SELECT 'j_project_institution' as junction,
       COUNT(*) as total_rows,
       SUM(CASE WHEN p.id IS NULL THEN 1 ELSE 0 END) as orphaned_project,
       SUM(CASE WHEN i.id IS NULL THEN 1 ELSE 0 END) as orphaned_institution
FROM core.j_project_institution j
LEFT JOIN core.project p ON j.project_id = p.id
LEFT JOIN core.institution i ON j.institution_id = i.id

UNION ALL

SELECT 'j_project_fundingprogramme' as junction,
       COUNT(*) as total_rows,
       SUM(CASE WHEN p.id IS NULL THEN 1 ELSE 0 END) as orphaned_project,
       SUM(CASE WHEN f.id IS NULL THEN 1 ELSE 0 END) as orphaned_funding
FROM core.j_project_fundingprogramme j
LEFT JOIN core.project p ON j.project_id = p.id
LEFT JOIN core.fundingprogramme f ON j.fundingprogramme_id = f.id;

------------------------------
-- Test 3: Sample data verification
------------------------------
-- Check some actual junction records look reasonable

SELECT p.id, p.title, i.id, i.legal_name
FROM cordis.project p
JOIN cordis.j_project_institution j on j.project_id = p.id
JOIN cordis.institution i on j.institution_id = i.id
WHERE p.id in (1,2,3, 4, 10000)
ORDER BY p.id, i.legal_name;

SELECT p.source_id, p.title, i.id, i.legal_name
FROM core.project p
JOIN core.j_project_institution j on j.project_id = p.id
JOIN core.institution i on j.institution_id = i.id
WHERE p.source_id::INT in (1,2,3, 4, 10000)
ORDER BY p.source_id, i.legal_name;

------------------------------
-- Test 4: Quick sanity check on largest tables
------------------------------
SELECT 'SUMMARY' as info,
       (SELECT COUNT(*) FROM core.researchoutput) as total_research_outputs,
       (SELECT COUNT(*) FROM core.author) as total_authors,
       (SELECT COUNT(*) FROM core.institution) as total_institutions,
       (SELECT COUNT(*) FROM core.project) as total_projects,
       (SELECT COUNT(*) FROM core.j_researchoutput_author) as ro_author_links,
       (SELECT COUNT(*) FROM core.j_project_institution) as project_institution_links;