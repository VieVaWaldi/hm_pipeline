-----------------------------------------------
-- CORE ENTITIES

SELECT COUNT(*) FROM core.project;
-- 91_956 (63_992 + 29_486)
-- 39_085 with doi
-- 69947 with objective
-- 91_956 with start_date
-- 85936 with total_cost
-- 28373 with funded_amount
-- 17533 with funder_total_cost
SELECT COUNT(*) FROM core.researchoutput;
-- 856_251 (99_200 + 633_393 + 140_352)
-- 286596 with doi
-- 750776 with publication_date
-- 140348 with language_code
-- 411714 with abstract
-- 50011 with fulltext
-- 104514 with comment
-- 383015 with journal_name
-- 442401 with publisher
-- 119914 with citation_count
-- 119914 with popularity
-- 0 with conference_place
SELECT count(*) FROM core.institution;
-- 176_014 (92_587 + 90_483)
-- 39216 with sme
-- 30578 with url
-- 84434 with city
-- 87077 with country
-- 65961 with geolocation
-- 52745 with type_title
-- 43115 with nuts_level_0
-- 24484 with country_code --> all openaire, we could use that to filter out the garbage names, this means just severing the j_ro_institution for openaire
-- --> Check if our geolocation script ignores the author names, if yes we are good!

SELECT COUNT(*) FROM core.author;
-- 1_630_893 (244_329 + 972_968 + 413_596)
SELECT COUNT(*) FROM core.topic;
-- doesnt exist yet, will come with enrichment
SELECT COUNT(*) FROM core.link;
-- 406_489 (222_513 + 183_976)
SELECT distinct code FROM core.fundingprogramme;
-- 8_569


-----------------------------------------------
-- JUNCTIONS

-----------------------------------------------
-- JUNCTIONS Institution

SELECT COUNT(*) FROM core.j_institution_author;
-- 0

-----------------------------------------------
-- JUNCTIONS Research Output

SELECT COUNT(*) FROM core.j_researchoutput_author;
-- 2_222_2570 (527_495 + 1_060_625 + 738_177)
SELECT COUNT(*) FROM core.j_researchoutput_topic;
-- 0
SELECT COUNT(*) FROM core.j_researchoutput_link;
-- 222_357 (222_535 + 246)1
SELECT COUNT(*) FROM core.j_researchoutput_institution;
-- 211_475 (3_869 + 215_537)

-----------------------------------------------
-- JUNCTIONS Project

SELECT COUNT(*) FROM core.j_project_topic;
-- doesnt exist yet, will come with enrichment
SELECT COUNT(*) FROM core.j_project_link;
-- 9_081
SELECT COUNT(*) FROM core.j_project_fundingprogramme;
-- 140_655
SELECT COUNT(*) FROM core.j_project_institution;
-- 436_946 (369_998 + 80_167) 
SELECT COUNT(*) FROM core.j_project_researchoutput;
-- 747_491 (633_393 + 140_331)