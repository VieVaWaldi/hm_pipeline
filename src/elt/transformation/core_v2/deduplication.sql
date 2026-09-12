-- =============================================================================
-- CORE DEDUPLICATION EXPLORATION
-- =============================================================================

-- =============================================================================
-- RESEARCH OUTPUT DEDUPLICATION EXPLORATION
-- =============================================================================

------------------------------
-- Research Output DOI Coverage and Duplicates
------------------------------

-- DOI Coverage across all sources
SELECT
    source_system,
    COUNT(*) as total_researchoutputs,
    COUNT(doi) as ro_with_doi,
    ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) as doi_coverage_pct
FROM core.researchoutput
GROUP BY source_system
ORDER BY total_researchoutputs DESC;

-- source, total ro, with doi, doi coverage pct
-- "cordis", 616902, 262519,42.55
-- "openaire", 140352, 0, 0.00 -> i forgot dois in the extraction phase, ignore this for now
-- "arxiv", 98997, 24077, 24.32

-- Cross-source DOI duplicates (potential matches for deduplication)
SELECT 
    doi,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids
FROM core.researchoutput
WHERE doi IS NOT NULL 
    AND TRIM(doi) != ''
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, sources_count DESC;

-- 577 duplicates all with source count 2. Do use!

-- Sample of records with same DOI to understand the patterns
SELECT ro.id, ro.source_system, ro.doi, ro.title, ro.publication_date, ro.type
FROM core.researchoutput ro
WHERE ro.doi IN (
    SELECT doi 
    FROM core.researchoutput 
    WHERE doi IS NOT NULL AND TRIM(doi) != ''
    GROUP BY doi 
    HAVING COUNT(*) > 1 
    LIMIT 5
)
ORDER BY ro.doi, ro.source_system;
-- All duplicate records share the same title, sometimes with small differences. 
-- Eg one different letter and capitalization

------------------------------
-- Research Output Title Analysis (with caution)
------------------------------

-- Cross-source title duplicates (very careful analysis needed)
WITH clean_titles AS (
    SELECT 
        id,
        source_system,
        doi,
        LOWER(TRIM(title)) as clean_title,
        title as original_title,
        publication_date,
        type
    FROM core.researchoutput
    WHERE title IS NOT NULL 
        AND LENGTH(TRIM(title)) > 10  -- Avoid very short generic 
		AND LOWER(TRIM(title)) NOT ILIKE ALL(ARRAY[
            '%data management plan%', '%project website%', '%dissemination and communication plan%', '%data management plan (dmp)%', '%dissemination plan%', '%final report%', '%communication and dissemination plan%', '%project handbook%', '%nature communications%', '%project management plan%', '%communication plan%', '%dissemination and exploitation plan%', '%website and project logo%', '%introduction%', '%project management handbook%', '%career development plan%', '%final project report%', '%progress report%', '%exploitation plan%', '%quality assurance plan%', '%initial data management plan%', '%project web site%', '%communication, dissemination and exploitation plan%', '%final conference%', '%plan for dissemination and exploitation including communication activities%', '%final data management plan%', '%reply on rc2%', '%data management plan v1%', '%project website and logo%', '%final dissemination report%', '%policy brief%', '%communication and dissemination strategy%', '%public project website%', '%risk management plan%', '%kick-off meeting%', '%supervisory board of the network%', '%quality management plan%', '%data management plan v2%', '%dissemination, communication and exploitation plan%', '%dissemination and communication strategy%', '%dissemination report%', '%updated data management plan%', '%dissemination, exploitation and communication plan%', '%policy brief 1%', '%project video%', '%plan for dissemination and communication activities%', '%project quality plan%', '%website and logo%', '%public website%', '%open research data pilot%', '%policy briefs%', '%policy recommendations%', '%project presentation%', '%project webpage%', '%data management plan 1%', '%kick-off meeting report%', '%final evaluation report%', '%project leaflet%', '%quality plan%', '%final report on dissemination activities%', '%communication toolkit%', '%press releases%', '%sustainability plan%', '%communication strategy%', '%training plan%', '%evaluation report%', '%final press release%', '%project flyer%', '%gender action plan%', '%press release%', '%ethics plan%', '%summer school%', '%publications%', '%dissemination strategy%', '%cluster brochure%', '%dissemination materials%', '%dissemination and communication report%', '%progress report 1%', '%validation report%', '%visual identity%', '%dissemination material%', '%final management report%', '%report on dissemination activities%', '%evaluation plan%', '%policy brief 2%', '%project brochure%', '%exploitation strategy%', '%data management plan 2%', '%cluster web portal and visual identity%', '%innovation management plan%', '%training materials%', '%study initiation package%', '%project website and social media accounts%', '%evaluation framework%', '%final exploitation plan%', '%progress report 2%', '%project final report%', '%call 1 - link to euraxess website%', '%communication materials%', '%impact assessment%', '%final report on dissemination and communication activities%', '%international conference%', '%scientific publications%', '%communication kit%', '%newsletters%', '%consortium agreement%', '%project website and social media%', '%website online%', '%first policy brief%', '%external project website%', '%final publishable report%', '%project newsletter%', '%report on the standardization landscape and applicable standards%', '%audio-visual material%', '%initial communication and dissemination plan%', '%research data management%', '%final dissemination and communication report%', '%mid-term meeting%', '%data management plan - initial version%', '%quality and risk management plan%', '%data management plan.%', '%dissemination and communication plan v1%', '%public final activity report%', '%design options paper%', '%research data management plan%', '%impact assessment report%', '%final event%', '%final conference report%', '%first data management plan%', '%potential inputs for policy feedback%', '%final dissemination and exploitation report%', '%report on impact assessment%', '%project quality handbook%', '%website launch%', '%data management plan update%', '%first periodic report%', '%open data management plan%', '%newsletter 1%', '%first progress report%', '%promotional material%', '%final workshop%', '%promotional video%', '%data management plan v3%', '%project web portal%', '%data management plan - first version%', '%final dissemination plan%', '%awareness campaign%', '%newsletter 2%', '%exploitation and sustainability plan%', '%standardization report%', '%final public report%', '%business plan%', '%system architecture%', '%exploitation and dissemination plan%', '%project logo and website%', '%report on communication activities%', '%report on communication and dissemination activities%', '%report on kick-off meeting%', '%newsletter 3%', '%training material%', '%website and social media%', '%user requirements%', '%call 1 - report on dissemination activities%', '%demonstration report%', '%project communication kit%', '%workshop report%', '%data management report%', '%final dissemination and exploitation plan%', '%project management manual%', '%white paper%', '%communication strategy and plan%', '%literature review%', '%dissemination and communication plan v2%', '%activities during the night%', '%case studies%', '%report on dissemination and communication activities%', '%outreach activities%', '%project visual identity%', '%report on the awareness campaign%', '%project dissemination and communication plan%', '%synthesis report%', '%evaluation methodology%', '%dissemination and communication strategy and plan%', '%data management plan - update%', '%data management plan - final version%', '%stakeholder engagement plan%', '%communication report%', '%inception report%', '%data management plan i%', '%management guidelines%', '%project website and social media presence%', '%mid-term review meeting%', '%factsheet and project presentation%', '%project web-site%', '%initial dissemination plan%', '%stakeholder workshop%', '%data management plan (v1)%', '%data management plan 3%', '%report on the activities during the night-period 2%', '%project newsletters%', '%dissemination toolkit%', '%project communication plan%', '%introduzione%', '%final report on communication activities%', '%project management plan (pmp)%', '%policy brief 3%', '%data management plan version 1%', '%website completion%', '%minutes of the kick-off meeting%', '%second progress report%', '%second periodic report%', '%internal and external it communication infrastructure and project website%', '%final communication and dissemination report%', '%conference report%', '%newsletter 4%', '%stakeholder mapping%', '%project website and visual identity%', '%report on final conference%', '%final publishable activity report%', '%project plan%', '%project public website%', '%final impact assessment report%', '%project quality management plan pqmp%', '%communication tools%', '%project data management plan%', '%plan for the dissemination and exploitation including communication activities%', '%call 2 - link to euraxess website%', '%updated dissemination and exploitation plan%', '%communication and dissemination report%', '%preliminary exploitation plan%', '%first report on dissemination activities%', '%state-of-the-art report%', '%final dissemination and communication plan%', '%data management plan (m6)%', '%first press release%', '%kick-off meeting minutes%', '%evolution of the publications in high impact journals in the relevant research fields%', '%call 1 link to euraxess website%', '%exploitation report%', '%dissemination and communication activities%', '%data management plan (initial)%', '%engagement strategy%', '%communication pack%', '%nature medicine%', '%summer school 1%', '%project handbook and quality plan%', '%d1.1 data management plan%', '%second policy brief%', '%dissemination and exploitation report%', '%summer school 2%', '%reference architecture%', '%remote sensing%', '%interim report%', '%d1.2 data management plan%', '%design option paper%', '%website and communication material%', '%updated dissemination plan%', '%report on awareness and wider societal implications%', '%pilot evaluation report%', '%ordp: open research data pilot%', '%final public project report%', '%project website, social media channels and communication activities%', '%ecosystem report%', '%state of the art report%', '%data management plan version 2%', '%management and quality plan%', '%h - requirement no. 1%', '%project web page%', '%project management and quality guidelines%', '%promotional materials%', '%1st workshop%', '%data management%', '%dissemination and outreach plan%', '%sustainability strategy%', '%2nd workshop%', '%3rd workshop%', '%initial impact creation report%', '%ethics report%', '%proceedings of the final conference%', '%data management plan m6%', '%project management guide%', '%environmental research letters%', '%data management plan (initial version)%', '%website up and running%', '%outreach plan%', '%market analysis report%', '%state of the art%', '%final version of the data management plan%', '%plan for communication, dissemination and exploitation%', '%project fact sheet%', '%communication and dissemination final report%', '%quality control plan%', '%website development%', '%dissemination pack%', '%plan for dissemination, exploitation and communication%', '%network website%', '%periodic report%', '%data management plan ii%', '%position paper%', '%project video presentation%', '%call 1 report on dissemination activities%', '%business models%', '%training activities report%', '%management report%', '%updated dissemination and communication plan%', '%initial exploitation plan%', '%conclusions%', '%impact master plan%', '%dissemination and communication plan v3%', '%initial dissemination and exploitation plan%', '%case study reports%', '%dissemination and communication final report%', '%midterm report%', '%final impact report%', '%implementation plan%', '%plan for exploitation and dissemination of results (pedr)%', '%cluster newsletter 1%', '%conference proceedings%', '%intermediate dissemination and communication report%', '%interim impact creation report%', '%project leaflet and poster%', '%methodological framework%', '%project web presence%', '%data management plan - v1%', '%initial communication plan%', '%decision support tool%', '%communications plan%', '%impact assessment methodology%', '%dmp - data management plan%', '%intermediate report on dissemination activities%', '%workshop reports%', '%stakeholder analysis report%', '%first dissemination report%', '%final impact creation report%', '%update of data management plan%', '%final publication%', '%final report on dissemination%', '%conference website%', '%open call documentation%', '%plan for communication and dissemination%', '%press release and communication kit%', '%communication activities report%', '%project branding%', '%project communication and dissemination plan%', '%dissemination action plan%', '%list of participants%', '%initial dmp%', '%marketing material%', '%final communication report%', '%stakeholders engagement plan%', '%use cases and requirements%', '%management handbook%', '%dissemination master plan%', '%report on communication, dissemination and exploitation actions%', '%communication, dissemination and exploitation plan (cdep)%', '%project impact assessment%', '%cross-project collaboration plan%', '%initial data management plan (dmp)%', '%publishable final report%', '%best practices report%', '%policy strategy of the cluster%', '%plan for the exploitation and dissemination of results (pedr)%', '%report on standardization activities%', '%gap analysis report%', '%report on the awareness campaign -period 2%', '%final conference proceedings%', '%edited volume%', '%dissemination package 2%', '%dissemination and communication plans%', '%requirement analysis%', '%scientific strategy of the cluster%', '%dissemination package 1%', '%requirements analysis%', '%converging on a semantic interoperability framework for the european data space for science, research and innovation (eosc)%', '%book review%', '%1st progress report%', '%quality assurance plan (qap)%', '%requirements and use cases%', '%impact assessment framework%', '%introducción%', '%integration plan%', '%project quality assurance plan%', '%update of the data management plan%', '%fair computational workflows%', '%data management and rri plan%', '%final project progress report%', '%call 2 link to euraxess website%', '%mid-term report on dissemination and communication activities%', '%call documentation%', '%impact assessment results%', '%project website.%', '%dissemination, communication, and exploitation plan%', '%logo and website%', '%policy paper%', '%open call documents kit%', '%documentary film%', '%dissemination, exploitation and communication strategy%', '%state of the art analysis%', '%final report on dissemination and communication%', '%report on the final conference%', '%data management plan final version%', '%data management plan (version 1)%', '%final report on dissemination and exploitation%', '%open research data pilot report%', '%communication and dissemination strategy and plan%', '%report on dissemination activities v1%', '%detailed dissemination and communication plan%', '%policy briefing%', '%reuse of semantic models for emerging smart grids applications%', '%interim project report%', '%promotional materials including a dedicated website%', '%risk register%', '%data management plan (final)%', '%data management plan dmp%', '%dissemination, exploitation, and communication plan%', '%final periodic report%', '%website development and creation of social accounts%', '%call 2 report on dissemination activities%', '%project dissemination plan%', '%final policy report%', '%occurrence of plasmids pxf64-hb_esvl and pucla-esvl associated with infections caused by xylella fastidiosa subsp. multiplex st6 in the demarcated area of alicante, spain%', '%open data use plan%', '%mid term meeting%', '%european vision for the 6g network ecosystem%', '%final synthesis report%', '%interim dissemination report%', '%communication and dissemination toolkit%', '%social media report%', '%microbiological quality assessment of chicken thigh fillets using spectroscopic sensors and multivariate data analysis%', '%awareness campaign period 2%', '%country reports%', '%project web presence (website and social media)%', '%report on the historical construction of the corpus%', '%user manual%', '%report on workshop 2%', '%project visual identity and website%', '%final data management report%', '%communication and dissemination mid-term report%', '%projects website%', '%analysis report%', '%small-scale evaluation report%', '%market study%', '%enabling vertical industries adoption of 5g technologies: a cartography of evolving solutions%', '%integration in geoss%', '%using ontolex-lemon for representing and interlinking german multiword expressions in odenet and mmorph%', '%risk management and quality assurance plan%', '%exploratory research plan (erp)%', '%stakeholder map%', '%communication, dissemination, and exploitation plan%', '%project public website and social media%', '%environmental impact assessment%', '%legal and ethical requirements%', '%data management plan - dmp%', '%dissemination and communication%', '%dissemination plan v1%', '%visual identity and project website%', '%marketing materials%', '%report on project clustering and synergies%', '%conference information kit%', '%communication basics%', '%scientific quality assurance plan%', '%assessment report and impact analysis%', '%internal communication plan%', '%under the spotlight: web tracking in indian partisan news websites%', '%market analysis, roadmapping and business modelling report%', '%report from the kick-off meeting%', '%an overview of monitoring methods for assessing the performance of nature-based solutions against natural hazards%', '%pilot implementation plan%', '%project management%', '%2nd newsletter%', '%project logo%', '%practice abstracts batch 1%', '%plans for publicity, dissemination and exploitation%', '%dissemination events%', '%practice abstract - batch 1%', '%quality assurance - risk management plan%', '%draft framework%', '%1st newsletter%', '%assessment plan%', '%advertising vacancies%', '%development of project website%', '%communication channels and project website%', '%action plan%', '%3rd newsletter%', '%website available%', '%h - requirement no. 2%', '%report on workshop 1%', '%innovation plan%', '%knowledge platform%', '%intermediate report%', '%interim evaluation report%', '%evaluation framework and demonstrators planning%', '%the european language technology landscape in 2020: language-centric and human-centric ai for cross-cultural communication in multilingual europe%', '%utilizing layout effects for analog logic locking%', '%quantum technologies in space%', '%fast traversability estimation for wild visual navigation%', '%packaging research artefacts with ro-crate%', '%project web presence (website, wiki, blog, social media)%', '%adapting term recognition to an under-resourced language: the case of irish%', '%archiving activism in the digital age%'
        ])
)
SELECT 
    clean_title,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    -- Show sample original titles to verify they're actual duplicates
    array_agg(DISTINCT original_title ORDER BY original_title) as sample_titles
FROM clean_titles
GROUP BY clean_title
HAVING COUNT(*) > 1
    AND COUNT(DISTINCT source_system) > 1  -- Only cross-source duplicates
ORDER BY duplicate_count DESC, sources_count DESC;

-- Havent tested yet, takes really long :c do not use for now. RO is not as important as the other right now.

-- =============================================================================
-- PROJECT DEDUPLICATION EXPLORATION  
-- =============================================================================

------------------------------
-- Project DOI Coverage and Duplicates
------------------------------

-- DOI Coverage across sources
SELECT
    source_system,
    COUNT(*) as total_projects,
    COUNT(doi) as projects_with_doi,
    ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) as doi_coverage_pct
FROM core.project
GROUP BY source_system
ORDER BY total_projects DESC;

-- source, total projects, with doi, pct
-- "cordis", 63583, 34270, 53.90
-- "openaire", 28373, 4815, 16.97

-- Cross-source DOI duplicates
SELECT 
    doi,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids
FROM core.project
WHERE doi IS NOT NULL 
    AND TRIM(doi) != ''
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, sources_count DESC;

-- 2135 duplicates with count 2. Do use!

------------------------------
-- Project Title + Acronym Analysis
------------------------------

-- Cross-source title duplicates
WITH clean_project_titles AS (
    SELECT 
        id,
        source_system,
        doi,
        LOWER(TRIM(title)) as clean_title,
        UPPER(TRIM(acronym)) as clean_acronym,
        title as original_title,
        acronym as original_acronym,
        start_date,
        end_date,
        total_cost
    FROM core.project
    WHERE title IS NOT NULL 
        AND LENGTH(TRIM(title)) > 5
)
SELECT 
    clean_title,
    clean_acronym,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    array_agg(DISTINCT original_title ORDER BY original_title) as sample_titles,
    array_agg(DISTINCT original_acronym ORDER BY original_acronym) as sample_acronyms
FROM clean_project_titles
GROUP BY clean_title, clean_acronym
HAVING COUNT(*) > 1
    AND COUNT(DISTINCT source_system) > 1  -- Only cross-source duplicates
ORDER BY duplicate_count DESC, sources_count DESC;

-- 2803 duplicates with count 2 each. Do use!

-- Project title duplicates (ignoring acronym in case acronyms differ)
WITH clean_project_titles AS (
    SELECT 
        id,
        source_system,
        doi,
        LOWER(TRIM(title)) as clean_title,
        UPPER(TRIM(acronym)) as clean_acronym,
        title as original_title,
        acronym as original_acronym,
        start_date,
        end_date,
        total_cost
    FROM core.project
    WHERE title IS NOT NULL 
        AND LENGTH(TRIM(title)) > 5
)
SELECT 
    clean_title,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    array_agg(DISTINCT original_title ORDER BY original_title) as sample_titles,
    array_agg(DISTINCT clean_acronym ORDER BY clean_acronym) as acronyms
FROM clean_project_titles
GROUP BY clean_title
HAVING COUNT(*) > 1
    AND COUNT(DISTINCT source_system) > 1
ORDER BY duplicate_count DESC, sources_count DESC;

-- 2828 duplicates with count 2 each. Do use!

-- =============================================================================
-- INSTITUTION DEDUPLICATION EXPLORATION
-- =============================================================================

------------------------------
-- Institution Legal Name Analysis
------------------------------

-- Cross-source legal name duplicates
WITH clean_institution_names AS (
    SELECT 
        id,
        source_system,
        LOWER(TRIM(legal_name)) as clean_legal_name,
        legal_name as original_legal_name,
        short_name,
        url,
        vat_number,
        city,
        country,
        country_code
    FROM core.institution
    WHERE legal_name IS NOT NULL 
        AND LENGTH(TRIM(legal_name)) > 3
        AND LOWER(TRIM(legal_name)) NOT IN ('university', 'institute', 'company')  -- Too generic
)
SELECT 
    clean_legal_name,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    array_agg(DISTINCT original_legal_name ORDER BY original_legal_name) as sample_names,
    array_agg(DISTINCT country ORDER BY country) as countries,
    array_agg(DISTINCT city ORDER BY city) as cities
FROM clean_institution_names
GROUP BY clean_legal_name
HAVING COUNT(*) > 1
    AND COUNT(DISTINCT source_system) > 1  -- Only cross-source duplicates
ORDER BY duplicate_count DESC, sources_count DESC;

-- 10812 duplicates with count 2 each. Do use!

------------------------------
-- Institution URL Analysis
------------------------------

-- URL-based duplicates (strong signal for same institution)
SELECT 
    url,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT source_system) as sources_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    array_agg(DISTINCT legal_name ORDER BY legal_name) as legal_names,
    array_agg(DISTINCT country ORDER BY country) as countries
FROM core.institution
WHERE url IS NOT NULL 
    AND TRIM(url) != ''
    AND url NOT ILIKE '%example%'  -- Filter out placeholder URLs
GROUP BY url
HAVING COUNT(*) > 1
    AND COUNT(DISTINCT source_system) > 1
ORDER BY duplicate_count DESC, sources_count DESC;

-- 1530, very useful. But more for getting departments. do not use.

------------------------------
-- Institution VAT Number Analysis (CORDIS only, but good for validation)
------------------------------

-- VAT Number duplicates (should indicate same legal entity)
SELECT 
    vat_number,
    COUNT(*) as duplicate_count,
    array_agg(DISTINCT source_system ORDER BY source_system) as sources,
    array_agg(id ORDER BY source_system, id) as record_ids,
    array_agg(DISTINCT legal_name ORDER BY legal_name) as legal_names,
    array_agg(DISTINCT country ORDER BY country) as countries
FROM core.institution
WHERE vat_number IS NOT NULL 
    AND TRIM(vat_number) != ''
    AND TRIM(vat_number) NOT IN ('MISSING', 'NOTAPPLICABLE', 'N/A', 'UNKNOWN')
GROUP BY vat_number
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 106 rows with count up to 11, but all just cordis. do not use.

-- =============================================================================
-- CROSS-TABLE DUPLICATE ANALYSIS
-- =============================================================================

------------------------------
-- Sample Records for Manual Verification
------------------------------

-- Show sample duplicate groups to manually verify our deduplication logic
SELECT 'RESEARCHOUTPUT DOI SAMPLE' as verification_type, doi as duplicate_key, source_system, title, publication_date
FROM core.researchoutput 
WHERE doi IN (
    SELECT doi FROM core.researchoutput 
    WHERE doi IS NOT NULL AND TRIM(doi) != ''
    GROUP BY doi 
    HAVING COUNT(*) > 1 AND COUNT(DISTINCT source_system) > 1 
    LIMIT 3
)

UNION ALL

SELECT 'PROJECT DOI SAMPLE' as verification_type, doi as duplicate_key, source_system, title, start_date::text as publication_date
FROM core.project 
WHERE doi IN (
    SELECT doi FROM core.project 
    WHERE doi IS NOT NULL AND TRIM(doi) != ''
    GROUP BY doi 
    HAVING COUNT(*) > 1 AND COUNT(DISTINCT source_system) > 1 
    LIMIT 3
)

UNION ALL

SELECT 'INSTITUTION URL SAMPLE' as verification_type, url as duplicate_key, source_system, legal_name as title, country as publication_date
FROM core.institution 
WHERE url IN (
    SELECT url FROM core.institution 
    WHERE url IS NOT NULL AND TRIM(url) != ''
    GROUP BY url 
    HAVING COUNT(*) > 1 AND COUNT(DISTINCT source_system) > 1 
    LIMIT 3
)
ORDER BY verification_type, duplicate_key, source_system;