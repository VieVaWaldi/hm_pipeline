-- DOI

-- PROJECT Dois and paper per year
SELECT 
    EXTRACT(YEAR FROM start_date) AS year,
    COUNT(*) AS total_projects,
    COUNT(doi) AS projects_with_doi,
    COUNT(*) - COUNT(doi) AS projects_without_doi,
    ROUND(
        (COUNT(doi)::DECIMAL / COUNT(*)) * 100, 
        2
    ) AS pct_with_doi
FROM core.project
WHERE start_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM start_date)
ORDER BY year;

-- RO Dois and paper per year
SELECT 
    EXTRACT(YEAR FROM publication_date) AS year,
    COUNT(*) AS total_ro,
    COUNT(doi) AS ro_with_doi,
    COUNT(*) - COUNT(doi) AS ro_without_doi,
    ROUND(
        (COUNT(doi)::DECIMAL / COUNT(*)) * 100, 
        2
    ) AS pct_with_doi
FROM core.researchoutput
WHERE publication_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM publication_date)
ORDER BY year;


SELECT * FROM core.project limit 10;
SELECT * FROM core.researchoutput limit 10;

SELECT language_code, COUNT(*)
FROM core.researchoutput
GROUP by language_code
order by count(*) DESC;

