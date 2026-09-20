-- grants index docs: derived, one doc per fundingStream.id (e.g. EC::H2020::RIA -> funder EC, programme H2020, action RIA).
COPY (
WITH pf AS (
    SELECT p.id AS pid, p.is_ch, m.funded_amount_eur, len(p.fundings) AS n_fund, unnest(p.fundings) AS f
    FROM project p LEFT JOIN p_money m ON m.pid = p.id
)
SELECT f.fundingStream.id AS id,
       split_part(f.fundingStream.id, '::', 1) AS level1_funder,
       nullif(split_part(f.fundingStream.id, '::', 2), '') AS level2_programme,
       nullif(split_part(f.fundingStream.id, '::', 3), '') AS level3_action,
       any_value(f.fundingStream.description) AS description,
       any_value(f.shortName) AS funder_short, any_value(f.name) AS funder_name, any_value(f.jurisdiction) AS jurisdiction,
       count(DISTINCT pid)::INTEGER AS project_count,
       count(DISTINCT pid) FILTER (is_ch)::INTEGER AS dch_project_count,
       sum(funded_amount_eur / n_fund) AS total_funded_eur      -- PLACEHOLDER: project amount split equally over its funding entries
FROM pf WHERE f.fundingStream.id IS NOT NULL
GROUP BY f.fundingStream.id
) TO '__OUT__/grants.parquet' (FORMAT parquet, COMPRESSION zstd);
