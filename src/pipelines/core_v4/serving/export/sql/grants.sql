-- grants index docs: derived, one doc per funding stream (D7/D20). id = cleaned fundingStream.id, or 'NONE::<funder>' for the funder-level
-- pseudo stream of fundings without a stream id. funder = fundings.shortName. Deviation from the design doc's "1 bucket": one per funder.
COPY (
SELECT stream_id AS id, any_value(funder) AS funder, any_value(programme) AS programme, any_value(action) AS action,
       any_value(description) AS description, any_value(funder_name) AS funder_name, any_value(jurisdiction) AS jurisdiction,
       stream_id LIKE 'NONE::%' AS is_pseudo,
       count(DISTINCT pf.pid)::INTEGER AS project_count,
       (count(DISTINCT pf.pid) FILTER (pf.is_ch))::INTEGER AS dch_project_count,
       sum(m.funded_amount_eur / pf.n_fund) AS total_funded_eur    -- project amount split equally over its funding entries
FROM p_fundings pf LEFT JOIN p_money m ON m.pid = pf.pid
GROUP BY stream_id
ORDER BY stream_id
) TO '{OUT}/grants/grants.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
