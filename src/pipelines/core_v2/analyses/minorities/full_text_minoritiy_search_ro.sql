select count(*) from core.researchoutput
where title is not null
and abstract is not null;

-- -> Searching on 411714 RO's

SELECT *
FROM core.researchoutput
WHERE title IS NOT NULL 
  AND abstract IS NOT NULL
  AND (
    to_tsvector('english', title) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
    OR 
    to_tsvector('english', coalesce(abstract, '')) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
  );

-- 1. Simple one word Keyword search

-- ladin: 1 result
-- sami: 31 results
-- jewish: 98 results

-- 2. Multi Keyword search

-- ladin | ladino | gardenese | badiese | fascian | marebbano | ampezzan
-- -> 12 results
-- sami | saami | sapmi | same | joik | yoik
-- -> 43 results
-- jewish | jew | jews | judaism | hebrew | yiddish
-- -> 187 results