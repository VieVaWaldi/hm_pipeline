select count(*) from core.project
where title is not null
and objective is not null;

-- Searching on 69,947 projects

SELECT id, acronym, title, objective as description
FROM core.project
WHERE title IS NOT NULL 
  AND objective IS NOT NULL
  AND (
    to_tsvector('english', title) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
    OR 
    to_tsvector('english', coalesce(objective, '')) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
  );

-- 1. Simple one word Keyword search

-- ladin: 1 result -> Only DIGICHer
-- sami: 8 results
-- jewish: 148 results

-- 2. Multi Keyword search

-- ladin | ladino | gardenese | badiese | fascian | marebbano | ampezzan
-- -> 8 results
-- sami | sámi | saami | sapmi | same | joik | yoik
-- -> 16 results
-- jewish | jew | jews | judaism | hebrew | yiddish
-- -> 224 results