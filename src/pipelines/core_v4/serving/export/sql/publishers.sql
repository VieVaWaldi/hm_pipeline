-- D24: publisher typeahead list for the works filter: top 3,000 publishers by number of works (covers ~88% of works with a publisher).
-- Built from the CLEANED publisher values (hm_clean, D27) so the api list equals the indexed `works.publisher` keyword values exactly.
-- Raw variants that clean to the same string merge (e.g. `Wiley &amp; Sons` and `Wiley & Sons`): their counts are summed.
-- Held in the api's memory (like topics); the filter is an exact `term` on the keyword. No further normalisation of publisher variants in v1.
-- Group on the raw value first (310k distinct), clean only those, then merge: hm_clean is not run on 50M rows.
COPY (
SELECT publisher, sum(n)::BIGINT AS works
FROM (SELECT hm_clean(publisher) AS publisher, n FROM (SELECT publisher, count(*) AS n FROM work GROUP BY publisher))
WHERE publisher IS NOT NULL
GROUP BY publisher ORDER BY works DESC, publisher LIMIT 3000
) TO '{OUT}/api/publishers.json' (FORMAT json, ARRAY true);
