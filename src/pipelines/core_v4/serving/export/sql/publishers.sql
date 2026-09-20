-- D24: publisher typeahead list for the works filter: top 3,000 publishers by number of works (covers ~88% of works with a publisher).
-- Held in the api's memory (like topics); the filter is an exact `term` on works.publisher. Only exact-string values, no normalisation.
COPY (
SELECT publisher, count(*)::BIGINT AS works
FROM work WHERE publisher IS NOT NULL AND publisher <> ''
GROUP BY publisher ORDER BY works DESC, publisher LIMIT 3000
) TO '{OUT}/api/publishers.json' (FORMAT json, ARRAY true);
