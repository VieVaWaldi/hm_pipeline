CREATE MATERIALIZED VIEW table_view_work AS                                                                                                                                                                                                                                             
SELECT DISTINCT ON (w.id)                                                                                                                                                                                                                                                               
w.id,
w.title,                                                                                                                                                                                                                                                                              
w."publicationDate"        AS publication_date,
w.publisher,
w."openAccessColor"        AS open_access_color,
w."citationCount"          AS citation_count,                                                                                                                                                                                                                                         
w.influence,
-- pre-extract DOI (currently an inline subquery on every API call)                                                                                                                                                                                                                   
(                                                                                                                                                                                                                                                                                     
  SELECT p->>'value'
  FROM jsonb_array_elements(w.pids::jsonb) AS p                                                                                                                                                                                                                                       
  WHERE p->>'scheme' = 'doi'                                                                                                                                                                                                                                                          
  LIMIT 1
) AS doi,                                                                                                                                                                                                                                                                             
-- pre-compute FTS vector (avoids re-computing on every search)
setweight(to_tsvector('english', COALESCE(w.title, '')), 'A') ||                                                                                                                                                                                                                      
setweight(to_tsvector('english', COALESCE(array_to_string(w.descriptions, ' '), '')), 'B')                                                                                                                                                                                            
  AS search_vector,                                                                                                                                                                                                                                                                   
-- topic classification (from relation_topic + topic, best score wins via DISTINCT ON)                                                                                                                                                                                                
rt.topic_id,                                                                                                                                                                                                                                                                          
t.subfield_id,                                                                                                                                                                                                                                                                        
t.field_id,                                                                                                                                                                                                                                                                           
t.domain_id   
FROM work w
LEFT JOIN relation_topic rt ON rt.source_id = w.id AND rt.type = 'work'
LEFT JOIN topic t ON t.id = rt.topic_id                                                                                                                                                                                                                                                 
ORDER BY w.id, rt.score DESC;
																																																																					  
-- Index for FTS                                                                                                                                                                                                                                                                        
CREATE INDEX ON table_view_work USING GIN (search_vector);

CREATE INDEX ON table_view_work USING GIN (search_vector);
  FROM jsonb_array_elements(w.pids::jsonb) AS p
  WHERE p->>'scheme' = 'doi'
  LIMIT 1
) AS doi,
-- pre-compute FTS vector (avoids re-computing on every search)
setweight(to_tsvector('english', COALESCE(w.title, '')), 'A') ||
setweight(to_tsvector('english', COALESCE(array_to_string(w.descriptions, ' '), '')), 'B')
  AS search_vector,
-- topic classification (from relation_topic + topic, best score wins via DISTINCT ON)
rt.topic_id,
t.subfield_id,
t.field_id,
t.domain_id
FROM work w
LEFT JOIN relation_topic rt ON rt.source_id = w.id AND rt.type = 'work'
LEFT JOIN topic t ON t.id = rt.topic_id
ORDER BY w.id, rt.score DESC;