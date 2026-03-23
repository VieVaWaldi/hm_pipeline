select confidence from core.topicoa;

select * from core.project where source_system = 'openaire' and objective is not null;

select p.source_system, p.title, t.topic_name, jpt.score from core.project as p
inner join core.j_project_topicoa as jpt on jpt.project_id = p.id
inner join core.topicoa as t on jpt.topic_id = t.id
where p.source_system != 'cordis';

select count(*) from core.researchoutput where abstract is not null;

select count(*) from core.researchoutput as p
inner join core.j_researchoutput_topicoa as jpt on jpt.researchoutput_id = p.id
inner join core.topicoa as t on jpt.topic_id = t.id;

-- Gotta add this by hand

select distinct domain_name from core.topicoa;
-- 4516 rows with unique topic_name
-- 245 unique subfield_names
-- 26 unique field_names
-- 4 unique domain_names

-----------------------------------------------
-- Topic
-----------------------------------------------

-- Precreated OpenAlex Topics
CREATE TABLE core.topicoa (
    id SERIAL PRIMARY KEY,
    subfield_id TEXT NOT NULL,
    field_id TEXT NOT NULL,
    domain_id TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    subfield_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    domain_name TEXT NOT NULL,
    keywords TEXT NOT NULL,
    summary TEXT NOT NULL,
    wikipedia_url TEXT,
    is_ch BOOL NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Topicoa Junction to Project
CREATE TABLE core.j_project_topicoa (
    project_id TEXT,
    topic_id INTEGER,
	score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, topic_id)
);

CREATE TABLE core.j_researchoutput_topicoa (
    researchoutput_id TEXT,
    topic_id INTEGER,
	score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (researchoutput_id, topic_id)
);

-----------------------------------------------
-- Topics and RO sorted with confidence and TF score
-----------------------------------------------

SELECT 
    t.topic_name,
    t.is_ch,
    t.confidence,
    ro.publication_date,
    ro.title,
    ro.abstract,
    jrt.score
FROM core.topicoa t
INNER JOIN LATERAL (
    SELECT jrt.researchoutput_id, jrt.score
    FROM core.j_researchoutput_topicoa jrt
    WHERE jrt.topic_id = t.id
    ORDER BY jrt.score DESC
    LIMIT 10
) jrt ON true
INNER JOIN core.researchoutput ro ON ro.id = jrt.researchoutput_id
WHERE t.confidence > 0.59
  AND t.is_ch = false
ORDER BY t.confidence DESC, t.topic_name, ro.publication_date DESC, jrt.score DESC;