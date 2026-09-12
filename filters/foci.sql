------------------------------
-- Foci Topic Selection

select * from topic 
where topic_name = 'Wine Industry and Tourism';

select field_id, field_name, subfield_id, subfield_name, id, topic_name from topic
ORDER BY field_id, subfield_id, id;

select distinct topic_name from topic;

select distinct subfield_id, subfield_name from topic
where subfield_id
order by subfield_id;



--

-- 1.

select distinct field_id, field_name from topic;

-- 2. 

select distinct subfield_id, subfield_name from topic
where field_id != '20'
order by subfield_id;

-- 3. 

select id, topic_name from topic
where field_id != '20'
and  subfield_id != '1409'
order by id;

------------------------------
-- Economy

-- Searching for keyword 19600 results

-- Fields
-- 20, Economics, Econometrics and Finance

-- 45009 results

-- SubFields
-- None

-- Topics
-- 12033, Agricultural Economics and Policy
-- 14219, Local Economic Development and Planning
-- 12312, Culture, Economy, and Development Studies
-- 14352, Globalization and Economic Impact | Cant find this in webapp, but its in the DB
-- 14433, Post-Communist Economic and Political Transition

-- 1144 results without Field 20

-- 0 results with Field 20 and selected topics in webapp | Probably a bug in the webapp ...
-- A hack would be to just ignore the 1k extra results

------------------------------
-- Tourism

-- Searching for keyword tourism: 2804 results

-- Fields
-- None

-- SubFields
-- 1409, Tourism, Leisure and Hospitality Management
-- 476 results

-- Topics
-- 10055, Diverse Aspects of Tourism Research
-- 11793, Recreation, Leisure, Wilderness Management
-- 11925, Culinary Culture and Tourism
-- 12399, Wine Industry and Tourism
-- 12456, Geotourism and Geoheritage Conservation
-- 12584, Hospitality and Tourism Education
-- 12402, Travel-related health issues
-- 11474, Sport and Mega-Event Impacts
-- 11410, Cultural Industries and Urban Development

-- 3415 results without SubField 1409

-- 0 results with Field 20 and selected topics in webapp | Probably a bug in the webapp ...
-- A hack would be to simply add the subfields topics: 

-- 12584, Hospitality and Tourism Education
-- 12399, Wine Industry and Tourism

-- 3891 results with the 2 additional topics