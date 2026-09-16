-----------------------------------------------
-- Refresh all Mat Views                     --
-----------------------------------------------

CREATE SCHEMA IF NOT EXISTS core_mats;

REFRESH MATERIALIZED VIEW core_mats.collaboration_network_view;
REFRESH MATERIALIZED VIEW core_mats.collaboration_by_topic;
REFRESH MATERIALIZED VIEW core_mats.institution_view;