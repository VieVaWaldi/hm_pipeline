"""Index settings + mappings (prototype). Index names carry the `proto_` prefix locally; drop the prefix for prod.

Conventions: raw columns keep their DuckDB names (camelCase), derived fields are snake_case.
Ids are strings (UBIGINT > signed long). Only text that users search is `text`; everything filtered/faceted is `keyword`.
"""

PREFIX = "proto_"

# HDD notes from heritagemonitor/infra/PRODUCTION.md; laptop-sized shards. Prod: see PROTOTYPE_REPORT.md.
def settings(shards: int = 1, refresh: str = "30s") -> dict:
    return {
        "number_of_shards": shards,
        "number_of_replicas": 0,
        "refresh_interval": refresh,
        "codec": "best_compression",
        "merge.scheduler.max_thread_count": 1,
        "translog.flush_threshold_size": "1gb",
        "analysis": {
            "filter": {"en_stem": {"type": "stemmer", "language": "light_english"}},
            "analyzer": {
                # searched prose (title, summary, keywords): folded + light stemming
                "hm_text": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding", "en_stem"]},
                # names / acronyms: folded, no stemming
                "hm_name": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"]},
            },
        },
    }


KW = {"type": "keyword"}
KW_STORED = {"type": "keyword", "index": False, "doc_values": False}  # only shown, never searched/aggregated
TEXT = {"type": "text", "analyzer": "hm_text"}
NAME = {"type": "text", "analyzer": "hm_name"}
SAYT = {"type": "search_as_you_type", "analyzer": "hm_name"}          # autocomplete (adds 3 shingle subfields)
TEXT_KW = {"type": "text", "analyzer": "hm_name", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
INT = {"type": "integer"}
DBL = {"type": "double"}
BOOL = {"type": "boolean"}
DATE = {"type": "date"}
OBJ_OFF = {"type": "object", "enabled": False}                      # kept in _source only

PROJECTS = {
    "dynamic": "strict",
    "properties": {
        "id": KW, "openaireId": KW_STORED, "grantId": KW, "doi": KW_STORED,
        "title": {**TEXT, "fields": {"sayt": SAYT}},
        "acronym": {**NAME, "fields": {"sayt": SAYT, "keyword": {"type": "keyword", "ignore_above": 64}}},
        "summary": TEXT, "keywords": TEXT, "subjects": KW_STORED,
        "websiteUrl": KW_STORED, "callIdentifier": KW,
        "startDate": DATE, "endDate": {**DATE, "index": False}, "year": INT,
        "openAccessMandateForPublications": {**BOOL, "index": False}, "openAccessMandateForDataset": {**BOOL, "index": False},
        "fundings": OBJ_OFF, "frameworkProgrammes": KW,
        "currency": KW, "funded_amount": DBL, "total_cost": {**DBL, "index": False},
        "funded_amount_eur": DBL, "funded_eur_per_org": DBL,
        "is_translated": BOOL, "is_ch": BOOL, "pred": {"type": "float"},
        "minority_qids": KW, "pillars": {"type": "byte"}, "pillar_list": KW, "theme": KW,
        "topic_id": KW, "subfield_id": KW, "field_id": KW, "domain_id": KW,
        "org_ids": KW, "org_names": {"type": "text", "analyzer": "hm_name", "norms": False},  # positions kept: phrase search on institution names
        "org_regions": KW, "org_countries": KW, "coordinator_id": KW, "org_count": INT, "work_count": INT,
        "funder_short": KW, "funder_names": KW, "funding_stream_ids": KW,
    },
}

ORGANISATIONS = {
    "dynamic": "strict",
    "properties": {
        "id": KW, "openaireId": KW_STORED,
        "legalName": {**NAME, "fields": {"sayt": SAYT}},
        "legalShortName": {**NAME, "fields": {"sayt": SAYT}},
        "alternativeNames": {**NAME, "fields": {"sayt": SAYT}},
        "websiteUrl": KW_STORED, "countryCode": KW, "rorId": KW, "wikiId": KW_STORED,
        "pids": OBJ_OFF, "rorStatus": KW, "rorEstablished": {**INT, "index": False}, "rorTypes": KW,
        "rorLocations": KW_STORED, "rorRelationships": KW_STORED,
        "geo": {"type": "geo_point"}, "geolocation_source": KW,
        "address_street": KW_STORED, "address_postalcode": KW_STORED, "address_city": NAME, "address_country": KW,
        "nuts3": KW, "region": KW,
        "project_count": INT, "work_count": INT, "dch_project_count": INT, "has_dch_project": BOOL, "total_funding_eur": DBL,
    },
}

WORKS = {
    "dynamic": "strict",
    "properties": {
        "id": KW_STORED,  # get-by-id / mget use _id; indexing the copy costs ~58 B/doc (~3 GB at 50M)
        "title": TEXT,
        "authors": {**NAME, "index_options": "positions"}, "author_count": {**INT, "index": False},
        "publication_date": {**DATE, "index": False}, "year": INT,
        "publisher": KW, "container_name": {**NAME, "norms": False},
        "open_access_color": KW, "best_access_right": KW, "language": KW,
        "citation_count": INT, "doi": KW,
        "pdf_url": KW_STORED, "landing_url": KW_STORED,
        "project_ids": KW, "organisation_ids": KW, "link_tier": {"type": "byte"},
    },
}

MINORITIES = {
    "dynamic": "strict",
    # the blob is searchable but never returned
    "_source": {"excludes": ["project_title_blob"]},
    "properties": {
        "qid": KW, "merged_qids": KW,
        "group_name_en": {**TEXT_KW, "fields": {**TEXT_KW["fields"], "sayt": SAYT}},
        "countries": TEXT_KW, "source_class": KW, "population": DBL,
        "religions": TEXT_KW, "native_languages": TEXT_KW, "subclass_of": TEXT_KW,
        "part_of": KW_STORED, "diaspora": KW_STORED, "has_parts": KW_STORED,
        "ancestral_home": KW, "admin_territory": KW,
        "known_subgroups": {"type": "nested", "properties": {"name": NAME, "qid": KW}},
        "search_keywords": NAME, "has_subgroups": BOOL, "is_seed": BOOL,
        "project_count": INT, "dch_project_count": INT, "org_count": INT,
        "topic_ids": KW, "topic_counts": OBJ_OFF,
        "project_title_blob": TEXT,
    },
}

GRANTS = {
    "dynamic": "strict",
    "properties": {
        "id": KW, "level1_funder": KW, "level2_programme": KW, "level3_action": KW,
        "description": {**TEXT, "fields": {"sayt": SAYT}},
        "funder_short": KW, "funder_name": {**NAME, "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
        "jurisdiction": KW, "project_count": INT, "dch_project_count": INT, "total_funded_eur": DBL,
    },
}

# name -> (mapping, shards for the local run)
INDICES = {
    "projects": (PROJECTS, 1),
    "organisations": (ORGANISATIONS, 1),
    "works": (WORKS, 1),
    "minorities": (MINORITIES, 1),
    "grants": (GRANTS, 1),
}
