"""Final index settings + mappings (JSON copies live in mappings/*.json, regenerate with `python mappings.py --dump`).

Conventions: raw columns keep their DuckDB names (camelCase), derived fields are snake_case. Ids are strings (UBIGINT > signed long).
Only text users search is `text`; everything filtered/faceted is `keyword`; shown-only fields are `index: false`. `dynamic: strict`
everywhere, so a column added to an export SQL without a mapping fails loudly at load time.
Decision numbers (D..) refer to ../SERVING_DESIGN.md.
"""
import argparse
import json
from pathlib import Path

# prod shard plan (D-topology): works 4 (HDD: fewer shards = fewer random reads, still 4 threads per query), projects 1 (user decision 2026-09-21: 3.9M docs / ~8 GB is fine
# for one Lucene shard and every facet/terms count is EXACT with a single shard; fallback if the aggs are too slow on the VM: projects=2 + shard_size 500), rest 1.
# The laptop test uses 1 everywhere (--shards works=1,projects=1 in load.py).
PROD_SHARDS = {"works": 4, "projects": 1, "organisations": 1, "minorities": 1, "grants": 1}
# title.sayt (project title autocomplete, user wants it): max_shingle_size 3 -> 2 shrinks it, see SERVING_DESIGN.md section 6 for the measurement
TITLE_SHINGLE = 3


# Stored-field codec per index. Measured on real project data (200k slice, 2000-hit fetch of the query network): best_compression 255 ms,
# default (LZ4) 33 ms for +17% disk, so everything the api fetches in bulk (projects, organisations mget) uses the default codec. Only
# works (26+ GB, fetched 10-20 docs per page) keeps best_compression: smaller index = more of it in the page cache on the HDD VM.
CODEC = {"works": "best_compression"}


# HDD notes from heritagemonitor/infra/PRODUCTION.md.
def settings(shards: int = 1, refresh: str = "30s", codec: str = "default") -> dict:
    return {
        "number_of_shards": shards,
        "number_of_replicas": 0,
        "refresh_interval": refresh,
        "codec": codec,
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
TEXT_KW = {"type": "text", "analyzer": "hm_name", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
INT = {"type": "integer"}
DBL = {"type": "double"}
BOOL = {"type": "boolean"}
DATE = {"type": "date"}
OBJ_OFF = {"type": "object", "enabled": False}                      # kept in _source only
RANK = {"type": "rank_feature"}                                     # D33: must be > 0, NULL in the export when 0


def sayt(max_shingle_size: int = 3) -> dict:
    """search_as_you_type autocomplete field (adds prefix + shingle subfields)."""
    return {"type": "search_as_you_type", "analyzer": "hm_name", "max_shingle_size": max_shingle_size}


def projects(title_shingle: int = TITLE_SHINGLE) -> dict:
    return {
        "dynamic": "strict",
        "properties": {
            "id": KW, "openaireId": KW_STORED, "grantId": KW, "doi": KW_STORED,
            "title": {**TEXT, "fields": {"sayt": sayt(title_shingle)}},
            "acronym": {**NAME, "fields": {"sayt": sayt(3), "keyword": {"type": "keyword", "ignore_above": 64}}},
            "summary": TEXT, "keywords": TEXT, "subjects": KW_STORED,
            "websiteUrl": KW_STORED, "callIdentifier": KW,
            "startDate": DATE, "endDate": {**DATE, "index": False}, "year": INT,
            "openAccessMandateForPublications": {**BOOL, "index": False}, "openAccessMandateForDataset": {**BOOL, "index": False},
            "fundings": OBJ_OFF, "frameworkProgrammes": KW,
            "currency": KW, "funded_amount": DBL, "funded_amount_eur": DBL, "funded_eur_per_org": DBL,   # D13/D14/D21
            "is_translated": BOOL, "is_ch": BOOL,
            "pred": {"type": "float", "index": False, "doc_values": False},   # D8/D31: display-only, no filter/agg/sort
            "minority_qids": KW, "pillars": {"type": "byte"}, "pillar_list": KW, "theme": KW,
            "topic_id": KW, "subfield_id": KW, "field_id": KW, "domain_id": KW,      # nullable (D29)
            "org_ids": {**KW, "eager_global_ordinals": True},   # aggregated by experts / org network / funding map: build the global ordinals at refresh, not on the first query
            "org_names": {"type": "text", "analyzer": "hm_name", "norms": False},     # positions kept: phrase search on institution names
            "org_regions": KW, "org_countries": KW, "coordinator_ids": KW,           # D30
            "org_count": INT, "work_count": INT,
            "funder": KW, "programme": KW, "funder_names": KW, "funding_stream_ids": KW,   # D20 facets
        },
    }


ORGANISATIONS = {
    "dynamic": "strict",
    "properties": {
        "id": KW, "openaireId": KW_STORED,
        "legalName": {**NAME, "fields": {"sayt": sayt(3)}},
        "legalShortName": {**NAME, "fields": {"sayt": sayt(3)}},
        "alternativeNames": {**NAME, "fields": {"sayt": sayt(3)}},
        "websiteUrl": KW_STORED, "countryCode": KW, "rorId": KW, "wikiId": KW_STORED,
        "pids": OBJ_OFF, "rorStatus": KW, "rorEstablished": {**INT, "index": False}, "rorTypes": KW,   # 'unknown' bucket (D26)
        "rorLocations": KW_STORED, "rorRelationships": KW_STORED,
        "geo": {"type": "geo_point"}, "geolocation_source": KW,
        "address_street": KW_STORED, "address_postalcode": KW_STORED, "address_city": NAME, "address_country": KW,
        "nuts3": KW, "region": KW,                                                                       # 'Unknown' bucket (D26)
        "name_key": KW,                                                                                  # D19: duplicate institutions
        "project_count": INT, "work_count": INT, "dch_project_count": INT, "has_dch_project": BOOL, "total_funding_eur": DBL,
        "rank_projects": RANK, "rank_works": RANK, "rank_funding": RANK,                                 # D33
    },
}

WORKS = {
    "dynamic": "strict",
    "properties": {
        "id": KW_STORED,  # get-by-id / mget use _id; indexing the copy costs ~58 B/doc (~3 GB at 50M)
        "title": TEXT,
        "authors": {**NAME, "norms": False, "index_options": "positions"}, "author_count": {**INT, "index": False},
        "publication_date": {**DATE, "index": False}, "year": INT,
        "publisher": KW, "container_name": {**NAME, "norms": False},
        "open_access_color": KW, "best_access_right": KW, "language": KW,     # language normalised (D23)
        "citation_count": INT, "doi": KW,
        "pdf_url": KW_STORED, "landing_url": KW_STORED,                        # D5/D25
        "project_ids": KW, "organisation_ids": KW, "org_count": {**INT, "index": False}, "link_tier": {"type": "byte"},
        "is_ch_via_project": BOOL,   # proxy (D4): any linked project is_ch; never a classification of the work itself
        "minority_qids": KW,         # proxy (D4b): union over linked projects
    },
}

MINORITIES = {
    "dynamic": "strict",
    "_source": {"excludes": ["project_title_blob"]},   # the blob is searchable but never returned
    "properties": {
        "qid": KW, "merged_qids": KW,
        "group_name_en": {**TEXT_KW, "fields": {**TEXT_KW["fields"], "sayt": sayt(3)}},
        "countries": TEXT_KW, "source_class": KW, "population": DBL,
        "religions": TEXT_KW, "native_languages": TEXT_KW, "subclass_of": TEXT_KW,
        "part_of": KW_STORED, "diaspora": KW_STORED, "has_parts": KW_STORED,
        "ancestral_home": KW, "admin_territory": KW,
        "known_subgroups": {"type": "nested", "properties": {"name": NAME, "qid": KW}},
        "search_keywords": NAME, "has_subgroups": BOOL, "is_seed": BOOL,
        "project_count": INT, "dch_project_count": INT, "org_count": INT, "work_count": INT,
        "topic_ids": KW, "topic_counts": OBJ_OFF,
        "project_title_blob": TEXT,
    },
}

GRANTS = {
    "dynamic": "strict",
    "properties": {
        "id": KW, "funder": KW, "programme": KW, "action": KW,   # D20: same two facets as projects
        "description": {**TEXT, "fields": {"sayt": sayt(3)}},
        "funder_name": {**NAME, "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
        "jurisdiction": KW, "is_pseudo": BOOL,                    # is_pseudo: 'NONE::<funder>' = fundings without a stream id
        "project_count": INT, "dch_project_count": INT, "total_funded_eur": DBL,
    },
}


def mapping_for(name: str, title_shingle: int = TITLE_SHINGLE) -> dict:
    return {"projects": projects(title_shingle), "organisations": ORGANISATIONS, "works": WORKS,
            "minorities": MINORITIES, "grants": GRANTS}[name]


INDEX_NAMES = list(PROD_SHARDS)


def parse_shards(spec: str | None) -> dict[str, int]:
    """'works=1,projects=1' -> overrides on top of PROD_SHARDS."""
    out = dict(PROD_SHARDS)
    for part in (spec or "").split(","):
        if part.strip():
            k, v = part.split("=")
            out[k.strip()] = int(v)
    return out


def dump(out_dir: Path) -> None:
    out_dir.mkdir(exist_ok=True)
    for name in INDEX_NAMES:
        body = {"settings": settings(PROD_SHARDS[name], codec=CODEC.get(name, "default")), "mappings": mapping_for(name)}
        (out_dir / f"{name}.json").write_text(json.dumps(body, indent=2) + "\n")
        print("wrote", out_dir / f"{name}.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", action="store_true", help="write mappings/<index>.json (prod shard counts)")
    if ap.parse_args().dump:
        dump(Path(__file__).parent / "mappings")
