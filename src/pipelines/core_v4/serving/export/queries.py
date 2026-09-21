"""API-side query building (what heritagemonitor/apps/api does against these indexes). Pure functions returning OpenSearch bodies.

Rules encoded here (SERVING_DESIGN.md): D6 query syntax + typo fallback, 10,000-result window and `track_total_hits` cap (UI shows "10,000+"),
D4 works corpus proxy, D19 name_key de-duplication, D33 rank_feature blending.
"""
import re
import time
from collections import defaultdict

TOTAL_CAP = 10_000   # track_total_hits: exact up to 10k, then relation "gte" -> UI "10,000+" (an exact count over 50M works is the expensive part)
MAX_WINDOW = 10_000  # index.max_result_window: from + size may not exceed it

# simple_query_string flags: AND(+) OR(|) NOT(-) PHRASE(") PRECEDENCE(()) ESCAPE(\) WHITESPACE.
# Deliberately NOT enabled: PREFIX(*), FUZZY(~N), SLOP("..."~N), NEAR -> no wildcard/fuzzy cost, no leading-wildcard blowups.
SQS_FLAGS = "AND|OR|NOT|PHRASE|PRECEDENCE|ESCAPE|WHITESPACE"


# ---- pagination / totals ---------------------------------------------------------------------------------------------
def page_window(page: int, size: int) -> tuple[int, int] | None:
    """1-based page -> (from, size) inside the 10k window; None if the page is beyond it (api: 400 / "refine your search")."""
    off = (max(page, 1) - 1) * size
    if off >= MAX_WINDOW:
        return None
    return off, min(size, MAX_WINDOW - off)


def max_page(size: int) -> int:
    return -(-MAX_WINDOW // size)


def total_of(resp: dict) -> tuple[int, bool]:
    """(value, capped): capped=True means 'at least value' (relation gte) -> show '10,000+'."""
    t = resp["hits"]["total"]
    return t["value"], t.get("relation") == "gte"


# ---- text query ------------------------------------------------------------------------------------------------------
def rewrite_query(q: str) -> str:
    """Google-like text -> simple_query_string syntax. Literal AND/OR/NOT outside quotes become + | -.

    - segments inside "..." are left untouched; an unbalanced trailing quote is dropped (would otherwise make the
      rest of the string one phrase)
    - `~N` (fuzzy/slop) is stripped: with those flags off, `word~2` would silently become `word AND 2`
    - `NOT x` -> `-x`; `a AND b` -> `a + b`; `a OR b` -> `a | b`; dangling operators at the ends are removed
    """
    q = q.strip()
    if q.count('"') % 2 == 1:
        i = q.rfind('"')
        q = q[:i] + q[i + 1:]
    parts = re.split(r'("[^"]*")', q)
    out = []
    for part in parts:
        if part.startswith('"') and part.endswith('"') and len(part) >= 2:
            out.append(part)
            continue
        part = re.sub(r"~\d*", "", part)
        part = re.sub(r"\bNOT\s+", "-", part)
        part = re.sub(r"\bAND\b", "+", part)
        part = re.sub(r"\bOR\b", "|", part)
        out.append(part)
    s = "".join(out)
    s = re.sub(r"^\s*[+|]\s*", "", s)
    s = re.sub(r"\s*[+|\-]\s*$", "", s)
    return re.sub(r"\s+", " ", s).strip()


def sqs(q: str, fields: list[str]) -> dict:
    rq = rewrite_query(q)
    if not rq:
        return {"match_all": {}}
    return {"simple_query_string": {"query": rq, "fields": fields, "default_operator": "AND", "flags": SQS_FLAGS,
                                    "lenient": True, "analyze_wildcard": False}}


PROJECT_FIELDS = ["acronym^5", "title^3", "summary", "keywords", "grantId", "org_names^0.5"]
WORK_FIELDS = ["title^3", "authors", "container_name^0.5"]
ORG_FIELDS = ["legalName^3", "legalShortName^2", "alternativeNames"]
GRANT_FIELDS = ["description", "id"]
MINORITY_FIELDS = ["group_name_en^5", "search_keywords^4", "native_languages^2", "countries^2", "religions", "subclass_of",
                   "project_title_blob"]


# ---- filters ---------------------------------------------------------------------------------------------------------
def _terms(f: list[dict], pairs) -> None:
    for name, val in pairs:
        if val:
            f.append({"terms": {name: val if isinstance(val, list) else [val]}})


def project_filters(*, corpus: str | None = None, year: tuple[int, int] | None = None, theme=None, pillar=None, topic=None,
                    field=None, funder=None, programme=None, stream=None, region=None, minority=None, has_minority=False,
                    org=None) -> list[dict]:
    f: list[dict] = []
    if corpus == "DCH":
        f.append({"term": {"is_ch": True}})
    if year:
        f.append({"range": {"year": {"gte": year[0], "lte": year[1]}}})
    _terms(f, (("theme", theme), ("pillar_list", pillar), ("topic_id", topic), ("field_id", field), ("funder", funder),
               ("programme", programme), ("funding_stream_ids", stream), ("org_regions", region), ("minority_qids", minority),
               ("org_ids", org)))
    if has_minority:
        f.append({"exists": {"field": "minority_qids"}})
    return f


def work_filters(*, corpus: str | None = None, year: tuple[int, int] | None = None, oa=None, language=None, publisher=None,
                 project=None, org=None, minority=None) -> list[dict]:
    """D4/D4b: corpus DCH = is_ch_via_project (proxy, only works with a DCH project). D18: year, OA colour, language, publisher."""
    f: list[dict] = []
    if corpus == "DCH":
        f.append({"term": {"is_ch_via_project": True}})
    if year:
        f.append({"range": {"year": {"gte": year[0], "lte": year[1]}}})
    _terms(f, (("open_access_color", oa), ("language", language), ("publisher", publisher), ("project_ids", project),
               ("organisation_ids", org), ("minority_qids", minority)))
    return f


def org_filters(*, corpus: str | None = None, region=None, ror_type=None, country=None, has_geo=False) -> list[dict]:
    f: list[dict] = []
    if corpus == "DCH":
        f.append({"term": {"has_dch_project": True}})
    _terms(f, (("region", region), ("rorTypes", ror_type), ("countryCode", country)))
    if has_geo:
        f.append({"exists": {"field": "geo"}})
    return f


# ---- bodies ----------------------------------------------------------------------------------------------------------
def projects_body(q="", *, size=10, offset=0, sort=None, aggs=None, **filters) -> dict:
    body = {"track_total_hits": TOTAL_CAP, "size": size, "from": offset,
            "query": {"bool": {"must": sqs(q, PROJECT_FIELDS), "filter": project_filters(**filters)}}}
    if sort == "budget":
        body["sort"] = [{"funded_amount_eur": {"order": "desc", "missing": "_last"}}, "_score"]
    if aggs:
        body["aggs"] = aggs
    return body


def works_body(q="", *, size=10, offset=0, sort=None, **filters) -> dict:
    body = {"track_total_hits": TOTAL_CAP, "size": size, "from": offset,
            "query": {"bool": {"must": sqs(q, WORK_FIELDS), "filter": work_filters(**filters)}}}
    body["sort"] = [{"citation_count": "desc"}] if sort == "citations" else ["_score", {"citation_count": "desc"}]
    return body


ORG_SORTS = {"funding": "total_funding_eur", "projects": "project_count", "works": "work_count"}


def orgs_body(q="", *, size=10, offset=0, sort=None, **filters) -> dict:
    """Ranking (use case 1): explicit sort = funding > projects > works (plain doc values, outliers do not matter for a sort);
    default with a text query = BM25 + rank_feature log blend (D33), so a giant org does not drown an exact name match."""
    bool_q: dict = {"must": sqs(q, ORG_FIELDS), "filter": org_filters(**filters)}
    body = {"track_total_hits": TOTAL_CAP, "size": size, "from": offset, "query": {"bool": bool_q}}
    if sort in ORG_SORTS:
        body["sort"] = [{ORG_SORTS[sort]: "desc"}, {"project_count": "desc"}, {"work_count": "desc"}]
    elif not q.strip():
        body["sort"] = [{"total_funding_eur": "desc"}, {"project_count": "desc"}, {"work_count": "desc"}]   # blank page default
    else:
        bool_q["should"] = [{"rank_feature": {"field": "rank_projects", "log": {"scaling_factor": 1.0}, "boost": 0.5}}]
    return body


def org_autocomplete_body(prefix: str, size: int = 8) -> dict:
    """Type-ahead over legalName / short name / alternative names, ranked by number of projects (log, not linear)."""
    return {"size": size, "_source": ["legalName", "legalShortName", "countryCode", "project_count", "name_key"],
            "query": {"bool": {
                "must": {"multi_match": {"query": prefix, "type": "bool_prefix",
                                         "fields": ["legalName.sayt", "legalName.sayt._2gram", "legalName.sayt._3gram",
                                                    "legalShortName.sayt", "legalShortName.sayt._2gram",
                                                    "alternativeNames.sayt", "alternativeNames.sayt._2gram"]}},
                "should": [{"rank_feature": {"field": "rank_projects", "log": {"scaling_factor": 1.0}}}]}}}


def project_autocomplete_body(prefix: str, size: int = 8) -> dict:
    return {"size": size, "_source": ["acronym", "title"],
            "query": {"multi_match": {"query": prefix, "type": "bool_prefix",
                                      "fields": ["acronym.sayt^3", "acronym.sayt._2gram^3", "title.sayt", "title.sayt._2gram", "title.sayt._3gram"]}}}


def topic_modal_aggs() -> dict:
    """Corpus-aware topic tree counts: run with filter is_ch for DCH, cache per corpus in the api. Names come from api memory."""
    return {"t": {"terms": {"field": "topic_id", "size": 5000}}, "s": {"terms": {"field": "subfield_id", "size": 500}},
            "f": {"terms": {"field": "field_id", "size": 100}}, "d": {"terms": {"field": "domain_id", "size": 10}}}


def terms_agg(field: str, size: int, shard_size: int | None = None, **kw) -> dict:
    """terms agg for facets/networks with an EXPLICIT shard_size. With 1 shard (projects, since 2026-09-21) counts are exact and shard_size is a no-op; with 2+ shards (works: 4, or projects if it is ever raised) the default shard_size (size*1.5+10) makes the
    counts approximate: measured on the real-data sample, 5 of 50 topic counts were 1-2 too low; shard_size 500 made them exact (error bound 0).
    The extra shard work is tiny (a few hundred buckets per shard). Use this helper for every facet / experts / network / funding aggregation."""
    return {"terms": {"field": field, "size": size, "shard_size": shard_size or max(size * 10, 500), **kw}}


# ---- experts / networks / funding (aggregations over projects) -------------------------------------------------------
def experts_aggs(size: int = 200) -> dict:
    return {"orgs": terms_agg("org_ids", size, order={"_count": "desc"})}


def org_network_body(org_id: str, size: int = 500) -> dict:
    return {"size": 0, "query": {"term": {"org_ids": org_id}}, "aggs": {"partners": terms_agg("org_ids", size + 1)}}


def query_network_body(q: str, max_projects: int = 2000, **filters) -> dict:
    """Query network (5.2): the top-N matching projects, org_ids only. docvalue_fields, no _source. The cost is the per-hit fetch of stored fields
    (`_id`): ~0.13 ms/hit with best_compression, ~0.017 ms/hit with the default codec projects use (SERVING_DESIGN.md section 6). The api builds the
    edges from these lists (cap orgs per project, cap max_edges) and never sends pair lists over the wire."""
    return {"size": max_projects, "_source": False, "docvalue_fields": ["org_ids"], "track_total_hits": False,
            "query": {"bool": {"must": sqs(q, PROJECT_FIELDS), "filter": project_filters(**filters)}}}


def funding_aggs(size: int = 500) -> dict:
    """Funding map: per-org sum of the equal-split EUR share over the projects that match the query + filters (D10/D13)."""
    return {"orgs": {**terms_agg("org_ids", size, shard_size=max(size * 4, 2000), order={"funding": "desc"}),
                     "aggs": {"funding": {"sum": {"field": "funded_eur_per_org"}}}}}


def merge_by_name_key(buckets: list[dict], org_docs: dict[str, dict], center: dict | None = None) -> list[dict]:
    """D19: duplicate institutions (same normalised name + country under several ids). Sum their counts under one entry (the biggest id) and,
    for a network around `center`, drop partners that are the center institution itself (fake self-collaboration)."""
    groups: dict[str, dict] = defaultdict(lambda: {"doc_count": 0, "ids": []})
    for b in buckets:
        d = org_docs.get(b["key"])
        key = d["name_key"] if d and d.get("name_key") else b["key"]
        if center and key == center.get("name_key"):
            continue
        g = groups[key]
        g["doc_count"] += b["doc_count"]
        g["ids"].append(b["key"])
        g.setdefault("name", d.get("legalName") if d else None)
    return sorted(({"name_key": k, **g} for k, g in groups.items()), key=lambda g: -g["doc_count"])


# ---- typo tolerance (D6): strict first, fuzzy match fallback, "did you mean" -----------------------------------------------
# threshold: fall back when the strict query has fewer hits. max_expansions/timeout bound the fuzzy cost (works is the expensive one on HDD).
TYPO = {"projects": dict(threshold=5, max_expansions=20, timeout="2s"),
        "organisations": dict(threshold=5, max_expansions=20, timeout="2s"),
        "works": dict(threshold=3, max_expansions=20, timeout="1500ms")}


def split_terms(q: str) -> tuple[str, list[str]]:
    """(positive words, negated words) from the rewritten query: operators/quotes/parens dropped, -term kept as a negation."""
    rq = rewrite_query(q)
    neg = re.findall(r'(?:^|\s)-(\w[\w\-]*)', rq)
    pos = re.sub(r'(?:^|\s)-\w[\w\-]*', " ", rq)
    pos = re.sub(r'[+|()"\\]', " ", pos)
    return re.sub(r"\s+", " ", pos).strip(), neg


def fuzzy_query(q: str, fields: list[str], max_expansions: int = 20) -> dict:
    """Fallback: every positive word must match (AND) with fuzziness AUTO (0 edits <=2 chars, 1 edit 3-5, 2 edits >5), prefix_length 2
    (first two chars must be right: cuts the candidate terms a lot), capped max_expansions. Negations stay strict."""
    pos, neg = split_terms(q)
    if not pos:
        return {"match_all": {}}
    must = {"multi_match": {"query": pos, "fields": fields, "type": "best_fields", "operator": "and", "fuzziness": "AUTO",
                            "prefix_length": 2, "max_expansions": max_expansions, "fuzzy_transpositions": True}}
    if not neg:
        return must
    return {"bool": {"must": must, "must_not": [{"multi_match": {"query": n, "fields": fields}} for n in neg]}}


def suggest_block(q: str, field: str) -> dict:
    """'Did you mean': per-word term suggester on the unstemmed name/title field (the phrase suggester fails on the sayt fields)."""
    pos, _ = split_terms(q)
    return {"text": pos, "did_you_mean": {"term": {"field": field, "suggest_mode": "missing", "min_word_length": 4, "prefix_length": 2, "size": 3}}}


def search_typo_tolerant(es, index: str, q: str, fields: list[str], filters: list[dict], *, threshold: int = 5, size: int = 10,
                         extra: dict | None = None, suggest_field: str | None = None, max_expansions: int = 20,
                         timeout: str | None = None) -> dict:
    """Strict simple_query_string first; if fewer than `threshold` hits, rerun as fuzzy (AND, prefix_length 2, capped, with a body timeout so
    a slow fallback returns partial results instead of hanging). Returns hits + timings + did-you-mean."""
    base = {"track_total_hits": TOTAL_CAP, "size": size, **(extra or {})}
    t = time.time()
    r = es.search(index=index, body={**base, "query": {"bool": {"must": sqs(q, fields), "filter": filters}}})
    strict_ms = (time.time() - t) * 1000
    out = {"mode": "strict", "total": r["hits"]["total"]["value"], "strict_ms": strict_ms, "fuzzy_ms": None, "hits": r["hits"]["hits"],
           "suggest": None, "timed_out": False}
    if out["total"] < threshold and q.strip():
        t = time.time()
        body = {**base, "query": {"bool": {"must": fuzzy_query(q, fields, max_expansions), "filter": filters}}}
        if timeout:
            body["timeout"] = timeout
        if suggest_field:
            body["suggest"] = suggest_block(q, suggest_field)
        r2 = es.search(index=index, body=body)
        out.update(mode="fuzzy", total=r2["hits"]["total"]["value"], fuzzy_ms=(time.time() - t) * 1000, hits=r2["hits"]["hits"],
                   timed_out=bool(r2.get("timed_out")))
        if suggest_field:
            out["suggest"] = [o["text"] for e in r2["suggest"]["did_you_mean"] for o in e["options"]]
    return out
