"""API-side query building for the prototype (what apps/api would do). Pure functions returning OpenSearch bodies."""
import re

# simple_query_string flags: AND(+) OR(|) NOT(-) PHRASE(") PRECEDENCE(()) ESCAPE(\) WHITESPACE.
# Deliberately NOT enabled: PREFIX(*), FUZZY(~N), SLOP("..."~N), NEAR -> no wildcard/fuzzy cost, no leading-wildcard blowups.
SQS_FLAGS = "AND|OR|NOT|PHRASE|PRECEDENCE|ESCAPE|WHITESPACE"


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
        # fuzzy/slop suffixes (~2, "a b"~3) are disabled via flags but would leave a stray "2" as an extra AND term
        part = re.sub(r"~\d*", "", part)
        part = re.sub(r"\bNOT\s+", "-", part)
        part = re.sub(r"\bAND\b", "+", part)
        part = re.sub(r"\bOR\b", "|", part)
        out.append(part)
    s = "".join(out)
    s = re.sub(r"^\s*[+|]\s*", "", s)   # dangling leading operator
    s = re.sub(r"\s*[+|\-]\s*$", "", s)  # dangling trailing operator
    return re.sub(r"\s+", " ", s).strip()


def sqs(q: str, fields: list[str]) -> dict:
    rq = rewrite_query(q)
    if not rq:
        return {"match_all": {}}
    return {"simple_query_string": {"query": rq, "fields": fields, "default_operator": "AND", "flags": SQS_FLAGS,
                                    "lenient": True, "analyze_wildcard": False}}


PROJECT_FIELDS = ["acronym^5", "title^3", "summary", "keywords", "grantId", "org_names^0.5"]
WORK_FIELDS = ["title^3", "authors", "container_name^0.5"]
MINORITY_FIELDS = ["group_name_en^5", "search_keywords^4", "native_languages^2", "countries^2", "religions", "subclass_of",
                   "project_title_blob"]


def project_filters(*, corpus: str | None = None, year: tuple[int, int] | None = None, theme=None, pillar=None, topic=None,
                    field=None, funder=None, programme=None, stream=None, region=None, minority=None, has_minority=False, org=None) -> list[dict]:
    f: list[dict] = []
    if corpus == "DCH":
        f.append({"term": {"is_ch": True}})
    if year:
        f.append({"range": {"year": {"gte": year[0], "lte": year[1]}}})
    for name, val in (("theme", theme), ("pillar_list", pillar), ("topic_id", topic), ("field_id", field),
                      ("funder", funder), ("programme", programme), ("funding_stream_ids", stream), ("org_regions", region),
                      ("minority_qids", minority), ("org_ids", org)):
        if val:
            f.append({"terms": {name: val if isinstance(val, list) else [val]}})
    if has_minority:
        f.append({"exists": {"field": "minority_qids"}})
    return f


def projects_body(q="", *, size=10, offset=0, sort=None, aggs=None, **filters) -> dict:
    body = {"track_total_hits": True, "size": size, "from": offset,
            "query": {"bool": {"must": sqs(q, PROJECT_FIELDS), "filter": project_filters(**filters)}}}
    if sort == "budget":
        body["sort"] = [{"funded_amount_eur": {"order": "desc", "missing": "_last"}}, "_score"]
    if aggs:
        body["aggs"] = aggs
    return body


# ---- typo tolerance (D6): strict first, fuzzy match fallback, "did you mean" -----------------------------------------------
def split_terms(q: str) -> tuple[str, list[str]]:
    """(positive words, negated words) from the rewritten query: operators/quotes/parens dropped, -term kept as a negation."""
    rq = rewrite_query(q)
    neg = re.findall(r'(?:^|\s)-(\w[\w\-]*)', rq)
    pos = re.sub(r'(?:^|\s)-\w[\w\-]*', " ", rq)
    pos = re.sub(r'[+|()"\\]', " ", pos)
    return re.sub(r"\s+", " ", pos).strip(), neg


def fuzzy_query(q: str, fields: list[str], max_expansions: int = 20) -> dict:
    """Fallback query: every positive word must match (AND) with fuzziness AUTO (0 edits <=2 chars, 1 edit 3-5, 2 edits >5),
    prefix_length 2 (first two chars must be right: cuts the candidate terms a lot), capped max_expansions. Negations stay strict."""
    pos, neg = split_terms(q)
    if not pos:
        return {"match_all": {}}
    must = {"multi_match": {"query": pos, "fields": fields, "type": "best_fields", "operator": "and", "fuzziness": "AUTO",
                            "prefix_length": 2, "max_expansions": max_expansions, "fuzzy_transpositions": True}}
    if not neg:
        return must
    return {"bool": {"must": must, "must_not": [{"multi_match": {"query": n, "fields": fields}} for n in neg]}}


def suggest_block(q: str, field: str) -> dict:
    """'Did you mean' on the (unstemmed) name/title field. term suggester per word: cheap; phrase suggester tried in the tests."""
    pos, _ = split_terms(q)
    return {"text": pos, "did_you_mean": {"term": {"field": field, "suggest_mode": "missing", "min_word_length": 4, "prefix_length": 2, "size": 3}}}


def search_typo_tolerant(es, index: str, q: str, fields: list[str], filters: list[dict], *, threshold: int = 5, size: int = 10,
                         extra: dict | None = None, suggest_field: str | None = None, max_expansions: int = 20) -> dict:
    """Strict simple_query_string first; if fewer than `threshold` hits, rerun as fuzzy (AND, prefix_length 2). Returns hits + info."""
    import time
    base = {"track_total_hits": True, "size": size, **(extra or {})}
    t = time.time()
    r = es.search(index=index, body={**base, "query": {"bool": {"must": sqs(q, fields), "filter": filters}}})
    strict_ms = (time.time() - t) * 1000
    out = {"mode": "strict", "total": r["hits"]["total"]["value"], "strict_ms": strict_ms, "fuzzy_ms": None, "hits": r["hits"]["hits"], "suggest": None}
    if out["total"] < threshold and q.strip():
        t = time.time()
        body = {**base, "query": {"bool": {"must": fuzzy_query(q, fields, max_expansions), "filter": filters}}}
        if suggest_field:
            body["suggest"] = suggest_block(q, suggest_field)
        r2 = es.search(index=index, body=body)
        out.update(mode="fuzzy", total=r2["hits"]["total"]["value"], fuzzy_ms=(time.time() - t) * 1000, hits=r2["hits"]["hits"])
        if suggest_field:
            out["suggest"] = [o["text"] for e in r2["suggest"]["did_you_mean"] for o in e["options"]]
    return out
