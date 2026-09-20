"""Exercise every use case of SERVING_DESIGN.md section 5 against the proto_ indices. Prints PASS/FAIL + timing + notes.

    .venv-serving/bin/python test_usecases.py [--parquet data/serving_proto]
Test data is picked dynamically from the Parquet exports (no hard-coded ids), assertions cross-check OpenSearch against DuckDB.
"""
import argparse
import collections
import itertools
import json
import re
import sys
import time
import traceback
from pathlib import Path

import duckdb

from load import client
from mappings import PREFIX
from queries import (MINORITY_FIELDS, PROJECT_FIELDS, WORK_FIELDS, projects_body, rewrite_query, sqs)

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", default="data/serving_proto")
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9201)
ARGS = ap.parse_args()
P = Path(ARGS.parquet)
es = client(ARGS.host, ARGS.port)
db = duckdb.connect()
pq = lambda n: f"read_parquet('{P / (n + '.parquet')}')"  # noqa: E731
IX = {n: PREFIX + n for n in ["projects", "organisations", "works", "minorities", "grants"]}

# api-side static table (topics are not an index): id -> names, held in memory
TOPICS = {r[0]: r for r in db.execute(f"select id, topic_name, subfield_id, subfield_name, field_id, field_name, domain_id, domain_name from {pq('topics')}").fetchall()}

RESULTS: list[tuple[str, bool, float, str]] = []


def search(index, body):
    t = time.time()
    r = es.search(index=IX[index], body=body)
    return r, (time.time() - t) * 1000


def total(r):
    return r["hits"]["total"]["value"]


def kb(obj) -> int:
    return len(json.dumps(obj, default=str).encode())


def case(fn):
    t = time.time()
    try:
        note = fn() or ""
        ok = True
    except Exception as e:  # noqa: BLE001
        ok, note = False, f"{type(e).__name__}: {e}\n{traceback.format_exc(limit=2)}"
    RESULTS.append((fn.__name__, ok, (time.time() - t) * 1000, note))
    print(f"{'PASS' if ok else 'FAIL'}  {fn.__name__:34s} {(time.time() - t) * 1000:7.0f} ms  {note}")


# ---- test data ---------------------------------------------------------------------------------------------------
STOP = set("the and for with from into that this using based new towards through their which project research study development".split())
words = collections.Counter(w for (t,) in db.execute(f"select lower(title) from {pq('projects')}").fetchall()
                            for w in set(re.findall(r"[a-z]{5,}", t)) if w not in STOP)
WORD, WORD2 = [w for w, _ in words.most_common(2)]
TOP_ORG = db.execute(f"select id, legalName, project_count, work_count from {pq('organisations')} order by project_count desc limit 1").fetchone()
print(f"test words: {WORD!r}, {WORD2!r}; top org: {TOP_ORG}")


def all_ids(index, body, size=10000):
    body = {**body, "size": size, "_source": False}
    r = es.search(index=IX[index], body=body)
    return [h["_id"] for h in r["hits"]["hits"]]


# ---- tests -------------------------------------------------------------------------------------------------------
@case
def rewrite_unit():
    cases = {
        "photogrammetry AND heritage preservation -consumer": "photogrammetry + heritage preservation -consumer",
        "a NOT b": "a -b", '"a AND b" AND c': '"a AND b" + c', 'unbalanced "quote': "unbalanced quote",
        "trailing OR": "trailing", "word~2": "word", '"a b"~3': '"a b"', "AND leading": "leading", "(a OR b) AND c": "(a | b) + c",
    }
    for q, exp in cases.items():
        assert rewrite_query(q) == exp, (q, rewrite_query(q), exp)
    return f"{len(cases)} rewrites ok"


@case
def query_syntax_semantics():
    n = lambda q: total(search("projects", {"track_total_hits": True, "size": 0, "query": sqs(q, PROJECT_FIELDS)})[0])  # noqa: E731
    a, b, both, neg = n(WORD), n(WORD2), n(f"{WORD} AND {WORD2}"), n(f"{WORD} -{WORD2}")
    assert both <= min(a, b) and neg == a - both, (a, b, both, neg)          # AND narrows, negation is exact complement
    assert n(f"{WORD} {WORD2}") == both                                       # default operator AND
    either = n(f"{WORD} OR {WORD2}")
    assert either == a + b - both, (either, a, b, both)
    assert n(f"{WORD} NOT {WORD2}") == neg                                    # literal NOT rewritten
    pref = WORD[:4]
    assert n(pref + "*") == n(pref), "wildcard must NOT expand (PREFIX flag off)"
    assert n(f"{WORD}~2") == a, "fuzzy must NOT be active (FUZZY flag off)"
    phrase = db.execute(f"select regexp_extract(lower(title), '([a-z]+ [a-z]+)', 1) from {pq('projects')} where title is not null limit 1").fetchone()[0]
    assert n(f'"{phrase}"') <= n(phrase), "phrase narrows"
    # garbage must never raise
    for g in ['"unbalanced', "a:b", "(", ")", "*", "-", "+", "~", "\\", "((a)", 'a "b', "field:*", "AND", "OR OR NOT", "-" * 50, "a" * 3000]:
        n(g)
    # for contrast: raw query_string throws on the same input
    try:
        es.search(index=IX["projects"], body={"query": {"query_string": {"query": 'a AND (', "fields": ["title"]}}})
        raw = "query_string did NOT throw"
    except Exception as e:  # noqa: BLE001
        raw = f"query_string raises {type(e).__name__} on 'a AND (' (why we use simple_query_string)"
    return f"a={a} b={b} and={both} or={either} neg={neg}; wildcard off; garbage ok; {raw}"


@case
def projects_search_facets():
    aggs = {"topics": {"terms": {"field": "topic_id", "size": 50, "order": {"_count": "desc"}}},
            "years": {"date_histogram": {"field": "startDate", "calendar_interval": "year", "format": "yyyy", "min_doc_count": 1}},
            "regions": {"terms": {"field": "org_regions", "size": 10}}, "orgs": {"stats": {"field": "org_count"}},
            "themes": {"terms": {"field": "theme", "size": 10}}, "pillars": {"terms": {"field": "pillar_list", "size": 5}}}
    r, ms = search("projects", projects_body(WORD, aggs=aggs, year=(2010, 2030)))
    ids = all_ids("projects", projects_body(WORD, year=(2010, 2030)))
    assert len(ids) == total(r)
    idl = ",".join(ids) or "0"
    duck = dict(db.execute(f"select topic_id, count(*) from {pq('projects')} where id in ({','.join(repr(i) for i in ids) or 'null'}) group by 1").fetchall())
    got = {b["key"]: b["doc_count"] for b in r["aggregations"]["topics"]["buckets"]}
    assert all(duck[k] == v for k, v in got.items()), "topic agg != duckdb"
    counts = [b["doc_count"] for b in r["aggregations"]["topics"]["buckets"]]
    assert counts == sorted(counts, reverse=True)
    r2, ms2 = search("projects", projects_body(WORD, sort="budget", year=(2010, 2030), theme=None, corpus="DCH"))
    return f"{total(r)} hits, {len(got)} topics exact vs duckdb, {ms:.0f}ms; budget-sort+DCH {total(r2)} hits {ms2:.0f}ms"


@case
def project_overview_by_id():
    pid = db.execute(f"select id from {pq('projects')} where is_ch and pred is not null and len(fundings)>0 limit 1").fetchone()[0]
    d = es.get(index=IX["projects"], id=pid)["_source"]
    need = {"openaireId", "pred", "is_translated", "fundings", "summary", "org_ids", "topic_id", "minority_qids"}
    assert need <= set(d), need - set(d)
    return f"all overview fields present ({len(d)} fields, {kb(d)} bytes _source)"


@case
def project_to_works_tab():
    pid, wc = db.execute(f"select id, work_count from {pq('projects')} order by work_count desc limit 1").fetchone()
    r, ms = search("works", {"track_total_hits": True, "size": 20, "query": {"term": {"project_ids": pid}},
                             "sort": [{"citation_count": "desc"}], "_source": ["title", "pdf_url", "landing_url", "citation_count"]})
    assert total(r) == wc, (total(r), wc)
    return f"project {pid}: {wc} works == work_count, {ms:.0f}ms"


@case
def org_to_works_tab():
    oid, wc = db.execute(f"select id, work_count from {pq('organisations')} order by work_count desc limit 1").fetchone()
    r, ms = search("works", {"track_total_hits": True, "size": 20, "query": {"term": {"organisation_ids": oid}}, "sort": [{"citation_count": "desc"}]})
    assert total(r) == wc, (total(r), wc)
    return f"org {oid}: {wc} works == work_count, {ms:.0f}ms"


@case
def org_to_projects_tab():
    oid, pc = TOP_ORG[0], TOP_ORG[2]
    r, ms = search("projects", {"track_total_hits": True, "size": 10, "query": {"term": {"org_ids": oid}}, "sort": [{"funded_amount_eur": {"order": "desc", "missing": "_last"}}]})
    assert total(r) == pc, (total(r), pc)
    return f"{pc} projects == project_count, {ms:.0f}ms"


@case
def works_search_and_pdf():
    w = db.execute(f"select regexp_extract(lower(title), '([a-z]{{5,}})', 1) from {pq('works')} where pdf_url is not null limit 1").fetchone()[0]
    r, ms = search("works", {"track_total_hits": True, "size": 10, "query": {"bool": {"must": sqs(f"{w} -zzzzzz", WORK_FIELDS), "filter": [{"range": {"year": {"gte": 2000}}}]}},
                             "sort": ["_score", {"citation_count": "desc"}], "_source": ["title", "authors", "pdf_url", "landing_url", "open_access_color", "citation_count"]})
    hits = [h["_source"] for h in r["hits"]["hits"]]
    assert hits and all("landing_url" in h for h in hits)
    ai = es.search(index=IX["works"], body={"query": {"exists": {"field": "pdf_url"}}, "size": 1})  # _source-only field still filterable via exists?
    return f"{w!r}: {total(r)} hits {ms:.0f}ms; {sum(1 for h in hits if h.get('pdf_url'))}/{len(hits)} rows with pdf_url; exists(pdf_url)={total(ai)} (index:false => 0 expected)"


@case
def works_dch_proxy():
    r, ms = search("works", {"track_total_hits": True, "size": 5, "query": {"bool": {"filter": [{"term": {"is_ch_via_project": True}}]}}, "_source": ["title", "project_ids", "link_tier"]})
    exp = db.execute(f"select count(*) from {pq('works')} where is_ch_via_project").fetchone()[0]
    assert total(r) == exp and all(h["_source"]["link_tier"] == 0 for h in r["hits"]["hits"])
    # cross-check against projects: every hit has >=1 linked project with is_ch
    pids = {p for h in r["hits"]["hits"] for p in h["_source"]["project_ids"]}
    ok = db.execute(f"select count(*) from {pq('projects')} where id in ({','.join(repr(p) for p in pids)}) and is_ch").fetchone()[0]
    assert ok >= 1
    t1 = db.execute(f"select count(*) from {pq('works')} where link_tier=1 and is_ch_via_project").fetchone()[0]
    assert t1 == 0
    return f"{total(r)} DCH-proxy works (tier 0 only, tier-1 = 0) == duckdb, {ms:.0f}ms"


@case
def org_autocomplete():
    name = TOP_ORG[1]
    tokens = name.split()
    for prefix in (name[:4], " ".join(tokens[:2])[:-1] if len(tokens) > 1 else name[:5]):
        body = {"size": 5, "_source": ["legalName", "project_count"],
                "query": {"function_score": {
                    "query": {"multi_match": {"query": prefix, "type": "bool_prefix",
                                              "fields": ["legalName.sayt", "legalName.sayt._2gram", "legalName.sayt._3gram",
                                                         "legalShortName.sayt", "legalShortName.sayt._2gram", "alternativeNames.sayt", "alternativeNames.sayt._2gram"]}},
                    "field_value_factor": {"field": "project_count", "modifier": "log2p", "missing": 0}, "boost_mode": "multiply"}}}
        r, ms = search("organisations", body)
        names = [h["_source"]["legalName"] for h in r["hits"]["hits"]]
        assert TOP_ORG[1] in names[:3], (prefix, names)
    # short name / acronym
    sn = db.execute(f"select legalShortName from {pq('organisations')} where legalShortName is not null order by project_count desc limit 1").fetchone()[0]
    r, _ = search("organisations", {"size": 3, "query": {"multi_match": {"query": sn, "type": "bool_prefix", "fields": ["legalShortName.sayt", "legalShortName.sayt._2gram"]}}})
    assert total(r) > 0
    return f"prefix {name[:4]!r} -> top-3 {names[:3]} ({ms:.0f}ms); acronym {sn!r} ok"


@case
def project_autocomplete():
    a = db.execute(f"select acronym from {pq('projects')} where acronym is not null limit 1").fetchone()[0]
    r, ms = search("projects", {"size": 5, "_source": ["acronym", "title"], "query": {"multi_match": {"query": a[:3], "type": "bool_prefix",
                    "fields": ["acronym.sayt", "acronym.sayt._2gram", "title.sayt", "title.sayt._2gram"]}}})
    assert any(h["_source"].get("acronym") == a for h in r["hits"]["hits"]), [h["_source"] for h in r["hits"]["hits"]]
    return f"{a[:3]!r} -> {total(r)} suggestions {ms:.0f}ms"


@case
def experts():
    r, ms = search("projects", {"size": 0, "query": sqs(WORD, PROJECT_FIELDS),
                                "aggs": {"orgs": {"terms": {"field": "org_ids", "size": 200, "order": {"_count": "desc"}}}}})
    buckets = r["aggregations"]["orgs"]["buckets"]
    t = time.time()
    docs = es.mget(index=IX["organisations"], body={"ids": [b["key"] for b in buckets]}, _source=["legalName", "project_count", "total_funding_eur", "region"])["docs"]
    mg = (time.time() - t) * 1000
    by = {d["_id"]: d["_source"] for d in docs if d["found"]}
    ranked = sorted(buckets, key=lambda b: (-b["doc_count"], -by[b["key"]]["project_count"]))
    # verify top bucket vs duckdb
    ids = all_ids("projects", {"query": sqs(WORD, PROJECT_FIELDS)})
    exp = db.execute(f"select count(*) from {pq('projects')} where id in ({','.join(repr(i) for i in ids)}) and list_contains(org_ids, '{ranked[0]['key']}')").fetchone()[0]
    assert exp == ranked[0]["doc_count"]
    # score-ordered variant: scripted max of _score
    try:
        r2, ms2 = search("projects", {"size": 0, "query": sqs(WORD, PROJECT_FIELDS), "aggs": {"orgs": {
            "terms": {"field": "org_ids", "size": 50, "order": {"best": "desc"}},
            "aggs": {"best": {"max": {"script": {"source": "_score"}}}}}}})
        sc = f"scripted max(_score) ordering works ({ms2:.0f}ms)"
    except Exception as e:  # noqa: BLE001
        sc = f"scripted _score ordering FAILS: {str(e)[:120]}"
    return f"{total(r) if 'hits' in r else len(ids)} projects -> {len(buckets)} orgs, agg {ms:.0f}ms + mget {mg:.0f}ms; {sc}"


@case
def org_network():
    x = TOP_ORG[0]
    body = {"size": 0, "query": {"term": {"org_ids": x}},
            "aggs": {"partners": {"terms": {"field": "org_ids", "size": 500, "exclude": [x]}}}}
    r, ms = search("projects", body)
    partners = r["aggregations"]["partners"]["buckets"]
    t = time.time()
    docs = es.mget(index=IX["organisations"], body={"ids": [x] + [b["key"] for b in partners]}, _source=["legalName", "geo", "project_count", "countryCode"])["docs"]
    mg = (time.time() - t) * 1000
    by = {d["_id"]: d["_source"] for d in docs if d["found"]}
    nodes = [{"id": k, "name": v["legalName"], "lat": v["geo"]["lat"], "lng": v["geo"]["lon"]} for k, v in by.items() if v.get("geo")]
    payload = {"center": x, "nodes": nodes, "edges": [{"to": b["key"], "w": b["doc_count"]} for b in partners if by.get(b["key"], {}).get("geo")]}
    y = partners[0]["key"]
    r2, ms2 = search("projects", {"track_total_hits": True, "size": 3, "_source": ["title", "acronym"], "query": {"bool": {"filter": [{"term": {"org_ids": x}}, {"term": {"org_ids": y}}]}}})
    assert total(r2) == partners[0]["doc_count"]
    return f"{len(partners)} partners, {len(nodes)} with geo, agg {ms:.0f}ms + mget {mg:.0f}ms, payload {kb(payload)} B; lazy pair query {ms2:.0f}ms"


@case
def query_network():
    max_projects, max_edges = 2000, 1000
    t = time.time()
    r, ms = search("projects", {"size": max_projects, "_source": ["org_ids"], "query": sqs(WORD, PROJECT_FIELDS)})
    pairs = collections.Counter()
    for h in r["hits"]["hits"]:
        ids = sorted(set(h["_source"]["org_ids"]))[:40]        # cap orgs/project: k*(k-1)/2 blows up for big consortia
        pairs.update(itertools.combinations(ids, 2))
    pre = len(pairs)
    top = pairs.most_common(max_edges)
    node_ids = sorted({i for (a, b), _ in top for i in (a, b)})
    docs = es.mget(index=IX["organisations"], body={"ids": node_ids}, _source=["legalName", "geo"])["docs"]
    idx = {d["_id"]: i for i, d in enumerate(docs)}
    payload = {"nodes": [{"id": d["_id"], "name": d["_source"]["legalName"]} for d in docs if d["found"]],
               "edges": [[idx[a], idx[b], w] for (a, b), w in top]}
    return f"{len(r['hits']['hits'])} projects -> {pre} distinct pairs, capped to {len(top)} edges / {len(node_ids)} nodes, payload {kb(payload)} B, {(time.time() - t) * 1000:.0f}ms total (search {ms:.0f}ms)"


@case
def funding_map():
    body = projects_body(WORD, size=0, aggs={"orgs": {"terms": {"field": "org_ids", "size": 500, "order": {"funding": "desc"}},
                                                        "aggs": {"funding": {"sum": {"field": "funded_eur_per_org"}}}}})
    r, ms = search("projects", body)
    b = r["aggregations"]["orgs"]["buckets"]
    ids = all_ids("projects", {"query": sqs(WORD, PROJECT_FIELDS)})
    exp = db.execute(f"select sum(funded_eur_per_org) from {pq('projects')} where id in ({','.join(repr(i) for i in ids)}) and list_contains(org_ids, '{b[0]['key']}')").fetchone()[0]
    assert abs(exp - b[0]["funding"]["value"]) < 1e-3 * max(1, exp), (exp, b[0])
    # global consistency: empty query, per-org agg sum == organisations.total_funding_eur rollup (same attribution rule)
    r2, ms2 = search("projects", {"size": 0, "query": {"match_all": {}}, "aggs": {"orgs": {"terms": {"field": "org_ids", "size": 1000, "order": {"funding": "desc"}}, "aggs": {"funding": {"sum": {"field": "funded_eur_per_org"}}}}}})
    top = r2["aggregations"]["orgs"]["buckets"][0]
    roll = es.get(index=IX["organisations"], id=top["key"])["_source"]["total_funding_eur"]
    assert abs(roll - top["funding"]["value"]) < 1e-3 * max(1, roll)
    docs = es.mget(index=IX["organisations"], body={"ids": [x["key"] for x in b]}, _source=["legalName", "geo"])["docs"]
    cols = [{"id": d["_id"], "lat": d["_source"]["geo"]["lat"], "lng": d["_source"]["geo"]["lon"], "v": x["funding"]["value"]}
            for d, x in zip(docs, b) if d["found"] and d["_source"].get("geo")]
    return f"{len(b)} orgs, {len(cols)} columns w/ geo, {ms:.0f}ms (all-projects agg {ms2:.0f}ms), payload {kb(cols)} B; agg sum == rollup == duckdb"


@case
def minorities_text_and_institution():
    q, w = db.execute(f"""select p.minority_qids[1], regexp_extract(lower(p.title), '([a-z]{{7,}})', 1) from {pq('projects')} p
                          where len(p.minority_qids)=1 and regexp_extract(lower(p.title), '([a-z]{{7,}})', 1) <> '' limit 1""").fetchone()
    r, ms = search("minorities", {"size": 20, "_source": ["qid", "group_name_en", "project_count"], "query": sqs(w, MINORITY_FIELDS)})
    found = [h["_source"]["qid"] for h in r["hits"]["hits"]]
    # two-step alternative: projects text/institution search -> terms agg minority_qids -> mget minorities
    t = time.time()
    r2 = es.search(index=IX["projects"], body={"size": 0, "query": {"bool": {"must": sqs(w, PROJECT_FIELDS), "filter": [{"exists": {"field": "minority_qids"}}]}},
                                              "aggs": {"m": {"terms": {"field": "minority_qids", "size": 300}}}})
    two = [b["key"] for b in r2["aggregations"]["m"]["buckets"]]
    ms2 = (time.time() - t) * 1000
    assert q in found, f"blob search missed {q} for word {w!r}: {found}"
    assert q in two
    inst = db.execute(f"select any_value(org_names[1]), minority_qids[1] from {pq('projects')} where len(minority_qids)>0 and len(org_names)>0 group by minority_qids[1] limit 1").fetchone()
    r3, _ = search("minorities", {"size": 50, "query": sqs(f'"{inst[0]}"', MINORITY_FIELDS)})
    blob_inst = inst[1] in [h["_source"]["qid"] for h in r3["hits"]["hits"]]
    r4 = es.search(index=IX["projects"], body={"size": 0, "query": {"bool": {"must": sqs(f'"{inst[0]}"', ["org_names"]), "filter": [{"exists": {"field": "minority_qids"}}]}},
                                              "aggs": {"m": {"terms": {"field": "minority_qids", "size": 300}}}})
    two_inst = inst[1] in [b["key"] for b in r4["aggregations"]["m"]["buckets"]]
    assert two_inst, "two-step institution search must find the minority"
    return (f"title word {w!r}: blob finds {q} ({len(found)} hits, {ms:.0f}ms), two-step finds it ({len(two)} minorities, {ms2:.0f}ms); "
            f"institution {inst[0]!r}: blob finds={blob_inst}, two-step finds={two_inst}")


@case
def minorities_facets_and_map():
    r, ms = search("minorities", {"size": 10, "query": {"match_all": {}}, "sort": [{"is_seed": "desc"}, {"project_count": "desc"}],
                                  "aggs": {"topics": {"terms": {"field": "topic_ids", "size": 20}}, "countries": {"terms": {"field": "countries.keyword", "size": 20}},
                                           "types": {"terms": {"field": "source_class", "size": 10}}, "lang": {"terms": {"field": "native_languages.keyword", "size": 10}}}})
    assert r["aggregations"]["topics"]["buckets"]
    t = time.time()
    m = es.search(index=IX["projects"], body={"size": 0, "query": {"bool": {"filter": [{"exists": {"field": "minority_qids"}}]}},
                                             "aggs": {"m": {"terms": {"field": "minority_qids", "size": 100}, "aggs": {"o": {"terms": {"field": "org_ids", "size": 30}}}}}})
    pairs = {(mb["key"], ob["key"]) for mb in m["aggregations"]["m"]["buckets"] for ob in mb["o"]["buckets"]}
    oids = sorted({o for _, o in pairs})
    docs = es.mget(index=IX["organisations"], body={"ids": oids}, _source=["geo"])["docs"]
    geo = {d["_id"]: d["_source"]["geo"] for d in docs if d["found"] and d["_source"].get("geo")}
    payload = [{"q": q, "o": o, **geo[o]} for q, o in pairs if o in geo]
    return f"facets {ms:.0f}ms; map: {len(pairs)} (minority,org) pairs -> {len(payload)} with geo, {(time.time() - t) * 1000:.0f}ms, payload {kb(payload)} B"


@case
def topic_modal_corpus():
    def counts(corpus):
        body = {"size": 0, "query": {"bool": {"filter": [{"term": {"is_ch": True}}] if corpus == "DCH" else []}},
                "aggs": {"t": {"terms": {"field": "topic_id", "size": 5000}}, "s": {"terms": {"field": "subfield_id", "size": 5000}},
                         "f": {"terms": {"field": "field_id", "size": 500}}, "d": {"terms": {"field": "domain_id", "size": 50}}}}
        r, ms = search("projects", body)
        return {k: {b["key"]: b["doc_count"] for b in r["aggregations"][k]["buckets"]} for k in "tsfd"}, ms, r
    sci, ms1, r1 = counts("SCI")
    dch, ms2, _ = counts("DCH")
    assert set(dch["t"]) <= set(sci["t"]) and all(dch["t"][k] <= sci["t"][k] for k in dch["t"])
    exp = dict(db.execute(f"select topic_id, count(*) from {pq('projects')} where is_ch group by 1").fetchall())
    assert exp == dch["t"]
    # tree is rolled up API-side from leaf counts; check rollup == the per-level aggs
    roll = collections.Counter()
    for tid, n in sci["t"].items():
        roll[TOPICS[tid][4]] += n
    assert dict(roll) == sci["f"]
    names = [TOPICS[t][1] for t in dch["t"]][:3]
    return f"SCI {len(sci['t'])} topics {ms1:.0f}ms, DCH {len(dch['t'])} topics {ms2:.0f}ms; DCH counts == duckdb; field rollup == agg; e.g. {names}"


@case
def grants_index():
    r, ms = search("grants", {"size": 5, "track_total_hits": True, "query": {"match_all": {}}, "sort": [{"project_count": "desc"}],
                              "aggs": {"funders": {"terms": {"field": "level1_funder", "size": 20}}, "programmes": {"terms": {"field": "level2_programme", "size": 20}}}})
    top = r["hits"]["hits"][0]["_source"]
    rp, _ = search("projects", {"track_total_hits": True, "size": 0, "query": {"term": {"funding_stream_ids": top["id"]}}})
    assert total(rp) == top["project_count"], (total(rp), top["project_count"])
    rs, _ = search("grants", {"size": 3, "query": sqs("horizon OR erc OR marie", ["description", "id"])})
    return f"{total(r)} grants, top {top['id']} = {top['project_count']} projects == projects filter; funders {[b['key'] for b in r['aggregations']['funders']['buckets']][:4]}"


@case
def organisations_filters_ranking():
    r, ms = search("organisations", {"track_total_hits": True, "size": 5, "query": {"bool": {"filter": [{"term": {"region": "Western Europe"}}, {"term": {"rorTypes": "education"}}, {"exists": {"field": "geo"}}]}},
                                     "sort": [{"total_funding_eur": "desc"}, {"project_count": "desc"}, {"work_count": "desc"}],
                                     "aggs": {"regions": {"terms": {"field": "region"}}, "types": {"terms": {"field": "rorTypes"}}}})
    assert total(r) > 0
    return f"{total(r)} orgs (region+type+geo), sort funding>projects>works {ms:.0f}ms"


@case
def deep_pagination_limit():
    try:
        es.search(index=IX["works"], body={"from": 10000, "size": 10, "query": {"match_all": {}}})
        return "from=10000 allowed (mini index too small to matter?)"
    except Exception as e:  # noqa: BLE001
        assert "max_result_window" in str(e) or "Result window" in str(e)
        return "from+size > 10000 rejected (index.max_result_window=10000): UI must cap at page ~1000 or use search_after"


# ---- summary ---------------------------------------------------------------------------------------------------
bad = [r for r in RESULTS if not r[1]]
print(f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} passed")
sys.exit(1 if bad else 0)
