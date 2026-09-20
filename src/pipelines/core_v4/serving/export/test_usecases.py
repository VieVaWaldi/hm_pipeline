"""Exercise every use case of SERVING_DESIGN.md section 5 against the loaded indexes. Prints PASS/FAIL + timing + notes.

    .venv-serving/bin/python test_usecases.py --parquet <export dir> [--prefix hm_] [--port 9201]
Test data is picked dynamically from the Parquet exports (no hard-coded ids), assertions cross-check OpenSearch against DuckDB.
Uses the export/ modules (queries.py = what the api will do, mappings.py, load.py), not prototype/.
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
from queries import (MAX_WINDOW, MINORITY_FIELDS, ORG_FIELDS, PROJECT_FIELDS, TYPO, WORK_FIELDS, funding_aggs, merge_by_name_key,
                     org_autocomplete_body, orgs_body, page_window, project_autocomplete_body, projects_body, rewrite_query,
                     search_typo_tolerant, split_terms, sqs, topic_modal_aggs, total_of, work_filters, works_body)

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", default="data/serving_final")
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9201)
ap.add_argument("--prefix", default="hm_")
ARGS = ap.parse_args()
P = Path(ARGS.parquet)
es = client(ARGS.host, ARGS.port)
db = duckdb.connect()
pq = lambda n: f"read_parquet('{P / n / (n + '*.parquet')}')" if n != "works" else f"read_parquet('{P / 'works' / '*.parquet'}')"  # noqa: E731
IX = {n: ARGS.prefix + n for n in ["projects", "organisations", "works", "minorities", "grants"]}
SQL_DIR = Path(__file__).parent / "sql"

# api-side static tables (topics / publishers are not indexes): held in memory
TOPICS = {r[0]: r for r in db.execute(f"select id, topic_name, subfield_id, subfield_name, field_id, field_name, domain_id, domain_name from read_json_auto('{P / 'api' / 'topics.json'}')").fetchall()}
PUBLISHERS = db.execute(f"select publisher, works from read_json_auto('{P / 'api' / 'publishers.json'}')").fetchall()

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


def corrupt(word: str) -> dict[str, str]:
    """typo variants that keep the first two chars (prefix_length 2): deletion, transposition, substitution, insertion"""
    return {"deletion": word[:3] + word[4:], "transposition": word[:2] + word[3] + word[2] + word[4:],
            "substitution": word[:3] + ("x" if word[3] != "x" else "z") + word[4:], "insertion": word[:4] + word[3] + word[4:]}


@case
def typo_tolerance_projects():
    rows = []
    for kind, typo in corrupt(WORD).items():
        strict = total(search("projects", {"track_total_hits": True, "size": 0, "query": sqs(typo, PROJECT_FIELDS)})[0])
        o = search_typo_tolerant(es, IX["projects"], typo, PROJECT_FIELDS, [], suggest_field="title.sayt", **TYPO["projects"])
        assert strict < 5 and o["mode"] == "fuzzy" and o["total"] > 0, (typo, strict, o["mode"], o["total"])
        title_words = " ".join(h["_source"]["title"].lower() for h in o["hits"] if "title" in h.get("_source", {})) or ""
        rows.append(f"{kind}:{typo!r} {strict}->{o['total']} ({o['strict_ms']:.0f}+{o['fuzzy_ms']:.0f}ms) dym={o['suggest'][:2]}")
    # two words, one typo, still AND
    two = f"{WORD2[:3]}{WORD2[4:]} {WORD}"
    o = search_typo_tolerant(es, IX["projects"], two, PROJECT_FIELDS, [], suggest_field="title.sayt", **TYPO["projects"])
    assert o["total"] > 0
    # good queries must stay strict (no fallback cost)
    o2 = search_typo_tolerant(es, IX["projects"], WORD, PROJECT_FIELDS, [], **TYPO["projects"])
    assert o2["mode"] == "strict" and o2["fuzzy_ms"] is None
    # negation survives the fallback
    o3 = search_typo_tolerant(es, IX["projects"], f"{corrupt(WORD)['deletion']} -{WORD2}", PROJECT_FIELDS, [], **TYPO["projects"])
    assert o3["mode"] == "fuzzy"
    return " | ".join(rows) + f" | 2-word typo -> {o['total']}"


@case
def typo_tolerance_organisations():
    name = TOP_ORG[1]
    w = name.split()[0]
    typo_q = f"{w[:3]}{w[4:]} {name.split()[1]}" if len(name.split()) > 1 else corrupt(w)["deletion"]
    o = search_typo_tolerant(es, IX["organisations"], typo_q, ORG_FIELDS, [], suggest_field="legalName.sayt", extra={"_source": ["legalName", "project_count"]}, **TYPO["organisations"])
    names = [h["_source"]["legalName"] for h in o["hits"]]
    assert name in names, (typo_q, names)
    return f"{typo_q!r} -> {o['mode']} {o['total']} hits incl. {name!r} ({o['strict_ms']:.0f}+{o['fuzzy_ms'] or 0:.0f}ms) did-you-mean={o['suggest']}"


@case
def did_you_mean_phrase_suggester():
    typo = f"{corrupt(WORD)['transposition']} {corrupt(WORD2)['deletion']}"
    for fld, kind in (("title.sayt", "term"), ("title.sayt._2gram", "phrase")):
        body = {"size": 0, "suggest": {"text": typo}}
        body["suggest"]["s"] = ({"term": {"field": fld, "suggest_mode": "missing", "prefix_length": 2, "size": 2}} if kind == "term"
                                 else {"phrase": {"field": fld, "gram_size": 2, "size": 2, "direct_generator": [{"field": "title.sayt", "suggest_mode": "missing", "prefix_length": 2}]}})
        t = time.time()
        try:
            r = es.search(index=IX["projects"], body=body)
            opts = [o["text"] for e in r["suggest"]["s"] for o in e["options"]]
            res = f"{kind} on {fld}: {opts[:3]} ({(time.time() - t) * 1000:.0f}ms)"
        except Exception as e:  # noqa: BLE001
            res = f"{kind} on {fld}: FAILS {str(e)[:100]}"
        print("       ", res)
    return "see lines above (term suggester per word is the safe default)"


@case
def typo_tolerance_works():
    """Works: small threshold, max_expansions 20, body timeout; no did-you-mean. Timings at real scale: see real_smoke.py."""
    w = db.execute(f"select regexp_extract(lower(title), '([a-z]{{7,}})', 1) w, count(*) c from {pq('works')} where title is not null group by 1 having w <> '' order by c desc limit 1").fetchone()[0]
    typo = corrupt(w)["deletion"]
    strict = total(search("works", {"track_total_hits": True, "size": 0, "query": sqs(typo, WORK_FIELDS)})[0])
    o = search_typo_tolerant(es, IX["works"], typo, WORK_FIELDS, [], size=10, extra={"sort": ["_score", {"citation_count": "desc"}]}, **TYPO["works"])
    assert strict < TYPO["works"]["threshold"] and o["mode"] == "fuzzy" and o["total"] > 0 and not o["timed_out"], (typo, strict, o["mode"], o["total"])
    o2 = search_typo_tolerant(es, IX["works"], w, WORK_FIELDS, [], **TYPO["works"])
    assert o2["mode"] == "strict"
    return f"{typo!r}: strict {strict} -> fuzzy {o['total']} ({o['strict_ms']:.0f}+{o['fuzzy_ms']:.0f}ms), clean word stays strict"


@case
def funder_programme_facets():
    r, ms = search("projects", {"size": 0, "track_total_hits": True, "query": {"match_all": {}},
                                "aggs": {"funder": {"terms": {"field": "funder", "size": 50}}, "programme": {"terms": {"field": "programme", "size": 50}}}})
    fu = {b["key"]: b["doc_count"] for b in r["aggregations"]["funder"]["buckets"]}
    pr = {b["key"]: b["doc_count"] for b in r["aggregations"]["programme"]["buckets"]}
    dfu = dict(db.execute(f"select f, count(*) from (select unnest(funder) f from {pq('projects')}) group by 1").fetchall())
    dpr = dict(db.execute(f"select f, count(*) from (select unnest(programme) f from {pq('projects')}) group by 1").fetchall())
    assert fu == {k: v for k, v in dfu.items() if k in fu} and pr == {k: v for k, v in dpr.items() if k in pr}
    # filter funder=EC + programme=H2020 -> equals duckdb; programme facet under a funder filter only shows that funder's programmes
    r2, _ = search("projects", projects_body("", size=0, funder="EC", programme="H2020", aggs={"programme": {"terms": {"field": "programme"}}}))
    exp = db.execute(f"select count(*) from {pq('projects')} where list_contains(funder,'EC') and list_contains(programme,'H2020')").fetchone()[0]
    assert total(r2) == exp and exp > 0
    g, _ = search("grants", {"size": 0, "query": {"match_all": {}}, "aggs": {"funder": {"terms": {"field": "funder", "size": 50}}, "programme": {"terms": {"field": "programme", "size": 50}}}})
    gd = {b["key"]: b["doc_count"] for b in g["aggregations"]["funder"]["buckets"]}
    dg = dict(db.execute(f"select funder, count(*) from {pq('grants')} group by 1").fetchall())
    assert gd == dg
    return f"projects: {len(fu)} funders / {len(pr)} programmes (== duckdb), EC+H2020 filter {exp} projects ({ms:.0f}ms); grants funder facet == duckdb ({len(gd)} funders)"


@case
def works_filters_and_dch_corpus():
    # take the attributes of a real DCH-proxy work so the combined filter is guaranteed non-empty
    y, oa, lang, pub = db.execute(f"select year, open_access_color, language, publisher from {pq('works')} where is_ch_via_project and publisher is not null and year is not null and open_access_color is not null and language is not null limit 1").fetchone()
    flt = [{"range": {"year": {"gte": y - 1, "lte": y + 1}}}, {"term": {"open_access_color": oa}}, {"term": {"language": lang}}, {"term": {"publisher": pub}}, {"term": {"is_ch_via_project": True}}]
    r, ms = search("works", {"track_total_hits": True, "size": 5, "query": {"bool": {"filter": flt}}, "sort": [{"citation_count": "desc"}]})
    exp = db.execute("select count(*) from " + pq('works') + " where year between ? and ? and open_access_color=? and language=? and publisher=? and is_ch_via_project", [y - 1, y + 1, oa, lang, pub]).fetchone()[0]
    assert total(r) == exp and exp >= 1, (total(r), exp)
    return f"year({y}±1)+OA({oa})+language({lang})+publisher+DCH-proxy: {total(r)} hits == duckdb ({ms:.0f}ms)"


@case
def pred_is_display_only():
    d = es.get(index=IX["projects"], id=db.execute(f"select id from {pq('projects')} where pred is not null limit 1").fetchone()[0])["_source"]
    assert "pred" in d
    try:
        es.search(index=IX["projects"], body={"size": 0, "query": {"range": {"pred": {"gte": 0.5}}}})
        q = total(es.search(index=IX["projects"], body={"size": 0, "track_total_hits": True, "query": {"range": {"pred": {"gte": 0.0}}}}))
        return f"pred returned in _source; range query matches {q} (not indexed => 0 expected)"
    except Exception as e:  # noqa: BLE001
        return f"pred in _source; filter not possible ({type(e).__name__}) as intended"


@case
def works_by_minority():
    q, n_exp = db.execute(f"select m, count(*) c from (select unnest(minority_qids) m from {pq('works')}) group by 1 order by c desc limit 1").fetchone()
    r, ms = search("works", {"track_total_hits": True, "size": 10, "query": {"term": {"minority_qids": q}}, "sort": [{"citation_count": "desc"}],
                             "_source": ["title", "link_tier", "minority_qids", "project_ids"]})
    assert total(r) == n_exp and all(h["_source"]["link_tier"] == 0 for h in r["hits"]["hits"])
    # independent route: projects carrying q -> works linked to those projects (what the proxy field precomputes)
    pids = all_ids("projects", {"query": {"term": {"minority_qids": q}}})
    r2, ms2 = search("works", {"track_total_hits": True, "size": 0, "query": {"terms": {"project_ids": pids}}})
    assert total(r2) == total(r), (total(r2), total(r))
    # with a text query and the DCH proxy on top, as the minorities use case would
    r3, ms3 = search("works", {"track_total_hits": True, "size": 5, "query": {"bool": {"must": sqs("model OR data OR research", WORK_FIELDS), "filter": [{"term": {"minority_qids": q}}]}}})
    tier1 = db.execute(f"select count(*) from {pq('works')} where link_tier=1 and len(minority_qids)>0").fetchone()[0]
    assert tier1 == 0
    return f"{q}: {total(r)} works == duckdb == via-projects route ({total(r2)}); term filter {ms:.0f}ms vs {len(pids)}-id terms route {ms2:.0f}ms; +text {total(r3)} hits {ms3:.0f}ms; tier-1 has none"


@case
def currency_conversion():
    cur, amt, eur = db.execute(f"select currency, funded_amount, funded_amount_eur from {pq('projects')} where currency = 'GBP' and funded_amount > 0 limit 1").fetchone()
    assert abs(eur - amt / 0.85880) < 1e-6, (amt, eur)
    hrk = db.execute(f"select funded_amount, funded_amount_eur from {pq('projects')} where currency = 'HRK' and funded_amount > 0 limit 1").fetchone()
    assert abs(hrk[1] - hrk[0] / 7.53450) < 1e-6
    chf = db.execute(f"select count(*) from {pq('projects')} where currency = 'CHF' and list_contains(funder, 'SNSF')").fetchone()[0]
    assert chf >= 1, "SNSF projects with NULL currency must be CHF (D21)"
    assert db.execute(f"select count(*) from {pq('projects')} where currency = '$'").fetchone()[0] == 0
    assert db.execute(f"select max(funded_amount_eur) from {pq('projects')}").fetchone()[0] <= 1e9
    assert db.execute(f"select count(*) from {pq('projects')} where funded_amount = 0").fetchone()[0] == 0   # 0 -> NULL
    r, _ = search("projects", {"size": 0, "query": {"bool": {"must_not": {"exists": {"field": "funded_amount_eur"}}, "filter": {"range": {"funded_amount": {"gt": 0}}}}}, "track_total_hits": True})
    return f"GBP {amt:.0f} -> {eur:.0f} EUR, HRK ok, {chf} SNSF->CHF, no '$', max EUR <= 1e9; OS sees {total(r)} projects with amount but no EUR (unlisted currency)"


@case
def org_autocomplete():
    name = TOP_ORG[1]
    tokens = name.split()
    for prefix in (name[:4], " ".join(tokens[:2])[:-1] if len(tokens) > 1 else name[:5]):
        r, ms = search("organisations", org_autocomplete_body(prefix, 5))
        names = [h["_source"]["legalName"] for h in r["hits"]["hits"]]
        assert TOP_ORG[1] in names[:3], (prefix, names)
    sn = db.execute(f"select legalShortName from {pq('organisations')} where legalShortName is not null order by project_count desc limit 1").fetchone()[0]
    r, _ = search("organisations", org_autocomplete_body(sn[:3], 5))
    assert total(r) > 0
    return f"prefix {name[:4]!r} -> top-3 {names[:3]} ({ms:.0f}ms); acronym {sn!r} ok"


@case
def project_autocomplete():
    a = db.execute(f"select acronym from {pq('projects')} where acronym is not null limit 1").fetchone()[0]
    r, ms = search("projects", project_autocomplete_body(a[:3], 5))
    assert any(h["_source"].get("acronym") == a for h in r["hits"]["hits"]), [h["_source"] for h in r["hits"]["hits"]]
    t = db.execute(f"select title from {pq('projects')} where title is not null and length(title) > 40 limit 1").fetchone()[0]
    prefix = " ".join(t.split()[:2])
    r2, _ = search("projects", project_autocomplete_body(prefix, 8))
    assert any(h["_source"].get("title") == t for h in r2["hits"]["hits"]) or total(r2) > 0
    return f"acronym {a[:3]!r} -> {total(r)} suggestions {ms:.0f}ms; title prefix {prefix!r} -> {total(r2)}"


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
                              "aggs": {"funders": {"terms": {"field": "funder", "size": 20}}, "programmes": {"terms": {"field": "programme", "size": 20}}}})
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
    assert page_window(1, 20) == (0, 20) and page_window(500, 20) == (9980, 20) and page_window(501, 20) is None
    assert page_window(11, 1000) is None and page_window(10, 1000) == (9000, 1000)
    try:
        es.search(index=IX["works"], body={"from": MAX_WINDOW, "size": 10, "query": {"match_all": {}}})
        return "from=10000 allowed (should not be)"
    except Exception as e:  # noqa: BLE001
        assert "max_result_window" in str(e) or "Result window" in str(e)
        return "page_window() caps at 10,000; from+size > 10000 rejected by OpenSearch as well"


@case
def total_cap_relation():
    assert total_of({"hits": {"total": {"value": 10000, "relation": "gte"}}}) == (10000, True)
    assert total_of({"hits": {"total": {"value": 37, "relation": "eq"}}}) == (37, False)
    r = es.search(index=IX["works"], body={"track_total_hits": 5, "size": 0, "query": {"match_all": {}}})
    assert total_of(r) == (5, True), r["hits"]["total"]        # capped counting works: '5+'
    return "track_total_hits cap -> relation gte -> UI '10,000+' (checked with cap=5 on the small index)"


@case
def works_corpus_and_filters_via_helpers():
    y, oa, lang, pub = db.execute(f"select year, open_access_color, language, publisher from {pq('works')} where is_ch_via_project and publisher is not null and year is not null and open_access_color is not null and language is not null limit 1").fetchone()
    body = works_body("", corpus="DCH", year=(y - 1, y + 1), oa=oa, language=lang, publisher=pub, size=5, sort="citations")
    r, _ = search("works", body)
    exp = db.execute("select count(*) from " + pq('works') + " where year between ? and ? and open_access_color=? and language=? and publisher=? and is_ch_via_project", [y - 1, y + 1, oa, lang, pub]).fetchone()[0]
    assert total(r) == exp >= 1
    return f"works_body(corpus=DCH, year, oa, language, publisher) == duckdb ({exp})"


@case
def works_language_and_org_cap():
    bad = db.execute(f"select count(*) from {pq('works')} where language like '%/%' or language = 'und' or (language is not null and length(language) <> 3)").fetchone()[0]
    assert bad == 0, bad
    r, _ = search("works", {"size": 0, "aggs": {"l": {"terms": {"field": "language", "size": 50}}}})
    langs = {b["key"] for b in r["aggregations"]["l"]["buckets"]}
    assert not {"und", "fra/fre", "esl/spa"} & langs
    mx = db.execute(f"select max(len(organisation_ids)) from {pq('works')}").fetchone()[0]
    assert mx <= 100
    # macro unit test on the pair shapes from the cluster analysis
    con = duckdb.connect()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    got = con.execute("select hm_lang('fra/fre'), hm_lang('dut/nld'), hm_lang('esl/spa'), hm_lang('und'), hm_lang(NULL), hm_lang('eng'), hm_lang('ger/deu')").fetchone()
    assert got == ("fra", "nld", "spa", None, None, "eng", "deu"), got
    return f"languages {sorted(langs)[:6]} normalised, no und / pairs; organisation_ids max {mx} (cap 100); hm_lang unit ok"


@case
def unknown_buckets_region_rortypes():
    r, _ = search("organisations", {"size": 0, "aggs": {"region": {"terms": {"field": "region", "size": 20}}, "types": {"terms": {"field": "rorTypes", "size": 20}}}})
    reg = {b["key"]: b["doc_count"] for b in r["aggregations"]["region"]["buckets"]}
    typ = {b["key"]: b["doc_count"] for b in r["aggregations"]["types"]["buckets"]}
    dreg = db.execute(f"select count(*) from {pq('organisations')} where region = 'Unknown'").fetchone()[0]
    dtyp = db.execute(f"select count(*) from {pq('organisations')} where list_contains(rorTypes, 'unknown')").fetchone()[0]
    assert reg.get("Unknown", 0) == dreg and typ.get("unknown", 0) == dtyp and dreg > 0 and dtyp > 0, (reg, typ, dreg, dtyp)
    assert db.execute(f"select count(*) from {pq('organisations')} where region is null or len(rorTypes) = 0").fetchone()[0] == 0
    pr, _ = search("projects", {"size": 0, "aggs": {"r": {"terms": {"field": "org_regions", "size": 20}}}})
    assert "Unknown" in {b["key"] for b in pr["aggregations"]["r"]["buckets"]}
    org_f, _ = search("organisations", orgs_body("", region="Unknown", ror_type="unknown", size=3))
    return f"organisations region Unknown={dreg}, rorTypes unknown={dtyp} (== duckdb), also a project org_regions bucket; filter works ({total(org_f)} orgs)"


@case
def publishers_list():
    assert PUBLISHERS and PUBLISHERS == sorted(PUBLISHERS, key=lambda x: (-x[1], x[0]))
    top, n = PUBLISHERS[0]
    r, _ = search("works", {"track_total_hits": True, "size": 0, "query": {"term": {"publisher": top}}})
    assert total(r) == n, (top, total(r), n)
    exp = db.execute(f"select count(distinct publisher) from {pq('works')} where publisher is not null").fetchone()[0]
    assert len(PUBLISHERS) == min(3000, exp)
    return f"{len(PUBLISHERS)} publishers in the api list, top {top!r} = {n} works == term filter"


@case
def hm_clean_unit():
    """D27 macro cases (whitelisted tags only, entities decoded in 3 passes, &amp; last, idempotent, NULL for empty)."""
    con = duckdb.connect()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    cases = [
        ("CO<sub>2</sub> emissions", "CO2 emissions"),                 # inline tag -> ''
        ("H<sub>2</sub>O and x<sup>2</sup>", "H2O and x2"),
        ("p < 0.05 and x > 3", "p < 0.05 and x > 3"),                   # plain comparison untouched
        ("if x<5 and y>3", "if x<5 and y>3"),
        ("R&amp;amp;D", "R&D"),                                          # double escaped
        ("&amp;amp;#8220;Glaciers&amp;amp;#8221; by R. &amp; E.", "\u201cGlaciers\u201d by R. & E."),   # triple escaped (real title)
        ("&amp;lt;3", "<3"), ("&lt;3", "<3"), ("&amp;lt;sub&amp;gt;3&amp;lt;/sub&amp;gt;", "3"),
        ("a<br>b", "a b"), ("a<br/>b<BR>c", "a b c"), ("<p>Hello</p><p>World</p>", "Hello World"),   # block tag -> ' '
        ("Fish &amp; Chips", "Fish & Chips"), ("Fish & Chips", "Fish & Chips"),
        ("&quot;q&quot; &apos;s&apos; &#39;t&#39; &#x27;u&#x27;", "\"q\" 's' 't' 'u'"),
        ("&eacute;t&eacute; &pound;5 &ndash; &#8211; &#8217;", "été £5 – – ’"),
        ("a&nbsp;b&#160;c &#8232; d", "a b c d"),                        # nbsp / line separator -> space
        ("x&#19;y&#0;z", "xyz"),                                         # control chars / NUL dropped
        ("&unknownentity; stays", "&unknownentity; stays"),
        ("<unknown>keep</unknown>", "<unknown>keep</unknown>"),
        ('<a href="http://x.org/?a=1&amp;b=2">link</a> t', "link t"),
        ('<mml:math xmlns:mml="x"><mml:mi>x</mml:mi></mml:math>y', "xy"),
        ("<jats:italic>Vibrio</jats:italic> sp.", "Vibrio sp."),
        ("  a \t b\n c  ", "a b c"),
        ("  ", None), ("", None), (None, None), ("<br>", None),
    ]
    con.execute("create temp table hc(raw varchar, exp varchar)")
    con.executemany("insert into hc values (?, ?)", cases)
    rows = con.execute("select raw, exp, hm_clean(raw) got, hm_clean(hm_clean(raw)) got2 from hc").fetchall()
    for raw, exp, got, got2 in rows:
        assert got == exp, (raw, got, exp)
        assert got2 == got, ("not idempotent", raw, got, got2)
    assert con.execute("select hm_dec('Smith &amp; Sons, M&uuml;ller &lt;b&gt;x')").fetchone()[0] == "Smith & Sons, Müller <b>x"   # authors: entities only, no tag strip
    assert con.execute("select hm_clean_list(['a &amp; b', '  ', NULL, '<br>', 'c'])").fetchone()[0] == ["a & b", "c"]
    assert con.execute("select hm_name_key('Johns Hopkins  University &amp; Co.', 'US')").fetchone()[0] == "johns hopkins university co|us"
    return f"{len(cases)} hm_clean cases + hm_dec / hm_clean_list / name_key ok"


@case
def no_html_entities_in_text():
    ent = "regexp_matches({c}, '&(?:#[0-9]+|#[xX][0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);')"
    tag = "regexp_matches({c}, '(?i)</?(?:sub|sup|i|b|em|strong|u|small|span|a|italic|bold|sc|scp|inf|p|br|div|li|ul|ol|h[1-6]|mml:[a-z0-9]+|jats:[a-z0-9-]+)(?:\\s+[a-zA-Z:_-]+\\s*=[^<>]*)?\\s*/?>')"
    checks = {"projects": ["title", "summary", "keywords", "acronym"], "organisations": ["legalName", "legalShortName", "address_street", "address_city"],
              "works": ["title", "publisher", "container_name"], "grants": ["id", "description", "funder_name"], "minorities": ["group_name_en"]}
    checked = 0
    for table, cols in checks.items():
        for c in cols:
            bad = db.execute(f"select count(*) from {pq(table)} where {ent.format(c=c)} or {tag.format(c=c)}").fetchone()[0]
            assert bad == 0, (table, c, bad)
            checked += 1
    for table, c in (("works", "authors"), ("organisations", "alternativeNames"), ("projects", "org_names")):
        bad = db.execute(f"select count(*) from (select unnest({c}) x from {pq(table)}) where {ent.format(c='x')}").fetchone()[0]
        assert bad == 0, (table, c, bad)
        checked += 1
    # the api publishers list is built from the same cleaned values as works.publisher
    pubs = {p for p, _ in PUBLISHERS}
    indexed = {r[0] for r in db.execute(f"select distinct publisher from {pq('works')} where publisher is not null").fetchall()}
    assert pubs <= indexed, list(pubs - indexed)[:5]
    return f"{checked} text fields free of entities and whitelisted tags; api publishers list is a subset of the indexed publisher values"


@case
def money_rules_unit():
    """D14/D21 on synthetic rows: cap > 1e9 EUR -> NULL, 0 -> NULL, SNSF NULL -> CHF, '$' -> USD / NHMRC AUD, unknown currency NULL."""
    con = duckdb.connect()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    rows = con.execute("""
      WITH c AS (SELECT * FROM (VALUES (1, 2500000000.0, 'EUR', ['EC']), (2, 0.0, 'EUR', ['EC']), (3, 1000.0, NULL, ['SNSF']),
                                      (4, 1000.0, '$', ['NIH']), (5, 1000.0, '$', ['NHMRC']), (6, 1000.0, 'XYZ', ['EC']), (7, 3640000000.0, 'INR', ['Wellcome Trust']),
                                      (8, 900.0, 'GBP', ['UKRI'])) t(id, amt, cur, funders)),
           n AS (SELECT id, amt, hm_cur(cur, funders) AS cur FROM c)
      SELECT n.id, n.cur, CASE WHEN n.amt > 0 AND n.amt / fx.units_per_eur <= 1e9 THEN n.amt / fx.units_per_eur END AS eur
      FROM n LEFT JOIN fx ON fx.currency = n.cur ORDER BY n.id""").fetchall()
    d = {r[0]: r for r in rows}
    assert d[1][2] is None and d[2][2] is None                        # junk 2.5bn EUR and 0 -> NULL
    assert d[3][1] == "CHF" and abs(d[3][2] - 1000 / 0.9462) < 1e-6   # SNSF
    assert d[4][1] == "USD" and d[5][1] == "AUD"                      # '$'
    assert d[6][2] is None                                            # unlisted currency
    assert d[7][2] is not None and d[7][2] < 1e9                      # 3.64bn INR = ~33M EUR is kept (only EUR amounts > 1e9 are junk)
    assert abs(d[8][2] - 900 / 0.85880) < 1e-6
    return "cap, zero, SNSF->CHF, $ -> USD/AUD, unlisted -> NULL, INR ok"


@case
def org_rank_not_dominated_by_outliers():
    """D33: (a) an exact full-name search must find that org near the top even if much bigger orgs partially match (log blend, not linear);
    (b) explicit sort orders are strictly by funding > projects > works; (c) no org sum exceeds sum of capped project shares."""
    row = db.execute(f"select id, legalName, project_count from {pq('organisations')} where project_count between 1 and 3 and array_length(string_split(legalName, ' ')) >= 3 order by project_count limit 1").fetchone()
    if row:
        r, _ = search("organisations", orgs_body(f'"{row[1]}"', size=5))
        assert row[0] in [h["_id"] for h in r["hits"]["hits"]], (row, [h["_source"]["legalName"] for h in r["hits"]["hits"]])
    r, _ = search("organisations", orgs_body("", sort="funding", size=10))
    vals = [h["_source"]["total_funding_eur"] for h in r["hits"]["hits"]]
    assert vals == sorted(vals, reverse=True)
    r, _ = search("organisations", orgs_body("", sort="projects", size=10))
    pc = [h["_source"]["project_count"] for h in r["hits"]["hits"]]
    assert pc == sorted(pc, reverse=True)
    lim = db.execute(f"select sum(funded_amount_eur) from {pq('projects')}").fetchone()[0]
    mx = db.execute(f"select max(total_funding_eur) from {pq('organisations')}").fetchone()[0]
    assert mx <= lim
    # rank fields are absent when 0 (rank_feature must be > 0)
    assert db.execute(f"select count(*) from {pq('organisations')} where rank_projects is not null and project_count = 0").fetchone()[0] == 0
    return "exact-name org found in top 5, funding/projects sorts monotone, org max funding <= sum of project EUR, no rank on 0 values"


@case
def org_network_name_key_dedupe():
    """D19: a partner that is the same institution as the center (same name_key) must be dropped; duplicates of a partner merge."""
    dup = db.execute(f"""
      WITH o AS (SELECT id, name_key FROM {pq('organisations')}),
           pp AS (SELECT p.id pid, unnest(p.org_ids) oid FROM {pq('projects')} p),
           j AS (SELECT a.oid a, b.oid b, count(*) n FROM pp a JOIN pp b ON a.pid = b.pid AND a.oid < b.oid GROUP BY 1, 2)
      SELECT j.a, j.b, j.n FROM j JOIN o oa ON oa.id = j.a JOIN o ob ON ob.id = j.b WHERE oa.name_key = ob.name_key ORDER BY n DESC LIMIT 1""").fetchone()
    if dup:
        x, y, n = dup
        r, _ = search("projects", {"size": 0, "query": {"term": {"org_ids": x}}, "aggs": {"partners": {"terms": {"field": "org_ids", "size": 500, "exclude": [x]}}}})
        b = r["aggregations"]["partners"]["buckets"]
        assert y in {k["key"] for k in b}, "duplicate org of the center shows up as a partner (the bug D19 fixes)"
        docs = es.mget(index=IX["organisations"], body={"ids": [x] + [k["key"] for k in b]}, _source=["legalName", "name_key"])["docs"]
        by = {d["_id"]: d["_source"] for d in docs if d["found"]}
        merged = merge_by_name_key(b, by, center=by[x])
        assert all(y not in g["ids"] for g in merged), "self-pair not removed"
        note = f"center {by[x]['legalName']!r} shares {n} projects with its own duplicate id: partner removed, {len(b)} buckets -> {len(merged)} institutions"
    else:
        merged = merge_by_name_key([{"key": "a", "doc_count": 3}, {"key": "b", "doc_count": 2}, {"key": "c", "doc_count": 5}],
                                   {"a": {"name_key": "k1", "legalName": "A"}, "b": {"name_key": "k1", "legalName": "A"}, "c": {"name_key": "k0", "legalName": "C"}},
                                   center={"name_key": "k0"})
        assert merged == [{"name_key": "k1", "doc_count": 5, "ids": ["a", "b"], "name": "A"}], merged
        note = "no duplicate pair inside the mini data: unit-tested merge_by_name_key on synthetic buckets"
    return note


@case
def null_funding_share_in_agg():
    """Projects without org or amount have funded_eur_per_org NULL (D13): sum agg ignores them, no NaN/zero-division."""
    n_null = db.execute(f"select count(*) from {pq('projects')} where funded_eur_per_org is null").fetchone()[0]
    assert n_null > 0
    r, _ = search("projects", projects_body("", size=0, aggs=funding_aggs(50)))
    assert all(b["funding"]["value"] is not None and b["funding"]["value"] == b["funding"]["value"] for b in r["aggregations"]["orgs"]["buckets"])
    r2, _ = search("projects", {"size": 0, "track_total_hits": True, "query": {"bool": {"must_not": {"exists": {"field": "funded_eur_per_org"}}}}})
    assert total(r2) == n_null
    return f"{n_null} projects without a per-org share are absent from the sum (exists-query == duckdb)"


@case
def dynamic_strict_rejects_unknown_fields():
    try:
        es.index(index=IX["grants"], id="__strict_probe__", body={"id": "x", "definitely_not_mapped": 1}, refresh=False)
        es.delete(index=IX["grants"], id="__strict_probe__")
        raise AssertionError("dynamic strict did not reject an unmapped field")
    except AssertionError:
        raise
    except Exception as e:  # noqa: BLE001
        assert "strict_dynamic_mapping" in str(e) or "mapping set to strict" in str(e), str(e)[:200]
    return "unmapped field rejected (a new export column without a mapping fails loudly at load time)"


@case
def coordinators_and_topic_missing():
    assert db.execute(f"select count(*) from {pq('projects')} where len(coordinator_ids) > 0").fetchone()[0] > 0
    r, _ = search("projects", {"size": 0, "track_total_hits": True, "query": {"bool": {"must_not": {"exists": {"field": "topic_id"}}}},
                               "aggs": {"t": {"terms": {"field": "topic_id", "size": 5, "missing": "__none__"}}}})
    n_missing = db.execute(f"select count(*) from {pq('projects')} where topic_id is null").fetchone()[0]
    assert total(r) == n_missing
    return f"coordinator_ids present; topic_id nullable ({n_missing} projects without topic == exists-query), agg has a `missing` bucket option"


@case
def minorities_keep_all_278_groups():
    """The minorities index always holds all 278 groups, also groups that end with 0 projects after --minority-exclude / --override (decision D32)."""
    n_file = db.execute(f"select count(*) from {pq('minorities')}").fetchone()[0]
    n_ix = es.count(index=IX["minorities"])["count"]
    assert n_file == 278 and n_ix == 278, f"minorities: parquet {n_file}, index {n_ix} (expected 278)"
    manifest = json.loads((P / "export_manifest.json").read_text())
    src = manifest.get("minority_source", {})
    emptied = src.get("groups_emptied", [])
    zero = db.execute(f"select count(*) from {pq('minorities')} where project_count = 0").fetchone()[0]
    assert zero >= len(emptied)
    for q in emptied:   # a group that had projects before and has none now: doc present, rollups empty
        d = es.get(index=IX["minorities"], id=q)["_source"]
        assert d["project_count"] == 0 and d["work_count"] == 0 and d["org_count"] == 0 and d["dch_project_count"] == 0, d
        assert not d.get("topic_ids") and not d.get("topic_counts") and not d.get("project_title_blob"), f"{q}: rollups not empty"
        r, _ = search("projects", {"size": 0, "track_total_hits": True, "query": {"term": {"minority_qids": q}}})
        assert total(r) == 0, f"{q}: {total(r)} projects still carry it"
        r, _ = search("works", {"size": 0, "track_total_hits": True, "query": {"term": {"minority_qids": q}}})
        assert total(r) == 0, f"{q}: {total(r)} works still carry it"
    # the exported tags agree with the manifest: projects with a minority == index count of exists(minority_qids)
    r, _ = search("projects", {"size": 0, "track_total_hits": True, "query": {"exists": {"field": "minority_qids"}}})
    assert total(r) == src.get("projects_with_minority", total(r)), (total(r), src)
    # rollups add up: sum of project_count over minorities == number of (project, group) tags in the projects file
    tags = db.execute(f"select coalesce(sum(len(minority_qids)), 0) from {pq('projects')}").fetchone()[0]
    assert db.execute(f"select coalesce(sum(project_count), 0) from {pq('minorities')}").fetchone()[0] == tags
    return (f"278 groups in file and index; mode={src.get('mode', 'stored')}; {len(emptied)} group(s) ended with 0 projects and are still there with empty rollups "
            f"({emptied[:4]}); {zero} groups have no project; projects with a minority {total(r)}, tags {tags}")


# ---- summary ---------------------------------------------------------------------------------------------------
bad = [r for r in RESULTS if not r[1]]
print(f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} passed")
sys.exit(1 if bad else 0)
