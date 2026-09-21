"""Full-scale smoke + latency check for the loaded indices (run it ON THE VM after the load; it also works on the laptop sample).

    cd src/pipelines/core_v4/serving/export
    export OPENSEARCH_USERNAME=admin OPENSEARCH_PASSWORD=...           # only if the cluster has basic auth (prod does)
    uv run --frozen --only-group serving python vm_smoke.py --host 127.0.0.1 --port 9200 [--runs 15] [--json ~/vm_smoke.json]

Self-contained: needs only the serving dependency group, `queries.py` next to it and a reachable OpenSearch. NO Parquet, NO DuckDB: every test input
(biggest org / biggest project / frequent words / an existing work id) is discovered from the indices themselves.

What it prints: (1) cluster + index overview (docs, size, shards, heap), (2) a latency table: `first` = first call after `_cache/clear` (empty OpenSearch caches;
the OS page cache is NOT dropped, for a really cold disk read restart the container or `echo 3 > /proc/sys/vm/drop_caches` first), then p50 / p95 / max over the
remaining runs, (3) RED FLAGS: anything with p50 or p95 above --red-ms (default 1000 ms) or an error.

Useful flags:
    --groups projects,works,misc,count  run only some groups (misc = organisations, grants, minorities; count = the `_count` checks behind the UI's "about N projects")
    --only "experts,org network"     run only checks whose name contains one of these substrings
    --runs 1                         first-call latencies only (do this right after a container restart to measure the cold global-ordinals cost on org_ids)
    --set-eager-ordinals             PUT projects mapping org_ids eager_global_ordinals=true first (a fresh load from mappings.py already has it)
    --no-clear                       do not clear caches before each check
`_count` section: the api fires a parallel `POST /<index>/_count` (same query + filters, no aggs, no sort) when a search hits the 10,000 total cap, with a 1200 ms client timeout;
above it the UI falls back to "10,000+". Those checks are marked RED when any single call (first or max) exceeds 1200 ms (fixed, independent of --red-ms).
Exit code 1 if a check errors (red-flag latencies alone do not fail the run, read the table).

WATCH ON THE VM: `projects` is ONE shard (exact facet counts, no shard_size needed), so every projects aggregation runs on a single thread over 3.9M docs. The checks to watch are the
funding agg over ALL projects (blank query), the topic modal counts, the blank-query facets and the experts agg over all projects. If any of them is > ~2 s cold, the fallback is
2 shards + shard_size 500 (`load.py --shards projects=2 --recreate`, `queries.terms_agg()` already sends shard_size).
"""
import argparse
import json
import os
import statistics
import sys
import time

from opensearchpy import OpenSearch

from queries import (PROJECT_FIELDS, TYPO, WORK_FIELDS, MINORITY_FIELDS, experts_aggs, funding_aggs, org_autocomplete_body, org_network_body, orgs_body,
                     project_autocomplete_body, projects_body, query_network_body, search_typo_tolerant, sqs, terms_agg, topic_modal_aggs, works_body)

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="127.0.0.1")
ap.add_argument("--port", type=int, default=9200)
ap.add_argument("--ssl", action="store_true")
ap.add_argument("--prefix", default="")
ap.add_argument("--runs", type=int, default=15)
ap.add_argument("--groups", default="projects,works,misc,count")
ap.add_argument("--only", default="")
ap.add_argument("--red-ms", type=float, default=1000.0)
ap.add_argument("--json", default="")
ap.add_argument("--no-clear", action="store_true")
ap.add_argument("--set-eager-ordinals", action="store_true")
args = ap.parse_args()

user, pw = os.environ.get("OPENSEARCH_USERNAME"), os.environ.get("OPENSEARCH_PASSWORD")
es = OpenSearch(hosts=[{"host": args.host, "port": args.port}], http_compress=True, timeout=60, max_retries=1, use_ssl=args.ssl, verify_certs=False,
                ssl_show_warn=False, **({"http_auth": (user, pw)} if user and pw else {}))
IX = lambda n: args.prefix + n  # noqa: E731
GROUPS = set(args.groups.split(","))
ONLY = [x for x in args.only.split(",") if x]
rows, errors = [], 0
COUNT_TIMEOUT_MS = 1200.0  # client timeout of the api's parallel _count; above it the UI shows "10,000+" instead of "about N"


def s(index, body):
    return es.search(index=IX(index), body=body)


def hits(r):
    t = r["hits"]["total"]
    return f"{t['value']}{'+' if t.get('relation') == 'gte' else ''} hits"


def bench(name, fn, runs=None):
    global errors
    if ONLY and not any(o in name for o in ONLY):
        return
    runs = runs or args.runs
    if not args.no_clear:
        es.indices.clear_cache(index=f"{IX('*')}")
    ts, note = [], ""
    for _ in range(runs):
        t = time.time()
        try:
            note = fn()
        except Exception as e:  # noqa: BLE001
            note = f"ERROR {type(e).__name__}: {str(e)[:140]}"
            errors += 1
            ts.append((time.time() - t) * 1000)
            break
        ts.append((time.time() - t) * 1000)
    warm = sorted(ts[1:]) or ts
    p95 = warm[min(len(warm) - 1, int(round(0.95 * (len(warm) - 1))))]
    rows.append({"check": name, "first": ts[0], "p50": statistics.median(warm), "p95": p95, "max": max(warm), "note": note})
    print(f"  {name}: first {ts[0]:.0f} ms, p50 {statistics.median(warm):.0f}, p95 {p95:.0f}  {note}", flush=True)


def count(index, q=None):
    return es.count(index=IX(index), body={"query": q} if q else None)["count"]


# ---- overview -------------------------------------------------------------------------------------------------------------------------------
print("== cluster ==")
h = es.cluster.health()
print(f"status {h['status']}, nodes {h['number_of_nodes']}, active shards {h['active_shards']}, unassigned {h['unassigned_shards']}")
try:
    for n in es.nodes.stats(metric="jvm")["nodes"].values():
        m = n["jvm"]["mem"]
        print(f"jvm heap used {m['heap_used_in_bytes'] / 2**30:.1f} / max {m['heap_max_in_bytes'] / 2**30:.1f} GiB")
except Exception as e:  # noqa: BLE001
    print("nodes stats not available:", e)
for r in es.cat.indices(index=IX("*"), format="json", bytes="b", h="index,pri,docs.count,store.size", s="index"):
    if not r["index"].startswith("."):
        print(f"  {r['index']:15s} pri {r['pri']}  docs {int(r['docs.count']):>12,}  store {int(r['store.size']) / 2**30:7.2f} GiB")
if args.set_eager_ordinals:
    print(es.indices.put_mapping(index=IX("projects"), body={"properties": {"org_ids": {"type": "keyword", "eager_global_ordinals": True}}}))
    es.indices.refresh(index=IX("projects"))

# ---- discover test inputs from the indices --------------------------------------------------------------------------------------------------
big_org = s("organisations", {"size": 1, "sort": [{"project_count": "desc"}], "_source": ["legalName", "project_count", "work_count"]})["hits"]["hits"][0]
big_work_org = s("organisations", {"size": 1, "sort": [{"work_count": "desc"}], "_source": ["legalName", "project_count", "work_count"]})["hits"]["hits"][0]
big_proj = s("projects", {"size": 1, "sort": [{"work_count": "desc"}], "_source": ["title", "work_count"]})["hits"]["hits"][0]
mids = s("organisations", {"size": 1, "sort": [{"project_count": "desc"}], "_source": ["legalName", "project_count"], "query": {"range": {"project_count": {"gte": 100, "lte": 400}}}})["hits"]["hits"]
mid_org = mids[0] if mids else big_org
# the biggest org by projects is often a US institution with single-org projects (few partners): also test the most COLLABORATIVE org (top org among projects with >= 5 orgs)
collab_id = s("projects", {"size": 0, "track_total_hits": False, "query": {"range": {"org_count": {"gte": 5}}}, "aggs": {"o": terms_agg("org_ids", 1)}})["aggregations"]["o"]["buckets"]
collab = es.get(index=IX("organisations"), id=collab_id[0]["key"])["_source"] if collab_id else big_org["_source"]
collab_org_id = collab_id[0]["key"] if collab_id else big_org["_id"]
PCAND = ["cultural", "digital", "heritage", "education", "energy", "learning", "health", "innovation", "training", "sustainable", "network", "materials", "conservation", "management", "archaeological"]
WCAND = ["analysis", "study", "system", "cultural", "digital", "model", "review", "clinical", "heritage", "energy", "education", "climate", "network", "conservation", "protein"]


def pick(index, fields, cands):
    cnt = {w: es.count(index=IX(index), body={"query": sqs(w, fields)})["count"] for w in cands}
    ranked = sorted(cnt, key=cnt.get, reverse=True)
    common = ranked[0]
    mid = next((w for w in ranked if cnt[w] <= max(cnt[common] // 20, 50)), ranked[-1])
    return common, mid, cnt


COMMON, MID, pc = pick("projects", PROJECT_FIELDS, PCAND)
WCOMMON, WMID, wc = pick("works", WORK_FIELDS, WCAND)
NEG = next(w for w in PCAND if w not in (COMMON, MID))
WNEG = next(w for w in WCAND if w not in (WCOMMON, WMID))
print(f"\nprojects words: common {COMMON!r} ({pc[COMMON]:,}), mid {MID!r} ({pc[MID]:,}); works words: common {WCOMMON!r} ({wc[WCOMMON]:,}), mid {WMID!r} ({wc[WMID]:,})")
print(f"biggest org by projects: {big_org['_source']['legalName']!r} ({big_org['_source']['project_count']:,} projects); by works: {big_work_org['_source']['legalName']!r} ({big_work_org['_source']['work_count']:,} works)")
print(f"biggest project by works: {big_proj['_source']['title'][:60]!r} ({big_proj['_source']['work_count']:,} works); mid org: {mid_org['_source']['legalName']!r}\n")


def typo(w, index, fields):
    for cand in (w[:3] + w[4:], w[:2] + w[3] + w[2] + w[4:], w[:3] + "x" + w[4:], w[:4] + w[3] + w[4:], w[:2] + w[3:6] + w[2] + w[6:]):
        if cand != w and es.count(index=IX(index), body={"query": sqs(cand, fields)})["count"] < 3:
            return cand
    raise RuntimeError(f"no typo of {w!r} found that misses")


FACETS = {"topics": terms_agg("topic_id", 50), "funder": terms_agg("funder", 20), "programme": terms_agg("programme", 20),
          "years": {"date_histogram": {"field": "startDate", "calendar_interval": "year"}}}
GOOGLE_P = f'"{COMMON}" AND {MID} -{NEG}'
GOOGLE_W = f'"{WCOMMON}" AND {WMID} -{WNEG}'


def query_network_payload(q, max_projects=2000, cap_orgs=20, max_edges=2000):
    r = s("projects", query_network_body(q, max_projects))
    lists = [h.get("fields", {}).get("org_ids", []) for h in r["hits"]["hits"]]
    edges = {}
    for org_ids in lists:
        o = org_ids[:cap_orgs]
        for i in range(len(o)):
            for j in range(i + 1, len(o)):
                k = (o[i], o[j]) if o[i] < o[j] else (o[j], o[i])
                edges[k] = edges.get(k, 0) + 1
    top = sorted(edges.items(), key=lambda kv: -kv[1])[:max_edges]
    nodes = {}
    for (a, b), _ in top:
        nodes.setdefault(a, len(nodes))
        nodes.setdefault(b, len(nodes))
    payload = json.dumps({"nodes": [{"id": n} for n in nodes], "edges": [[nodes[a], nodes[b], w] for (a, b), w in top]}, separators=(",", ":"))
    return f"{len(lists)} projects -> {len(edges)} pairs, {len(top)} edges, {len(nodes)} nodes, payload {len(payload) / 1024:.0f} KB (ids only)"


def typo_note(o):
    return f"{o['mode']} {o['total']} hits strict {o['strict_ms']:.0f} fuzzy {o['fuzzy_ms'] or 0:.0f} ms timed_out={o['timed_out']}"


big_org_id, big_work_org_id, big_proj_id, mid_org_id = big_org["_id"], big_work_org["_id"], big_proj["_id"], mid_org["_id"]

if "projects" in GROUPS:
    print("== projects ==")
    bench("P 1 common word + topic/funder/programme/year facets", lambda: hits(s("projects", projects_body(COMMON, size=10, aggs=FACETS))))
    bench("P google-style query + facets", lambda: hits(s("projects", projects_body(GOOGLE_P, size=10, aggs=FACETS))))
    bench("P DCH corpus + 1 word + facets", lambda: hits(s("projects", projects_body(COMMON, size=10, corpus="DCH", aggs=FACETS))))
    bench("P DCH corpus, blank query, facets", lambda: hits(s("projects", projects_body("", size=10, corpus="DCH", aggs=FACETS))))
    bench("P query + DCH + year + funder EC + budget sort", lambda: hits(s("projects", projects_body(MID, size=10, sort="budget", corpus="DCH", year=(2010, 2030), funder="EC"))))
    bench("P blank query default page + facets (all projects)", lambda: hits(s("projects", projects_body("", size=20, aggs=FACETS))))
    bench("P deep page 500 (from 9980)", lambda: hits(s("projects", projects_body(COMMON, size=20, offset=9980))))
    bench("P typo fallback (strict miss -> fuzzy AND)", lambda: typo_note(search_typo_tolerant(es, IX("projects"), typo(COMMON, "projects", PROJECT_FIELDS), PROJECT_FIELDS, [], suggest_field="title.sayt", **TYPO["projects"])))
    bench("P topic modal counts SCI (all projects)", lambda: str(len(s("projects", {"size": 0, "aggs": topic_modal_aggs()})["aggregations"]["t"]["buckets"])) + " topics")
    bench("P topic modal counts DCH", lambda: str(len(s("projects", {"size": 0, "query": {"term": {"is_ch": True}}, "aggs": topic_modal_aggs()})["aggregations"]["t"]["buckets"])) + " topics")
    bench("P funding map: blank query, top 500 orgs (all projects)", lambda: str(len(s("projects", projects_body("", size=0, aggs=funding_aggs(500)))["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench("P funding map: 1 word query, top 500 orgs", lambda: str(len(s("projects", projects_body(COMMON, size=0, aggs=funding_aggs(500)))["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench("P funding map: DCH + year filter, top 500 orgs", lambda: str(len(s("projects", projects_body("", size=0, corpus="DCH", year=(2015, 2030), aggs=funding_aggs(500)))["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench("P experts: 1 word query, org_ids agg size 200", lambda: str(len(s("projects", {"size": 0, "query": sqs(COMMON, PROJECT_FIELDS), "aggs": experts_aggs(200)})["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench("P experts: google query, org_ids agg size 200", lambda: str(len(s("projects", {"size": 0, "query": sqs(GOOGLE_P, PROJECT_FIELDS), "aggs": experts_aggs(200)})["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench("P experts: blank query (all projects), org_ids agg size 200", lambda: str(len(s("projects", {"size": 0, "aggs": experts_aggs(200)})["aggregations"]["orgs"]["buckets"])) + " orgs")
    bench(f"P org network: biggest org ({big_org['_source']['legalName'][:24]!r}, {big_org['_source']['project_count']:,} projects), partners 500", lambda: str(len(s("projects", org_network_body(big_org_id, 500))["aggregations"]["partners"]["buckets"])) + " partners")
    bench(f"P org network: most collaborative org ({collab['legalName'][:24]!r}, {collab.get('project_count', 0):,} projects), partners 500", lambda: str(len(s("projects", org_network_body(collab_org_id, 500))["aggregations"]["partners"]["buckets"])) + " partners")
    bench(f"P org network: mid org ({mid_org['_source']['legalName'][:24]!r})", lambda: str(len(s("projects", org_network_body(mid_org_id, 500))["aggregations"]["partners"]["buckets"])) + " partners")
    ids = [b["key"] for b in s("projects", org_network_body(collab_org_id, 500))["aggregations"]["partners"]["buckets"]]
    bench("P org network: mget 500 partner org docs of the collaborative org (geo, name_key)", lambda: str(len(es.mget(index=IX("organisations"), body={"ids": ids}, _source=["legalName", "geo", "name_key"])["docs"])) + " docs")
    bench("P query network: top 2000 projects org_ids + edge build, 1 word", lambda: query_network_payload(COMMON))
    bench("P query network: same, 'heritage'", lambda: query_network_payload("heritage"))
    bench("P project title autocomplete (2 words)", lambda: hits(s("projects", project_autocomplete_body(COMMON[:5] + " " + MID[:3]))))
    bench("P project acronym autocomplete (3 chars)", lambda: hits(s("projects", project_autocomplete_body("dig"))))

if "works" in GROUPS:
    print("== works ==")
    bench("W 1 common word, sort score+citations", lambda: hits(s("works", works_body(WCOMMON, size=10))))
    bench("W 1 mid word", lambda: hits(s("works", works_body(WMID, size=10))))
    bench("W google-style query (phrase + AND + negation)", lambda: hits(s("works", works_body(GOOGLE_W, size=10))))
    bench("W 2 words AND", lambda: hits(s("works", works_body(f"{WCOMMON} {WMID}", size=10))))
    bench("W query + year + OA + language", lambda: hits(s("works", works_body(WMID, size=10, year=(2000, 2030), oa="gold", language="eng"))))
    bench("W DCH proxy corpus + 1 word", lambda: hits(s("works", works_body(WMID, size=10, corpus="DCH"))))
    bench("W blank query default page (all works)", lambda: hits(s("works", works_body("", size=20))))
    bench("W blank query sorted by citations (all works)", lambda: hits(s("works", works_body("", size=20, sort="citations"))))
    bench("W typo fallback, common word (works settings, 1500 ms timeout)", lambda: typo_note(search_typo_tolerant(es, IX("works"), typo(WCOMMON, "works", WORK_FIELDS), WORK_FIELDS, [], size=10, extra={"sort": ["_score", {"citation_count": "desc"}]}, **TYPO["works"])))
    bench("W typo fallback, mid word", lambda: typo_note(search_typo_tolerant(es, IX("works"), typo(WMID, "works", WORK_FIELDS), WORK_FIELDS, [], size=10, extra={"sort": ["_score", {"citation_count": "desc"}]}, **TYPO["works"])))
    bench(f"W project -> works tab (biggest project, {big_proj['_source']['work_count']:,} works)", lambda: hits(s("works", works_body("", size=20, project=big_proj_id, sort="citations"))))
    bench(f"W org -> works tab (biggest by works, {big_work_org['_source']['work_count']:,} works)", lambda: hits(s("works", works_body("", size=20, org=big_work_org_id, sort="citations"))))
    bench("W org -> works tab + query word", lambda: hits(s("works", works_body(WMID, size=20, org=big_work_org_id, sort="citations"))))
    one = s("works", {"size": 1, "query": {"bool": {"filter": [{"exists": {"field": "project_ids"}}]}}, "_source": ["project_ids"]})["hits"]["hits"]
    if one:
        pid = one[0]["_source"]["project_ids"][0]
        bench("W project -> works tab (typical project)", lambda: hits(s("works", works_body("", size=20, project=pid, sort="citations"))))
        wid = one[0]["_id"]
        bench("W get work by id", lambda: str(es.get(index=IX("works"), id=wid)["found"]))

if "misc" in GROUPS:
    print("== organisations / minorities / grants ==")
    nm = big_org["_source"]["legalName"]
    bench(f"O org autocomplete (4-char prefix of {nm[:20]!r})", lambda: hits(s("organisations", org_autocomplete_body(nm[:4]))))
    bench("O org autocomplete 'univ'", lambda: hits(s("organisations", org_autocomplete_body("univ"))))
    bench("O org search 'university of' (rank_feature blend)", lambda: hits(s("organisations", orgs_body("university of", size=10))))
    bench("O org blank query default ranking (funding)", lambda: hits(s("organisations", orgs_body("", size=20))))
    bench("O org filters region + ror_type", lambda: hits(s("organisations", orgs_body("", size=20, region="Western Europe", ror_type="education"))))
    tq = max(nm.split(), key=len)
    bench("O org typo fallback", lambda: typo_note(search_typo_tolerant(es, IX("organisations"), typo(tq.lower(), "organisations", ["legalName^3", "legalShortName^2", "alternativeNames"]), ["legalName^3", "legalShortName^2", "alternativeNames"], [], suggest_field="legalName", **TYPO["organisations"])) if len(tq) > 6 else "skipped (short name)")
    bench("G grants: match_all + funder/programme facets", lambda: hits(s("grants", {"size": 10, "aggs": {"f": terms_agg("funder", 50), "p": terms_agg("programme", 50)}})))
    bench("M minorities: text 'heritage' (title blob)", lambda: hits(s("minorities", {"track_total_hits": True, "size": 20, "query": sqs("heritage", MINORITY_FIELDS)})))
    bench("M minorities: name 'sami'", lambda: hits(s("minorities", {"track_total_hits": True, "size": 20, "query": sqs("sami", MINORITY_FIELDS)})))

    def two_step(org_phrase):
        r = s("projects", {"size": 0, "track_total_hits": False, "query": {"bool": {"must": sqs(org_phrase, ["org_names"]), "filter": [{"exists": {"field": "minority_qids"}}]}},
                           "aggs": {"m": terms_agg("minority_qids", 50)}})
        qids = [b["key"] for b in r["aggregations"]["m"]["buckets"]]
        d = s("minorities", {"size": 20, "query": {"ids": {"values": qids}}}) if qids else {"hits": {"total": {"value": 0}}}
        return f"{len(qids)} minorities -> {d['hits']['total']['value']} docs"

    bench("M minorities two-step: institution 'Hebrew University'", lambda: two_step('"Hebrew University"'))
    bench("M minorities two-step: institution 'University of Oslo'", lambda: two_step('"University of Oslo"'))
    bench("M projects of a minority (Sami) + facets", lambda: hits(s("projects", projects_body("", size=10, minority="Q48199", aggs=FACETS))))
    bench("M works of a minority (Sami, tier-0 proxy)", lambda: hits(s("works", works_body("", size=10, minority="Q48199"))))

if "count" in GROUPS:
    print("== _count (approximate totals) ==")

    def cnt(index, query):
        return lambda: f"{es.count(index=IX(index), body={'query': query})['count']:,} counted"

    bench("C works blank (match_all)", cnt("works", works_body("")["query"]))
    bench(f"C works common word ({WCOMMON!r})", cnt("works", works_body(WCOMMON)["query"]))
    bench(f"C works mid word ({WMID!r})", cnt("works", works_body(WMID)["query"]))
    bench("C works DCH proxy (is_ch_via_project) + common word", cnt("works", works_body(WCOMMON, corpus="DCH")["query"]))
    bench("C works year range + oa filter + common word", cnt("works", works_body(WCOMMON, year=(2000, 2030), oa="gold")["query"]))
    bench("C projects blank", cnt("projects", projects_body("")["query"]))
    bench(f"C projects common word ({COMMON!r})", cnt("projects", projects_body(COMMON)["query"]))
    bench("C projects DCH corpus (is_ch)", cnt("projects", projects_body("", corpus="DCH")["query"]))
    bench("C projects common word + year range", cnt("projects", projects_body(COMMON, year=(2010, 2030))["query"]))
    bench("C organisations blank", cnt("organisations", orgs_body("")["query"]))
    bench("C organisations 'university' (autocomplete-style text)", cnt("organisations", org_autocomplete_body("university")["query"]))

# ---- report ---------------------------------------------------------------------------------------------------------------------------------
print("\n| check | first ms | p50 ms | p95 ms | max ms | note |\n|---|---|---|---|---|---|")
red = []
for r in rows:
    flag = ""
    is_count = r["check"].startswith("C ")
    if r["note"].startswith("ERROR") or (r["max"] > COUNT_TIMEOUT_MS or r["first"] > COUNT_TIMEOUT_MS if is_count else r["p50"] > args.red_ms or r["p95"] > args.red_ms):
        flag = " **RED**"
        red.append(r)
    print(f"| {r['check']}{flag} | {r['first']:.0f} | {r['p50']:.0f} | {r['p95']:.0f} | {r['max']:.0f} | {r['note']} |")
if any(r["check"].startswith("C ") for r in rows):
    print(f"\nNote: C rows are RED above {COUNT_TIMEOUT_MS:.0f} ms for a single _count (first or max): that is the api's client timeout, so the UI then shows '10,000+' instead of 'about N'.")
print(f"\n{len(rows)} checks, {len(red)} red flag(s) (> {args.red_ms:.0f} ms p50/p95 or error), {errors} error(s)")
slow = [r for r in rows if r["check"].startswith("P ") and (("funding map: blank" in r["check"]) or ("topic modal" in r["check"]) or ("blank query" in r["check"])) and r["first"] > 2000]
if slow:
    print("PROJECTS IS 1 SHARD: these aggregations were > 2 s cold -> consider 2 shards + shard_size 500 (load.py --shards projects=2 --recreate):")
    for r in slow:
        print(f"  {r['check']}  first {r['first']:.0f} ms")
for r in red:
    print(f"  RED: {r['check']}  first {r['first']:.0f} / p50 {r['p50']:.0f} / p95 {r['p95']:.0f} ms  {r['note'][:100]}")
if args.json:
    with open(args.json, "w") as f:
        json.dump(rows, f, indent=1)
sys.exit(1 if errors else 0)
