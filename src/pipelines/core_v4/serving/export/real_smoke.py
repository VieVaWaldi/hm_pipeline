"""Latency smoke test on a REAL-data slice (random ~5% of projects, ~0.4% of works of the cluster dry-run export), loaded with load.py --prefix slice_.

    .venv-serving/bin/python real_smoke.py --parquet data/slice --prefix slice_ --port 9201
Prints a markdown table (median / max of 7 runs, first run dropped as warm-up) that is pasted into SERVING_DESIGN.md section 6.
The slice is random, so links between slice projects/works/orgs are partial: only latency and size are meaningful, not the counts.
"""
import argparse
import statistics
import time
from pathlib import Path

import duckdb

from load import client
from queries import (PROJECT_FIELDS, TYPO, WORK_FIELDS, experts_aggs, funding_aggs, org_autocomplete_body, org_network_body, orgs_body,
                     project_autocomplete_body, projects_body, query_network_body, search_typo_tolerant, sqs, topic_modal_aggs, works_body)

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", default="data/slice")
ap.add_argument("--prefix", default="slice_")
ap.add_argument("--port", type=int, default=9201)
ap.add_argument("--runs", type=int, default=7)
args = ap.parse_args()
es = client("localhost", args.port)
P = Path(args.parquet)
db = duckdb.connect()
IX = lambda n: args.prefix + n  # noqa: E731
rows = []


def bench(name, fn):
    ts = []
    for i in range(args.runs):
        t = time.time()
        r = fn()
        ts.append((time.time() - t) * 1000)
    warm = ts[1:]
    note = r if isinstance(r, str) else ""
    rows.append((name, ts[0], statistics.median(warm), max(warm), note))


def s(index, body):
    return es.search(index=IX(index), body=body)


word_counts = db.execute(f"""select w, count(*) c from (select unnest(regexp_extract_all(lower(title), '[a-z]{{6,}}')) w from read_parquet('{P / "projects/projects.parquet"}')) group by 1 order by c desc limit 30""").fetchall()
COMMON, MID = word_counts[2][0], word_counts[25][0]
# the org with the most collaboration partners inside the slice (a big EU coordinator), not just the most projects (NIH orgs have 1 org/project)
big_id = db.execute(f"select o from (select unnest(org_ids) o, len(org_ids) - 1 k from read_parquet('{P / 'projects/projects.parquet'}')) group by 1 order by sum(k) desc limit 1").fetchone()[0]
big_org = db.execute(f"select id, legalName, project_count from read_parquet('{P / 'organisations/organisations.parquet'}') where id = '{big_id}'").fetchone()
wword = db.execute(f"select w from (select unnest(regexp_extract_all(lower(title), '[a-z]{{7,}}')) w from read_parquet('{P / 'works/works_00.parquet'}') ) group by 1 order by count(*) desc limit 1 offset 10").fetchone()[0]
print(f"projects words: common={COMMON!r} mid={MID!r}; works word={wword!r}; big org={big_org[1]!r} ({big_org[2]} projects in slice)")
def typo(w: str, index: str, fields: list[str]) -> str:
    """a 1-edit typo of w (keeps the first 2 chars) that the strict query does NOT match, so the fuzzy fallback really runs"""
    for cand in (w[:3] + w[4:], w[:2] + w[3] + w[2] + w[4:], w[:3] + "x" + w[4:], w[:4] + w[3] + w[4:], w[:2] + w[3:6] + w[2] + w[6:]):
        if es.search(index=IX(index), body={"size": 0, "track_total_hits": True, "query": sqs(cand, fields)})["hits"]["total"]["value"] < 3:
            return cand
    raise SystemExit("no typo found that misses")

bench("projects: 1 common word + topic/funder/programme/year facets", lambda: s("projects", projects_body(COMMON, size=10, aggs={
    "topics": {"terms": {"field": "topic_id", "size": 50}}, "funder": {"terms": {"field": "funder", "size": 20}},
    "programme": {"terms": {"field": "programme", "size": 20}}, "years": {"date_histogram": {"field": "startDate", "calendar_interval": "year"}}}))["hits"]["total"]["value"].__str__() + " hits")
bench("projects: query + filters (DCH, year, funder EC) + budget sort", lambda: s("projects", projects_body(MID, size=10, sort="budget", corpus="DCH", year=(2010, 2030), funder="EC"))["hits"]["total"]["value"].__str__() + " hits")
bench("projects: blank query, default page (match_all) + track_total_hits 10k", lambda: s("projects", projects_body("", size=20))["hits"]["total"]["value"].__str__() + " hits (capped)")
bench("projects: typo fallback (strict 0 -> fuzzy AND, prefix_length 2)", lambda: search_typo_tolerant(es, IX("projects"), typo(COMMON, "projects", PROJECT_FIELDS), PROJECT_FIELDS, [], suggest_field="title.sayt", **TYPO["projects"])["mode"])
bench("topic modal counts SCI (terms t/s/f/d)", lambda: s("projects", {"size": 0, "aggs": topic_modal_aggs()})["aggregations"]["t"]["buckets"].__len__().__str__() + " topics")
bench("topic modal counts DCH", lambda: s("projects", {"size": 0, "query": {"term": {"is_ch": True}}, "aggs": topic_modal_aggs()})["aggregations"]["t"]["buckets"].__len__().__str__() + " topics")
bench("funding map: blank query, top 500 orgs by sum(funded_eur_per_org)", lambda: s("projects", projects_body("", size=0, aggs=funding_aggs(500)))["aggregations"]["orgs"]["buckets"].__len__().__str__() + " orgs")
bench("funding map: 1 word query, top 500 orgs", lambda: s("projects", projects_body(COMMON, size=0, aggs=funding_aggs(500)))["aggregations"]["orgs"]["buckets"].__len__().__str__() + " orgs")
bench("experts: 1 word query, terms agg org_ids size 200", lambda: s("projects", {"size": 0, "query": sqs(COMMON, PROJECT_FIELDS), "aggs": experts_aggs(200)})["aggregations"]["orgs"]["buckets"].__len__().__str__() + " orgs")
bench(f"org network: {big_org[1][:28]!r} ({big_org[2]} projects globally, ~1/19 in slice), partners agg size 500", lambda: s("projects", org_network_body(big_org[0], 500))["aggregations"]["partners"]["buckets"].__len__().__str__() + " partners")
ids = [b["key"] for b in s("projects", org_network_body(big_org[0], 500))["aggregations"]["partners"]["buckets"]]
bench("org network: mget 500 partner org docs (geo)", lambda: len(es.mget(index=IX("organisations"), body={"ids": ids}, _source=["legalName", "geo", "name_key"])["docs"]).__str__() + " docs")
bench("query network source: top 2000 projects, org_ids via _source", lambda: len(s("projects", {"size": 2000, "_source": ["org_ids"], "query": sqs(COMMON, PROJECT_FIELDS)})["hits"]["hits"]).__str__() + " projects")
bench("query network source: top 2000 projects, org_ids via docvalue_fields", lambda: len(s("projects", query_network_body(COMMON, 2000))["hits"]["hits"]).__str__() + " projects")
bench("org autocomplete (prefix of biggest org, 4 chars)", lambda: s("organisations", org_autocomplete_body(big_org[1][:4]))["hits"]["total"]["value"].__str__() + " hits")
bench("org search: name words, rank_feature blend", lambda: s("organisations", orgs_body("university of", size=10))["hits"]["total"]["value"].__str__() + " hits")
bench("project autocomplete (title prefix, 12 chars)", lambda: s("projects", project_autocomplete_body(COMMON[:4] + " " + MID[:3]))["hits"]["total"]["value"].__str__() + " hits")
bench("works: 1 word query, sort score+citations", lambda: s("works", works_body(wword, size=10))["hits"]["total"]["value"].__str__() + " hits")
bench("works: query + year + OA + language + DCH proxy", lambda: s("works", works_body(wword, size=10, corpus="DCH", year=(2000, 2030), oa="gold", language="eng"))["hits"]["total"]["value"].__str__() + " hits")
bench("works: typo fallback (works settings: threshold 3, max_expansions 20, 1500ms timeout)", lambda: (lambda o: f"{o['mode']} {o['total']} hits, timed_out={o['timed_out']}")(search_typo_tolerant(es, IX("works"), typo(wword, "works", WORK_FIELDS), WORK_FIELDS, [], size=10, extra={"sort": ["_score", {"citation_count": "desc"}]}, **TYPO["works"])))
pid = db.execute(f"select p from (select unnest(project_ids) p from read_parquet('{P / 'works/works_00.parquet'}')) limit 1").fetchone()[0]
bench("project -> works tab (term project_ids, sort citations)", lambda: s("works", works_body("", size=20, project=pid, sort="citations"))["hits"]["total"]["value"].__str__() + " hits")
oid = db.execute(f"select o from (select unnest(organisation_ids) o from read_parquet('{P / 'works/works_00.parquet'}')) group by 1 order by count(*) desc limit 1").fetchone()[0]
bench("org -> works tab (term organisation_ids, sort citations)", lambda: s("works", works_body("", size=20, org=oid, sort="citations"))["hits"]["total"]["value"].__str__() + " hits")
bench("grants: match_all + funder/programme facets", lambda: s("grants", {"size": 10, "aggs": {"f": {"terms": {"field": "funder", "size": 50}}, "p": {"terms": {"field": "programme", "size": 50}}}})["hits"]["total"]["value"].__str__() + " grants")

print("\n| check | first ms | median ms | max ms | note |\n|---|---|---|---|---|")
for n, f, m, mx, note in rows:
    print(f"| {n} | {f:.0f} | {m:.0f} | {mx:.0f} | {note} |")
