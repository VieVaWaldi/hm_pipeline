"""Build a small, COHERENT sample of the real export (same schema, same file layout) for laptop tests.

    .venv-serving/bin/python build_sample.py [--src data/serving_export] [--out data/serving_export_sample] [--seed 7]
        [--projects-random 14000] [--org-cap 30000] [--works-t0 24000] [--works-t1 5000]

Reads the full export read-only (DuckDB, memory_limit 4 GB) and writes under --out (git-ignored via /data):
    projects/projects.parquet   ~5k DCH + ~5k minority-tagged + ALL projects of one anchor org + a moderately big project + random rest
    organisations/...parquet    ALL organisations referenced by the sampled projects (cap --org-cap; the random part is shrunk until it fits)
    works/works_00,01.parquet   tier-0 works linked to sampled projects (is_ch_via_project / minority first, all works of the anchor project),
                                + works of the anchor org + ~5k tier-1 works of sampled orgs + a few tier-0 works without any project
    minorities/, grants/, api/  complete copies (small)
Coherence rules: every id in a project's org_ids exists in organisations; works.project_ids / organisation_ids are FILTERED to ids that are in the
sample (a link to a project/org outside the sample is dropped, never dangling); denormalised numbers (project.work_count, organisation.project_count /
work_count / total_funding_eur / rank_*, `org_count`) keep their FULL-dataset values, so they do not equal the number of docs you find in the sample.
Latencies on the sample say nothing about the real scale; use it for correctness only.
"""
import argparse
import json
import shutil
import time
from pathlib import Path

import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--src", default="data/serving_export")
ap.add_argument("--out", default="data/serving_export_sample")
ap.add_argument("--seed", type=int, default=7)
ap.add_argument("--projects-dch", type=int, default=5000)
ap.add_argument("--projects-minority", type=int, default=5000)
ap.add_argument("--projects-random", type=int, default=14000)
ap.add_argument("--org-cap", type=int, default=30000)
ap.add_argument("--works-t0", type=int, default=22000)
ap.add_argument("--works-t1", type=int, default=5000)
ap.add_argument("--works-anchor-org", type=int, default=1200)
ap.add_argument("--works-t0-noproject", type=int, default=500)
a = ap.parse_args()
SRC, OUT = Path(a.src), Path(a.out)
con = duckdb.connect()
con.execute("SET memory_limit='4GB'; SET threads=4; SET preserve_insertion_order=false")
H = f"hash(id || '{a.seed}')"   # deterministic pseudo-random order
t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:5.0f}s] {msg}", flush=True)


PQ, OQ = SRC / "projects/projects.parquet", SRC / "organisations/organisations.parquet"
WQ = SRC / "works/works_*.parquet"

# ---- anchors ------------------------------------------------------------------------------------------------------------------------------
con.execute(f"CREATE TEMP TABLE pmeta0 AS SELECT id, org_ids, org_count FROM read_parquet('{PQ}')")
anchor_org = con.execute(f"""select o.id, o.legalName, o.project_count from read_parquet('{OQ}') o join
    (select o, count(*) n, sum(org_count - 1) k from (select unnest(org_ids) o, org_count from pmeta0) group by o having n between 300 and 2500) x on x.o = o.id
    where o.geo is not null order by x.k desc limit 1""").fetchone()
anchor_proj = con.execute(f"select id, title, work_count from read_parquet('{PQ}') where work_count between 300 and 800 and org_count between 2 and 30 "
                          f"order by work_count desc limit 1").fetchone()
log(f"anchor org {anchor_org[1]!r} ({anchor_org[2]} projects); anchor project {anchor_proj[1][:60]!r} ({anchor_proj[2]} works)")
con.execute(f"CREATE TEMP TABLE pmeta AS SELECT id, is_ch, minority_qids, org_ids, org_count, work_count, {H} AS h FROM read_parquet('{PQ}')")
con.execute(f"CREATE TEMP TABLE s_fixed AS SELECT DISTINCT id FROM pmeta WHERE list_contains(org_ids, '{anchor_org[0]}') OR id = '{anchor_proj[0]}'")
con.execute(f"CREATE TEMP TABLE s_dch AS SELECT id FROM pmeta WHERE is_ch AND org_count BETWEEN 1 AND 100 ORDER BY h LIMIT {a.projects_dch}")
con.execute(f"CREATE TEMP TABLE s_min AS SELECT id FROM pmeta WHERE len(minority_qids) > 0 AND org_count BETWEEN 1 AND 100 ORDER BY h LIMIT {a.projects_minority}")

n_random = a.projects_random
while True:
    con.execute(f"CREATE OR REPLACE TEMP TABLE s_rand AS SELECT id FROM pmeta WHERE org_count BETWEEN 0 AND 100 ORDER BY h LIMIT {n_random}")
    con.execute("CREATE OR REPLACE TEMP TABLE sp AS SELECT id FROM s_fixed UNION SELECT id FROM s_dch UNION SELECT id FROM s_min UNION SELECT id FROM s_rand")
    n_org = con.execute("select count(distinct o) from (select unnest(org_ids) o from pmeta where id in (select id from sp))").fetchone()[0]
    n_p = con.execute("select count(*) from sp").fetchone()[0]
    log(f"random={n_random}: {n_p} projects -> {n_org} organisations")
    if n_org <= a.org_cap or n_random < 500:
        break
    n_random = int(n_random * 0.8)
con.execute("CREATE TEMP TABLE so AS SELECT DISTINCT o AS id FROM (SELECT unnest(org_ids) o FROM pmeta WHERE id IN (SELECT id FROM sp))")

# ---- works ----------------------------------------------------------------------------------------------------------------------------------
con.execute(f"""CREATE TEMP TABLE wmeta AS SELECT id, link_tier, project_ids, organisation_ids, is_ch_via_project, len(minority_qids) AS nmin,
                pdf_url IS NOT NULL AS has_pdf, {H} AS h FROM read_parquet('{WQ}')""")
log("works metadata loaded")
con.execute("""CREATE TEMP TABLE w0 AS
  SELECT w.id, ANY_VALUE(w.is_ch_via_project) AS ch, ANY_VALUE(w.nmin) AS nmin, ANY_VALUE(w.h) AS h, ANY_VALUE(w.has_pdf) AS has_pdf,
         bool_or(u.pid = ? ) AS of_anchor_project
  FROM (SELECT id, is_ch_via_project, nmin, h, has_pdf, unnest(project_ids) AS pid FROM wmeta WHERE link_tier = 0) w
  JOIN (SELECT id AS pid FROM sp) u ON u.pid = w.pid GROUP BY w.id""".replace("?", f"'{anchor_proj[0]}'"))
n0 = con.execute("select count(*) from w0").fetchone()[0]
log(f"tier-0 works linked to sampled projects: {n0}")
con.execute(f"""CREATE TEMP TABLE sw AS
  SELECT id FROM (SELECT id, of_anchor_project, ch OR nmin > 0 AS pri, has_pdf, h FROM w0)
  ORDER BY of_anchor_project DESC, pri DESC, has_pdf DESC, h LIMIT {a.works_t0}""")
# the anchor org's works (any tier), tier-1 works of sampled orgs, tier-0 works without any project
con.execute(f"""INSERT INTO sw SELECT id FROM (SELECT DISTINCT id, h FROM (SELECT id, h, unnest(organisation_ids) o FROM wmeta WHERE h % 20 = 0) WHERE o = '{anchor_org[0]}'
                ORDER BY h LIMIT {a.works_anchor_org}) WHERE id NOT IN (SELECT id FROM sw)""")
con.execute(f"""INSERT INTO sw SELECT id FROM (SELECT id, ANY_VALUE(h) h FROM (SELECT id, h, unnest(organisation_ids) o FROM wmeta WHERE link_tier = 1 AND h % 100 = 0)
                WHERE o IN (SELECT id FROM so) GROUP BY id ORDER BY h LIMIT {a.works_t1}) WHERE id NOT IN (SELECT id FROM sw)""")
con.execute(f"""INSERT INTO sw SELECT id FROM (SELECT id, h FROM wmeta WHERE link_tier = 0 AND len(project_ids) = 0 AND len(organisation_ids) > 0 AND h % 50 = 0 ORDER BY h
                LIMIT {a.works_t0_noproject}) WHERE id NOT IN (SELECT id FROM sw)""")
n_w = con.execute("select count(*) from sw").fetchone()[0]
log(f"works in sample: {n_w}")

# ---- write ----------------------------------------------------------------------------------------------------------------------------------
if OUT.exists():
    shutil.rmtree(OUT)
for d in ("projects", "organisations", "works", "minorities", "grants", "api"):
    (OUT / d).mkdir(parents=True)
Z = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 10000"
con.execute(f"COPY (SELECT * FROM read_parquet('{PQ}') WHERE id IN (SELECT id FROM sp) ORDER BY id) TO '{OUT}/projects/projects.parquet' ({Z})")
log("projects written")
con.execute(f"COPY (SELECT * FROM read_parquet('{OQ}') WHERE id IN (SELECT id FROM so) ORDER BY id) TO '{OUT}/organisations/organisations.parquet' ({Z})")
log("organisations written")
con.execute(f"CREATE TEMP TABLE wsel AS SELECT * FROM read_parquet('{WQ}') WHERE id IN (SELECT id FROM sw)")
con.execute("""CREATE TEMP TABLE wp_f AS SELECT w.id, list(w.pid ORDER BY w.pid) AS l FROM (SELECT id, unnest(project_ids) pid FROM wsel) w
               JOIN sp ON sp.id = w.pid GROUP BY w.id""")
con.execute("""CREATE TEMP TABLE wo_f AS SELECT w.id, list(w.oid ORDER BY w.oid) AS l FROM (SELECT id, unnest(organisation_ids) oid FROM wsel) w
               JOIN so ON so.id = w.oid GROUP BY w.id""")
sel = """SELECT w.* REPLACE (coalesce(pf.l, []::VARCHAR[]) AS project_ids, coalesce(of.l, []::VARCHAR[]) AS organisation_ids)
         FROM wsel w LEFT JOIN wp_f pf ON pf.id = w.id LEFT JOIN wo_f of ON of.id = w.id"""
for k in (0, 1):
    con.execute(f"COPY (SELECT * FROM ({sel}) WHERE hash(id) % 2 = {k} ORDER BY id) TO '{OUT}/works/works_0{k}.parquet' ({Z})")
log("works written")
for d, f in (("minorities", "minorities/minorities.parquet"), ("grants", "grants/grants.parquet")):
    shutil.copy(SRC / f, OUT / f)
shutil.copy(SRC / "export_manifest.json", OUT / "export_manifest.json")   # minorities/grants are complete copies, the tests read the minority_source block
for f in (SRC / "api").glob("*.json"):
    shutil.copy(f, OUT / "api" / f.name)

# ---- report ---------------------------------------------------------------------------------------------------------------------------------
def one(sql):
    return con.execute(sql).fetchone()[0]


P = f"read_parquet('{OUT}/projects/projects.parquet')"
O = f"read_parquet('{OUT}/organisations/organisations.parquet')"
W = f"read_parquet('{OUT}/works/works_*.parquet')"
stats = {
    "anchor_org": {"id": anchor_org[0], "name": anchor_org[1], "projects_full": anchor_org[2]},
    "anchor_project": {"id": anchor_proj[0], "title": anchor_proj[1], "works_full": anchor_proj[2]},
    "projects": one(f"select count(*) from {P}"), "organisations": one(f"select count(*) from {O}"), "works": one(f"select count(*) from {W}"),
    "projects_dch": one(f"select count(*) from {P} where is_ch"), "projects_minority": one(f"select count(*) from {P} where len(minority_qids) > 0"),
    "projects_no_topic": one(f"select count(*) from {P} where topic_id is null"), "projects_coordinator": one(f"select count(*) from {P} where len(coordinator_ids) > 0"),
    "projects_dangling_org_ids": one(f"select count(*) from (select unnest(org_ids) o from {P}) where o not in (select id from {O})"),
    "orgs_geo": one(f"select count(*) from {O} where geo is not null"), "orgs_dch": one(f"select count(*) from {O} where has_dch_project"),
    "works_tier0": one(f"select count(*) from {W} where link_tier = 0"), "works_tier1": one(f"select count(*) from {W} where link_tier = 1"),
    "works_dch_proxy": one(f"select count(*) from {W} where is_ch_via_project"), "works_minority": one(f"select count(*) from {W} where len(minority_qids) > 0"),
    "works_pdf": one(f"select count(*) from {W} where pdf_url is not null"),
    "works_with_project": one(f"select count(*) from {W} where len(project_ids) > 0"), "works_with_org": one(f"select count(*) from {W} where len(organisation_ids) > 0"),
    "works_dangling_project_ids": one(f"select count(*) from (select unnest(project_ids) p from {W}) where p not in (select id from {P})"),
    "works_dangling_org_ids": one(f"select count(*) from (select unnest(organisation_ids) o from {W}) where o not in (select id from {O})"),
    "anchor_project_works_in_sample": one(f"select count(*) from {W} where list_contains(project_ids, '{anchor_proj[0]}')"),
    "anchor_org_projects_in_sample": one(f"select count(*) from {P} where list_contains(org_ids, '{anchor_org[0]}')"),
    "anchor_org_works_in_sample": one(f"select count(*) from {W} where list_contains(organisation_ids, '{anchor_org[0]}')"),
}
(OUT / "sample_manifest.json").write_text(json.dumps({"seed": a.seed, "args": vars(a), "stats": stats}, indent=1, default=str))
print(json.dumps(stats, indent=1, default=str))
log("done")
