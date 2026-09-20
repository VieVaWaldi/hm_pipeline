"""D27 verification on the cluster dry-run Parquet (read-only, DuckDB in memory, no OpenSearch): before/after counts of `&amp;`, `&[a-z#0-9]+;`
and `<[a-zA-Z/]` per text field after `hm_clean` / `hm_dec`, the top residual patterns and before/after examples.

    .venv-serving/bin/python verify_clean.py [--dry-run ../agent_job/out] [--out ../agent_job/CLEAN_VERIFY.md]
The dry-run files predate the final export, so this applies the SAME macros (sql/00_macros.sql) to the raw values; it is what the cluster export
will produce for these fields (works: tier 0 exact + 1M-row tier-1 sample; projects: all 3.89M; organisations: all 494k; grants: 6k dry-run rows).
"""
import argparse
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
ap = argparse.ArgumentParser()
ap.add_argument("--dry-run", default=str(HERE.parent / "agent_job" / "out"))
ap.add_argument("--out", default=str(HERE.parent / "agent_job" / "CLEAN_VERIFY.md"))
args = ap.parse_args()
D = Path(args.dry_run)
con = duckdb.connect()
con.execute("SET threads=8")
con.execute((HERE / "sql" / "00_macros.sql").read_text())

W = f"(SELECT * FROM '{D / 'works_tier0.parquet'}' UNION ALL SELECT * EXCLUDE (is_ch_via_project), NULL::BOOLEAN FROM '{D / 'works_tier1_sample.parquet'}')"
# name -> (from-expression, value expression, cleaner)
FIELDS = [
    ("works.title", f"(SELECT title AS v FROM {W})", "hm_clean"),
    ("works.publisher", f"(SELECT publisher AS v FROM {W})", "hm_clean"),
    ("works.container_name", f"(SELECT container_name AS v FROM {W})", "hm_clean"),
    ("works.authors[]", f"(SELECT unnest(authors) AS v FROM {W})", "hm_dec"),
    ("projects.title", f"(SELECT title AS v FROM '{D / 'projects_full.parquet'}')", "hm_clean"),
    ("projects.summary", f"(SELECT summary AS v FROM '{D / 'projects_full.parquet'}')", "hm_clean"),
    ("projects.keywords", f"(SELECT keywords AS v FROM '{D / 'projects_full.parquet'}')", "hm_clean"),
    ("projects.acronym", f"(SELECT acronym AS v FROM '{D / 'projects_full.parquet'}')", "hm_clean"),
    ("projects.subjects[]", f"(SELECT unnest(subjects) AS v FROM '{D / 'projects_full.parquet'}')", "hm_clean"),
    ("organisations.legalName", f"(SELECT legalName AS v FROM '{D / 'organizations.parquet'}')", "hm_clean"),
    ("organisations.legalShortName", f"(SELECT legalShortName AS v FROM '{D / 'organizations.parquet'}')", "hm_clean"),
    ("organisations.alternativeNames[]", f"(SELECT unnest(alternativeNames) AS v FROM '{D / 'organizations.parquet'}')", "hm_clean"),
    ("organisations.address_street", f"(SELECT address_street AS v FROM '{D / 'organizations.parquet'}')", "hm_clean"),
    ("organisations.address_city", f"(SELECT address_city AS v FROM '{D / 'organizations.parquet'}')", "hm_clean"),
    ("grants.description", f"(SELECT description AS v FROM '{D / 'grants.parquet'}')", "hm_clean"),
    ("grants.id", f"(SELECT id AS v FROM '{D / 'grants.parquet'}')", "hm_clean"),
]
ENT = "'&[a-zA-Z#0-9]+;'"
TAG = "'<[a-zA-Z/]'"
CNT = lambda c: (f"count(*) FILTER (contains({c}, '&amp;')), count(*) FILTER (regexp_matches({c}, {ENT})), "  # noqa: E731
                 f"count(*) FILTER (regexp_matches({c}, {TAG})), count(*) FILTER (regexp_matches({c}, '&lt;|&gt;|&quot;'))")

out = ["# D27 text cleaning: before / after on the real cluster dry-run data", "",
       "Rows = values with the pattern. `&amp;` = contains `&amp;`; `&ent;` = any `&[a-zA-Z#0-9]+;`; `<tag` = `<[a-zA-Z/]`; `&lt;..` = `&lt;`, `&gt;` or `&quot;`.", "",
       "| field | rows | `&amp;` before | after | `&ent;` before | after | `<tag` before | after | `&lt;..` before | after | changed rows | s |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]
for name, src, fn in FIELDS:
    t = time.time()
    r = con.execute(f"""SELECT count(v), {CNT('v')}, {CNT('c')}, count(*) FILTER (v IS DISTINCT FROM c)
                        FROM (SELECT v, {fn}(v) AS c FROM {src})""").fetchone()
    n, ba, be, bt, bl, aa, ae, at, al, ch = r[0], *r[1:5], *r[5:9], r[9]
    out.append(f"| {name} | {n:,} | {ba:,} | {aa:,} | {be:,} | {ae:,} | {bt:,} | {at:,} | {bl:,} | {al:,} | {ch:,} | {time.time() - t:.1f} |")
    print(out[-1], flush=True)

# top residual patterns after cleaning, over every field
union = " UNION ALL ".join(f"SELECT '{n}' AS f, {fn}(v) AS c FROM {src}" for n, src, fn in FIELDS)
con.execute(f"CREATE TEMP TABLE cleaned AS SELECT f, c FROM ({union}) WHERE c IS NOT NULL AND (contains(c, '&') OR contains(c, '<'))")
res_n = con.execute("select count(*) from cleaned").fetchone()[0]
out += ["", f"## Residual patterns after cleaning ({res_n:,} values still contain `&` or `<`; plain `&` / `<` in text are fine)", "",
        "### Top 20 residual entity-like patterns (`&[a-zA-Z#0-9]+;`)", "", "| pattern | rows | example field |", "|---|---|---|"]
for pat, n, f in con.execute(f"select e, count(*) n, any_value(f) from (select f, unnest(regexp_extract_all(c, {ENT})) e from cleaned) group by e order by n desc limit 20").fetchall():
    out.append(f"| `{pat}` | {n:,} | {f} |")
out += ["", "### Top 20 residual tag-like patterns (`<[a-zA-Z/][^>]{0,25}>?`)", "", "| pattern | rows | example field |", "|---|---|---|"]
for pat, n, f in con.execute("select e, count(*) n, any_value(f) from (select f, unnest(regexp_extract_all(c, '<[a-zA-Z/][^<>]{0,25}>?')) e from cleaned) group by e order by n desc limit 20").fetchall():
    out.append(f"| `{pat.replace('|', '/')}` | {n:,} | {f} |")

# before/after examples: a mix of changes
out += ["", "## 15 before / after examples", "", "| field | before | after |", "|---|---|---|"]
ex = []
for name, src, fn, pat in [("works.title", FIELDS[0][1], "hm_clean", "&amp;amp;"), ("works.title", FIELDS[0][1], "hm_clean", "<sub>"), ("works.title", FIELDS[0][1], "hm_clean", "&#[0-9]+;"),
                           ("works.title", FIELDS[0][1], "hm_clean", "<[a-zA-Z/]"), ("works.container_name", FIELDS[2][1], "hm_clean", "&amp;"),
                           ("works.publisher", FIELDS[1][1], "hm_clean", "&amp;"), ("works.authors[]", FIELDS[3][1], "hm_dec", "&[a-z#0-9]+;"),
                           ("projects.title", FIELDS[4][1], "hm_clean", "&[a-z#0-9]+;|<[a-zA-Z/]"), ("projects.summary", FIELDS[5][1], "hm_clean", "<[a-zA-Z/]"),
                           ("projects.summary", FIELDS[5][1], "hm_clean", "&[a-z#0-9]+;"), ("organisations.legalName", FIELDS[9][1], "hm_clean", "&[a-z#0-9]+;"),
                           ("organisations.address_city", FIELDS[13][1], "hm_clean", "&[a-z#0-9]+;"), ("grants.id", FIELDS[15][1], "hm_clean", "&[a-z#0-9]+;"),
                           ("works.title", FIELDS[0][1], "hm_clean", "&lt;"), ("works.title", FIELDS[0][1], "hm_clean", "&amp;#")]:
    row = con.execute(f"select v, {fn}(v) from {src} where regexp_matches(v, '{pat}') and length(v) < 220 order by random() limit 1").fetchone()
    if row:
        ex.append((name, row[0], row[1]))
for name, b, a in ex[:15]:
    out.append(f"| {name} | `{(b or '').replace('|', '/')}` | `{(a or '').replace('|', '/')}` |")

# publisher merge effect
pm = con.execute(f"""select count(distinct publisher) raw, count(distinct hm_clean(publisher)) cleaned from (select publisher from {W} where publisher is not null)""").fetchone()
out += ["", f"## Publisher variants: {pm[0]:,} distinct raw values -> {pm[1]:,} after cleaning (in the works dry-run sample; variants such as `A &amp; B` and `A & B` merge)."]
Path(args.out).write_text("\n".join(out) + "\n")
print(f"\nwrote {args.out}")
