"""Project title autocomplete: search_as_you_type max_shingle_size 2 vs 3 vs plain title (baseline), size + quality on real titles.

    .venv-serving/bin/python measure_sayt.py --parquet data/slice/projects/projects.parquet --port 9201
Creates/deletes scratch indices sayt_none / sayt_2 / sayt_3 (title only, 1 shard). Quality = does the source title show up in the top 8 when
the user has typed the first k words (last word cut to a 3+ char prefix), k = 1..4.
"""
import argparse
import random
import time

import duckdb
from opensearchpy.helpers import bulk

from load import client
from mappings import KW, NAME, TEXT, settings, sayt

ap = argparse.ArgumentParser()
ap.add_argument("--parquet", required=True)
ap.add_argument("--port", type=int, default=9201)
ap.add_argument("--n-queries", type=int, default=300)
args = ap.parse_args()
es = client("localhost", args.port)
rows = duckdb.connect().execute(f"select id, title, acronym from read_parquet('{args.parquet}') where title is not null").fetchall()
print(f"{len(rows):,} titles")


def build(name: str, shingle: int | None) -> int:
    if es.indices.exists(index=name):
        es.indices.delete(index=name)
    title = {**TEXT, **({"fields": {"sayt": sayt(shingle)}} if shingle else {})}
    es.indices.create(index=name, body={"settings": settings(1, "-1"), "mappings": {"dynamic": "strict", "properties": {"id": KW, "title": title}}})
    bulk(es, ({"_index": name, "_id": i, "_source": {"id": i, "title": t}} for i, t, _ in rows), chunk_size=2000, request_timeout=120)
    es.indices.put_settings(index=name, body={"refresh_interval": "1s"})
    es.indices.refresh(index=name)
    es.indices.forcemerge(index=name, max_num_segments=1)
    es.indices.flush(index=name, force=True)
    time.sleep(2)
    return int(es.indices.stats(index=name, metric="store")["indices"][name]["primaries"]["store"]["size_in_bytes"])


sizes = {"none": build("sayt_none", None), "2": build("sayt_2", 2), "3": build("sayt_3", 3)}
for k, v in sizes.items():
    print(f"title.sayt shingle={k:5s} store {v / 1e6:8.1f} MB  {v / len(rows):7.0f} B/doc  (+{(v - sizes['none']) / len(rows):5.0f} B/doc vs plain title)")

random.seed(7)
sample = random.sample([r for r in rows if len(r[1].split()) >= 5], args.n_queries)


def fields(shingle):
    f = ["title.sayt"]
    if shingle >= 2:
        f.append("title.sayt._2gram")
    if shingle >= 3:
        f.append("title.sayt._3gram")
    return f


print("\nhit@8 (source title in top 8 after typing the first k words, last word cut to a prefix) and mean latency:")
for shingle in (2, 3):
    cells = []
    for kwords in (1, 2, 3, 4, 5):
        hit, ms = 0, 0.0
        for pid, t, _acr in sample:
            w = t.split()
            typed = " ".join(w[:kwords - 1] + [w[kwords - 1][: max(3, len(w[kwords - 1]) - 1)]])
            t0 = time.time()
            r = es.search(index=f"sayt_{shingle}", body={"size": 8, "_source": False,
                          "query": {"multi_match": {"query": typed, "type": "bool_prefix", "fields": fields(shingle)}}})
            ms += (time.time() - t0) * 1000
            hit += any(h["_id"] == pid for h in r["hits"]["hits"])
        cells.append(f"k={kwords}: {100 * hit / len(sample):5.1f}% ({ms / len(sample):4.1f}ms)")
    print(f"shingle {shingle}: " + " | ".join(cells))
for n in ("sayt_none", "sayt_2", "sayt_3"):
    es.indices.delete(index=n)
