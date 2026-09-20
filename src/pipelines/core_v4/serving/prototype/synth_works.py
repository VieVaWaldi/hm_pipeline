"""INDICATIVE scale test only: synthesise N works from the 1000 real ones (shuffled title words, sampled authors/org ids)
to measure local indexing throughput and B/doc at a larger scale. Synthetic text => sizes are rough, not prod numbers.
    .venv-serving/bin/python synth_works.py 500000
"""
import itertools
import random
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from opensearchpy.helpers import parallel_bulk

from load import client, store_bytes
from mappings import PREFIX, WORKS, settings

N = int(sys.argv[1])
rows = pq.read_table(Path("../../../../../data/serving_proto/works.parquet")).to_pylist()
random.seed(1)
# Zipf-distributed pseudo vocabulary (~400k word types built from syllables => realistic clusters of near neighbours for fuzzy queries)
SYL = ["ba", "be", "bi", "bo", "ca", "ce", "co", "cu", "da", "de", "di", "do", "fa", "fe", "fi", "ga", "ge", "gi", "go", "ha", "he", "hi", "ja", "ka", "ke", "ki",
       "la", "le", "li", "lo", "lu", "ma", "me", "mi", "mo", "mu", "na", "ne", "ni", "no", "pa", "pe", "pi", "po", "ra", "re", "ri", "ro", "sa", "se", "si", "so", "su",
       "ta", "te", "ti", "to", "tu", "va", "ve", "vi", "za", "ze", "tion", "ment", "ing", "ter", "der", "ous", "al", "ic", "ent", "ist"]
VOCAB = list({"".join(random.choices(SYL, k=random.randint(2, 4))) for _ in range(600_000)})[:400_000]
random.shuffle(VOCAB)
CUM = list(itertools.accumulate(1 / (r + 1) ** 0.9 for r in range(len(VOCAB))))
words = VOCAB
authors = [random.choice(VOCAB).capitalize() + ", " + random.choice("ABCDEFGHJKLMNPRSTW") + "." for _ in range(300_000)]
org_pool = [str(random.getrandbits(63)) for _ in range(200_000)]
proj_pool = [str(random.getrandbits(63)) for _ in range(500_000)]
# prod-like link density (NOT the mini DB's): ~2.8 orgs/work, 10% of works project-linked (~1.3 projects each)


def gen():
    for i in range(N):
        b = dict(random.choice(rows))
        b["id"] = str(random.getrandbits(63))
        b["title"] = " ".join(random.choices(VOCAB, cum_weights=CUM, k=random.randint(6, 18)))
        b["authors"] = random.sample(authors, k=random.randint(1, 8))
        b["organisation_ids"] = random.sample(org_pool, k=random.choice([0, 1, 2, 3, 4, 6]))
        b["project_ids"] = random.sample(proj_pool, k=1) if random.random() < 0.1 else []
        b["doi"] = f"10.{random.randint(1000, 9999)}/{random.getrandbits(40):x}"
        b["pdf_url"] = f"https://repo.example.org/{random.getrandbits(40):x}.pdf" if random.random() < 0.5 else None
        b["landing_url"] = "https://doi.org/" + b["doi"]
        yield {"_index": PREFIX + "works_synth", "_id": b["id"], "_source": b}


es = client("localhost", 9201)
idx = PREFIX + "works_synth"
if es.indices.exists(index=idx):
    es.indices.delete(index=idx)
es.indices.create(index=idx, body={"settings": settings(1, "-1"), "mappings": WORKS})
t = time.time()
ok = sum(1 for s, _ in parallel_bulk(es, gen(), thread_count=4, chunk_size=2000) if s)
t_load = time.time() - t
es.indices.put_settings(index=idx, body={"refresh_interval": "30s"})
es.indices.refresh(index=idx)
t = time.time()
es.indices.forcemerge(index=idx, max_num_segments=1)
t_merge = time.time() - t
sb = store_bytes(es, idx)
print(f"{ok:,} docs: load {t_load:.0f}s ({ok / t_load:,.0f} docs/s, 4 threads, local SSD), forcemerge {t_merge:.0f}s, store {sb / 1e6:.0f} MB = {sb / ok:.0f} B/doc")
print(f"  vocabulary {len(VOCAB):,} word types, most frequent: {VOCAB[:3]}")
for q in ({"match": {"title": VOCAB[10]}}, {"simple_query_string": {"query": f"{VOCAB[20]} + {VOCAB[30]} -{VOCAB[40]}", "fields": ["title^3", "authors"], "default_operator": "AND"}}):
    t = time.time()
    h = es.search(index=idx, body={"track_total_hits": True, "size": 10, "query": q, "sort": ["_score", {"citation_count": "desc"}]})["hits"]["total"]["value"]
    print(f"  query {str(q)[:70]}: {h:,} hits, {(time.time() - t) * 1000:.0f} ms")
