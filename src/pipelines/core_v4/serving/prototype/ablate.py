"""Per-field size cost by ablation (OpenSearch has no _disk_usage API): index one Parquet with fields dropped, compare store size.
    .venv-serving/bin/python ablate.py works|projects|organisations
Scratch indices proto_abl_* are deleted afterwards. Mini DB is denser than prod: scale each field by (prod avg / mini avg)."""
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
from opensearchpy.helpers import bulk

from load import client, store_bytes
from mappings import INDICES, settings

name = sys.argv[1]
PARQ = Path("../../../../../data/serving_proto")
DROPS = {
    "works": ["id", "authors", "organisation_ids", "project_ids", "title", "pdf_url,landing_url", "publisher,container_name", "doi"],
    "projects": ["id", "summary", "org_names", "org_ids", "fundings", "title", "keywords,subjects", "funder_names,funder,programme,funding_stream_ids"],
    "organisations": ["rorLocations,rorRelationships", "pids", "alternativeNames", "address_street,address_postalcode,address_city"],
}[name]
es = client("localhost", 9201)
mapping, _ = INDICES[name]
docs = pq.read_table(PARQ / f"{name}.parquet").to_pylist()
raw = sum(len(json.dumps(d, default=str)) for d in docs) / len(docs)


def size(drop: list[str]) -> int:
    idx = "proto_abl_" + name
    if es.indices.exists(index=idx):
        es.indices.delete(index=idx)
    m = {**mapping, "properties": {k: v for k, v in mapping["properties"].items() if k not in drop}}
    es.indices.create(index=idx, body={"settings": settings(1, "-1"), "mappings": m})
    bulk(es, ({"_index": idx, "_id": d["id"], "_source": {k: v for k, v in d.items() if k not in drop}} for d in docs))
    es.indices.refresh(index=idx)
    es.indices.forcemerge(index=idx, max_num_segments=1)
    b = store_bytes(es, idx)
    es.indices.delete(index=idx)
    return b


full = size([])
print(f"{name}: {len(docs)} docs, raw JSON {raw:.0f} B/doc, indexed {full / len(docs):.0f} B/doc (all fields)")
for d in DROPS:
    b = size(d.split(","))
    print(f"  without {d:48s} {b / len(docs):7.0f} B/doc  (field cost ≈ {(full - b) / len(docs):6.0f} B/doc)")
