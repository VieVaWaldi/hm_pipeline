"""Stream Parquet -> OpenSearch with parallel_bulk (no LIMIT/OFFSET paging, constant memory).

    .venv-serving/bin/python load.py [--parquet data/serving_proto] [--host localhost --port 9201] [--only works]
Recreates the proto_ index, loads with refresh_interval -1, then restores 30s + forcemerge + refresh.
"""
import argparse
import time
from pathlib import Path

import pyarrow.parquet as pq
from opensearchpy import OpenSearch
from opensearchpy.helpers import parallel_bulk

from mappings import INDICES, PREFIX, settings


def client(host: str, port: int) -> OpenSearch:
    return OpenSearch(hosts=[{"host": host, "port": port}], http_compress=True, timeout=120, max_retries=3, retry_on_timeout=True)


def store_bytes(es, index: str) -> int:
    """Primary store size after flush; polls until two consecutive readings agree (a single read right after forcemerge can be off)."""
    import time as _t
    es.indices.flush(index=index, force=True)
    es.indices.refresh(index=index)
    prev = -1
    for _ in range(20):
        _t.sleep(1)
        cur = int(es.indices.stats(index=index, metric="store")["indices"][index]["primaries"]["store"]["size_in_bytes"])
        if cur == prev and cur > 0:
            return cur
        prev = cur
    return prev


def actions(path: Path, index: str, batch_rows: int):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_rows):
        for doc in batch.to_pylist():
            yield {"_index": index, "_id": doc["id"] if "id" in doc else doc["qid"], "_source": doc}


def load(es: OpenSearch, name: str, parquet_dir: Path, threads: int = 4, chunk: int = 1000, shards: int | None = None) -> int:
    mapping, default_shards = INDICES[name]
    index = PREFIX + name
    if es.indices.exists(index=index):
        es.indices.delete(index=index)
    es.indices.create(index=index, body={"settings": settings(shards or default_shards, refresh="-1"), "mappings": mapping})
    t = time.time()
    ok = failed = 0
    for success, info in parallel_bulk(es, actions(parquet_dir / f"{name}.parquet", index, chunk * 5), thread_count=threads,
                                       chunk_size=chunk, raise_on_error=False):
        ok += success
        if not success:
            failed += 1
            if failed <= 5:
                print("  FAILED:", str(info)[:500])
    es.indices.put_settings(index=index, body={"refresh_interval": "30s"})
    es.indices.refresh(index=index)
    es.indices.forcemerge(index=index, max_num_segments=1)
    print(f"{index:22s} {ok:>9,} ok {failed} failed  {time.time() - t:6.1f}s")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="data/serving_proto")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9201)
    ap.add_argument("--only", default=",".join(INDICES))
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--shards", type=int, default=None)
    args = ap.parse_args()
    es = client(args.host, args.port)
    for name in args.only.split(","):
        load(es, name, Path(args.parquet), threads=args.threads, shards=args.shards)


if __name__ == "__main__":
    main()
