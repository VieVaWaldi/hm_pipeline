"""Stream the export Parquet into OpenSearch (constant memory, resumable per Parquet file). Needs only the `serving` dependency group.

    python load.py --parquet <export dir> [--host localhost --port 9200] [--prefix hm_] [--only works] [--shards works=1,projects=1]
                   [--threads 4 --chunk 1000] [--recreate] [--suffix _v1] [--no-forcemerge]

Per index:  create (refresh_interval -1, replicas 0) -> bulk every <parquet>/<index>/*.parquet -> refresh 30s -> refresh -> forcemerge -> verify count.
Resume:     `<parquet>/.load_state/<index>.json` lists finished files; rerunning skips them (docs are idempotent by _id). A half-loaded file is
            simply indexed again. An existing index without a state file is refused unless --recreate.
Auth:       OPENSEARCH_USERNAME / OPENSEARCH_PASSWORD env vars (basic auth, like heritagemonitor prod), --ssl for https.
Versioning: with --suffix _v1 the index is <prefix><name>_v1 and the alias <prefix><name> is switched to it at the end (rebuild without downtime).
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from opensearchpy import OpenSearch
from opensearchpy.helpers import parallel_bulk

from mappings import CODEC, INDEX_NAMES, PROD_SHARDS, mapping_for, parse_shards, settings


def client(host: str, port: int, ssl: bool = False, timeout: int = 120) -> OpenSearch:
    user, pw = os.environ.get("OPENSEARCH_USERNAME"), os.environ.get("OPENSEARCH_PASSWORD")
    kw = {"http_auth": (user, pw)} if user and pw else {}
    return OpenSearch(hosts=[{"host": host, "port": port}], http_compress=True, timeout=timeout, max_retries=3, retry_on_timeout=True,
                      use_ssl=ssl, verify_certs=False, ssl_show_warn=False, **kw)


def store_bytes(es: OpenSearch, index: str) -> int:
    """Primary store size after flush; polls until two consecutive readings agree (a single read right after forcemerge can be off)."""
    es.indices.flush(index=index, force=True)
    es.indices.refresh(index=index)
    prev = -1
    for _ in range(20):
        time.sleep(1)
        cur = int(es.indices.stats(index=index, metric="store")["indices"][index]["primaries"]["store"]["size_in_bytes"])
        if cur == prev and cur > 0:
            return cur
        prev = cur
    return prev


def parquet_files(root: Path, name: str) -> list[Path]:
    return sorted((root / name).glob("*.parquet"))


def docs(path: Path, index: str, batch_rows: int):
    """Parquet -> bulk actions. NULLs are dropped from _source (absent field == NULL), empty lists are kept."""
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_rows):
        for doc in batch.to_pylist():
            yield {"_index": index, "_id": doc["id"] if "id" in doc else doc["qid"],
                   "_source": {k: v for k, v in doc.items() if v is not None}}


def load_index(es: OpenSearch, root: Path, name: str, args) -> int:
    index_name = f"{args.prefix}{name}{args.suffix}"
    files = parquet_files(root, name)
    if not files:
        print(f"[{name}] no parquet files in {root / name}, skipped")
        return 0
    expected = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    state_path = root / ".load_state" / f"{index_name}.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"done": {}}

    exists = es.indices.exists(index=index_name)
    if exists and args.recreate:
        es.indices.delete(index=index_name)
        exists, state = False, {"done": {}}
    if exists and not state_path.exists():
        sys.exit(f"[{name}] index {index_name} exists but there is no load state: use --recreate (or delete it)")
    if not exists:
        shards = parse_shards(args.shards)[name]
        es.indices.create(index=index_name, body={"settings": settings(shards, refresh="-1", codec=CODEC.get(name, "default")),
                                                  "mappings": mapping_for(name, args.title_shingle)})
        state = {"done": {}}
        print(f"[{name}] created {index_name}: {shards} shard(s), replicas 0, refresh -1")

    t_all, total_ok, total_failed = time.time(), 0, 0
    for f in files:
        if f.name in state["done"]:
            print(f"[{name}] skip {f.name} (done, {state['done'][f.name]:,} docs)")
            continue
        n_file = pq.ParquetFile(f).metadata.num_rows
        t, ok, failed = time.time(), 0, 0
        for success, info in parallel_bulk(es, docs(f, index_name, args.chunk * 5), thread_count=args.threads, chunk_size=args.chunk,
                                           queue_size=args.threads * 2, raise_on_error=False, raise_on_exception=False):
            ok += bool(success)
            if not success:
                failed += 1
                if failed <= 5:
                    print(f"  FAILED: {str(info)[:400]}", flush=True)
            if (ok + failed) % 200_000 == 0:
                print(f"[{name}] {f.name}: {ok + failed:>10,}/{n_file:,}  {(ok + failed) / (time.time() - t):8.0f} docs/s", flush=True)
        total_ok += ok
        total_failed += failed
        if failed:
            sys.exit(f"[{name}] {f.name}: {failed} failed docs, not marked done (fix the mapping/data and rerun)")
        state["done"][f.name] = ok
        state_path.parent.mkdir(exist_ok=True)
        state_path.write_text(json.dumps(state, indent=1))
        print(f"[{name}] {f.name}: {ok:,} docs in {time.time() - t:.0f}s ({ok / max(time.time() - t, 1e-9):.0f} docs/s)", flush=True)
    print(f"[{name}] bulk done: {total_ok:,} docs this run in {time.time() - t_all:.0f}s")
    return expected


def finalize(host_args, root: Path, name: str, expected: int, args) -> bool:
    index_name = f"{args.prefix}{name}{args.suffix}"
    es = client(args.host, args.port, args.ssl, timeout=86400)      # forcemerge on HDD can take hours
    t = time.time()
    es.indices.put_settings(index=index_name, body={"refresh_interval": "30s"})
    es.indices.refresh(index=index_name)
    if not args.no_forcemerge:
        print(f"[{name}] forcemerge to {args.segments} segment(s)/shard ...", flush=True)
        es.indices.forcemerge(index=index_name, max_num_segments=args.segments)
    size = store_bytes(es, index_name)
    count = es.count(index=index_name)["count"]
    ok = count == expected
    print(f"[{name}] {index_name}: {count:,} docs (expected {expected:,}) {'OK' if ok else 'MISMATCH'}, "
          f"{size / 1e9:.2f} GB primary store, finalize {time.time() - t:.0f}s", flush=True)
    if ok and args.suffix:
        alias = f"{args.prefix}{name}"
        actions = [{"add": {"index": index_name, "alias": alias}}]
        if es.indices.exists_alias(name=alias):
            actions += [{"remove": {"index": i, "alias": alias}} for i in es.indices.get_alias(name=alias) if i != index_name]
        es.indices.update_aliases(body={"actions": actions})
        print(f"[{name}] alias {alias} -> {index_name}")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True, help="export dir (contains works/, projects/, ...)")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ssl", action="store_true")
    ap.add_argument("--prefix", default="", help="index name prefix (local tests: hm_ or real_; prod: empty)")
    ap.add_argument("--suffix", default="", help="e.g. _v1: create <prefix><name>_v1 and switch the alias <prefix><name>")
    ap.add_argument("--only", default=",".join(INDEX_NAMES))
    ap.add_argument("--shards", default=None, help="override, e.g. works=1,projects=1 (defaults: %s)" % PROD_SHARDS)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=1000, help="docs per bulk request")
    ap.add_argument("--recreate", action="store_true", help="delete and recreate the index, ignore the load state")
    ap.add_argument("--no-forcemerge", action="store_true")
    ap.add_argument("--segments", type=int, default=1)
    ap.add_argument("--title-shingle", type=int, default=3, help="projects title.sayt max_shingle_size (2 or 3)")
    ap.add_argument("--finalize-only", action="store_true", help="skip loading, only refresh/forcemerge/verify")
    args = ap.parse_args()

    root = Path(args.parquet)
    es = client(args.host, args.port, args.ssl)
    bad = []
    for name in args.only.split(","):
        expected = sum(pq.ParquetFile(f).metadata.num_rows for f in parquet_files(root, name))
        if not args.finalize_only:
            load_index(es, root, name, args)
        if expected and not finalize(args, root, name, expected, args):
            bad.append(name)
    if bad:
        sys.exit(f"count mismatch in: {bad}")


if __name__ == "__main__":
    main()
