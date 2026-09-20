"""Index sizes (after forcemerge) + bytes/doc + per-field breakdown if the cluster supports _disk_usage."""
import json
import sys

from load import client, store_bytes
from mappings import PREFIX

es = client("localhost", 9201)
rows = es.cat.indices(index=PREFIX + "*", format="json", bytes="b", h="index,docs.count,segments.count")
print(f"{'index':22s} {'docs':>8s} {'store B':>10s} {'B/doc':>8s}")
for r in sorted(rows, key=lambda r: r["index"]):
    n, b = int(r["docs.count"]), store_bytes(es, r["index"])
    print(f"{r['index']:22s} {n:8d} {b:10d} {b / n:8.0f}   segments={r['segments.count']}")
if len(sys.argv) > 1:
    for name in sys.argv[1:]:
        try:
            r = es.transport.perform_request("POST", f"/{PREFIX}{name}/_disk_usage", params={"run_expensive_tasks": "true"})
            fields = r[PREFIX + name]["fields"]
            print(f"\n{name}: total {r[PREFIX + name]['store_size_in_bytes']} B")
            for f, v in sorted(fields.items(), key=lambda kv: -kv[1]["total_in_bytes"])[:25]:
                print(f"  {f:34s} {v['total_in_bytes']:>9d} B")
        except Exception as e:  # noqa: BLE001
            print(f"_disk_usage unavailable for {name}: {str(e)[:150]}")
