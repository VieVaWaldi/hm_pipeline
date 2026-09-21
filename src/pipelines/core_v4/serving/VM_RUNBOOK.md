# VM runbook: get the core_v4 export into the production OpenSearch on DIGICHerVM

Status 2026-09-21. Draft by the planning session; items marked **VERIFY** are unknowns that must be checked on the VM before relying on them.
Related: `EXPORT_README.md` (export/load details), `SERVING_DESIGN.md` (decisions, mappings), `LOCAL_LOAD_REPORT.md` (sample run, vm_smoke.py).

## 0. Facts about the VM (from `heritagemonitor/infra/PRODUCTION.md` and the prod compose)
- 8 cores, 31 GB RAM, **HDD** (`rota=1`), 5 TB disk. The site (web, api, postgres, caddy) runs on the same box: the load competes with it.
- OpenSearch container `hm-opensearch`, 3.8.0, heap 12g, container `mem_limit: 24g` (page cache is charged to the container), `bootstrap.memory_lock`,
  **auth on** (`admin` + `OPENSEARCH_INITIAL_ADMIN_PASSWORD`), plain **http**, published only on loopback `127.0.0.1:9200`. So the loader must run **on the VM**.
- Outbound internet goes through an institutional proxy (docker daemon has its own proxy drop-in). **VERIFY** whether the shell can reach PyPI / astral.sh (uv).
- Data lives in the named volume `os-data-prod`. `vm.max_map_count=262144`, `vm.swappiness=1` are set (re-check after any reboot).
- Old data: 10M works in Postgres ran "slow but usable" on this box; 50M works in OpenSearch will be slower to build than on the laptop (loader speeds measured on an SSD laptop are an upper bound).

## 1. Preflight (10 min, on the VM)
**The VM shell has an outbound proxy set** (a `curl localhost:9200` returned a Squid error page): always bypass it for local calls, otherwise every curl (and possibly tools that honour
`http_proxy`) goes to the proxy instead of OpenSearch. Once per shell: `export NO_PROXY=localhost,127.0.0.1 no_proxy=localhost,127.0.0.1` (or use `curl --noproxy '*'`). Also check that the
password variable is really set: `echo ${#PW}` must print a number > 0. (Keep the proxy for uv/pip: PyPI needs it.)
```bash
export NO_PROXY=localhost,127.0.0.1 no_proxy=localhost,127.0.0.1
docker ps --format '{{.Names}} {{.Status}}'                       # hm-opensearch healthy?
curl -s -u admin:$PW localhost:9200/_cluster/health?pretty         # status, number_of_nodes 1
curl -s -u admin:$PW 'localhost:9200/_cat/indices?v&bytes=gb'      # known: the VM holds copies of the old test data: `minorities` and `demo_collaboration_edges` (user 2026-09-21)
df -h / ; free -g ; sysctl vm.max_map_count vm.swappiness ; nproc  # need ~90 GB free in the docker volume location (final ~30 GB, peak while loading/merging ~2x), 5 TB is plenty
```
Decide the **window**: works loading (hours) and forcemerge saturate the HDD; site latency suffers. Prefer night / low traffic. `--threads 4` leaves 4 cores for the site.

### 1b. REMINDER (user asked to be reminded): turn OFF audit logging on the VM before the big load
The security plugin's audit log is very chatty (on the Mac dev instance: 14 daily `security-auditlog-*` indices, ~5 GB, 5-12M docs/day). On an HDD box that is wasted I/O, and it competes with the works load.
Two ways (**VERIFY** which one works with this image; the REST one needs no restart):
```bash
# (a) REST, dynamic, no restart: disable the audit log (admin cert/permissions permitting)
curl --noproxy '*' -s -u admin:$PW -X PUT 'localhost:9200/_plugins/_security/api/audit/config' -H 'Content-Type: application/json' -d '{"enabled": false}'
# (b) static, needs a container restart: in opensearch.yml (mounted config) set   plugins.security.audit.type: noop
```
Afterwards: `curl --noproxy '*' -s -u admin:$PW 'localhost:9200/_cat/indices/security-auditlog*?v&bytes=mb'` must stop growing; old `security-auditlog-*` indices can be deleted to free disk.

## 2. Code and dependencies on the VM
- `git clone` / `git pull` hm_pipeline (same commit as the export). Only the light group is needed (duckdb, pyarrow, opensearch-py, no torch):
  `cd ~/hm_pipeline && uv sync --frozen --only-group serving`. **VERIFY** uv is installed and the proxy env (`HTTPS_PROXY`) reaches PyPI.
- Fallback if PyPI is blocked: on the Mac build a wheel bundle for Linux (`pip download --only-binary=:all: --platform manylinux2014_x86_64 --python-version <VM python> -d wheels duckdb pyarrow opensearch-py <its deps>`), rsync `wheels/`, `pip install --no-index --find-links wheels ...` into a venv.
- Credentials for the loader: `export OPENSEARCH_USERNAME=admin OPENSEARCH_PASSWORD=...` (the loader reads them; host 127.0.0.1, port 9200).

## 3. Upload the data (about 25 min at 50 Mbps, 7.8 GB) - can start right now
```bash
# on the Mac (tmux/nohup, resumable)
rsync -av --partial --progress /Users/wehrenberger/Code/DIGICHer/hm_pipeline/data/serving_export/ digicher:~/serving_export/
# on the VM: compare with the cluster verification
du -sh ~/serving_export/*; ls ~/serving_export/works | wc -l          # works_00..09, 10 files; projects 719 MB, organisations 59 MB, works ~7.1 GB
```
Keep `~/serving_export` on the VM: it is the rebuild source (5 TB disk).

## 4. Load, small indexes first (the site can use them while works loads)
```bash
cd ~/hm_pipeline/src/pipelines/core_v4/serving/export
uv run --frozen --only-group serving python load.py --parquet ~/serving_export --host 127.0.0.1 --port 9200 --only organisations,minorities,grants,projects
```
- Expected: minorities/grants seconds, organisations minutes, projects roughly 15-40 min on the HDD (3.9M docs, 2 shards, title autocomplete). Uses refresh -1 while loading, then restores 30 s, refresh, forcemerge, count check (exit code 1 on mismatch).
- Existing index of the same name: the old test `minorities` (copy of the Mac dev one, test data) **may be overwritten, breaking the current minorities page is accepted (user 2026-09-21)**:
  load it with `--recreate` (drops and rebuilds). `demo_collaboration_edges` has a different name and is left alone. For the other indices (empty names) a plain first load is fine; for later rebuilds without
  downtime use `--suffix _v2` + alias switch (this fails while a concrete index of the alias name exists, so the first load has to be a plain one).
- Projects now have **1 shard** (exact facet counts; decision 2026-09-21). If `vm_smoke.py` shows the projects aggregations > ~2 s cold, the fallback is 2 shards + `shard_size` 500 (rebuild of projects only, ~30 min).
- Verify: counts organisations 494,099, projects 3,893,065, minorities 278, grants 6,119; 3 sample searches (`curl` or the local checks).

## 5. Load works (long): tmux/nohup, resumable
```bash
tmux new -s works
uv run --frozen --only-group serving python load.py --parquet ~/serving_export --host 127.0.0.1 --port 9200 --only works --threads 4 2>&1 | tee ~/load_works.log
```
- Measure docs/s on the first file (5M docs each), extrapolate the total; my guess on HDD is a few thousand to ~10k docs/s = 1.5-5 h **(unmeasured, treat as a guess)**, then forcemerge of ~25 GB on HDD another 1-2 h (I/O heavy, run at night; `--no-forcemerge` and do it later if needed).
- Watch: `docker stats hm-opensearch`, `curl .../_cat/indices/works?v&bytes=gb`, `curl .../_nodes/stats/jvm,os` (heap < 75%), `iostat -x 5`, site latency. HTTP 429 / bulk timeouts -> lower `--threads` (2) or `--chunk` (500). Interrupted: rerun the same command, finished files are skipped (`--finalize-only` afterwards if it stopped after loading).
- Do not restart the container during the load unless necessary (refresh is -1 and translog is large).

## 6. Verify on the VM
- Doc counts (table in `EXPORT_README.md`), `_cluster/health` yellow/green (replicas 0 -> green), shard sizes (`_cat/shards?v`), `refresh_interval` restored to 30s, `number_of_replicas` 0.
- Full-scale smoke and latency table: `python vm_smoke.py --host 127.0.0.1 --port 9200` (from the local agent; prints cold/warm latencies for: typo fallback on works, big-org aggregations, funding agg over all projects, first-query global ordinals on `org_ids`, biggest-project works tab). **Run it twice** (cold vs warm page cache); on an HDD the first run after a load/restart is slow.
- The `== _count (approximate totals) ==` section times the api's parallel `_count` (works/projects/organisations, blank/word/filter variants); a single `_count` above 1200 ms (first or max) is RED = the api times out and the UI falls back to '10,000+' instead of 'about N'.
- Red flags to act on: works typo fallback > 2 s (lower `max_expansions`/threshold or disable the fallback for works), `org_ids` first query > 3 s (set `eager_global_ordinals` on `org_ids`), heap > 85%.

## 7. Backup / rebuild / rollback
- The Parquet in `~/serving_export` is the rebuild source: `load.py --recreate` (downtime) or `--suffix _v2` then alias switch (no downtime). Rebuild time = the load time above.
- Optional faster restore: an OpenSearch filesystem snapshot (needs `path.repo` in the opensearch config = container restart) after the successful load. Only worth it if the load took many hours.
- Tell the app team (heritagemonitor phase, later): index/alias names `projects organisations works minorities grants`, basic-auth env vars, `api/topics.json` and `api/publishers.json` (copy them into the api, they are the in-memory topic tree and publisher typeahead sources).

## 7b. Keeping the indexes warm (HDD) and the RAM budget (measured 2026-09-21)
- Smoke results: after the merges the WARM latencies are excellent (collaboration 5-15 ms agg, 160-190 ms for the 2000-project query network); the very first call after a fresh state was 24-30 s (query network) and 4.6 s (500 org docs).
  A container restart only clears OpenSearch's own caches; the Linux page cache survives it, so `vm_smoke.py --runs 1` right after a restart still shows warm numbers (181-186 ms). A VM reboot or heavy works traffic that evicts
  the projects/organisations pages brings the cold behaviour back. Budget: the container has 24g (12g heap + ~12g page cache); works 18 GB + projects 5.6 GB + organisations 0.6 GB = 24 GB of index files, so they compete.
- Keep the collaboration-critical indexes (projects + organisations = 6.2 GB) resident: warm them at boot and every 30 min by reading their files inside the container (the page cache is charged to the container):
```bash
cat > ~/warm_os.sh <<'EOF'
#!/bin/bash
source ~/.os_env
for idx in projects organisations; do
  uuid=$(curl -s -u "admin:$PW" "localhost:9200/_cat/indices/$idx?h=uuid" | tr -d ' \n')
  docker exec hm-opensearch sh -c "cat /usr/share/opensearch/data/nodes/0/indices/$uuid/*/index/* > /dev/null"   # the image has no `find`: shell globs
done
EOF
chmod +x ~/warm_os.sh && ~/warm_os.sh            # ~6 GB sequential read, about a minute on the HDD
( crontab -l 2>/dev/null; echo '*/30 * * * * ~/warm_os.sh' ) | crontab -
```
  Also do it after any VM reboot / container restart. Only projects + organisations: warming works would evict them.
- If `docker stats hm-opensearch` shows the cache is too small: lower the OpenSearch heap from 12g to 10g (peak heap in the smoke run was 6.5 GiB) rather than raising the container limit.
- Host budget (31 GB): caddy 128m + web 768m + api 1g + postgres 512m + opensearch 24g = 26.4 GB of limits. The api holds reference data in memory (topics, publishers, an org table of 494k rows stored compactly, blank-page caches): plan for a
  compact table (~60-120 MB); recommended: api `mem_limit: 1536m` with `NODE_OPTIONS=--max-old-space-size=1152`, opensearch heap 10g / `mem_limit: 23g` (total ~25.9 GB, ~5 GB left for the OS).
- The blank-query funding map (top 500 orgs over all projects) is the one slow query (1.1 s cold): the api caches it.

## 8. Open questions to answer on the VM (Section 1 and 2)
1. Does `hm-opensearch` currently hold indices (name clashes)? 2. Is uv/PyPI reachable? 3. Site traffic window? 4. Keep heap at 12g / limit 24g (recommended by `PRODUCTION.md`; the local agent's suggestion of 8g is unmeasured) - change only if `vm_smoke.py` shows page-cache pressure. 5. Is `plugins.security.ssl.http.enabled=false` really giving plain http (the compose marks it "UNVERIFIED"): `curl -u admin:$PW http://localhost:9200` must answer.
