# core_v4 monitoring agent: brief

You are the **monitoring agent** for the core_v4 build on the HPC (Draco). You run in `/vast/lu72hip/hm_pipeline`
(the prod checkout, `ENV=prod`). Your job, in this order:

1. Run the **limit run** of core_v4 (`--config limit=50`, tiny sample, separate `*_limit*` paths) until it is finished and
   verified.
2. Then run the **full run** of core_v4 (real data, 50M works) until it is finished and verified.
3. Fix whatever breaks on the way **without losing your monitoring**: when a job fails, gather the evidence, spawn a
   sub-agent with a precise bug report to fix it, and keep watching the rest of the run while it works.

The user is on a deadline. **What the user needs first** is an *index-ready pair of files*: `core_v4_projects.duckdb`
(fully enriched projects + all organizations + project<->org relations + topics) **and** `core_v4_works_base.duckdb` (ALL 50M
works and their relations, but **without any work enrichment**: no NLLB, topics, DCH, minorities, pillars, theme). ATTACHed
together they hold every entity, so the user can build all OpenSearch indexes except the work-enrichment-specific ones. The
user cannot wait for the work enrichments. Build these two first: it is the target `core_v4_base`. Only afterwards (if time
allows) run the work enrichments (tier 0 first, then tier 1), which end in `core_v4_works.duckdb` (replaces the base file).
Serving / OpenSearch index building is OUT of scope; you only build core_v4.

The user commits and pushes themselves (they also work on this prod checkout: `gst`, `gC`, `gP`). You never do.

---

## 0. First five minutes (do this before anything else)

1. **Is the clock-skew fix present?** (It may be missing from the checkout.)
   ```bash
   grep -n "time.sleep(1.5)" orchestration/rules/pipeline/core_v4/enrichment.smk
   ```
   If there is no hit, apply it. In `_core_v4_finish(...)` in `orchestration/rules/pipeline/core_v4/enrichment.smk`, add at
   the very top of the function body (after the docstring, before the `from pipelines...` imports or right after them):
   ```python
   import time
   # The shard sentinels live in .snakemake/ (sub-second mtimes) while the markers land on /work, whose mtimes have
   # whole-second resolution: a marker written within the same second as the last sentinel gets an older mtime and
   # Snakemake aborts with a "clock skew" WorkflowError. Waiting past the next second boundary avoids it.
   time.sleep(1.5)
   ```
   Record it in `FIXES.md` (see section 4). Without it, `core_v4_success` fails on the first shard that finishes.
2. `pgrep -af snakemake; squeue --me` — make sure no old Snakemake or job of the user is still around. Never run two
   Snakemake instances in this directory at the same time (directory lock).
3. Check the model files exist: `ls data/models data/models/nllb` must show `bert_classifier`, `nllb/{1.3B,600M,lid218e.bin}`.
4. Check DCH's tokenizer cache (see risk R1 in section 7): `bert-base-uncased` must be in the HF cache of the user that runs
   the jobs, because the DCH job may have no internet.
5. Read the docs listed in section 8 (at least the first three) and `orchestration/README.md` (core_v4 section).

---

## 1. Hard rules

- **Never `git commit`, `git push`, `git checkout`, `git stash`, `git reset`.** You may edit files in the checkout; the user
  commits. Keep every change small and log it (section 4).
- **Never touch core_v3** (files, outputs, rules). It is dead but its outputs are not yours to delete.
- **Never delete or overwrite expensive or unique data** without the user's explicit OK: `core_v4_staging.duckdb`,
  `data/enrichment/core_v4/nllb/**` (about 37 GPU-hours of translations), `data/cache/mapbox.duckdb`,
  `openaire_staging_v4.duckdb`, `openaire_raw.duckdb`, `data/pile/**`. You may delete the *partial* outputs of the job that
  failed, and everything under `data/enrichment/core_v4_limit/` and the `*_limit*` duckdbs (limit run only).
- **Geolocation stays OFF.** Do not pass `--config geolocation_max_requests`. Mapbox has a paid/limited budget; the user
  decides when to spend it (see section 6.5). Same for `geolocation_permanent`.
- **No heavy work on the login node.** Anything that takes more than a few minutes or a few GB (DuckDB scans of the 350 GB
  files, pytest with big fixtures, model loading) goes through `sbatch` / Snakemake. Light queries and `tail`/`grep` are fine.
- **One writer per DuckDB file.** Never open a DuckDB that a running job writes to (use `read_only=True` only on finished
  files; a `.wal` next to a file means a writer is or was active).
- **Do not rerun the full transformation or NLLB casually.** They are the expensive steps. Prefer fixing the failing step
  and letting Snakemake resume.
- Do not change the pipeline's design decisions (section 6). If a decision looks wrong, write it into `STATUS.md` for the
  user; do not silently redesign.
- Ask the user (write a clear `QUESTION FOR USER:` block in `STATUS.md` and, if you can, stop and wait) before anything
  destructive, anything that spends money, or anything that changes results semantics.

---

## 2. The loop

Keep Snakemake running in `tmux` (it must stay alive to submit the next jobs). You monitor it from a second shell.

### 2.1 Limit run

```bash
tmux new -s v4limit
cd /vast/lu72hip/hm_pipeline
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4 \
    --config limit=50 --keep-going
```

- `core_v4` = projects chain, works tier 0, works tier 1 (all in limit sizes, 1 shard each). Also make sure the new
  `core_v4_base` target works in the limit variant (`... core_v4_base --config limit=50`): it writes
  `core_v4_limit_works_base.duckdb` and `reports/pipelines/core_v4_limit/works_base_limit.md`. `--keep-going` lets independent
  jobs continue while you fix a failed one.
- A previous limit run got as far as: transformation (5 min, OK), report of the limit staging (OK), NLLB (finished, "wrote no
  rows": all 50 sample projects were English, fine), regions (OK), then failed on the clock-skew bug (section 0). Its
  outputs are still there and Snakemake resumes from them. If you change `limit=` to another value, first delete
  `data/enrichment/core_v4_limit/` (stale side outputs; the fingerprint guard would refuse them anyway).
- The limit run is **finished** when all three targets of `core_v4` completed: `core_v4_limit_projects.duckdb`,
  `core_v4_limit_works_linked.duckdb`, `core_v4_limit_works.duckdb` exist, and the reports
  `reports/pipelines/core_v4_limit/{staging_limit,projects_limit,works_linked_limit,works_limit}.md` exist.
- Then **verify** (section 9, checks A). Read the reports and look for anything odd (empty columns, weird counts). Only then
  go to the full run. If anything in the limit run reveals a design-level problem, stop and write it to `STATUS.md`.

### 2.2 Full run

Run it in stages so the important result exists as early as possible. Each stage is a separate Snakemake invocation (never two
at the same time):

```bash
tmux new -s v4full
# STAGE 1 (the deliverable): enriched projects + all works without work enrichments
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_base --keep-going
# STAGE 2 (optional, after stage 1 is verified): project-linked works enriched, then everything
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_works_linked --keep-going
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_works --keep-going
```

- `core_v4_base` = `core_v4_projects` (transformation, NLLB/topics/theme/minorities/pillars/DCH/regions for projects, assemble
  projects) + `core_v4_works_base` (assemble of ALL works with `--skip nllb,topics,theme,dch,minorities,pillars`, needs only
  the transformation, so it runs right after it, in parallel with the project enrichments; log
  `data/logs/core_v4_assemble_works_base.log`, output `core_v4_works_base.duckdb`). `core_v4_works_base` alone builds just the
  works file, and it is the first result you can hand to the user (about the transformation time + an hour or so).
- `core_v4_works` includes `core_v4_works_linked`, which includes `core_v4_projects` (works enrichment jobs take the projects
  duckdb as an ordering-only input). The stage-2 targets only replace/extend the works side; the user does not wait for them.
- **The user may have already started the full transformation by hand** with `sbatch ... pipelines.core_v4.transformation`
  (writes `core_v4_staging.duckdb`, log `data/logs/core_v4_transformation.log`). Check `ls -la /work/lu72hip/data/duckdb/core/`
  and `squeue --me` before starting: if a full `core_v4_staging.duckdb` already exists and is newer than
  `openaire_staging_v4.duckdb`, Snakemake will accept it (it has no metadata but the file exists); do not rebuild it. If it
  is still being written by a job, wait for that job.
- Expected DAG sizes (dry run `-n` first, always): `core_v4_projects` 45 jobs, `core_v4_works_base` 5, `core_v4_base` 47,
  `core_v4_works_linked` 85, `core_v4_works` 214, `core_v4` 215. (Limit run: `core_v4_base` is smaller.) A very different number
  means something is off (config, skip flags).
- Expected order: transformation, then NLLB (projects: 4 shards on `gpu-test`), then topics model, topics (8 shards),
  minorities (8), pillars (8), DCH (2 shards on an 80 GB A100), regions; theme after topics; assemble projects; reports.
- **Stage 1 is finished** when `/work/lu72hip/data/duckdb/core/core_v4_projects.duckdb`,
  `/work/lu72hip/data/duckdb/core/core_v4_works_base.duckdb` and
  `reports/pipelines/core_v4/{staging,projects,works_base}.md` exist and checks B and C0 pass. Write a clear milestone block into
  `STATUS.md` (paths, sizes, counts) at that point; this is what the user is waiting for. Stage 2: `core_v4_works_linked.duckdb`
  (tier-0 works enriched), then `core_v4_works.duckdb` (all works enriched).

### 2.3 Polling

- Poll every **2 to 5 minutes** while the run is young or a fix is in progress, every **10 to 20 minutes** during long GPU/CPU
  stretches. Never tighter than 60 seconds. Use your scheduling/monitor tools, not a busy loop.
- Keep your own context small: `tail -n 40`, `grep -E "ERROR|Error|Traceback|WARNING"`, never `cat` a big log.
- Each cycle: (1) `squeue --me -o "%.10i %.28j %.8T %.10M %.12l %R"`, (2) the tmux Snakemake pane (`tmux capture-pane -pt v4full -S -40`),
  (3) new files in `data/logs/` and `.snakemake/slurm_logs/`, (4) append one line to `STATUS.md`.
- Snakemake only prints when a job finishes or fails. Silence while a job is PENDING/RUNNING is normal.

---

## 3. Bug protocol

When a job fails (Snakemake prints `Error in rule ...`, a SLURM job goes FAILED/TIMEOUT/OUT_OF_MEMORY/CANCELLED, or a check
in section 9 fails):

1. **Stop the noise, keep the run alive.** With `--keep-going`, independent jobs continue. Do not kill Snakemake unless needed.
2. **Collect evidence** (small): the job's log (`data/logs/<rule>...log`), its SLURM log
   (`.snakemake/slurm_logs/rule_<rule>/<...>/<jobid>.log`), `scontrol show job <id>` (finished jobs vanish from
   squeue/scontrol after 300 s, `MinJobAge`; `sacct` does NOT work on this cluster: "invalid option -- '1'"), the last 40 lines,
   the exit reason (OOM, TIMEOUT, Traceback).
3. **Classify:**
   - *Infrastructure* (queue, time limit, memory, node problem, missing model/cache): fix by changing resources in the `.smk`
     rules or by rerunning; no sub-agent needed for trivial cases.
   - *Code bug* (Traceback in our code, wrong results, failed check): spawn a **fix agent**.
   - *Data problem* (unexpected values, schema surprise): fix agent, plus a note in `STATUS.md`.
4. **Spawn a fix agent** (Agent tool, general-purpose) with the template below. One fix agent per bug; never two agents on the
   same file at once. Meanwhile you keep monitoring.
5. When the fix agent reports: read its patch, run its tests, make sure it did not touch forbidden things, then **resume**
   by rerunning the same Snakemake command (it continues from what exists). Never rerun with `--forcerun` unless you must; if
   you must, put the target first and give absolute paths (`--forcerun` takes many values and swallows following positionals;
   deleting an enrichment's `_SUCCESS` alone reruns nothing, you must `--forcerun /abs/path/to/<name>/<entity>/_SUCCESS`).
6. Log it in `FIXES.md` and `STATUS.md`.

### Fix-agent prompt template

```
You are fixing ONE bug in the core_v4 pipeline in /vast/lu72hip/hm_pipeline (prod checkout, ENV=prod). Do not run heavy jobs on the login node (submit with sbatch if you must run something heavy).
RULES: never git commit/push/checkout/stash/reset; do not touch core_v3; do not delete data, side outputs (data/enrichment/core_v4*), caches or duckdbs; do not run Snakemake (the monitoring agent owns it); keep the change minimal and match the surrounding style; add or adjust a unit test; run the relevant tests by path, e.g. `ENV=prod uv run --no-sync python -m pytest src/pipelines/core_v4/<file> -q` (never `pytest src`); tests that need a GPU are skipped, that is fine. Show the planned change (files + intent) in your first message and check `git status` before editing.
BUG: <one-paragraph symptom>
EVIDENCE: <log lines / traceback / job ids / paths>
SUSPECTED FILES: <paths>
CONTEXT: read src/pipelines/core_v4/README.md section 0, the READ_*.md next to the failing module, and this.md sections 6-7.
DELIVER: the fix, the test, `git diff` of your change saved to fixes/<NN>-<slug>.patch, and a report (< 200 words): root cause, what you changed, how you verified, what the monitoring agent must rerun.
```

---

## 4. Files you maintain (repo root, untracked, the user reads them)

- `STATUS.md`: append-only, one timestamped line per cycle when something changed, plus a short summary block at each
  milestone (limit run done, projects done, ...). Use `QUESTION FOR USER:` blocks for decisions.
- `FIXES.md`: one entry per change: date, symptom, root cause, files, patch path, test.
- `fixes/*.patch`: `git diff` of each fix, so the user can review and commit.

---

## 5. Environment and cluster facts (learned the hard way today)

- Repo: `/vast/lu72hip/hm_pipeline` (run everything from here). Data: `/work/lu72hip/data` (`duckdb/{sources,core}`, `enrichment/`,
  `cache/`, `pile/`). Models: `/vast/lu72hip/hm_pipeline/data/models` (repo-relative). Logs: `data/logs/` (repo-relative) and
  `.snakemake/slurm_logs/`.
- Whole-second mtimes on `/work` vs sub-second in `.snakemake/` caused the "clock skew" abort (fixed by the sleep, section 0).
- **The `fat` partition is congested** (5 nodes; someone runs ~100-job arrays). CPU rules therefore request
  `slurm_partition="fat,standard,long"` with short time limits (transformation/assemble 360-720 min, reports 240, enrichments
  120-1440). Short limits backfill; a 72 h request sat pending for hours. For an already pending job:
  `scontrol update JobId=<id> TimeLimit=04:00:00 Partition=fat,standard,long`. To see free nodes:
  `sinfo -N -h -p fat,standard,long,short -o "%.10N %.9P %.7t %.14C %.9m %.9e"`; to estimate starts:
  `sbatch --test-only --partition=<p> --mem=200G --cpus-per-task=32 --time=04:00:00 --wrap true`.
- **GPU**: `gpu-test` has 3 idle nodes (gpu010-012), 12 h max. DCH needs an 80 GB A100 (`gres=gpu:a100:1`, `constraint=a100_80gb`).
  NLLB needs only ~10 GB (`gres=gpu:1`). Cluster driver is 530 (CUDA 12.1); torch is pinned `<2.12` cu126; CTranslate2 works.
  Compute nodes have **no internet**: `HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1` is set on the NLLB rule.
- **zsh gotchas** in the user's shell: `grep [s]nakemake` errors (glob); write `pgrep -af snakemake`. Inside `sbatch --wrap "..."`
  use `> file 2>&1`, not `&>` (not portable in sh). Snakemake rule shells are bash and use `&>`.
- `Directory cannot be locked`: another Snakemake runs here, or a stale lock. Check `pgrep -af snakemake`; if none, run
  `uv run snakemake -s orchestration/Snakefile --unlock`.
- DuckDB's `memory_limit` ignores the SLURM cgroup, so scripts cap themselves: `--mem-mb` minus 40 GB headroom. Never lower
  `--mem`/`mem_mb` without the flag following (`{resources.mem_mb}` is passed on).
- Snakemake's slurm plugin prints "No SLURM account given ... sacct: invalid option -- '1'": harmless.
- Shard sentinels (`.snakemake/sentinels/enrichment/core_v4*/<name>/<unit>/sXofN.done`) are `temp()`; the `_SUCCESS` markers
  under `/work/lu72hip/data/enrichment/core_v4*/<name>/<entity>/` are the real artifacts.

---

## 6. The pipeline (what core_v4 is)

### 6.1 Flow

```
openaire_staging_v4.duckdb (+ ror_raw + cordis_full + core_v2 geolocation files)
   | core_v4_transformation   (trim 218M works to 50M, ROR + Cordis + core_v2 coordinates, Cordis addresses)
   v
core_v4_staging.duckdb   (read-only from here on)
   | enrichments write side parquet files, sharded, resumable, independent (read staging read-only):
   |   nllb -> topics -> theme ; nllb -> minorities / pillars / dch ; regions ; geolocation (OFF, last)
   v
data/enrichment/core_v4/<name>/<entity>/part-*.parquet + _SUCCESS (markers carry a staging fingerprint)
   | assemble --entity project | work [--tier 0]
   v
core_v4_projects.duckdb   (project, organization, project<->org relation, topic, relation_topic[project])
core_v4_works_base.duckdb (ALL 50M works + relations, NO work enrichments; needs only the transformation)  <- with the
                          projects file this is index-ready: ATTACH both. THE FIRST DELIVERABLE (target core_v4_base).
core_v4_works_linked.duckdb (tier 0 works, enriched, ~5M)   then   core_v4_works.duckdb (all 50M works, enriched; replaces base)
```

Limit variant: same with `core_v4_limit_*` duckdbs and `data/enrichment/core_v4_limit/`.

### 6.2 Config keys (`config/pipelines.yaml`, block `core_v4`; limit block has `_limit` suffixes)

`path_duck_staging`, `path_duck_projects`, `path_duck_works_base`, `path_duck_works_linked`, `path_duck_works`, `path_duck` (unused legacy),
`path_enrichment_dir`, `path_cache_dir`, `work_cap: "50000000"` (string), `path_core_v2_geolocations`,
`path_core_v2_institution_pic`. Prod paths resolve under `/work/lu72hip/`.
Other inputs: `openaire_dump.path_duck_staging_v4`, `ror_dump.path_duck`, the Cordis full DB
(`data/duckdb/sources/cordis_full_projects_no_pdfs_raw.duckdb`; env `CORE_V4_CORDIS_DB` overrides), `minorities_raw.duckdb`
(prod: 278 groups), `data/pile/oa_topics/openalex_topic_mapping.csv`, checkpoint
`data/checkpoints/loading/cordis_full_projects_no_pdfs/mtime.cp` (a transformation input).

### 6.3 Snakemake

Targets: `core_v4_base` (= projects + works_base, the deliverable), `core_v4_projects`, `core_v4_works_base`, `core_v4_works_linked`, `core_v4_works`, `core_v4`. Rule files:
`orchestration/rules/pipeline/core_v4/{merge,enrichment,assemble}.smk`. `--config` keys: `limit=N` (switches everything to
the limit paths, transformation `--limit N`), `skip=nllb,dch,...` (drops rules, assemble gets `--skip`; skipping topics also
skips theme), `shards=N` / `shards_<name>[_t<tier>]=N`, `work_cap`, `geolocation_max_requests` (0/absent = off),
`geolocation_permanent`. Each enrichment = N shard jobs + a local `core_v4_success` rule that stamps the fingerprint and writes
`_SUCCESS` (sleeps 1.5 s first, see section 0). Default shards: NLLB projects 4 x 6 h, works tier 0 6 x 4 h, works tier 1 16 x 6 h
(`gpu-test`); DCH 2 (projects) on 80 GB A100.

### 6.4 Decisions (do not redesign)

- Keep all `instances/authors/sources` on works. Restored `work.countries` (codes) and `project.doi` in staging v4.
- **Trim to 50M at seed:** all project-linked works (tier 0, `link_tier=0`, about 5.0M) + org-only works (tier 1) newest first
  (cutoff about 2018-05-01); works with no relation dropped; publication dates after today -> NULL; tie-break: date, has
  description, id. Relations pointing at dropped works, or at missing projects/orgs, are dropped.
- **Cordis merge:** projects by grantId (+ DOI fallback); orgs PIC-first, name+country only for rows with no PIC match, never name
  only; `cordis_ec_contribution`/`cordis_type` on project->org relation rows; address columns
  (`address_street/postalcode/city/country`, `nuts3`) on matched orgs; pass 2 = org-level PIC (independent of project match).
- **Coordinates** (organization.geolocation `DOUBLE[]` = `[lat, lng]`, `geolocation_source`): ROR -> Cordis -> core_v2 legacy
  (PIC via institution_pic file, then name+country), each only where still NULL. Cordis stores `[lon, lat]`, core_v2 too:
  both are flipped.
- **NLLB:** fastText LID (`lid218e.bin`) on the text (the `language` label is ignored, 33% are `und`); only non-English is
  translated (about 20% of works); 1.3B distilled, CTranslate2 float16, beam 1; translations in side files; assemble overwrites
  title/summary (project), title and `descriptions[1]` (work) and sets `is_translated`.
- Topics: TF-IDF (spaCy `en_core_web_sm`) against the OpenAlex topic taxonomy; theme (Economy/Tourism) from the best topic above
  `--min-score` 0.1 (provisional); DCH: BERT classifier (English only), chunked; minorities: Aho-Corasick, typo variants only
  for keywords >= 8 chars and never a common English word; pillars: 5-bit `UTINYINT` (lowest bit first: inclusive, sustainable,
  resilient, innovative, global); regions: static country table.
- Assemble defaults for missing rows: `is_translated` false, `is_ch`/`pred` NULL, `minority_qid` `[]`, `pillars` 0, `theme` NULL.
  `link_tier` SMALLINT on works. Two gold files (projects first), works file has no organization/topic tables (attach both later).

### 6.5 Geolocation / Mapbox (off for you)

Mapbox batch geocoding of orgs with a real address and no coordinates (~16k expected), cached in `data/cache/mapbox.duckdb`
(with a `permanent` flag). The user decided to store free-tier (temporary) results for now (`geolocation_source =
'mapbox_temporary'`) and pay for permanent later. It is **not part of your run**; when the projects stage is finished, write in
`STATUS.md` that geolocation can be enabled with `--config geolocation_max_requests=N` and let the user decide N.

---

## 7. Stage by stage: numbers to expect, checks, and the risks I would look at first

Reference numbers (prod, Phase 1, 2026-09-19): works 218,421,450 (project-linked 5,001,873 = 2.29%; org-only 115,456,657;
neither 97,962,920); relations 296,915,370 (282.8M product->org, 8.6M project->product, 5.55M project->org); projects
3,893,065 (99,887 with `doi`); orgs 494,099 (126,400 with ROR coordinates; 69,883 with a PIC; 157,221 without country);
works with `countries` 34,507,649. At the 50M cap: all 5.0M tier 0 + 44,998,127 tier 1, cutoff 2018-05-01 (106,004 works share
that date, 2,750 dropped), 16,879 works had future dates. Cordis: 142,773 projects, 87,439 (61.2%) matched by grantId (was 68.6% in
core_v3: a diagnosis script `investigation/cordis_project_match.py` exists, not yet run); triplets in matched projects 480,133,
PIC 89.7%, name+country 52.0%; org-level PIC ~68,125 orgs. Limit run transformation (50 projects) took 5 min and gave: 50/50
projects matched, 296/325 triplets (295 by PIC), org-level pass +817 orgs, core_v2 tier +107 orgs, 0 dangling relations.

### Transformation (`core_v4_transformation`, 1 job)
Full run: expect tens of minutes to an hour or more (the v3 seed of 218M works alone took 14 min; v4 also ranks 218M works and
writes the 50M-row wide table). Log: `data/logs/core_v4_transformation.log` (limit: `..._limit.log`). Look for: tier counts and
cutoff date, "relation rows pointing at a dropped work (must be 0)", "... missing project or organization (must be 0)",
Cordis pass counts (compare with the numbers above), geolocation tier counts.
Risks: memory (DuckDB limit 160 GB of 200), disk spill (temp dir next to the db on /work), the `openaire_staging_v4` file being read
by other jobs (fine, read-only), it aborts on purpose if `project.doi`/`work.countries` are missing (rebuild staging with
`stage_openaire_dump_v4`).

### NLLB (`core_v4_nllb`, GPU)
Real run: expect about 13.3k source tokens/s per GPU on the 1.3B model, ~10 GB VRAM. Look at the first shard's log for the
throughput line and the non-English share (expected ~19% of titles, ~20% of works; projects may differ: many national-funder
titles are non-English). Resumable: rerun continues (anti-join on done ids). "shard i/n wrote no rows" is only a warning (the
`seen` companion output holds all processed rows).
Risks: model load offline (`data/models/nllb`), OOM on very long descriptions (they are truncated/chunked), TIMEOUT at 6 h
(rerun), GPU node problems. Do not delete `data/enrichment/core_v4/nllb/**` (expensive).

### Topics / theme / minorities / pillars / regions (CPU)
Topics is the biggest CPU job at works scale (fixture: ~1,400 works/s with 8 workers on 325-char texts; real text is several
times longer; sharded 8x). `core_v4_topics_model` builds the TF-IDF model from translated project text, so it needs NLLB done.
Minorities: prod has 278 groups (local dev 312, canonical for dev only). Pillars: pure SQL, fast; the log reports per-pillar
match rates (a pillar above ~30% is flagged: tell the user, do not change stems).
Risks: spaCy model `en_core_web_sm` present in the venv; memory with 16 workers; theme needs `topics` complete for the tier.

### DCH (GPU, 80 GB A100)
Risks: R1 below; per-shard runtime; the BERT model path `data/models/bert_classifier`.

### Assemble (`core_v4_assemble_*`, 200 GB, 32 CPUs)
Refuses side outputs without `_SUCCESS` or with a fingerprint that differs from the current staging
(`--allow-stale name1,name2` exists; do not use it without a reason). Fails if the assembled row count differs from staging.
Works assemble writes a `.tmp` file next to the output and needs lots of disk; runtime ceiling 720 min.

### Known risks, ranked by how likely I think they bite

- **R1 DCH tokenizer.** `DchClassifier.load` calls `BertTokenizerFast.from_pretrained("bert-base-uncased")` with no offline
  setting. If the GPU node has no HF cache for the running user, it fails with a connection error. Populate the cache once from
  the login node: `uv run python -c "from transformers import BertTokenizerFast as T; T.from_pretrained('bert-base-uncased')"`.
- **R2 Queue starvation** on `fat`: use the partition list + short limits (section 5).
- **R3 Time limits**: enrichment/assemble ceilings are guesses; on TIMEOUT raise the runtime in the `.smk` rule (keep it as low
  as sensible) and resume.
- **R4 Fingerprint mismatch** (`ShardStampMismatch`, or assemble stopping on "stale"): staging was rebuilt while shards ran, or
  an old `_SUCCESS` exists from a different staging. The limit variant's stale outputs: delete `data/enrichment/core_v4_limit/`.
- **R5 Memory at scale** in the transformation and assemble (DuckDB spill to `<db>.tmp` on /work).
- **R6 Login node limits** if Snakemake itself is heavy: it is not; just keep it in tmux.
- **R7 Silent data drift**: the checks in section 9 exist to catch results that "succeed" but are wrong.

### Already fixed today (do not redo)
Clock skew (sleep, section 0); `--forcerun` swallowing targets; silent skip of the DOI fallback (now aborts); staging v4 rebuilt
with `doi` and `countries`; minority typo false positives ("hazard" matched Hazara); stale side outputs (fingerprint guard);
dangling relations; CPU rules moved to `fat,standard,long` with short time limits.

### Open items you may run into (report, do not decide)
- Cordis project match dropped from 68.6% to 61.2%: `investigation/cordis_project_match.py` is written, not run
  (`ENV=prod uv run python -m pipelines.core_v4.investigation.cordis_project_match`, needs 32 GB, via sbatch).
- `investigation/geolocation_coverage.py` computes how many projects have >= 2 geolocated participants (the user's real
  geolocation metric): run it after the projects stage via sbatch:
  `sbatch --partition=fat,standard,long --cpus-per-task=16 --mem=128G --time=02:00:00 --wrap "cd /vast/lu72hip/hm_pipeline && ENV=prod uv run python -m pipelines.core_v4.investigation.geolocation_coverage --mem-mb 128000 --threads 16 --staging-db /work/lu72hip/data/duckdb/sources/openaire_staging_v4.duckdb > data/logs/geolocation_coverage.log 2>&1"`.
- Theme `--min-score` 0.1 is provisional; the pillar match rates and minority precision want a human look.
- The fingerprint does not detect changed text with the same ids.

---

## 8. Read these (in this order)

1. `src/pipelines/core_v4/README.md` (section 0 = overview and decisions; sections 1-6 = Phase 1 measurements)
2. `src/pipelines/core_v4/READ_TRANSFORMATION.md` and `READ_ASSEMBLE.md`
3. `src/pipelines/core_v4/enrichment/README.md` (side-output contract, tiers, fingerprint) and `README_topics_theme_dch.md`
4. `orchestration/README.md` (core_v4 section: commands, config keys, local test tricks) and `orchestration/rules/pipeline/core_v4/*.smk`
5. `src/enrichment/nllb_translator/README.md` (measured numbers), `src/enrichment/geolocation/README.md`,
   `src/enrichment/minority_matching/README.md`
6. Code: `src/pipelines/core_v4/{transformation,assemble}.py`, `src/pipelines/core_v4/enrichment/{cli,text_sources,side_outputs,fingerprint}.py`

Tests: `ENV=prod uv run --no-sync python -m pytest src/pipelines/core_v4 src/enrichment -q` (last local result: 265 passed, 7 skipped;
GPU tests skip). `pytest src` as a whole crashes at import, always run by path.

---

## 9. Checks

DuckDB one-liners (`uv run python -c "..."`, always `read_only=True`, only on finished files).

**A. After the limit run**
- Reports exist and look sane: `reports/pipelines/core_v4_limit/*.md`.
- `core_v4_limit_projects.duckdb`: tables `project, organization, relation, topic, relation_topic`; row counts of `project` equal the
  limit staging's (`core_v4_limit_staging.duckdb`); no `work` table there. `core_v4_limit_works*.duckdb`: `work` (+ `relation`,
  `relation_topic`), no `organization`/`topic`.
- Columns present with expected types: `project.is_translated BOOLEAN, is_ch BOOLEAN, pred FLOAT, minority_qid VARCHAR[],
  pillars UTINYINT, theme VARCHAR`; `work.link_tier SMALLINT`; `organization.geolocation DOUBLE[], geolocation_source, region`.
- `select count(*) from work where link_tier is null` = 0; tier-0 file has only `link_tier = 0`.
- Re-running the same Snakemake command says "Nothing to be done".

**B. After the full projects stage**
```sql
-- projects file
select count(*) as n, count(theme) as with_theme, count(*) filter (where pillars > 0) as with_pillar,
       count(*) filter (where len(minority_qid) > 0) as with_minority, count(*) filter (where is_translated) as translated,
       count(is_ch) as with_dch from project;            -- n = 3,893,065
select geolocation_source, count(*) from organization group by 1 order by 2 desc;   -- ror ~126k, then cordis, core_v2
select count(*) from organization where region is not null;
select count(*) from relation_topic where type = 'project';   -- close to the number of projects with text
```
- `n` must equal the projects in `core_v4_staging.duckdb` (3,893,065 minus none: no project trimming). `is_translated` share plausible
  (funder titles: expect a sizeable share). DCH: `count(is_ch)` = n (minus rows without text). Theme/minority/pillars shares plausible
  (pillars: report the per-pillar rates from the logs to the user).
- Compare the transformation log numbers with section 7.

**C0. After `core_v4_works_base` (stage 1)**
- `core_v4_works_base.duckdb` has tables `work`, `relation`, `relation_topic` (empty), and no `organization`/`topic`.
- `select count(*) from work` = the number of works in `core_v4_staging.duckdb` (about 50,000,000; the transformation log has the
  exact trimmed count and the tier split, tier 0 about 5.0M); `count(*) filter (where link_tier = 0)` = tier-0 count.
- Defaults everywhere: `is_translated` all false, `is_ch` all NULL, `minority_qid` all `[]`, `pillars` all 0, `theme` all NULL.
- `relation` contains the work<->organization and project->work rows (no dangling: every `source`/`target` that is a work exists in `work`).
- Attach test (proves the pair is index-ready): in one session `ATTACH` projects and works_base read-only and run a join such as
  `select count(*) from base.relation r join proj.project p on p.id = r.source join work w on w.id = r.target` (project->work).

**C. After works (stage 2)**
- `work` rows in `core_v4_works.duckdb` = the number of works in staging (about 50,000,000; tier 0 about 5.0M, log line);
  tier-0 file rows = tier-0 count; no `work` with `link_tier` null; `publicationDate` never after today.
- Dangling relations = 0; `relation_topic` rows for `type='work'` about the number of works with text.

---

## 10. Definition of done and final report

Milestone 1 (the one that matters) = limit run verified and the full `core_v4_base` (projects file + works_base file) finished and
verified, reports generated. Milestone 2 (nice to have) = `core_v4_works_linked` and `core_v4_works` finished and verified.
At each milestone write a block in `STATUS.md`:

- what ran when (timeline, job durations, GPU hours actually used),
- output files and sizes, the reports' paths,
- the numbers from checks B and C next to the reference numbers,
- every fix (link to `FIXES.md`),
- open items and suggested next steps (geolocation with a budget, the two investigation scripts, theme `--min-score`, pillar match
  rates, minority precision review),
- anything you are unsure about.

If you are blocked and need the user, write `QUESTION FOR USER:` at the top of `STATUS.md` with the exact decision needed and what you
will do meanwhile.
