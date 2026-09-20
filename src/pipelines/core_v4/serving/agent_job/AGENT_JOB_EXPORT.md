# AGENT JOB 2: run, monitor and verify the core_v4 -> OpenSearch export on the cluster

You are the **export agent** on Draco. You run in `/vast/lu72hip/hm_pipeline` (prod checkout). The user is on a deadline (about 1.5 days left), so:
run the job, watch it, verify the output, and finish with a clear list of next steps. Do not wander into other work.

## Rules (same as the analysis job)
- Work **only from `/vast/lu72hip/hm_pipeline`**. Never read or write under `/home`. Scratch under `/vast/lu72hip/hm_pipeline/tmp_session/`.
- **Everything heavy runs through slurm (`sbatch`)**, never on the login node. Light checks (`squeue`, `sacct`, `tail`, `ls`, `du`, tiny DuckDB metadata reads) are fine on the login node.
- The database `/work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb` (132 GB) is opened **read-only** by the export. Never write to it or to anything under `/work`.
- Do not git commit or push. Do not change the pipeline or serving code unless the export fails because of a real bug: then make the **smallest** fix in the
  working tree, append the exact diff and the error to `tmp_session/EXPORT_FIXES.md` (the user brings it back to the laptop session), and rerun. If you are unsure, stop and report instead of guessing.
- **Minority tags:** the export is run with `--minority-exclude src/pipelines/core_v4/serving/export/minority_exclusions.csv` (already in `export.sbatch`): stored tags minus a small deny-list of obviously wrong pairs (2,878 pairs, decision D32), all 278 groups stay. **Do not pass `--minority-override`** (mutually exclusive, not wanted). If the export aborts because the file is missing or "does not belong to this database", stop and report (git sync problem), do not edit the CSV.
- Read first: `src/pipelines/core_v4/serving/EXPORT_README.md` (section 1) and `SERVING_DESIGN.md` (sections 3, 3a, 6). The code is `src/pipelines/core_v4/serving/export/`
  (`export.py`, `export.sbatch`, `sql/`). Everything runs with the light environment: `uv run --frozen --only-group serving ...` (duckdb only, no torch/CUDA).
  If `uv` tries to resolve/download torch, stop and report (the `serving` group must have been synced with git).

## Steps
### 1. Preflight (login node, seconds)
- `git log --oneline -3` and `git status --short` (report them); confirm `export/export.py`, `export/export.sbatch`, `export/sql/` exist and `pyproject.toml` has the `serving` dependency group.
- Check free space on `/vast/lu72hip/hm_pipeline` (need ~25 GB for the output, plus DuckDB spill space under `tmp_session/duck_tmp`).

### 2. Smoke run through slurm (0.1% of the works, same SQL as the full run)
- Create `tmp_session/export_smoke.sbatch` from `export/export.sbatch`: same partition line, `--time=01:00:00`, `--mem=150G`, `--cpus-per-task=16`,
  output `tmp_session/hm_serving_export_smoke-%j.out`, and the command with `--out /vast/lu72hip/hm_pipeline/data/serving_export_smoke --works-sample 1000 --works-chunks 1`
  (keep `--memory-limit 120GB --threads 16 --temp-dir /vast/lu72hip/hm_pipeline/tmp_session/duck_tmp` **and the line `--minority-exclude src/pipelines/core_v4/serving/export/minority_exclusions.csv`**; the smoke run must print `minority tags from EXCLUDE ... (2,878 pairs): 6,503 projects, 6,650 tags; 9 groups end with 0 projects` because projects/organisations/minorities are complete in the smoke run). `sbatch` it and monitor it (see "Monitoring").
- When finished: read `data/serving_export_smoke/export_manifest.json` (rows/bytes/seconds per file) and the slurm log. Report the runtime per index and the peak memory
  (`sacct -j <id> --format=JobID,Elapsed,MaxRSS,State,ExitCode`). Extrapolate the full-run runtime (works scale ~1000x rows; projects/organisations/minorities/grants are complete in the smoke run).
- If the smoke run fails, read the error, apply the smallest fix (see rules), rerun the smoke run. Do not start the full run before the smoke run succeeded.

### 3. Full run through slurm
- `sbatch src/pipelines/core_v4/serving/export/export.sbatch` (edit nothing unless a path is wrong; expected output dir `/vast/lu72hip/hm_pipeline/data/serving_export`, works in 10 files,
  6 h ceiling, the job resumes: existing finished files are skipped, so on a timeout or crash just resubmit the same script).
- Monitor it until it ends (see "Monitoring"). If it dies with an out-of-memory or timeout, resubmit (the finished files are kept), adjusting `--mem` / `--memory-limit` / `--threads` only if needed, and say so in the report.

### 4. Verify the output (light DuckDB reads over the Parquet, in a small sbatch job if it takes more than a minute)
Expected (from the cluster analysis; the row counts must match exactly, the others are checks, report any deviation):
| check | expected |
|---|---|
| `works/*.parquet` rows | 50,000,000 |
| `projects/*.parquet` rows | 3,893,065 |
| `organisations/*.parquet` rows | 494,099 |
| `minorities/*.parquet` rows | 278 |
| `grants/*.parquet` rows | about 6,120 (6,074 streams + per-funder placeholders) |
| works with `is_ch_via_project` true | 35,975 |
| tier-0 works with empty `project_ids` | 922,801 |
| projects with a non-empty `coordinator_ids` | about 81,043 |
| projects with NULL `topic_id` | about 8,795 |
| distinct funders in `projects` | about 103 |
| projects with a non-empty `minority_qids` | **6,503** (9,293 stored minus the deny-list; `export_manifest.json` -> `minority_source`: mode `exclude`, `excluded_pairs` 2,878, `projects_with_minority` 6,503, `project_group_tags` 6,650, `groups_emptied` = 9 groups) |
| `minorities/*.parquet` groups with `project_count` 0 | 191 (182 never had a project + 9 emptied by the deny-list); the file still has all 278 rows |
| works with `pdf_url` / `landing_url` | about 13.4% / 99.65% |
Also check: no NULL or duplicate `id` in any file set; ids are strings (VARCHAR); `organisation_ids` never longer than 100; `api/topics.json` and `api/publishers.json` exist and are valid JSON
(about 4,516 topics; publishers list about 3,000); total size of `data/serving_export` (`du -sh`, per subfolder) versus the expectation
(works about 8 GB, projects about 0.75 GB, organisations about 60 MB, minorities and grants under 1 MB). List every file with its size and row count.
Look at 5 random works, 5 random projects and 5 random organisations rows (print them) and say whether anything looks broken (empty strings, `&amp;`, broken arrays, wrong types).

### 5. Results and next steps
- Write `/vast/lu72hip/hm_pipeline/tmp_session/EXPORT_RESULTS.md` and copy it to `/vast/lu72hip/hm_pipeline/src/pipelines/core_v4/serving/agent_job/EXPORT_RESULTS.md`
  (the user syncs that path by git or copies it by hand). Contents: job ids, runtimes (smoke, full), peak memory, the verification table with the real numbers and OK/DEVIATION per row,
  the file list with sizes, any fixes made (with the diff, also in `tmp_session/EXPORT_FIXES.md`), and warnings.
- **Finish your last message with a block titled `NEXT STEPS FOR THE USER`** containing, ready to copy:
  1. Where the output is: `/vast/lu72hip/hm_pipeline/data/serving_export/` (the folder itself, there is no `out/` subfolder) and its total size.
  2. The rsync commands to run **on the laptop** (replace the host by the ssh alias the user uses for the cluster, keep the placeholder `<cluster>`), small indexes first, works last:
     ```
     mkdir -p /Users/wehrenberger/Code/DIGICHer/hm_pipeline/data/serving_export
     rsync -av --partial --progress <cluster>:/vast/lu72hip/hm_pipeline/data/serving_export/{api,organisations,projects,minorities,grants,export_manifest.json} \
         /Users/wehrenberger/Code/DIGICHer/hm_pipeline/data/serving_export/
     rsync -av --partial --progress <cluster>:/vast/lu72hip/hm_pipeline/data/serving_export/works \
         /Users/wehrenberger/Code/DIGICHer/hm_pipeline/data/serving_export/
     ```
     (`data/` is git-ignored in hm_pipeline; `*.parquet` is ignored everywhere.) Add the rsync flag `-e "ssh ..."` only if you know the user needs one.
  3. Then, in order: laptop test load (`load.py --parquet <that folder> --host 127.0.0.1 --port 9201 --prefix hm_` per `EXPORT_README.md`), then upload the same folder to the VM
     (`rsync -av --partial ./data/serving_export/ digicher:~/serving_export/`), then the VM load (`EXPORT_README.md` section 3). Quote the exact numbers to compare after loading
     (doc counts per index) from your verification.
  4. Which files to bring back into the laptop session: `agent_job/EXPORT_RESULTS.md` and, if it exists, `tmp_session/EXPORT_FIXES.md`.

## Monitoring (how to "tell the user when finished")
- After each `sbatch`, note the job id and poll with `squeue -j <id> -h -o "%T %M"` and `sacct -j <id> --format=JobID,State,Elapsed,MaxRSS` at a sensible interval
  (start at 1 minute, then every 3-5 minutes; do not busy-loop, do not poll faster than every 60 seconds). Read the tail of the slurm `.out` file to see progress
  (the export prints one line per finished file with rows and seconds).
- Send the user a **one-line status message** when: the smoke run finished (ok/failed + runtime), the full run started, a file set finished (organisations/projects/works chunks),
  anything failed or was resubmitted, and when everything is verified. When everything is done, your final message is the summary + the `NEXT STEPS FOR THE USER` block. Nothing else is needed.
