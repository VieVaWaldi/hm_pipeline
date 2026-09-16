# envs/

Empty for now. Once `pipelines/core_v4` rules exist, per-rule container images
go here (or are referenced directly via `container: "docker://..."` in the
`.smk` rule) — see `orchestration/profiles/slurm/config.yaml` for how the same
image runs under Docker locally and SingularityCE on Draco.

Snakemake's flag is named `--sdm apptainer` regardless — it looks for either an
`apptainer` or a `singularity` binary on PATH (`shutil.which`, so a real
executable, not a shell alias), and Draco's SingularityCE 3.11.4 at
`/usr/bin/singularity` satisfies that directly. Containers built with either
tool run fine under the other, so no image-format decision is forced by this.
