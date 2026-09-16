"""Existing enrichment scripts — kept runnable, not part of `rule all`.

These predate core_v4 and still write to postgres directly (see
src/common/database/postgres/). They're reference/starting-point code (per the
phase-2 restructure notes), not yet wired into a core_v4 enrichment stage —
run them explicitly by name when needed, e.g.:
    snakemake --workflow-profile orchestration/profiles/slurm update_geolocations
"""


rule update_geolocations:
    shell:
        "sbatch src/enrichment/geolocation/run_update_geo.sh"


rule topic_modelling_keyword_density:
    shell:
        "sbatch src/enrichment/topic_modelling/run_topicmodelling_keyworddensity.sh"
