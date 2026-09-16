"""meta_heritage — separate from the other sources on purpose.

Not core_v4 scope, kept for something else (see sources/ for what). Structurally
different too: each script here isn't an extractor hitting a live API — it's a
loader that reads pre-downloaded files already sitting in data/pile/meta_heritage/
{Public,Private}/ and writes straight to postgres (see common/database/postgres/).
No separate extract/load split, no duckdb involved, no checkpointing.

Because there's no output *file* per scraper (rows land in postgres, not on
disk), each job's output is a touch() sentinel under .snakemake/sentinels/meta_heritage/
just so re-running `snakemake` doesn't redo work you already did — delete a
sentinel to force that one scraper to rerun.

Not part of `rule all` — run explicitly:
    uv run snakemake -s orchestration/Snakefile meta_heritage_all
    uv run snakemake -s orchestration/Snakefile .snakemake/sentinels/meta_heritage/brussels_museums_done  # just one
"""

META_HERITAGE_SCRIPTS = [
    "B2B_event_participants",
    "brussels_cinemas",
    "brussels_museums",
    "brussels_theatres",
    "emilia_romagna_GLAM",
    "emilia_romagna_cinema",
    "emilia_romagna_places_of_cultural_interest",
    "emilia_romagna_points_of_interest",
    "emilia_romagna_theatre",
    "germany_tourism",
    "porto_points_of_interest",
    "porto_wine_tourism",
    "TMO_members",
    "vienna_castles",
    "vienna_hotels",
    "vienna_innovative_companies",
    "vienna_museums",
    "vienna_points_of_interest",
    "vienna_top_location",
    "vienna_tourist_location",
]


rule meta_heritage_all:
    input:
        expand(
            ".snakemake/sentinels/meta_heritage/{script}_done",
            script=META_HERITAGE_SCRIPTS,
        ),


rule run_meta_heritage_script:
    output:
        touch(".snakemake/sentinels/meta_heritage/{script}_done"),
    shell:
        "python -m sources.external.meta_heritage.{wildcards.script}"
