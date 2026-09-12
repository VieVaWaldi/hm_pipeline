okay this is a biggie. and i have to do the first step myself. what u can see is a mess of a pipeline.

The goal is to remove all shit thats not used anymore and make this an actual maintanable clean coded architecture.
That means nuking the folders and refactoring that shit. i know what needs to be removed, i dont know what the new folder structure will look like,
you can help me with that. 

The new corev4 data pipeline is basically:

1. extract/ download sources, all are in here src/sources. i ll delete some, and add some. cordis has this weird all python runner, i ll keep it cuz it works. i ll add source minorities which is its own thing, so most sources will have different extraction styles
   a)Canon sources will be: OpenAire, Cordis, Ror, Minorities. Remove rest
2. Load all sources to duckdb, so i have all of them as a duck db table
3. Merge process -> goal is basically core_v4 raw. guess some smart folders and naming is in order, to seperate corev5 from corev4 and so on.
   a) not sure yet, in corev3 which is already duck db i mainly use openaire, add ror, sprinkle cordis on top. I the new version i ll have to merge openaire with cordis properly. and add ror for the orgas
4. Analysis - Need a smart pattern for that - i guess this might need to happen before the merge process
   a) ie deduplication for projects and organisations and more depending on what analysis shows. 
5. Enrichment 
   a) Basically ML Models, keyword extraction for new columns n shit
   b) currently there are 5 steps, maybe 6 
6. preparing the data for some new tables, ie i need collaboration as an entity. basically some of the stuff i have currently as postgres mat tables.

-> the result is canon corev4

Which i then query to create denormalized tables in opensearch. These denormalized open search tables will be exported for the web app.

More thoughts:
* We are running the code on an HPC with slurm
* if its easy and just good i d like to swap venv for uv
* because of the cluster maybe having docker is good, ie to run open search seemlessly, same for a dag that controls this shit, i do it all by hand usually idk about this. future yes, i d like to poll cordis once a month to get new data and run it through the pipeline
* dags ayy, i have airflow installed, used twice for something simple, didnt really use it after. i ve heard of snakemake? both would also make me choose docker. currently on the cluster sbatch scripts run everything which is messy tbh
* i dont like that config is global, should this be 
* docker would help me with the logs also right? aaah
* data/pile is mounted somewhere else on the HPC, we are on dev locally

---

But before we can do all this I need to clean up and remove shit. The following is for me to keep track, and for u as an fyi

Remove
- Removing postgres, dbt, core v1, core v2
- /data/analysis (old and havent been used in 1 year) same for src/analytics
- no idea how to restructure orchestration, but a lot is unused and i can remove
- src/elt/transformation complete removal, dont need core_v2 nor dbt anymore 
- tests/ unused

actually i was thinking to remove arxiv & coreac because not used for corev4, but why not just keep em, dont even have to verify that they work. openalex ll be a corev5 thing. meta_heritage is for something else but needs to be kept.

Move
- filters/foci.sql is more like anslysis, even if outdated the sql code id docu i can reuse
- elt/core_v3 is the current canon core_v3 data model processing step. we ll remove postgres, but for documentation all in elt/core_v3 is kept
- elt/extraction and elt/loading are only used by cordis, so need to be moved maybe to a extraction step for similar data sources, no removal cuz the code works. src/interfaces should probably follow them idk
- src/enrichment is outdated but has lots of useful shit i want to reuse, use as a starting point for new shit.
- lots of useful shit in src/lib, this feels like a common folder.
- src/sources is i think the extraction and loading step for all of my raw source data, not sure how to clean this up.
- and then there were utils with a global config loader, error handling and a logger lol src/utils, lots of code smell in here too config/

More thoughts:
- data/checkpoints (same for data/loading) is ugly, ie saving checkpoints for cordis as text files, but only cordis uses it, ahh maybe a postgres would be good here afterall, idk. Arxiv and openaire checkpoints are unused.
- I think ORM is good to keep to make easy use of the data for ml models and non sql processing

---

So i have really gotten into modularized mono repos with code locality, separation of concerns etc. but i feel like that webapp thought process wont translate clearly to pipelines.
I think the 2nd step after removal will be to restructure all this in a way thats clean, this is where your first research session will go into. 
building clean scalable but non complicated etl architecture (i am a one man dev, though others must be able to work with it). Documentation ll be an important part.