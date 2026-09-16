# Source Extraction and Loading

All of these Sources have their very own considerations and therefore specific extraction strategy considerations.

**apis/** — incremental, checkpointed, query-driven (the old runner pattern — might wanna rework that at some point)
  * [Arxiv](apis/arxiv/README.md)
  * [Cordis](apis/cordis/README.md)
  * [Coreac](apis/coreac/README.md)

**dumps/** — periodic bulk snapshots, no checkpointing
  * OpenAire Dump
  * ROR
  * OpenAlex Dump (corev5 scope)

**external/** — not core_v4 scope, kept for something else
  * MetaHeritage

Data Source we are considering to add:

* Web of Science
* Scopus
* Europeana