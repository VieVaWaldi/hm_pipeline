# Source: Cordis

* [API & Data Schema](documentation/CORDIS_API_Defintion.pdf)
* [Searching](https://cordis.europa.eu/about/search/en) the cordis website
* [Swagger](https://cordis.europa.eu/dataextractions/api-docs-ui) for cordis

The Cordis API works is asynchronous and works with jobs. A search result in the website can look like this
```https://cordis.europa.eu/search?q=
contenttype%3D%27project%27%20AND%20
(%27cultural%27%20OR%20%27heritage%27%20OR%20%27digital%27%20OR%20%27humanities%27)
&p=1&num=10&srt=Relevance:decreasing&archived=true
```

## Data Model

The Cordis data model is quite extensive, find more information in the pdf attached above. 

The CORDIS data schema is structured in XML format, with each field represented by an xPath. The schema is organized into several main content types, each containing a block of main content fields followed by a block of relations to other content. These relations include associations (links to other content) and categories (classification based on taxonomies and lists).

Main Components of the CORDIS Data Schema:
1. Projects
2. Programmes (including topics and legal bases)
3. Articles (news and briefs)
4. Results (including report summaries, project publications, and project deliverables)
5. Topics - which are modeled using EuroSciVoc, see more here [data.europa.eu](https://data.europa.eu/en/publications/datastories/linking-data-european-science-vocabulary) and under 'Advanced View' you can look at the hierarchy here
[op.europa.eu](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/euroscivoc).

All with various metadata like organizations and authors etc.

## Checkpoint

Get data until 10 years in the future from today, reset checkpoint to closest checkpoint thats 5 years in the past.

## Rate Limits

* Only 1 extraction at the same time.
* 5 extractions saved at max, must remove old extractions.
* Limit of 25k records per extraction.
* Archived Content older than 5 years → Archived Content is enabled!


## Transformation notes

...

## Watch out

* API doesn't have sorting or max number of records (Web UI does have that though)