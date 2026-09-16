# Source: Arxiv

[Find the API here](https://info.arxiv.org/help/api/basics.html#quickstart)

[Find the Data Schema here](https://info.arxiv.org/help/api/user-manual.html#52-details-of-atom-results-returned)

An arxiv api call may look like this:

```
http://export.arxiv.org/api/query?
search_query=%27search_query=(all:digital+OR+all:heritage+OR+all:humanities+OR+all:cultural)
+AND+submittedDate:[199001010000+TO+199006300000]
&sortBy=submittedDate&sortOrder=ascending
```

## Checkpoint

We use submittedDate as a checkpoint which takes a start and end date, looking like
this: `[199001010000+TO+199006300000]`

## Data Model

1. Arxiv Entry is a Publication
2. Author with junction to entry
3. Link with junction to entry

## Rate Limits

* ...

## Transformation notes

* Removed all @schema because they do not matter
* Must ensure category.@term, author.ns0:name and author.ns1:affiliation are a list
* category[_].@term and affiliation[_] are list fields for entry and author

## Notes

* Arxiv automatically stems words eg same results for query: digital or digital, and humanities or human

## Watch Out

* Something Arxiv doenst return entries, even though it has them. Give the server some time with an exponential backoff
  or try later.