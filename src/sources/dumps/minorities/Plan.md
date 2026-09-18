# Cold Start: Minority Discovery via Wikidata

## Problem

We currently have 3 pilot minorities (Sami, Ladin, Jewish). Kat manually inspected Wikidata for each group to extract terms (self-designations, languages, spatial references). This approach doesn't scale -- we need a way to **discover** minority groups programmatically before we can extract their terms.

## Approach

Use Wikidata's SPARQL endpoint to harvest a candidate list of minority groups in Europe, then enrich each group with the term categories Kat defined.

### Phase 1: Harvest candidate minority groups

Query Wikidata for entities that are `instance of (P31)` one of:

| Wikidata class    | QID      | Count (approx.) |
| ----------------- | -------- | --------------- |
| ethnic group      | Q41710   | ~10,346         |
| tribe             | Q133311  | ~1,786          |
| indigenous people | Q1153484 | varies          |

Filter to **European context** using:

- `P17` (country) -- entities associated with a European country
- `P2341` (indigenous to) -- entities native to a European region
- Optionally `P27` or geographic qualifiers on statements

This should yield several hundred candidate groups relevant to European cultural heritage. (Phase 1 run produced **790 groups**.)

**Key challenge:** "Ethnic group" != "minority". Poles in Poland are not a minority. We need a heuristic to identify _minority_ status. Options:

1. **Country-relative filtering**: if a group's `P17` country differs from the country where they are the dominant ethnicity, treat as minority candidate
2. **Manual allowlist of European countries** + exclude the dominant/titular ethnicity per country
3. **Accept over-generation** and let project partners curate the list down

Recommendation: option 3 (generate broadly, curate manually). The list is small enough (~few hundred groups) for human review, and automated minority detection is fraught with edge cases. To support curation, we include rich filter dimensions (see below).

### Filter dimensions for curation

For each candidate group, we harvest the following dimensions so that partners can slice, filter, and prioritize the list. Coverage percentages are based on ~698 European ethnic group entities in Wikidata.

| Dimension              | Property | Coverage | Purpose                                                                             |
| ---------------------- | -------- | -------- | ----------------------------------------------------------------------------------- |
| **Population**         | `P1082`  | ~26%     | Filter by group size; small populations = more likely a true minority               |
| **Country**            | `P17`    | 100%     | Filter by country of interest                                                       |
| **Religion/worldview** | `P140`   | ~21%     | Identify religious minorities (Jewish, Pomak, Old Believers, etc.)                  |
| **Native language**    | `P103`   | ~19%     | Identify linguistic minorities; cross-ref with UNESCO endangered languages          |
| **Part of**            | `P361`   | ~38%     | Parent grouping (e.g. "part of: South Slavs") -- helps cluster and deduplicate      |
| **Subclass of**        | `P279`   | ~37%     | Taxonomic type -- e.g. "subclass of: indigenous people" vs generic "ethnic group"   |
| **Diaspora**           | `P3833`  | ~10%     | Flags diaspora communities -- strong minority signal                                |
| **Ancestral home**     | `P66`    | ~8%      | If ancestral home ≠ current country, that's a minority indicator                    |
| **Admin territory**    | `P131`   | ~10%     | Sub-national location -- useful for regional minorities (e.g. Ladin in South Tyrol) |
| **Has parts**          | `P527`   | ~5%      | Lists subgroups (e.g. Sami -> Southern Sami, Lule Sami)                             |

Not every group will have all dimensions filled. Sparse coverage is itself a useful signal -- groups with very little Wikidata presence may need more manual research, or may be less well-documented minorities worth investigating.

These dimensions enable filtering strategies like:

- **"Show me all groups with population < 100,000 in Italy"** -- find small minorities in a specific country
- **"Show me all groups whose subclass_of includes 'indigenous people'"** -- focus on indigenous communities
- **"Show me all diaspora communities in Germany"** -- find non-native minority communities
- **"Show me all groups with a native language that is UNESCO-endangered"** -- prioritize at-risk linguistic minorities

### Phase 2: Enrich each group with terms

For each candidate group, extract terms following Kat's 4 categories:

| Category                     | Wikidata properties                                                                   | Notes                                         |
| ---------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------- |
| Self-designation             | `P1705` (native label), `P1549` (demonym), `P527` (has parts/subgroups), `aliases`    | Includes all language variants                |
| Language                     | `P103` (native language), `P1412` (languages used/spoken)                             | Yields ISO 639 codes + labels                 |
| Spatial reference            | `P2341` (indigenous to), `P17` (country), `P276` (location), `P131` (admin territory) | Geographic anchors                            |
| Tangible/Intangible heritage | `P140` (religion), `P793` (significant event), `P2596` (culture)                      | Sparse in Wikidata; UNESCO/ICH sources better |

For languages specifically, we can follow the chain: group -> native language -> then get the language's own labels, ISO codes, and writing systems.

### Phase 3: Output structured CSV

Generate two CSVs:

**1. Candidate groups with filter dimensions** (`candidate_groups.csv`):

```
qid, group_name_en, country, population, religion, native_language, part_of, subclass_of, diaspora, ancestral_home, admin_territory, has_parts
```

This is the filterable overview for partners to select which groups to include.

**2. Enriched terms** matching Kat's schema (`enriched_terms.csv`):

```
group_id, group_name_en, term, term_type, sub_type, language_code, source, notes
```

Generated for all groups that pass the filter/curation step.

### Phase 4: Optional -- direct Wikidata linking

As Kat mentioned, we could also link directly to Wikidata's vocabulary at query time rather than flattening everything into a static CSV. This would mean:

- Store QIDs alongside terms
- Periodically re-query Wikidata to pick up new labels/aliases
- Use Wikidata's multilingual label system instead of maintaining our own translations

This is more complex but keeps the data fresh. Could be a later enhancement.

## Implementation

### Tech stack

- **Python** with `SPARQLWrapper` or raw `requests` to query `https://query.wikidata.org/sparql`
- Output to CSV using pandas
- A Jupyter notebook for exploration/visualization

### SPARQL queries needed

1. **Discovery query** -- get all ethnic groups with European country associations and filter dimensions:

```sparql
SELECT DISTINCT ?group ?groupLabel ?countryLabel ?population
       ?religionLabel ?nativeLangLabel ?partOfLabel
       ?subclassOfLabel ?diasporaLabel ?ancestralHomeLabel
       ?adminTerritoryLabel ?hasParts WHERE {
  ?group wdt:P31 wd:Q41710 .
  ?group wdt:P17 ?country .
  ?country wdt:P30 wd:Q46 .              # continent = Europe
  OPTIONAL { ?group wdt:P1082 ?population }
  OPTIONAL { ?group wdt:P140 ?religion }
  OPTIONAL { ?group wdt:P103 ?nativeLang }
  OPTIONAL { ?group wdt:P361 ?partOf }
  OPTIONAL { ?group wdt:P279 ?subclassOf }
  OPTIONAL { ?group wdt:P3833 ?diaspora }
  OPTIONAL { ?group wdt:P66 ?ancestralHome }
  OPTIONAL { ?group wdt:P131 ?adminTerritory }
  OPTIONAL { ?group wdt:P527 ?hasParts }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
```

2. **Enrichment query** -- for each group, get terms:

```sparql
SELECT ?group ?nativeLabel ?demonym ?langLabel ?regionLabel WHERE {
  BIND(wd:Q8060 AS ?group)  # parameterized per group
  OPTIONAL { ?group wdt:P1705 ?nativeLabel }
  OPTIONAL { ?group wdt:P1549 ?demonym }
  OPTIONAL { ?group wdt:P103 ?lang }
  OPTIONAL { ?group wdt:P2341 ?region }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
```

3. **Aliases query** -- get all language-specific labels/aliases:

```sparql
SELECT ?group ?label ?alias WHERE {
  BIND(wd:Q8060 AS ?group)
  OPTIONAL { ?group rdfs:label ?label }
  OPTIONAL { ?group skos:altLabel ?alias }
}
```

### Script structure

```
minorities/
  planning/
    Plan.md               # this file
  scripts/
    discover_groups.py    # Phase 1: harvest candidate groups
    enrich_groups.py      # Phase 2: get terms per group
    export_csv.py         # Phase 3: output structured CSV
  data/
    candidate_groups.csv  # raw Wikidata harvest
    enriched_terms.csv    # terms per group (Kat's schema)
    curated_terms.csv     # after partner review (manual)
```

### Estimated data volumes

- Candidate groups (European ethnic groups): ~200-500 entities
- Terms per group: ~5-30 (varies widely; Romani rich, Ladin sparse)
- Total terms in seed CSV: ~2,000-5,000 rows

### Limitations and mitigations

| Limitation                                              | Impact                             | Mitigation                                                               |
| ------------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------ |
| Wikidata coverage is uneven                             | Some groups will have few/no terms | Flag sparse groups for manual enrichment by partners                     |
| "Ethnic group" != "minority"                            | Over-generation of candidates      | Human curation step (Phase 3)                                            |
| Tangible/intangible heritage poorly covered in Wikidata | Category 4 will be mostly empty    | Supplement with UNESCO ICH lists, Europeana, national heritage databases |
| Wikidata labels change over time                        | CSV may go stale                   | Store QIDs for re-querying; consider periodic refresh                    |
| SPARQL endpoint rate limits                             | Queries may be throttled           | Batch queries, add delays, cache results locally                         |

## Next steps

1. Validate this plan with the team (especially Kat for domain alignment)
2. Implement Phase 1 discovery query and review candidate list
3. Run enrichment for the 3 pilot groups first to verify output matches Kat's CSV
4. Scale to all discovered groups
5. Send curated CSV to project partners for review
