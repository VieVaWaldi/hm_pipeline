# heritagemonitor adaptation plan (api + shared + search + web) for the core_v4 indexes

Revision 2026-09-21 (user decisions and answers applied, VM smoke results added). Planning only: nothing in `heritagemonitor` or `digicher_webinterface` was touched. Sources: heritagemonitor (CLAUDE.md, README, api/web RULES, the minorities slice, `common/url`, `common/llmchat/pageContext.ts`, the reworked `modules/demo/deckgl`), digicher_webinterface (`hooks/persistence/useFilters.ts`, `components/filter/useTopicFilter.tsx`, `hooks/scenarios/useForceLayout.ts`, `components/deckgl/NetworkGraphView.tsx`), `SERVING_DESIGN.md`, `export/queries.py`, `export/mappings/*.json`, `serving_plan.md`.

## 0. Glossary
- **Slice**: one feature cut vertically through all layers for one entity or use case: OpenSearch query, repository, service, api route, shared zod DTO, web hook, component, URL state, chat context. It can be built, reviewed and demoed alone. The minorities implementation (`packages/search/src/indices/minorities.ts`, `packages/shared/src/minorities.ts`, `apps/api/src/modules/minorities/*`, `apps/web/src/modules/search/minorities/*`) is the template.
- **F0 foundation**: the shared plumbing every slice needs and none owns: index names, response envelope, TypeScript port of `export/queries.py`, search executor, corpus filter, URL-state layer (`common/url`), chat-context helper, links helper, generic web hook and panel shell. It is built inside Step 1 together with the first slice, so Step 1 is already clickable.
- **Review gate**: the end of a step. The user runs `localhost:3000`, clicks through the page, and proposes changes. The next step does not start before that.
- Ids are strings everywhere (`z.string()`, Fastify querystring `type: 'string'`): project ids like `13508218431153968733` exceed 2^53. DTOs keep the index field names (as `MinorityDto` does); list rows use `_source` filtering (no `summary`), detail = whole source.

## 1. Decisions (resolved, do not reopen)
- **Blank-query defaults**: projects by `funded_amount_eur` desc, organisations by `total_funding_eur`, works by `citation_count`, grants by `total_funded_eur`, minorities seed-first (`is_seed` desc, then `project_count`, `work_count`). BM25 is meaningless without a query.
- **Dev data**: the coordinator loads the sample into heritagemonitor's own dev OpenSearch on 9200 with the prod index names (`projects organisations works minorities grants`, `minorities` recreated with the new schema). The dev api needs no compose or port change.
- **Indexes are final.** A slice that needs a missing field lists it in section 4 (index gaps), it does not assume it.
- **Funding = D10**: query on projects; list = organisations that worked on the matching projects, map = funding of those organisations (H3 hex layer). Grants become the "funding programmes explorer" (slice E).
- **Query network (G2)**: the **2D d3-force graph rendered in deck.gl** (as in the old app) is what the user wants, with the `maxEdges` cap; arcs stay as the fallback layer.
- **Deck.gl**: hex layer only for organisation funding, **no column layer**.
- **Project DOI**: show **every** project `doi` we have (EC or not) as a DOI link. The EC-only rule applies only to the derived CORDIS link.
- **Year presets: REMOVED** (user, after testing Step 2 live 2026-09-21): the histogram + dual slider are enough; no 'last n years' chips or numeric field. The year histogram stays.
- **Publisher filter**: a searchable menu over the in-memory top-3,000 `publishers.json` (no typeahead endpoint).
- **Programme/funder/region/theme typeahead** is a server endpoint `GET /v1/projects/facet-values?field=&q=&size=` (built in Step 2b), not a client-side top 200.
- **Approximate totals** (built in Step 2): exact below 10k, nearest hundred up to 100k, `k` for 100k-1M, `M` for millions ("about 3.7M"); the user confirmed the wording.
- **Deep links (user decision)**: clicking an organisation row (Step 3) or a work row (Step 4) in a project's tab goes to that entity's page with `only=<id>&sel=<id>` and a visible 'Showing 1 <entity>, clear' chip; clicking a project row in an organisation's Projects tab goes to `?e=projects&only=<id>&sel=<id>`; one shared `buildEntityLink()` in `common/url`.
- **Selection** clears when page, filters, sort, query or corpus change, unless `sel` is set in the same update.
- **Chat context**: caps as in 3.2 (rows 300, selected entity 5,000, total 15,000, 30 sources); Lucy must see ALL active URL parameters, generated generically from the URL param schema with human labels (a test fails on an unlabelled param).
- **UI polish rules**: long facet labels get a full-text tooltip; filter-button summaries are single-line, ellipsised, with a tooltip.
- **Process rules**: the tree must compile at every moment (the user may commit any time); every step ends with the exact production build sequence (`pnpm --filter @heritagemonitor/{shared,db,search,api,web} build`) plus the package tests and web lint before the user commits; deploy on the VM with `./hm-switch.sh new` from the feature branch (the RAM patch is committed on the branch).
- **After Step 3 review (user)**: the deep-link chip uses a theme.ts colour; links Lucy embeds that leave heritagemonitor open in a NEW tab while internal links keep client-side navigation, so the chat state stays alive across routes (Lucy's generated URL lists act as a table of contents); the deep-link Clear button returns to the first row of the full list; organisation name-search ranking boost decided by measurement (Fraunhofer Society first for `q=fraunhofer`); missing country/region on big US organisations is accepted (HM is EU-focused).
- **Build order**: the production build sequence is `shared -> db -> search -> api -> web` (the order the Dockerfiles run them, each package imports the compiled `dist` of the earlier ones), then package tests and web lint.
- **Status 2026-09-21**: Steps 1-7 built and reviewed by the user (Step 4 works, 5 minorities, 6 topics modal + DOI resolution, 7 experts all approved; typing-bug fixed at the root: raw text in box and URL, normalisation only at query time). Step 8 split: 8a (in-memory org table, boot warm-up, grants entity) in progress; 8b (funding page + deck.gl hex map migrated out of the demo) and 8c (minorities map tab) next; then Step 9 (organisation network) and Step 10 (query network, 2D d3-force in deck.gl with maxEdges cap).
- **Environment**: branch `feat/core-v4-serving` is checked out and the dev stack is running (dev OpenSearch on 9200 holds the sample indexes).
- **Duplicate organisations (D19)**: merged by `name_key` only in network and experts; plain org lists show both ids.
- **Minorities**: tags ship as stored minus the deny-list; UI shows a short disclaimer on counts; groups with 0 projects stay visible.
- **Theme and pillars stay in the UI** (facet and filter). `theme` has only `Economy` and `Tourism` today, more will come.
- **VM operations** as in `VM_RUNBOOK.md` (audit log off during the works load, `--recreate` for minorities).
- **No list/map switch** (it belonged to the old app). Map-first pages use a default map tab in the tabbed panel, like the reworked demo.
- **No downloads.** The disabled download button in `ResultsHeader` is removed or left disabled.
- "Filters stay local" from the first plan is dropped: filters are URL state (section 3.1).

## 2. What already exists and what does not

| Need | Exists (reuse) | Missing / must change |
|---|---|---|
| Paginated list | `common/components/PaginatedList` (generic, header slot) | header "10,000+" total, page count capped at 500 |
| Ranking button | `modules/search/minorities/RankingButton` | hard-wired to `MINORITY_SORT_OPTIONS`: generic `options` prop, move to `common/components` |
| Tabbed panel | `TabbedPanel` with `fill` and `keepMounted` per tab (added for the demo) | **uncontrolled** (`defaultValue` only): needs `value`/`onChange` so the active tab lives in the URL |
| Facets and filters | `FacetSidebar`, `FacetSection`, `FilterBar`, `FilterMenuButton` (checkbox list + search + `count`) | facet shaping (`useMinorityFacets`) is minority-specific and sorts by label: topic facet needs count order and labels |
| Sliders | `DualSlider` (with play), `SingleSlider` | year presets control (section 5 A), `maxEdges` control reuses `SingleSlider` |
| Autocomplete | `SearchBar` (`suggestions: string[]`), `ActionBar`, `useMinoritySuggestions` | plain strings, hero and search page call the minorities-only hook; organisation autocomplete needs `{id,label}` to deep-link |
| Map | `common/deckgl` (`DeckMapCanvas`, `MapControls`, `useDeckMapViewState`, globe toggle); demo `modules/demo/deckgl`: `DeckGlMapTab`, `ArcCollaborationLayer` (input `CollaborationEdge`), `createHexFundingLayer` + `hexBins.ts` (input demo `Organisation` with `geolocation [lng,lat]` and `funding`), `ExplorerRow`, `detail/*` | layers sit in `modules/demo` (move to `common/deckgl/layers` or the collaboration module); adapters from api payloads to the demo shapes; the view state is uncontrolled and not in the URL; the demo has **no column layer any more** (hex + arcs only) |
| Corpus | `CorpusContext` (`science`/`dch`), URL param `c`, synced by an effect in `useUseCaseSearch` | not sent to the api; api has no corpus concept |
| Route shell | `UseCaseSearchPage`, `SearchNav`, `useUseCaseSearch`; pages exist for `/search`, `/search/experts`, `/search/funding`, `/search/minorities`, `/search/collaboration/{organisationNetwork,queryNetwork}` | registry is keyed by use case, but Search has 4 entities behind one key (entity router by `e`); collaboration sub-use-cases need `hasResultsPanel` |
| URL logic | `common/url`: only `SEARCH_PARAM {q,e,c}` and `buildSearchUrl`; filters/sort/page/selection are local `useState` in `useMinoritySearch` / `useSelectedMinority` | the whole URL-state layer (section 3.1) |
| Chat context | `common/llmchat/pageContext.ts` (`buildPageContext`: sections, max 20 rows x 300 chars, max 20 `sources`; changes decided in section 3.2), `usePageChatContextPublisher`; minorities publishes list + selected group + subgroups, `sources` is empty | every other panel; URL list (works PDFs, project links) |
| Api plumbing | Fastify module per folder, `AppError`, `InMemoryCache` (`plugins/cache.ts`), Ajv array coercion, `packages/search` client with basic auth from env | shared search executor, reference data (topics, publishers) |
| Topic tree | nothing in heritagemonitor; digicher_webinterface has `useTopicFilter.tsx` (see slice H) | new |

## 3. Cross-cutting requirements (every slice must meet them)

### 3.1 URL state (all logic in `apps/web/src/common/url`)
Requirement: every filter, sort, page, corpus, selected entity id, active tab, selected topics and the deck.gl viewport is in the URL. A copied link reproduces the page; the browser back button restores the previous selection.

- **Layout of `common/url`** (one schema/serialiser/hook per concern, no URL string handling anywhere else; modules only import hooks):

| Concern | Param(s) | Codec rules |
|---|---|---|
| text | `q` | trimmed string |
| corpus | `c` = `science`\|`dch` | invalid -> default; replaces the sync effect in `useUseCaseSearch`, `CorpusContext` reads/writes through it on `/search*` |
| entity | `e` (only `/search`) | must be in `ENTITIES` |
| page, sort | `page`, `sort` | page >= 1, sort must be in the entity's option list |
| years | `years=2019-2025` | clamped 1980..current year + 3 (D28); presets are converted to a range |
| facet/filter lists | `theme pillar funder programme stream region ror country oa language publisher minority ...` (repeated params) | at most 50 values per param, unknown values kept (api decides), invalid shapes dropped |
| topics | `topic`, `subfield`, `field` (repeated) | shared cap **20 in total**, priority topic > subfield > field (as `MAX_URL_TOPICS` in the old app) |
| selection | `sel=<id>` (selected row, detail panel), `only=<id>` (deep link: list restricted to this entity, with a "clear" chip), `center=<orgId>` (organisation network) | strings, never parsed as numbers |
| tab | `tab=overview\|organisations\|works\|map...` | must be one of the panel's tabs |
| viewport | `view=lat,lng,zoom` (4 decimals, as the old `view`), `layer=arcs\|hexes\|network` | all three numbers valid or dropped |
| graph cap | `maxEdges` | 10..300 |

- **Hooks**: `useUrlState()` (reads `useSearchParams`, `update(patch, {history})`, merges with the latest URL so two updates in one tick do not overwrite each other, keeps unknown params, resets `page` when a filter/query/corpus changes), plus thin concern hooks `useUrlFilters(spec)`, `useUrlSort`, `useUrlSelection`, `useUrlTab`, `useUrlTopics`, `useUrlCorpus`, `useUrlMapView`. `buildSearchUrl` is rebuilt on the same codecs (the one builder for links between routes, e.g. project org row -> `/search?e=organisations&only=<id>&sel=<id>&c=dch`).
- **History policy** (this differs from the old app, which used `router.replace` for everything): discrete choices (filter, sort, page, corpus, selection, tab, topics applied, submitted query) use **`router.push`** so back restores them; continuous ones (viewport pan/zoom debounced 300 ms as in the old `debouncedSetViewState`, typing before submit, slider drags until release) use `router.replace`.
- **Viewport**: initial view from `view`; deck.gl stays uncontrolled (feeding the view state back would reinitialise layers, see the comment in `useDeckMapViewState`); only a URL change that did not originate from the map itself (back/forward) triggers the existing fly-to command.
- **Api params mirror the URL**: one zod request schema per entity in `packages/shared` is used by the url codecs (web) and the Fastify route's querystring type (api). The web search hook sends the URL's search params minus the web-only keys (`e sel tab view layer center`) to `/v1/<entity>/search`, so the api uses the same names: `q c page sort years <filters...> topic subfield field only`. Sorts are named values (`relevance`, `budget`, `funding`, `projects`, `works`, `citations`, `matches`). Topics beyond 20 -> api 400.
- **Tests**: `tsx --test` round-trip tests for each codec (serialise -> parse identity, invalid input dropped, cap respected).

### 3.2 Chat context for Lucy (`common/llmchat/pageContext.ts`)
Each entity panel calls `usePageChatContextPublisher(buildPageContext(...))` with:
1. a heading that states the current URL state in words (query, corpus, active filters, sort, page x of y, total, "10,000+" when capped), from one shared `describeSearchState(urlState)` helper;
2. one section with the current paginated list (one line per row, 20 rows; row length per the caps below);
3. one section with the selected entity's detail panel (the fields the overview shows), plus the active tab's list (organisations of a project, ...);
4. `sources`: URLs so Lucy can fetch and summarise all papers/projects (works: `pdf_url` else `landing_url`; projects: DOI/CORDIS/OpenAIRE links from the links helper). Order = the selected entity's links first, then the link of every list row; `buildPageContext` cap raised from 20 to **30** (see the decided caps below).
Minorities is the template (add its Wikipedia URL as a source, it is empty today). Every slice's acceptance list contains this.

**Caps (DECIDED)**: list rows keep **300** characters; `PageContextSection` gets an optional `maxCharsPerRow` and the **selected entity's detail section uses 5,000**; a **total budget of 15,000 characters** per context (tail rows dropped first); 20 rows per section; `MAX_SOURCES` raised from 20 to **30** so the selected entity's links plus a full page of 20 row links are all offered. The api's web_fetch `max_uses` stays **20**: it is the *fetch* cap, not the *offer* cap. Facts from the api code: `openrouter.client.ts` uses web_fetch with `max_uses: 20` and `max_content_tokens: 10,000` (about 40,000 characters, roughly the first 15-20 pages of a paper) per fetched page or PDF; `llmchat.service.ts` allowlists **by hostname** of the approved source URLs only. Full PDF text is never put into the context: Lucy fetches the `sources` herself.

**RISK (chat sources)**: `doi.org` links redirect to the publisher host (`cordis.europa.eu`, `link.springer.com`, ...), which may not be on the hostname allowlist (only `doi.org` is), so Lucy's fetch of a DOI link may be blocked after the redirect. Mitigations: prefer the direct `pdf_url` host as the chat source whenever it exists; test a DOI fetch in the works step (Step 4) and in the projects step (a CORDIS-redirecting `10.3030/...` DOI); if blocked, either add well-known DOI redirect targets to the allowlist or resolve the DOI api-side (HEAD/redirect follow) and publish the resolved URL as the source. The same applies to the CORDIS-derived link only if it redirects (it should not).

### 3.3 Links helper (`packages/shared/src/links.ts`, used by the overview and by pageContext)
- `projectLinks({doi, grantId, funder, programme, openaireId, websiteUrl})` returns `{label, url}[]`:
  - **every project with a `doi`** (EC or not) gets `https://doi.org/<doi>` labelled **DOI** (H2020/HE carry `10.3030/<grantId>`, it redirects to CORDIS; FP7 has none).
  - **EC-funded only** (`funder` includes `EC`), derived link: `programme` in `FP7|H2020|HE` and a numeric `grantId` -> `https://cordis.europa.eu/project/id/<grantId>` labelled **CORDIS** (exists for FP7, where doi.org 404s). `ERASMUS+` grant ids are not numeric -> no CORDIS link.
  - any project: **OpenAIRE** link from `openaireId`, **Website** from `websiteUrl` when present. (Check the OpenAIRE URL form on 2-3 real ids at the first review gate.)
- `workLink({pdf_url, landing_url})`: **PDF** if `pdf_url`, else **DOI** from `landing_url` (already doi.org-first, D25; labelled "Page" if it is not a doi.org link), else none. **For chat sources** `pdf_url` is preferred over any DOI link (allowlist risk, section 3.2).
- Rendered as real clickable `<a target="_blank" rel="noopener noreferrer">` links in the project DETAIL panel (overview tab) and in the works list button; the same helper output feeds the chat `sources`.
- Unit tests (`tsx --test`): H2020 (`689660`, `10.3030/689660`), HE (`101022163`-style), FP7 (no doi, numeric grant id), ERASMUS+ (non-numeric), non-EC with doi (NIH: DOI shown, no CORDIS), missing openaireId/websiteUrl.

### 3.4 F0 foundation tickets
| # | Ticket | Where |
|---|---|---|
| F0-1 | Index names `projectsIndexName organisationsIndexName worksIndexName grantsIndexName` (minorities exists), optional `OPENSEARCH_INDEX_PREFIX` (default empty); auth env already supported by the client, dev unchanged | `packages/search` |
| F0-2 | Shared conventions `packages/shared/src/search.ts`: `corpusSchema`, request schemas (section 3.1), envelope = existing minorities envelope (`hits`, `facetDistribution`, `estimatedTotalHits`, `page`, `pageCount`) + `totalCapped`, `mode: strict\|fuzzy`, `didYouMean`, `facetLabels`; `pageCount = min(ceil(total/20), 500)` | `packages/shared` |
| F0-3 | Query lib: port of `export/queries.py` to `packages/search/src/query/` (`rewriteQuery`, `sqs`, `splitTerms`, `fuzzyQuery`, `suggestBlock`, `pageWindow`, `termsAgg` with explicit `shard_size`, field lists, `projectFilters/workFilters/orgFilters/minorityFilters/grantFilters`, bodies for orgs, autocomplete, topic aggs, networks, funding, `mergeByNameKey`). **Regex gotcha**: Python `\w`/`\b` are Unicode, JS ASCII: use the `u` flag with `\p{L}\p{N}_`. `node:test` parity cases (unbalanced quote, `NOT x`, `a AND b OR c`, `word~2`, phrase kept) | `packages/search` |
| F0-4 | Search executor `apps/api/src/common/search/runSearch.ts`: strict query with `track_total_hits: 10000`, fuzzy rerun when `total < threshold` (projects/orgs 5 + 2 s, works 3 + 1500 ms), envelope with `totalCapped` from `relation === 'gte'`, page beyond the 10k window -> `AppError(400)`; blank default pages (projects and works: first 5 pages, per corpus) cached in `InMemoryCache` with a TTL (section 4a) | api |
| F0-5 | Corpus filters: projects `is_ch`; organisations `has_dch_project`; works `is_ch_via_project` ("via linked project"); minorities and grants `dch_project_count >= 1`. `getById`, `mgetByIds` (keeps order), `ids` query for `only` (`works.id` is `index:false`, so never `terms` on `id`) | search, api |
| F0-6 | URL layer (section 3.1), controlled `TabbedPanel` | web |
| F0-7 | Chat helpers (`describeSearchState`, source ordering) and links helper (3.2, 3.3) | web, shared |
| F0-8 | Generic web layer from the minorities panel: `useEntitySearch` (reads the URL, sends the mirrored params, abort, transition), `useSelectedEntity` (from `sel`), `EntityResultsPanel` shell (FacetSidebar + FilterBar + PaginatedList + TabbedPanel from a config), generic `RankingButton`, capped `ResultsHeader`, `facetOptions(distribution, labels, sort: label\|count)`; entity router for `/search` (by `e`, `key={entity}`) | web |
| F0-9 | Autocomplete: `GET /v1/<entity>/suggest?q` -> `{suggestions:[{id,label,hint?}]}` (projects `acronym.sayt`/`title.sayt`, organisations `legalName.sayt`+short+alternative ranked by `rank_projects` deduped by `name_key`, grants `description.sayt`, minorities `group_name_en.sayt`; none for works); web `useEntitySuggestions(entityKey, q)` replaces `useMinoritySuggestions` in `HeroPage` and `useUseCaseSearch` (per selected entity); `SearchBar` accepts `{label, id?}` | api, web |
| F0-10 | Reference data: copy `data/serving_export/api/{topics,publishers}.json` into `apps/api/src/reference/`, load into maps at boot (`topicById`, tree, publishers by works) | api |
| F0-11 | **In-memory org table** loaded at api boot (section 4a): `id -> {name, name_key, lat, lng, project_count}` from a scan of `organisations` with source filtering; used by all map/network payloads instead of `mget` (needed from Step 8 on; can land earlier) | api |
| F0-12 | **Cache warm-up at api start** (a few representative queries) and the documented VM page-cache warm-up command (section 4a) | api, ops |

Merge hotspots to wire once in Step 1: `apps/api/src/index.ts`, `packages/shared/src/index.ts`, `packages/search/src/indices/index.ts`, `resultsPanelRegistry.ts`, `common/catalog/useCases.ts`, `common/components/index.ts`. After Step 1 the slices touch separate folders and can be given to agents in parallel, but each still ends at its own review gate.

## 4. Index gaps (no index changes: workarounds only)
| Gap | Consequence / workaround |
|---|---|
| `topics.json` has only names (topic, subfield, field, domain), no keywords or descriptions | topic search matches names of the three levels only (the old app also had a "add keywords" todo) |
| No org-level region on projects: `org_regions` is per project | funding/experts region filter keeps projects with an org in the region; funding drops orgs of other regions after the org-table lookup (top-N truncation slightly inexact) |
| No org-pair index (D11) | shared projects = projects filtered by an AND of `term org_ids` clauses (`terms` would be OR); network edges built in the api |
| Query network has no per-project score in the docvalue fetch | tie-break by hit position (ranking under the current sort) of the pair's projects |
| Grants have no year, region, or organisation list | grant tabs use the projects index (`funding_stream_ids`) and org aggregations |
| Minority docs have counts and `topic_counts` but no organisation or project lists | tabs use projects filtered by `minority_qids` (+ aggregations) |
| Works have no abstract, topics, minorities of their own, DCH only via linked project | labelled "via linked project" |
| `funded_eur_per_org` is NULL when `org_count = 0`; only 58% of projects have an amount; amounts are approximate EUR | always "approx. EUR" plus coverage note |
| `works.language` has 0.13% junk values | language facet lists top values only |

## 4a. Performance notes for the api (VM smoke results)
VM smoke (`vm_smoke.py`): 52 checks, 0 red flags, warm p50 < 500 ms everywhere. Sizes: works 50M docs 19 GB, projects 6 GB, organisations 0.65 GB. The problem on the HDD is **cold first calls**, not warm latency. All mitigations are api-side; no index change.

1. **Cold first calls, especially collaboration.** Query network first call 24-30 s (warm 160 ms) when fetching the top 2,000 projects; organisation network `mget` of 500 partner org docs first call 4.6 s (warm 77 ms). Mitigations:
   - query-network project fetch uses `docvalue_fields` (`org_ids`, `coordinator_ids`) with `_source: false`, never `_source`;
   - an **in-memory org table** (`id -> {name, name_key, lat, lng, project_count}`) loaded at api boot by scanning `organisations` with docvalue/source filtering (494k rows, tens of MB, F0-11): map, network, funding and experts payloads never need an `mget`;
   - **cache warm-up at api start** (F0-12): a few representative queries (blank projects page + facets, blank works page, an org network of a collaborative org, a query network) run once after boot and once per TTL;
   - a documented **page-cache warm-up command** for the VM (run after a reboot or container restart, e.g. `cat` or `vmtouch` over the index data directory, or `vm_smoke.py --runs 1`), listed in the VM runbook.
2. **Blank default pages**: projects blank + facets p50 432 ms and works blank default p50 477 ms: cache **both** first pages (first 5 pages, per corpus) in api memory with a TTL, not only works. The **blank-query funding map** (top 500 orgs over all 3.9M projects) is the one slow query (1.1 s cold, OpenSearch caches it afterwards): cache it per corpus in the api next to the blank default pages, and include it in the warm-up.
3. **Deep pagination cap 10k** is fine (p50 25 ms at page 500): keep `track_total_hits: 10000` and the 500-page cap.
4. **Typo fallback** timings are fine (works 32-53 ms warm): stays in Step 2 as planned.
5. **Biggest org is not the best demo**: JOHNS HOPKINS (48k projects) has only 2 partners because NIH projects have a single org: not a bug. The organisation-network demo/test org must be a collaborative one, e.g. **Fraunhofer** (3,042 projects, 501 partners).
6. **API memory budget.** VM prod containers: caddy 128m, web 768m, api 1g (`NODE_OPTIONS --max-old-space-size=768`), postgres 512m, opensearch 24g (heap 12g). In-memory data: topics about 2 MB, publishers under 1 MB, blank-page and funding-map caches a few MB, org table (494k orgs) about **60-120 MB stored compactly** (parallel arrays / typed arrays: `Float32Array` lat/lng, `Int32Array` project counts, one string table for names and `name_key`, id -> row index map) which fits; naive JS objects for the org table can reach 250+ MB with GC pressure, so do **not** store it as an object per org. Recommendation: raise the api to `mem_limit` 1536m with `--max-old-space-size=1152`, and give opensearch heap 10g / container 23g (heap used peaked at 6.5 GiB in the smoke run). Both are `docker-compose.prod.yml` changes to be decided by the coordinator.

## 5. Slices

### A. Projects (`/search`, entity projects)
Index `projects`, text fields `acronym^5, title^3, summary, keywords, grantId, org_names^0.5`.

| Item | Detail |
|---|---|
| Routes | `GET /v1/projects/search`, `/:id`, `/:id/organisations?page`, `/suggest?q` |
| Params (= URL) | `q c page sort=relevance\|budget years theme pillar topic subfield field funder programme stream region minority hasMinority org orgAll only` (`org` any-of, `orgAll` all-of = one `term` each) |
| Filters | `year` range, `theme`, `pillar_list`, `topic_id`/`subfield_id`/`field_id`, `funder`, `programme`, `funding_stream_ids`, `org_regions`, `minority_qids`, `org_ids`, `is_ch` |
| Facets (one request, `termsAgg` with `shard_size`) | `topic_id` (top 25, labels from topics table, count order), `funder` (20), `programme` (20), `pillar_list`, `theme`, `org_regions` (with `Unknown`), year histogram (`histogram` on `year`, interval 1) shown behind the year control. Facets are computed on the filtered set (`post_filter` multi-select = later) |
| Year filter UI | preset list: **last year, last 2 years, last 3 years, last 5 years, last [n] years** (`n` number field); a preset becomes `years=<from>-<to>` with `to` = current year and `from` = `to - n + 1` (the current year counts); a custom range via `DualSlider` stays available. Same control for works, experts, funding, networks |
| Sort | `budget`: `[{funded_amount_eur:{order:desc,missing:_last}}, _score]`; no alphabetical. Text fuzzy fallback threshold 5, `did you mean` on `title` |
| Row | `id, acronym, title, year, funder, programme, funded_amount_eur, is_ch, org_count, work_count, topic_id` |
| Overview tab | every column; **real links** from the links helper (DOI for every project that has one, CORDIS for EC FP7/H2020/HE, OpenAIRE, Website, section 3.3); `is_translated` badge; `pred` displayed only (D31); coordinator marks only for EC (D30); topic name from the table |
| Tabs (`tab=`) | `overview`; `organisations` = `mget org_ids` slice of 20, coordinators first via `coordinator_ids`; `works` = `/v1/works/search?project=<id>`. Row click -> `buildSearchUrl` to `/search?e=organisations&only=<id>&sel=<id>` (works likewise) |
| Chat context | list rows `- ACRONYM: title (year, funder/programme, ~EUR, n orgs)`; selected project overview (all displayed fields) and its active tab list; `sources` = projectLinks of the selected project first, then the best link per list row |
| Gotchas | `summary` NULL for 86%, `acronym` NULL for 97%: no abstract promise. `org_regions` has `Unknown`. `programme` has 4.5k values: top-N only. Blank query with facets (p50 432 ms warm): cache the first 5 blank pages with a TTL (section 4a). `theme` almost empty today, kept |

### B. Organisations
Text fields `legalName^3, legalShortName^2, alternativeNames`.

| Item | Detail |
|---|---|
| Routes | `GET /v1/organisations/search`, `/:id`, `/suggest?q` |
| Params | `q c page sort=relevance\|funding\|projects\|works region ror country hasGeo only` |
| Ranking | explicit sort = `total_funding_eur` / `project_count` / `work_count` desc; blank default funding; with `q` and no sort BM25 + `rank_feature rank_projects` log blend (D33) |
| Filters / facets | `region` (with `Unknown`), `rorTypes` (with `unknown`), `countryCode`; corpus `has_dch_project` |
| Row | `id, legalName, legalShortName, countryCode, region, rorTypes, project_count, work_count, total_funding_eur, has_dch_project` |
| Tabs | `overview` (all fields, `rorId`/`websiteUrl` links, funding "equal-split, approx EUR"); `projects` = `/v1/projects/search?org=<id>`; `works` = `/v1/works/search?org=<id>` |
| Chat context | rows `- name (country, region, n projects, n works, ~EUR)`; selected overview; active tab list; sources = `websiteUrl` and ROR link of the selected org |
| Gotchas | D19 duplicates appear twice in lists; an org's tabs cover that id only; `geo` only for ~18% of project-connected orgs |

### C. Works
Index `works` (50M, 4 shards). No facets, no autocomplete, no abstract. Text fields `title^3, authors, container_name^0.5`, fuzzy threshold 3 + 1500 ms.

| Item | Detail |
|---|---|
| Routes | `GET /v1/works/search`, `/:id` (`client.get` by `_id`) |
| Params | `q c page sort=relevance\|citations years oa language publisher project org minority only` |
| Filters | `year`, `open_access_color`, `language`, `publisher` (exact term; UI = searchable menu over the in-memory top-3,000 `publishers.json`, one `GET /v1/publishers`, no typeahead endpoint), `project_ids`, `organisation_ids`, `minority_qids`, corpus `is_ch_via_project` (tooltip "works of a DCH project") |
| Row | `id, title, authors[0..2], author_count, year, publisher, container_name, open_access_color, citation_count, pdf_url, landing_url` |
| Button | from `workLink`: **PDF** if `pdf_url`, else **DOI** |
| Cache | the default request (blank query, no filters; per corpus) is cached in api memory for its **first 5 pages** with a TTL, because ordering 50M works by `citation_count` on the HDD-backed VM is a full doc-values scan (warm p50 477 ms, cold much worse); deeper pages and every other request go to OpenSearch as normal. The same first-5-pages cache applies to the projects blank default (section 4a) |
| Tabs | `overview`; `projects` = `mget project_ids`; `organisations` = `mget organisation_ids` (max 100) |
| Chat context | rows `- title (authors, year, publisher, N citations, OA colour)`; selected overview; **sources = `workLink` URL of every listed work** (PDF else DOI, up to the cap) so Lucy can summarise all papers on the page |
| Gotchas | totals "10,000+"; page max 500; fuzzy fallback cost on works must be timed on the VM |

### D. Minorities (new schema)
The old DTO should keep parsing after the recreate (extra keys are stripped, `.keyword` sub-fields remain): verify first.

| Item | Detail |
|---|---|
| DTO | add `is_seed, project_count, dch_project_count, org_count, work_count, topic_ids, topic_counts[{topic_id,n}]`, `merged_qids`; `population` double; drop the `manual_seed` label (seed = `is_seed`); `project_title_blob` is not in `_source` |
| Ranking | blank: `is_seed` desc, `project_count` desc, `work_count` desc; with `q`: `_score` + a `should` on `is_seed`; sort options add `project_count`, `work_count` |
| Facets | `MINORITY_FACET_FIELDS` order: toggle `has_subgroups`, Country, **Topics (`topic_ids`, 2nd, labelled, count order)**, Type, Religion, Language, then secondary tiers. Corpus `dch_project_count >= 1` |
| Search | `sqs(q, MINORITY_FIELDS)` incl. `project_title_blob`; two-step: projects search (text or `org_names`) + `exists minority_qids`, `terms minority_qids` size 300, added as `should terms {qid}` (the blob holds only the top 200 projects per group); suggest on `group_name_en.sayt` |
| Tabs | `overview` (+ counts, disclaimer), `subgroups`, `projects` = `/v1/projects/search?minority=<qid>`, `organisations` (projects agg `org_ids` + mget), `topics` (`topic_counts` labelled), `grants` (projects agg `funding_stream_ids`), `works` (`minority` filter, "via linked project"). Map tab (2nd request): matching qids -> projects agg `minority_qids` > `org_ids` -> mget `geo`; hex/points layer |
| State | filters, sort, page, `sel`, `tab`, `view` all in the URL (moves `useMinoritySearch`/`useSelectedMinority` state into `common/url`) |
| Chat context | existing rows plus counts; selected group with subgroups; sources = Wikipedia URL of the selected group |
| Gotchas | keyword-based tags are partly false (Russians 2,242, Turkish 1,676): disclaimer; 9 groups have 0 projects (stay visible); verify whether `projects.minority_qids` uses `qid` or a `merged_qids` value |

### H. Topics modal (copy from digicher_webinterface `useTopicFilter.tsx`, bring to heritagemonitor standards)
Reference: checkbox tree field > subfield > topic (domain excluded), selected-topic chips with "+n more" and Clear, name filter, `expandedNodes` set, selections as `tf/tsf/tt` in the URL capped at `MAX_URL_TOPICS = 20`. It was an inline panel without counts and with a plain substring filter; heritagemonitor gets a MUI `Dialog` in `common/components` (no tree dependency, recursive `Collapse`), opened from a "Topics" filter button.

| Item | Detail |
|---|---|
| Api (structure) | `GET /v1/topics/tree` from the in-memory `topics.json` (4,516 rows), static, cached forever |
| Api (counts) | `GET /v1/topics/counts?<current search params>` = `topicModalAggs()` (terms on `topic_id`, `subfield_id`, `field_id`) on `projects` under the **current query + filters + corpus, ignoring the topic params themselves**; blank + corpus results cached per corpus; nodes with 0 are hidden (DCH: 1,715 of 4,513 topics) |
| Api (search) | `GET /v1/topics/search?q` over the in-memory rows: normalise (lowercase, asciifold), tokenise, AND over tokens with prefix matching, fuzzy for tokens >= 4 characters (1 edit, 2 edits from 8), rank exact word > prefix > fuzzy and topic > subfield > field name; returns ranked nodes with their ancestors |
| Web | tree shows counts; typing calls the search (debounced) and the modal **expands the ancestors of the top matches, highlights the matched text, scrolls the first match into view**; selection -> `topic`/`subfield`/`field` params (URL cap 20 with a visible "max reached" hint); refetch counts when the query, filters or corpus change |
| Used by | projects, experts, minorities (api resolves a field/subfield to leaf `topic_ids` for minorities), funding |
| Chat context | selected topics (names) appear in the state heading |

### F. Experts (`/search/experts`)
No index (D9): projects query -> `terms org_ids` -> org docs.

| Item | Detail |
|---|---|
| Route | `GET /v1/experts/search` (project params + `sort=matches\|funding\|projects\|works`) |
| Query | one request `size:0` on `projects`: `sqs(q, PROJECT_FIELDS)` + project filters, aggs `orgs = termsAgg('org_ids', 200)` and `topic_id`; org data from the in-memory org table (F0-11) plus rollups by `mget` only for the page shown, `mergeByNameKey`, rank, page 20 in the api |
| Ranking | default matching projects (`doc_count`, corpus-aware); other sorts use org rollups |
| Facets / filters | topic count facet + topics modal; project filters (years, pillar, theme, funder, programme, region, corpus) |
| Tabs | `overview` = org overview (reuse B); `projects` = `/v1/projects/search?q=<same>&org=<id>&<same filters>` |
| Chat context | rows `- org (country, N matching projects, ~EUR)`; selected org overview; sources = org websites |
| Gotchas | org `project_count` is global, show `matchedProjects`; first `org_ids` aggregation after a restart may be slow (`eager_global_ordinals` set) |

### E. Grants and funding (`/search` entity grants; `/search/funding`)
**Grants list** (`/search?e=grants`, standard panel): `GET /v1/grants/search`, `/:id`, `/suggest`; params `q c page sort=funding\|projects\|relevance funder programme jurisdiction only`; text fields `description, id`; facets `funder` (label `funder_name`), `programme`, `jurisdiction`; corpus `dch_project_count >= 1`; row `id, funder, funder_name, programme, action, description, jurisdiction, project_count, dch_project_count, total_funded_eur, is_pseudo` (pseudo `NONE::<funder>` rows labelled "projects without a funding stream"; ids contain `::` and spaces, URL-encoded). Tabs: `overview` (hierarchy funder > programme > action, totals), `projects` (`stream=<id>`), `organisations` (top funded orgs); button "Explore funding of this programme" -> `/search/funding?stream=<id>`.

**Funding page** (`/search/funding`, D10; the "funding programmes explorer"): one page state in the URL shared by two entry points, free-text query and grant picker.

| Item | Detail |
|---|---|
| Page state | `q` (text on projects), `funder programme stream years region c`, `sel` (organisation), `tab`, `view`, `layer` |
| List (left) | organisations worked on the matching projects, ranked by funding: `GET /v1/funding/organisations` = `projects` query (`q` + filters) `size:0`, agg `orgs` (`termsAgg('org_ids', 500, shard_size 2000)` ordered by `sum(funded_eur_per_org)`), org data (name, geo, region) from the in-memory org table (F0-11), region filter applied on those rows; paged in the api |
| Tabs (right) | `map` (default, `fill`, `keepMounted`): **H3 hex layer of organisation funding** from the demo (`createHexFundingLayer`, `hexBins.ts`; api payload `orgs[{id,name,lat,lng,fundingEur,projectCount}]` adapted to the demo `Organisation` shape), arcs stay as a layer option; `organisation` (selected org detail + its funded projects among the matches); `programmes` (grant picker: `PaginatedList` of grants ranked by `total_funded_eur`, funder/programme/jurisdiction filter menus, `project_count`/`dch_project_count`); selecting a grant sets `funder/programme/stream` in the page state, so the list and hex map now show the organisations funded by that stream |
| Map request | second request for the map: `GET /v1/funding/map` with the same params returns all geolocated orgs of the top 500 (list is paged, map needs the whole set). The blank-query variant (no `q`, no filters, per corpus) is the one slow query (1.1 s cold): cached per corpus in the api and warmed at boot (section 4a) |
| Chat context | state heading (query, funder/programme/stream, corpus), list rows `- org (country, ~EUR, n projects)`, selected org and selected programme summary (hierarchy, totals), sources = programme/project links |
| Gotchas | EU-centric map (26% of projects have a geolocated org, 49% of DCH); equal-split funding (D13), 58% of projects have an amount: always "approx. EUR" plus coverage; hex layer only (no column layer, user decision) |

### G1. Organisation network (`/search/collaboration/organisationNetwork`)
Layout like the demo (demo/test organisation: a collaborative one, e.g. Fraunhofer, 3,042 projects and 501 partners, not JOHNS HOPKINS): list = collaborations (partners), `TabbedPanel` with `map` (deck.gl, default, `fill`, `keepMounted`) then detail tabs.

| Item | Detail |
|---|---|
| Entry | hero page and search bar autocomplete organisations ranked by `project_count` (F0-9); pick -> `/search/collaboration/organisationNetwork?center=<orgId>` |
| Route | `GET /v1/collaboration/organisations/:id/network?years funder topic subfield field c max=500` |
| Query | `orgNetworkBody(id, max)` (`term org_ids` + project filters, agg `partners = termsAgg('org_ids', max+1)`), partner docs from the **in-memory org table** (F0-11, no `mget`), `mergeByNameKey(buckets, docs, center)` drops the centre's own duplicate ids, keep partners with `geo`; payload `nodes[{id,ids,name,lat,lng,w,countryCode}]` (node 0 = centre), `edges[{a,b,w}]`, `meta{partners,withoutGeo}` |
| Map | the searched organisation centred, arcs to all partners weighted by shared projects (`ArcCollaborationLayer`; adapter payload -> `CollaborationEdge`, `projects` filled lazily) |
| Tabs | `map`; `detail` = the organisation overview and the **project(s) linking it to the selected partner**: `/v1/projects/search?orgAll=<centre>&org=<partner ids>&only=...` (shared projects need AND: `orgAll`); `projects` = the organisation's own projects (second paginated list). Partner click (list, arc, or detail) -> `center=<partner>` |
| URL | `center`, `sel` (selected partner), `tab`, `view`, `layer`, project filters, `c` |
| Chat context | centre + partner rows `- partner (country, N shared projects)`; selected partner detail; shared projects list; sources = project links of the selected pair |
| Gotchas | D19: the busiest pairs are one institution under two ids; only geolocated orgs can be drawn (~18%): show "n partners without location"; US orgs have few partners |

### G2. Query network (`/search/collaboration/queryNetwork`)
| Item | Detail |
|---|---|
| Route | `GET /v1/collaboration/query-network?q c years topic ... maxEdges=100` |
| Query | `queryNetworkBody(q, 2000)` (`size 2000`, `_source:false`, `docvalue_fields [org_ids, coordinator_ids]`, default codec; **never `_source`**, cold first call 24-30 s otherwise), pair counts in the api (cap 30 orgs per project, skip 1-org projects), nodes from the in-memory org table, `mergeByNameKey`, drop self-pairs and orgs without `geo` |
| Cap that respects ranking | keep the **top-K edges** by weight = shared projects, ties broken by the ranking of the underlying projects (sum over the pair's projects of `2000 - hit position`, i.e. the current sort/relevance); nodes = the endpoints of the kept edges; `maxEdges` UI control (`SingleSlider`, 10..300, URL `maxEdges`) with a hard server cap; `meta{projectsScanned,totalMatches,capped}` shown as "based on the top 2,000 projects" |
| Force graph | copy the approach of `useForceLayout.ts` + `NetworkGraphView.tsx`: d3-force layout (link, many-body, center, collide, 300 static ticks on a circular seed; 2D as confirmed by the user) rendered in a deck.gl `OrthographicView` (`LineLayer` + `ScatterplotLayer`), Louvain communities (`graphology`, `graphology-communities-louvain`, seeded RNG) as node colours. New dependencies: `d3-force`, `graphology`, `graphology-communities-louvain`. Confirmed: 2D layout in deck.gl, no 3D library. Wrapped in `common/deckgl` (RULES #4) |
| Tabs | `graph` (default; toggle layer `arcs` = geographic fallback, `network` = force graph; `layer` in the URL), `detail` (selected edge: the two organisations + their shared projects via `orgAll`), `projects` |
| Chat context | state heading with `maxEdges`, top edge rows `- A <-> B (N shared projects)`, selected edge detail, project links |
| Gotchas | only the top 2,000 matching projects are scanned; too many edges make the layout slow: hence the cap; the graph is static after the ticks (no live animation) |

## 6. Steps and review gates
Each step is a set of slices/tickets. Every step finishes with a **REVIEW GATE**: the user runs `localhost:3000`, interacts, and proposes changes; the next step starts only after that. Prerequisite for Step 1: the coordinator has loaded the sample into the dev OpenSearch on 9200.

| Step | Scope | REVIEW GATE: what the user checks |
|---|---|---|
| 1 | **F0 foundation + URL state + projects list on `/search`** (F0-1..F0-8, F0-10, links helper, corpus in URL): search box (Google-like syntax), projects list with ranking relevance/budget, pagination "10,000+", DCH/SCI switch in the URL, row select -> basic overview with DOI/CORDIS/OpenAIRE links, chat context of list + selection | open `/search`, type queries, switch corpus, paginate, sort, select a row, press back and forward (state restored), copy the link into a new tab (same page), ask Lucy about the list, click the project links |
| 2 | **Projects complete (A)**: filters (year presets, funder, programme, pillar, theme, region), facets (topic count, year histogram), typo tolerance and "did you mean", project autocomplete, organisations tab | filters and facets in the URL and back button; year presets; typo query; autocomplete; organisations tab; Lucy sees filters and tab list |
| 3 | **Organisations (B)** + `/search` entity switch + deep links (`only`, `sel`) from the project tab + organisation autocomplete | switch entity, organisation ranking and filters, click an organisation in a project, autocomplete on the hero page |
| 4 | **Works (C)**: list, PDF/DOI button, filters, works cache, project and organisation Works tabs; **test Lucy fetching a DOI link and a `pdf_url`** (redirect/allowlist risk, section 3.2) | works search, filters, PDF button, first pages instant, Lucy summarises the listed papers from the URL list, and reports whether the DOI fetch is blocked |
| 5 | **Minorities (D)** on the new schema, URL state moved to `common/url` | the old page on new data, seed-first order, topic facet (2nd), two-step search, tabs, back button |
| 6 | **Topics modal (H)** | open the modal in projects, corpus-aware counts, search with tolerance and auto-expansion, 20-topic URL cap |
| 7 | **Experts (F)** | expert ranking, facets, matching projects tab |
| 8 | **Grants + funding (E)**: grants list, funding page with hex map, programmes tab | pick a programme, map reacts, both entry points in one URL |
| 9 | **Organisation network (G1)**: map tab, partners list, shared-projects detail, viewport in the URL | pan/zoom, reload restores the view, partner click, autocomplete on the hero page; demo with Fraunhofer (501 partners), first call after an api restart is fast (warm-up, org table) |
| 10 | **Query network (G2)**: arcs, then the force graph with the cap | `maxEdges` control, ranking-respecting cap, layer switch |

## 7. Verification checklist (real VM indexes)
Set once on the VM: `OS=http://127.0.0.1:9200`, `AUTH="-u admin:$PW"`, `export NO_PROXY=localhost,127.0.0.1`. Api: `API=http://localhost:3001/v1` (site origin in prod). For every slice also check (cold = right after `docker restart hm-opensearch` and an api restart; note both cold and warm times, section 4a): (1) the URL contains the state after each interaction and a copied link reproduces the page, (2) back restores the previous selection, (3) Lucy's context contains list, selected detail and URLs, (4) DCH/SCI toggle changes counts, (5) one cold run after `docker restart hm-opensearch`.

| Slice | OpenSearch level | API level |
|---|---|---|
| F0 | `curl $AUTH $OS/_cat/indices?v` lists the five indexes, counts 3,893,065 / 494,099 / ~50M / 278 / 6,119 | `tsx --test` green (query parity, url codecs, links); `curl "$API/projects/search?q=%22digital%20heritage%22%20-museum&c=dch"` has `totalCapped`; `page=501` -> 400; unbalanced quote -> 200 |
| A | `curl $AUTH $OS/projects/_search -H 'Content-Type: application/json' -d '{"size":0,"track_total_hits":10000,"query":{"bool":{"must":{"simple_query_string":{"query":"photogrammetry","fields":["acronym^5","title^3","summary","keywords"],"default_operator":"AND"}},"filter":[{"term":{"is_ch":true}}]}},"aggs":{"t":{"terms":{"field":"topic_id","size":25,"shard_size":500}}}}'` | `curl "$API/projects/search?q=photogrammetry&c=dch&sort=budget&years=2019-2025&topic=<id>"`: facet counts equal the agg; `q=photogramtery` -> `mode:"fuzzy"`; `curl "$API/projects/<id>/organisations"` coordinators first (EC project); `projectLinks` output for an H2020, an FP7 and an NIH project |
| B | `.../organisations/_search -d '{"size":5,"sort":[{"total_funding_eur":"desc"}],"_source":["legalName","region","rorTypes"]}'` | `curl "$API/organisations/search?sort=projects&region=Western%20Europe"`; `.../suggest?q=fraunh` ranked by projects, no duplicate `name_key`; `only=<id>` returns one row |
| C | `.../works/_search -d '{"track_total_hits":10000,"size":3,"query":{"bool":{"must":{"simple_query_string":{"query":"cultural heritage","fields":["title^3","authors"],"default_operator":"AND"}},"filter":[{"range":{"year":{"gte":2015}}},{"term":{"open_access_color":"gold"}}]}}}'` cold vs warm; blank + citation sort timing | `curl "$API/works/search?q=heritage&oa=gold&years=2015-2025"`; `?project=<id>` count = `projects/<id>.work_count`; blank default served from cache on pages 1-5, page 6 hits OpenSearch; `GET /works/<id>` works although `id` is not indexed |
| D | `.../minorities/_search -d '{"size":5,"sort":[{"is_seed":"desc"},{"project_count":"desc"}],"_source":["qid","group_name_en","is_seed","project_count"]}'`; `.../projects/_count -d '{"query":{"term":{"minority_qids":"Q7325"}}}'` = 1,344 | `curl "$API/minorities/search?c=dch"` only groups with `dch_project_count >= 1`; `q=<institution>` finds a minority via projects; facet order shows topics 2nd |
| H | `.../projects/_search -d '{"size":0,"query":{"term":{"is_ch":true}},"aggs":{"t":{"terms":{"field":"topic_id","size":5000}}}}'` gives ~1,715 buckets | `curl "$API/topics/counts?c=dch&q=archaeology"` ignores `topic` params; `curl "$API/topics/search?q=archeology"` (typo) ranks the archaeology topics first with ancestors; second tree call from cache |
| F | `.../projects/_search -d '{"size":0,"query":{"simple_query_string":{"query":"3d scanning","fields":["title","summary"]}},"aggs":{"o":{"terms":{"field":"org_ids","size":200,"shard_size":2000}}}}'` | `curl "$API/experts/search?q=3d%20scanning&c=dch"`: top org = top bucket after `name_key` merge; time the first call after a restart |
| E | `.../grants/_search -d '{"size":3,"sort":[{"total_funded_eur":"desc"}]}'` | `curl "$API/grants/search?funder=EC&programme=H2020"`; `curl "$API/funding/map?stream=<id>"` sum equals the `sum(funded_eur_per_org)` agg for the same query; grant id with `::` round-trips through the URL |
| G1 (use Fraunhofer, not Johns Hopkins) | `.../projects/_search -d '{"size":0,"query":{"term":{"org_ids":"<orgId>"}},"aggs":{"p":{"terms":{"field":"org_ids","size":501,"shard_size":5010}}}}'` | `curl "$API/collaboration/organisations/<id>/network?max=200"`: node 0 = centre, no partner with the centre's `name_key`; shared projects via `orgAll` equal the edge weight |
| G2 | `.../projects/_search -d '{"size":2000,"_source":false,"docvalue_fields":["org_ids"],"track_total_hits":false,"query":{...}}'` | `curl "$API/collaboration/query-network?q=archaeology&maxEdges=100"`: `edges.length <= 100`, heaviest edges kept, ties follow the ranking, `meta.capped` set above 2,000 projects |

## 8. Open questions (genuinely open)
1. **API/OpenSearch container limits** (section 4a, item 6): raise the api to `mem_limit` 1536m (`--max-old-space-size=1152`) and lower opensearch to heap 10g / container 23g in `docker-compose.prod.yml`? A coordinator decision, not needed before Step 8.
2. **DOI fetch by Lucy** (section 3.2 risk): decided in Step 4 from the test result (allowlist redirect targets vs resolving the DOI api-side).

Resolved (now in section 1 and 3.2): chat caps (300-char rows, 5,000-char detail, 15,000 total, 30 sources offered, 20 fetched), 2D force graph, every `doi` shown, year presets include the current year, publisher menu, hex layer only.

## 9. Summary (steps and review gates)
1. Step 1: F0 foundation + URL state + projects list on `/search` (corpus, sort, page, selection in the URL, overview with real DOI/CORDIS links, Lucy context). Gate: click through, back button, copied link, ask Lucy.
2. Step 2 (projects complete: filters, year presets, topic facet, typo tolerance, autocomplete, organisations tab) and Step 3 (organisations, entity switch, deep links). Gate after each.
3. Step 4 works (PDF/DOI button, first-5-pages cache, Lucy summarises the listed papers) and Step 5 minorities on the new schema (topic facet 2nd). Gate after each.
4. Step 6 topics modal (corpus-aware counts, tolerant name search, auto-expand) and Step 7 experts. Gate after each.
5. Step 8 grants and funding explorer (hex map, programme picker sets the page state) and Step 9 organisation network (arcs, viewport in the URL). Gate after each.
6. Step 10 query network (capped, ranking-respecting force graph, arcs fallback). Final gate; nothing needs an index change (gaps are in section 4).
