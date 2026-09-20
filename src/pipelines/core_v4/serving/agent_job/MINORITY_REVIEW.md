# Precision review of keyword-based minority tagging on projects

Scope: 7 groups, 8,438 of the 9,293 tagged projects (a project may carry several groups). Sample sizes are 25 per group, judged by one reviewer (Claude, reading title + summary + the matched context), so **the error bars are wide** (Wilson 95% intervals shown below). The sample is only used for the TRUE/FALSE judgement; the keyword and "only via" counts are exact SQL/matcher counts over all projects of the group.

## Headline finding

**The dominant source of false positives is not the keyword list but the matcher's typo tolerance.** `MinorityMatcher` adds every edit-distance-1 variant (first letter fixed) of every keyword of 8+ characters and only drops a variant when it is a single common English word. Multi-word keywords are never checked, so ordinary English phrases become "typos" of group names:

| group keyword | variant that actually matched | projects |
|---|---|---|
| `manx people` | **many people** | 1,772 of Manx's 1,787 |
| `dom people` | **do people** | 218 of 218 (all of Dom) |
| `the jews` | **the news** / the jets / the jaws / the ews / the jes | 510 Jewish projects match only through these |
| `sami people` | **same people** | 46 Sámi projects match only through this |
| `silesians` | **silesian** (adjective: Moravian-Silesian Region, Silesian University, Silesian Basin) / salesians | 198 of 200 |
| `laz people` (not in scope) | lay people / Lao people | 99 of 99 |
| `svan people`, `akan people` (not in scope) | scan people / san people, aian people | 8 of 8, 7 of 7 |

I re-ran the pipeline matcher (`src/enrichment/minority_matching/matcher.py`, rules.yaml, the `minority` table plus `manual_term_overrides.csv`, typo=True, spaCy word list) on all tagged projects and it reproduces the stored counts exactly (Manx 1787, Russians 2242, Jewish 1870, Turkish 1676, Sámi 590, Dom 218, Silesians 200). Across **all** groups, **3,055 of the 9,528 (group, project) tags (32%) come only from typo variants**; the largest ones are the groups above plus Laz (99), Azoreans (72, singular `azorean`, probably fine), Aragonese/Franconian/Swabian/Székely singulars (~43, probably fine).

The second problem is that adjectives are used as group keywords for **titular-nation adjectives**: `russian` (2,195 projects) and `turkish` (1,631) are country/language adjectives, not references to the minority. That is a keyword design issue: the matcher cannot tell "Russian invasion" from "Russian-speaking minority".

## Summary table

Precision = TRUE / (TRUE + FALSE) in the 25-sample; UNSURE excluded and reported separately. 95% Wilson interval in brackets. "After fix" is an exact count from the matcher output over all projects except where marked est.

| group (QID) | projects now | sample | TRUE / FALSE / UNSURE | precision (95% CI) | recommendation | after fix |
|---|---|---|---|---|---|---|
| Manx people (Q125564) | 1,787 | 25 | 0 / 25 / 0 | 0% (0-13%) | disable typo variants; block `Manx shearwater/Loaghtan/cat/comet` | 8 (est. 2-6 genuine) |
| Russians (Q49542) | 2,242 | 25 | 0 / 22 / 3 | 0% (0-15%) | exclude from prominent display; if kept, require a second signal | 135 with second signal (est. ~80 genuine) |
| Turkish people (Q84072) | 1,676 | 25 | 2 / 23 / 0 | 8% (2-25%) | require a second signal | 122 with second signal (est. 60-80 genuine) |
| Jewish people (Q7325) | 1,870 | 25 | 12 / 6 / 7 | 67% (44-84%) | drop typo variants and `hebrew`; flag `Judaism` as religion | 1,216 (est. ~90% of definite cases, about 1/3 religion or text scholarship) |
| Sámi people (Q48199) | 590 | 25 | 10 / 13 / 2 | 43% (26-63%) | drop `same` and its typo variant `same people`; keep the rest | 359 (est. 90%+) |
| Dom people (Q2656122) | 218 | 25 | 0 / 25 / 0 | 0% (0-13%) | disable typo variants; group then matches nothing | 0 |
| Silesians (Q140472) | 200 | 25 | 1 / 21 / 3 | 5% (1-22%) | disable typo variants; add "Silesian" only with a second signal | 2 exact + ~3 with second signal |

Sample-based precision after the recommended fixes (Jewish: 12 TRUE / 1 FALSE / 6 UNSURE among the 19 sample rows that survive; Sámi: 10 / 0 / 2 among the 12 that survive, plus 19 TRUE / 0 FALSE / 1 UNSURE in a separate 20-project sample of Sami-only matches) rests on very few rows; treat as indicative only.

## Per-group findings and recommendations

### Manx people
- Keywords: `Manx`, `Manks`, `Manx people`, `Manks people`. Of 1,787 projects, **1,773 match only via a typo variant** (`many people` 1,772, `masks people` 1). Only 14 projects contain the word "Manx".
- Those 14: 2 TRUE (Manx language revival, 18th-century Manx), 4 UNSURE (Manx Museum, Manx National Heritage, Manx Blue Carbon Project, Manx as identity in a medieval hagiography), 8 FALSE (6 Manx shearwater bird, Manx Loaghtan lamb, and one mistranslation "Manx" for Mansi).
- Recommendation: no typo variants for multi-word keywords; add exclusion `Manx (shearwater|Loaghtan|cat|comet)` (6 projects). Result: 8 projects, 2-6 of them plausible. "Manx" is too rare in this corpus to matter; do not present it as a group with 1.8k projects.

### Dom people
- The only keyword is `Dom people`; it never matches literally (0 exact hits). All 218 projects are `do people` ("how do people ..."). Result after fix: 0. The group name is so generic that literal matches are unlikely to be added; if wanted, add `Domari` (language) as an exact keyword, but that is a new keyword to be reviewed.

### Silesians
- 198 of 200 match only via `silesian` (typo variant of `silesians`), plus 3 via `salesians` (the Catholic order, e.g. Salesians of Don Bosco). The `silesian` hits are regional adjectives: "Moravian-Silesian Region" (a Czech administrative region, dominant), Silesian University of Technology, Silesian Museum in Opava, Upper Silesian Basin (geology). Only 2 projects contain the exact word "Silesians". The one clear TRUE (Bytom regional dialect) is itself a typo-variant hit, so a narrow second-signal rule (`Silesian` within 2 words of language/dialect/ethnic/identity/minority/people/culture/heritage, or `Silesians`) is the only way to keep it: that gives about 5 projects in total (2 exact plus ~3).
- Recommendation: disable typo variants; add `Silesian language`, `Silesian dialect`, `Ślōnzokŏ`-type keywords only with a second signal; exclude `Moravian-Silesian`.

### Sámi people
- Of 590: exact `Sami` (capitalised, strict mode) 296, `Same` (capitalised) 186, `sapmi` 60, typo `same people` 46, `sami people` 28 exact, `saami` 39, `lapps` 4, `joik` 1. `Same` is deliberately included as the Swedish/Norwegian word for Sami (rules.yaml `common_words`), but the corpus is English: it matches "Same-sex", "Under the same sky", "Same as before". **184 projects match only via `Same`, 231 only via `Same` or the typo `same people`.**
- After dropping `same` and the `same people` variant: **359 projects**. In the sample every row matched on `Sami`, `Sápmi`, `Saami`, `Lapps` was TRUE except two UNSURE peripheral mentions (10 TRUE, 0 FALSE, 2 UNSURE of the 12 rows not driven by `Same`), and a separate random sample of 20 projects that match only via capitalised `Sami` had 19 TRUE and 1 UNSURE (Sami named among minorities in a student-exchange project); I found no case of Sami as a personal name.
- Recommendation: remove `same` from the keyword set (the override CSV adds it back: `manual_term_overrides.csv` row `Q48199,Sámi people,same`; delete that row and the `common_words` entry becomes moot), and disable typo variants of `sami people`.

### Jewish people
- Of 1,870: `jewish` 887, `the jews` typo variants 524 (of which `the news` 385, `the jets` 47, `the jaws` 37, `the ews` 31, `the jes` 5; only `the jew` 13 is a real variant), `Jews` 403, `hebrew` 236, `Judaism` 176, exact `the jews` 76, `jewish community` 54, `yiddish` 44, `Jew` 38.
- **510 projects match only via the bogus typo variants** ("the news" etc.); dropping them leaves 1,360. Dropping `hebrew` as well removes 144 more (Hebrew University of Jerusalem as partner, Hebrew Bible philology, medieval Hebrew translations), leaving 1,216. Keeping `Judaism` yields a mix of theology and ancient-religion projects (84 projects match only through it after the fix, plus 11 with `hebrew`); it is about the religion rather than the minority group, so flag or cut it (1,121 without it).
- In the sample the remaining Jewish hits are mostly clear (Jewish identity, German-Jewish architects, Jews of Ottoman Algeria) but a third are context or religion-scholarship. A separate review of 24 projects matching only through `hebrew`/`Judaism` found roughly 2 clearly about Jewish people or writers, about 10 UNSURE (religion, medieval Hebrew scholarship), about 12 FALSE (Hebrew University, Hebrew Bible text criticism).
- Recommendation: drop typo variants; drop `hebrew` (keep `Hebrew` only next to `Jewish|community|Israelite`); keep `Judaism` but distinguish it as religion in the UI; `Jews`/`Jewish` stay.

### Russians
- Of 2,242: `russian` 2,195 (all with the plain adjective in text; 2,171 match only via `russian`/`Rus`), `russians` 51, `Rus` 15 (medieval Rus'), `russian people` 12. Group is listed as a minority of Kazakhstan, Latvia, Ukraine; almost all hits are Russia-the-country (Russian government, Russian Federation, Russian opera, Russian language teaching, Russian HIV cohorts). Even the more specific keyword `russians` was TRUE in only 2 of 14 sampled projects.
- Sample: 0 TRUE, 22 FALSE, 3 UNSURE (Russian settlements on Svalbard, Russian Orthodox parish archives in Helsinki, Russian culture in a Finnish-Russian biography). Upper 95% bound for precision is 15%.
- Second-signal test (`russian(s)|russian people` within 4 words of minority/ethnic/diaspora/immigrant/migrant/emigrant/refugee/speaking/mother tongue/second generation/settler/newcomers/community/heritage/origin/descent): **135 projects**. In a sample of 22 of those: 13 TRUE (Russian-speaking youth/immigrants in Finland, heritage Russian, Russian exile culture, Russian community in Alaska), 8 FALSE (mostly `community`/`heritage`/`refugee` clashes: Russian invasion + refugees, Russian revolutionary heritage), 1 UNSURE, so about 60% precision, about 80 genuine projects.
- Recommendation: **exclude the group from prominent display**; if it must stay, use the second-signal rule with a narrower signal set (drop `community`, `heritage`, `refugee`, `origin`; keep `Russian-speaking`, `ethnic Russian`, `diaspora`, `emigrant/immigrant`, `minority`). Estimated result: about 100 projects, precision perhaps 70%.

### Turkish people
- Of 1,676: `turkish` 1,631 (1,614 match only via it), `Turks` 57, `Turkish people` 5. `turkish` is the adjective/language name: website language options, Turkish partner schools in Erasmus projects (Erasmus text lists languages), Turkish literature, Turkish highlands, Turkish willow. 476 of the projects have `Turkish` in the title, so a title-level match is not a usable second signal (Turkish literature, Turkish highlands, Turkish dictionary).
- Sample: 2 TRUE (Turkish migrant women; Roma and Turkish mother-tongue children in Bulgaria), 23 FALSE, 0 UNSURE. Precision 8% (CI 2-25%).
- Second-signal test (`turkish|turks|turkish people` within 4 words of minority/ethnic/diaspora/immigrant/migrant/emigrant/refugee/speaking/mother tongue/second generation/settler/newcomers/community/heritage/origin/descent/guest worker): **122 projects**. Sample of 22: 9 TRUE, 9 FALSE, 4 UNSURE, i.e. about 50% precision (about 60-80 genuine).
- Recommendation: require a second signal as above (drop `community`, `heritage`, `origin`, `refugee` from the signal list; add `Turkish-speaking`, `Turkish Cypriot`, `Turkish minority`, `guest worker`). Otherwise exclude from prominent display.

## Global recommendations

1. In `MinorityMatcher`, do not generate typo variants for multi-word keywords, or require that every changed token of a variant is not a common English word (checked per token, not as a phrase). This alone removes 3,055 tags (32% of all tags), including all of Manx, Dom, Laz and Svan.
2. Treat singular/plural or adjective/noun forms (`silesian`, `azorean`, `franconian`, `swabian`, `szekely`, `aragonese`) as explicit keywords chosen by a person, not as automatic typos, and review each with a second signal.
3. Do not use a country adjective (`Russian`, `Turkish`, and by extension `Albanian`, `Finnish`, `Egyptian`, `Uzbek` etc. groups: Finns 141, Albanians 121, Egyptians 52, Uzbeks 30 may have the same problem and were not reviewed) as a standalone keyword for a minority tag; require it to co-occur with a minority signal.
4. Remove `same` from the Sámi terms.
5. Re-run this sample after the fix; with 25-row samples per group the intervals above are wide, so a follow-up of about 100 rows per remaining group would be needed to quote a precision.

## Method notes and caveats

- Matching re-run: `src/enrichment/minority_matching/matcher.py` with the `minority` table from `data/duckdb/core/core_v4_noworkenrichment-min.duckdb`, `manual_term_overrides.csv`, `rules.yaml` and `english_words()`; text fields title, summary, keywords, subjects joined with a space, exactly as in `pipelines/core_v4/enrichment/minorities.py`. Match keyword and surface form per hit come from the matcher (`Match.keyword`, `Match.typo`, `Match.strict`); "matched text" in the tables is the surface string in the normalised text (accent-stripped, lower-cased except for strict matches).
- Sampling: `ORDER BY hash(id) LIMIT 25` within each group (DuckDB), so it is reproducible. Only projects that were tagged were reviewed; **recall was not evaluated** (the untagged 99.8% could contain genuine mentions that no keyword catches, e.g. Sami written only as "Saami" variants).
- Judgement rubric: TRUE = the project is about, or centrally involves, the group as people, community, culture, language or heritage; FALSE = the match is a homonym, typo variant, region or institution name, country adjective, language-option list or partner nationality; UNSURE = peripheral mention or religion/text scholarship where the link to the group is arguable. One reviewer; no second opinion. Titles/summaries were translated to English for about 3.5% of projects (`is_translated`); I saw one translation-induced false hit (a Mansi/Manchu project translated as "Manx language").
- The second-signal counts (135 and 122) use a proximity regex (up to 3 words between keyword and signal) over title, summary and keywords; this is an approximation and not implemented in the pipeline.

## Evidence: 25-project samples

Verdicts: T = TRUE, F = FALSE, U = UNSURE. "matched text" is the normalised surface string that triggered the tag (`(typo of X)` means an edit-distance-1 variant of keyword X).

### Manx people (Q125564), 1,787 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 6958265294960014248 | many people (typo of manx people) | F | Adult education to promote RElational Ability | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 2 | 6964684408916166154 | many people (typo of manx people) | F | Immunological tools for a seroprevalence and immune status map of Burk | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 3 | 13615333406460993210 | many people (typo of manx people) | F | The Diet, Material Culture and Earnings of the Labouring Poor in Engla | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 4 | 15938280374719249743 | many people (typo of manx people) | F | Extrinsic threats and biological predisposition in animal extinction a | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 5 | 10809629656813093992 | many people (typo of manx people) | F | North Manchester Education and Community Astronomy centre. | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 6 | 2078518817289328806 | many people (typo of manx people) | F | May ICT be with you | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 7 | 610322907274323423 | many people (typo of manx people) | F | Temporary Pacing Safety Monitor - Caseworks and User Interface | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 8 | 4958616084670092915 | many people (typo of manx people) | F | The design, development and clinical assessment of a new metacarpophal | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 9 | 17095680958247167544 | many people (typo of manx people) | F | Mapping the Genes and Neurons that Regulate Sleep Homeostasis | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 10 | 12826530737319089077 | many people (typo of manx people) | F | TREES: To Reinforce European Environment Sustainability | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 11 | 13118628792619712394 | many people (typo of manx people) | F | When Fear is Fun: An Empirical Investigation of Recreational Fear | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 12 | 14076134937511089574 | many people (typo of manx people) | F | Coreo - A platform for collecting, improving, managing and maintaining | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 13 | 31312024177498043 | many people (typo of manx people) | F | Ketamine augmentation of electroconvulsive therapy to improve outcomes | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 14 | 5248252758455491794 | many people (typo of manx people) | F | Strategies for the development and maturation of functional hepatocyte | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 15 | 1550580403789672277 | many people (typo of manx people) | F | Competent Active Conscious. Educating adults for the future | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 16 | 15878359084125088006 | many people (typo of manx people) | F | Socially Isolated and Digitally Excluded Service | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 17 | 15243995923381405316 | many people (typo of manx people) | F | Sharing the Road: Exploring transitions away from private vehicle owne | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 18 | 8281841310046671457 | many people (typo of manx people) | F | Collaboration on science cafés about forests and chemistry during the  | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 19 | 3102145931275403113 | many people (typo of manx people) | F | The knowledge strip: The art of communicating and collaborating beyond | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 20 | 18357003476930343074 | many people (typo of manx people) | F | Evaluation of recombinant complement factor H as therapy for orphan re | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 21 | 15263377904962386349 | many people (typo of manx people) | F | Artificial Intelligence posture scanning app with exercise programme f | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 22 | 4755690396804122264 | many people (typo of manx people) | F | The chemical behaviour of sulphur in magmas at high temperature and pr | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 23 | 16205631263861689731 | many people (typo of manx people) | F | SME Productivity Improvement Platform | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 24 | 8299523674850207864 | many people (typo of manx people) | F | Functional characterisation of a Maurer's cleft protein involved in ad | 'many people' matched as a 1-edit typo variant of 'manx people' |
| 25 | 8121793164574165245 | many people (typo of manx people) | F | Seeing and thinking: Interplay between externally and internally gener | 'many people' matched as a 1-edit typo variant of 'manx people' |

### Russians (Q49542), 2,242 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 3686312699178601933 | russian | U | Present and future tourism development of Russian settlements on Svalb | Russian settlements on Svalbard (Russian community abroad, arguably) |
| 2 | 2017431975122935377 | russian | F | How does conspiracism relate to representative democracy? | Russian government (adjective, politics) |
| 3 | 2686347332073302939 | russian | F | RCT of Russian IDU Peer Network HIV Prevent Intervention | Russian people who inject drugs, study population in Russia |
| 4 | 3235902458455428728 | russian | F | U.S.-Russia Cooperative Research: RA-Ven.NET/Russian American Virtual  | Russian-American enterprise network (country adjective) |
| 5 | 1115634129391578649 | russian | U | The road between East and West, Tito Colliander's cross-border journey | Russian culture / Orthodoxy in a Finnish-Russian writer biography |
| 6 | 8236027522822915564 | russian | F | The Baudelaire Song Project | Poems translated into Russian (language) |
| 7 | 17555876816398637036 | russian | F | Teaching Russian to Portuguese-speaking adults l1: theory and practice | Teaching Russian as a foreign language |
| 8 | 15882897644176026841 | russian | F | The corrspondence of M. F. Rajevskij with the Czechs | Czech-Russian cultural relations |
| 9 | 14535436385541760496 | russian | F | Invitation to Finland of Russian scientists Irina Ilina and Oleg Uljas | Russian scientists invited to Finland |
| 10 | 13253438069141426459 | russian | F | Computer-based Alcohol Reduction Intervention for alcohol-using HIV/HC | Russian women in HIV care in Russia (country population) |
| 11 | 9341839413879407670 | russian | F | Russian Dialectal Phonetics, a Multimedia Educational and Scientific I | Russian dialectal phonetics (language resource) |
| 12 | 10710473253512280645 | russian | F | Invitation to the Russian scientist Zhuravlev | Russian scientist invited |
| 13 | 17834504171900995953 | russian; russian people | F | Stigma, Risk Behaviors and Health Care among HIV-infected Russian Peop | Russian people who inject drugs in Russia |
| 14 | 6474887647567155457 | russian | F | The Dynamics of Migration in the Russian North.Problem out of control. | Migration in the Russian North (region of Russia) |
| 15 | 16300955538922055044 | russian | F | River discharge from the Russian Federation: An understanding of conte | Russian Federation river discharge |
| 16 | 6892148536646861549 | russian | F | Dangerous Russian Poets: The case of Natalya Gorbanevskaya and Joseph  | Russian poets, Soviet dissent (literature) |
| 17 | 7706355187031336778 | russian | F | Mussorgsky and the recitative in 19th-century Russian opera | Russian opera (music history) |
| 18 | 12481962897560145179 | russian | F | Representations of ¿Businessmen¿ in Russian Mass Media and Popular Cul | Russian mass media |
| 19 | 17436859362209244365 | russian | U | Russian orthodoxy in the archives of the Helsinki orthodox parish | Russian Orthodox parish archives in Helsinki (Russian community in Finland, arguably) |
| 20 | 9118340416486443614 | russian | F | Out of the Net | Partner in the Russian Federation |
| 21 | 16243901644635409432 | russian | F | Language Based Area Studies, Centre for Russian, Central and East Euro | Centre for Russian Studies (area studies) |
| 22 | 2717773451855565858 | russian | F | The Finnish-Russian border region from the 1890s to the 1990s | Finnish-Russian border region |
| 23 | 10919220471082559875 | russian | F | The restructuring of intellectual elites, social sciences, and transna | Russian post-communist discourse |
| 24 | 6463348959846050635 | russian; russian people | F | Stigma, Risk Behaviors and Health Care among HIV-infected Russian Peop | Russian people who inject drugs in Russia (duplicate project) |
| 25 | 1088832333601866978 | russian | F | Crossing European Borders: crossborder activities in German-Czech and  | Finnish-Russian border regions |

### Turkish people (Q84072), 1,676 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 3287463855812552138 | turkish | F | A comparative examination of the classical forms of the oral works com | Classical Turkish music (culture of Turkey) |
| 2 | 8484293850121281424 | turkish | F | Creative Fashion Recycling | Website in Turkish (language option) |
| 3 | 17101734596961207665 | turkish | F | Age-Sensitive Public Services: The Need for Competence Training | Guide available in Turkish (language option) |
| 4 | 947743018478475414 | turkish | F | Aiding Culturally Responsive Assessment in Schools | Website in Turkish (language option) |
| 5 | 1521771830256811708 | turkish | F | PeerCare - Peer learning on Emotional Intelligence for Informal Caregi | Materials in six languages incl. Turkish |
| 6 | 9848246443564220825 | turkish | F | Educational Development for Sustainable and Eco-friendly Cork Composit | Turkish universities as partners |
| 7 | 13815041764871535104 | turkish | F | Creating a compilation-based morphological marker and word-add-frequen | Turkish language dictionary (language of Turkey) |
| 8 | 7795703782760970368 | turkish | F | Developing Interventions in Schools for students's Mental Health | Romanian and Turkish schools (Erasmus partners) |
| 9 | 10616848581312433033 | turkish | F | CitizensHip cOmpetences to tackle clImate ChangEs | Materials in Turkish (language option) |
| 10 | 15645986013480702941 | turkish | F | Building Capacities of Special Athletes Federations on Family Support | Turkish sports federation (organisation) |
| 11 | 12725035739035548634 | turkish | F | Green Diversity?! - Inspiring Youth for Climate Action + Justice | Languages of a map (incl. Turkish) |
| 12 | 16633393790354586173 | turkish | T | Language Against Dropout: Second language promotion for children from  | Roma and Turkish mother-tongue minority children in Bulgaria |
| 13 | 13291646180103317628 | turkish | F | A new genre in Turkish literature: the story of the little boy | Turkish literature (national literature) |
| 14 | 339839600373668662 | turkish | F | 'Sharing without Solidarity: Politics, Heritage and Pilgrimage in a Di | Turkish-occupied Karpass peninsula, Cyprus (political adjective) |
| 15 | 2654611229158949201 | turkish | F | Global-Local Encounters: Impacts and Conflicts of Community-based Tour | Tourism in the Turkish highlands (Turkey) |
| 16 | 17528896738917272104 | turkish | F | ON-LINE WORLD OF BUSINESS | Turkish partner among four nationalities |
| 17 | 9248408122738613220 | turkish | F | YOUropean: What is Europe to you? | Materials in Turkish (language option) |
| 18 | 4751077258179991668 | turkish | F | Cross Diciplinary peer learning using Nautical Studies and ICT | Turkish school as partner |
| 19 | 11469064861845251233 | turkish | F | Research, training and Practice: building bridges around youth partici | Turkish university as partner |
| 20 | 4100587859174022306 | turkish | F | Digital transformation in vocational education | Modules prepared in Turkish (language option) |
| 21 | 1754211752890678057 | turkish | F | Development of gynecogenic plant breeding techniques for use in reform | Turkish willow plant (biology) |
| 22 | 15515341532945030587 | turkish | F | Gender Equality: Women in Progress | French and Turkish schools as partners |
| 23 | 12080118636036597948 | turkish | T | Guided Self-Help Groups for Female Turkish Migrants | Turkish migrant women, self-help groups |
| 24 | 16643601887198467354 | turks | F | Note in viaggio (notes on the road) | Turkish and Italian school partners (Erasmus exchange) |
| 25 | 4108170093996539543 | turkish | F | Tracing Informal Constitutional Changes using Word Embeddings | Turkish Constitutional Court case law |

### Jewish people (Q7325), 1,870 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 6830616015035745496 | judaism | U | Human Sacrifice and Judaism | Judaism (the religion) as topic: human sacrifice; religion rather than people |
| 2 | 5176221848108240617 | the news (typo of the jews) | F | Eko Życie | 'the news' (typo variant of 'the jews') |
| 3 | 18029290049736188913 | the news (typo of the jews) | F | AMPLIFYING SOUTH AFRICA’S CLIMATE HEALTH EXPERTS | 'the news media' |
| 4 | 15149925884191910429 | jewish | U | The early history of Spain's Gypsies from 1425 to 1783 | Spain's Jewish minority named as contrast to the Gypsies of the project |
| 5 | 1335277184446881174 | the news (typo of the jews) | F | Test bed for collective e-authorisations solution | 'the news' |
| 6 | 4862109118301874612 | Jews | U | Vulnerable Cities: Conflict Prevention in Urban Planning, Urban Regene | Israeli Jews vs Palestinian Arabs in urban conflict; groups involved, not the topic |
| 7 | 9768484801612489911 | jewish | T | Syncretic features in early Jewish (Rabbinic) magical literature | Early Jewish (Rabbinic) magical literature |
| 8 | 9174275714212979017 | judaism | T | Richard Beer-Hofmann. A scholarly Biography | Judaism keyword; biography of a Jewish-Austrian writer |
| 9 | 16169488107368007619 | jewish; judaism | T | Comprehending Judaizing practices and Zionist discourses among charism | Judaizing practices, appropriation of Jewish rituals |
| 10 | 3908935192243019196 | the jews; Jews | T | The Jews in Bohemia, Moravia and Silesia, Antisemitism, and the Retrib | Jews in Bohemia, Moravia, Silesia; antisemitism |
| 11 | 13186179805385971687 | judaism | U | Image of God and Abyss of Desires. The theological implications of ant | Hellenistic Judaism in theology / anthropology of the soul |
| 12 | 698246663651904145 | jewish | T | Wolfgang von Weisl (1896-1974) and his family history | Jewish identities, family history |
| 13 | 7618285947910140754 | Jews; jewish | T | The Spiritual Conversations of Giovanni Battista Eliano (1530-1589) an | Jesuit of Jewish origin |
| 14 | 11893148672991880873 | jewish | T | Veganism in Abrahamic Religion: An Exploration of the Everyday Experie | Jewish vegans in Britain |
| 15 | 4848910475594254765 | Jews | U | Criminals and Protectors: The Multiple Meanings of Gun Possession Amon | Jews mentioned in passing (study of Palestinian Arabs in Israel) |
| 16 | 292963336559894317 | hebrew | F | The Hexapla of 1-2 Samuel | Hebrew Bible philology (Hexapla) |
| 17 | 955918568214988307 | judaism | F | The Making of Angels in Late Antiquity: Theology and Aesthetics | Judaism only listed among religions of Late Antiquity |
| 18 | 2929438592206355567 | jewish | U | Identity, interculturality and interreligiosity | Jewish religion is one of five in an interreligious teacher guide |
| 19 | 12788217548346354682 | hebrew | U | The emergence of Modern Hebrew as a case-study of linguistic discontin | Modern Hebrew language revival; language, not the community |
| 20 | 18301342558385941131 | jewish | T | Perceptions of the other; aesthetics, ethics and prejudice | Jewish identity in contemporary art |
| 21 | 5806266144333063193 | jewish | T | Jewish Paths into Architecture. German-Jewish Architects in the First  | German-Jewish architects |
| 22 | 4445712722494799695 | the news (typo of the jews) | F | Modelling the pathways of Health and Social Care following on from the | 'the news' (typo variant) |
| 23 | 5071393675880763936 | Jews; jewish | T | The Im/Possibilities of Israeli/Palestinian Artistic Collaborations In | Jewishness in Israeli/Palestinian art |
| 24 | 9032261973703383139 | jewish | T | Jewish Physicians at the Court of Saladin. | Jewish physicians at Saladin's court |
| 25 | 13485953162936401197 | Jews; the jews | T | The Jews of Ottoman Algeria: Towards a New History | Jews of Ottoman Algeria |

### Sámi people (Q48199), 590 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 16166061824318404581 | Same | F | LAunching New SILOxane Treatments: assessing effluent, sludge and air  | 'Same as before' (English word, capitalised at sentence start) |
| 2 | 3596777526591278287 | Sami | T | Processes of representation in indigenous tourism development. Cases f | Sami communities, reindeer husbandry, indigenous tourism |
| 3 | 4604113511065461402 | same people (typo of sami people) | F | The Cultural History of AIDS in Denmark | 'the same people' (typo variant of 'sami people') |
| 4 | 10916075452337436248 | Same | F | Minority Stress and Mental Health among Same-Sex Couples | 'Same-Sex Couples' |
| 5 | 1190398863302208389 | Same | F | Under The Same Sky | 'Under The Same Sky' |
| 6 | 3943184456985055816 | Sami | T | Integration of Indigenous Knowledge Systems into Environmental Decisio | Sami reindeer herding community, indigenous knowledge |
| 7 | 14582198639145212869 | Same | F | Accuracy of Same-sex Couples Data in the American Community Survey | 'Same-sex' |
| 8 | 11307651213380302889 | Sami; sapmi | T | Cross-border Reindeer Husbandry During World War II: Continuities, Lim | Sapmi / Sami reindeer husbandry in WWII |
| 9 | 16166272729814892612 | Same | F | Integrating Non-Abelianity in Euclidean lattices: From Cayley lattices | 'Same' English word (lattice physics) |
| 10 | 16523465022615141639 | Same | F | Our Emotions Are the Same: Inclusion Through Art | 'Our Emotions Are the Same' |
| 11 | 2864102391927743490 | same people (typo of sami people) | F | High Current Switch | 'the same people' typo variant |
| 12 | 357939042766498940 | Sami | T | Revitalization against all odds? The South Sámi Language in Sweden | South Sami language revitalisation |
| 13 | 330485560468865317 | Same | F | Under The Same Sky | 'Under The Same Sky' |
| 14 | 12065993892753670098 | Same | F | Mechanisms of Same/Different Concept Learning by Pigeons | 'Same/Different' concept learning |
| 15 | 8177478729367225492 | Sami | U | Common Ground | Sami named once among minorities in Finland in a student-exchange project |
| 16 | 16887359813527886833 | Same | F | Proximal Effects of Alcohol on Same-Sex Intimate Partner Violence | 'Same-Sex' |
| 17 | 5619390164435669504 | Sami; sapmi | U | Savage Explorations: Carl Linnaeus and Eighteenth-Century Primitivism. | Sapmi named as a region in a Linnaeus / primitivism book project; Sami peripheral |
| 18 | 9914023787031788498 | same people (typo of sami people) | F | MUSIC FOR PEACE | 'the same people' typo variant |
| 19 | 14230606664905132567 | Sami | T | Visiting Professorship for Bénédicte Savoy, Department of Culture and  | Sami heritage, restitution from museums |
| 20 | 14797478646576207927 | Sami | T | Sami and Swedes: A Documentary Film about Social Straddlers in Norther | Sami and Swedes documentary film |
| 21 | 830594951807936905 | Sami | T | Arctic origins - archaeology and the search for the origins of the nor | Sami among northern peoples whose origins are studied |
| 22 | 5989124728475648191 | Sami; sapmi | T | Sámi Customary Rights in Modern Landscapes - Indigenous People and Nat | Sami customary rights and nature conservation |
| 23 | 15393536806165051427 | Sami | T | Climate, habitat and animal production - Reinforcing a northern pastor | Sami reindeer herding system |
| 24 | 5336353427289257103 | same people (typo of sami people) | F | An Error Theory about all Normative Judgements | 'these same people' typo variant |
| 25 | 15437641499256442376 | Sami | T | Cultures of Everyday Preparedness in Sensitive Arctic Borderlands | North Sami terms, borderland communities in Lapland/Finnmark |

### Dom people (Q2656122), 218 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 15338156726699597715 | do people (typo of dom people) | F | Pathways to understanding the changing climate: time and place in cult | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 2 | 6695443523669103932 | do people (typo of dom people) | F | Anthropology of Ebola: Transmission Dynamics and Outbreak Socialities | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 3 | 8688868534485909154 | do people (typo of dom people) | F | Computational development economics Applying ML to the multiple avoida | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 4 | 12012195745156445489 | do people (typo of dom people) | F | Grow or mow? Managing urban grasslands for pollinator conservation and | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 5 | 8359135868598829708 | do people (typo of dom people) | F | Adult learning, what do people with intellectual disabilities say? | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 6 | 13339204655469106954 | do people (typo of dom people) | F | The demise of the informal city? Economic growth and street work in ur | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 7 | 5514864464321334332 | do people (typo of dom people) | F | Phantom trust: Faith, language, and digital inequalities in Southwest  | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 8 | 10071777058278054173 | do people (typo of dom people) | F | Digital Wildfire: (Mis)information flows, propagation and responsible  | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 9 | 13351386074629220093 | do people (typo of dom people) | F | Local concerns with the global climate: An exploration of community-le | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 10 | 1527240818370968869 | do people (typo of dom people) | F | Civic and Cultural Identities in a Changing World. Analyzing the mortu | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 11 | 11911808612687635723 | do people (typo of dom people) | F | Why do people from different cultures think differently? Explaining cu | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 12 | 14633575704268763816 | do people (typo of dom people) | F | A novel sensory nerve stimulator to improve neuropathy in patients wit | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 13 | 10088598489323978496 | do people (typo of dom people) | F | Breaking Bad … pathways: Redirection of compensatory motivation in pro | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 14 | 7348527117481502127 | do people (typo of dom people) | F | In dialogue with users, the importance of opera for society. | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 15 | 11944624235832133455 | do people (typo of dom people) | F | Collecting, Debating and Contesting Data through Visualization | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 16 | 8081948676545778636 | do people (typo of dom people) | F | Regional variation and the commodification of language in southwest Fr | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 17 | 3547440966040343164 | do people (typo of dom people) | F | An ethnographic exploration of community engagement with proposals for | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 18 | 10836495283923918142 | do people (typo of dom people) | F | How do people make uncertain predictions? Exemplar-based and category- | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 19 | 8560909066446279641 | do people (typo of dom people) | F | Decolonising the Museum: Digital Repatriation of the Gaidinliu Collect | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 20 | 14130291169268496984 | do people (typo of dom people) | F | Discovery Projects - Grant ID: DP150104206 | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 21 | 8888967828162968630 | do people (typo of dom people) | F | Decision making in Web searching: what do searchers look at and why? | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 22 | 9117734203320911637 | do people (typo of dom people) | F | A Legal Anthropology of Personality Disorder | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 23 | 13227503015442247175 | do people (typo of dom people) | F | The Sociality of Tax: A Multiperspective Study of Fiscal Relations (So | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 24 | 13593216329830972027 | do people (typo of dom people) | F | EU'R'AQUATIC | 'do people' matched as a 1-edit typo variant of 'dom people' |
| 25 | 2176373208907140122 | do people (typo of dom people) | F | 'Interfaith connections at home: domestic space, practice and dialogue | 'do people' matched as a 1-edit typo variant of 'dom people' |

### Silesians (Q140472), 200 projects

| # | project id | matched text | verdict | title | reason |
|---|---|---|---|---|---|
| 1 | 7016631629840751871 | silesian (typo of silesians) | F | Attitudes of the population of the Moravian-Silesian Region to on the  | Moravian-Silesian Region (Czech administrative region) |
| 2 | 12308085247514692691 | silesian (typo of silesians) | F | Regional contact organization for 7th framework programme in the Morav | Moravian-Silesian region |
| 3 | 17435340100686137368 | silesian (typo of silesians) | F | Geotechnical evaluation of nappes of the outer West Carpathian (North  | Moravian-Silesian Beskydy Mts (geology) |
| 4 | 16227186295569963618 | silesian (typo of silesians) | F | Towards integrated stratigraphy of the Late Paleozoic in eastern equat | Upper Silesian Basin (geology) |
| 5 | 13123511858592130191 | silesian (typo of silesians) | F | Training on Plastic Mould Making | Moravian-Silesian Automotive Cluster |
| 6 | 8799215797117378488 | silesian (typo of silesians) | T | The role of the parents in the protection of a regional dialect - the  | Silesian regional dialect (Bytom) and its protection as cultural heritage |
| 7 | 11792002169986189294 | silesian (typo of silesians) | F | Comparison of the calciturbidite sedimentation in the Moravian Karst , | Moravian-Silesian Paleozoic (geology) |
| 8 | 12822339098975822241 | silesian (typo of silesians) | F | Creation of the database and the data evaluation of the botanic collec | Silesian Museum in Opava (institution, botany) |
| 9 | 14726117664596706405 | silesian (typo of silesians) | F | Petrogenesis and emplacement of deep-marine alkaline basaltoids: a cas | Silesian nappes (geology) |
| 10 | 7674186916839576772 | silesian (typo of silesians) | F | Historical Encyclopedia of the Czech, Moravian and Silesian Enterprise | Czech, Moravian and Silesian enterprisers (historical region) |
| 11 | 937000346605829848 | silesian (typo of silesians) | F | Resilience of Smart Cities and Villages of the Moravian-Silesian Regio | Moravian-Silesian Region |
| 12 | 3489908228454660521 | silesian (typo of silesians); silesians | U | """Entrepreneurship and language matters in life. Developing resourcef | "Silesians" as people mentioned in a Polish school project in the Silesian Voivodeship; marginal |
| 13 | 4488935760918671185 | silesian (typo of silesians) | U | Religious songs in the literature of Silesian emigrants in the 17th an | Silesian emigrants of the 17th-18th c. (historical region, not the modern ethnic group) |
| 14 | 15571044776732230540 | silesian (typo of silesians) | F | Problems and impacts of the use of solid alternative fuels | Moravian-Silesian Region |
| 15 | 1514845022955836274 | silesian (typo of silesians) | F | New Challenges in the Social Services Market for Seniors in the Moravi | Moravian-Silesian Region |
| 16 | 11729441810723362500 | silesian (typo of silesians) | U | The Silesian society in period later baroque and entrance enlightenmen | Silesian society in the Baroque (historical region, Czech Silesia) |
| 17 | 10315576554762675014 | silesian (typo of silesians) | F | Development of innovative training solutions in the field of functiona | Silesian University of Technology (institution) |
| 18 | 14393669740079132225 | silesian (typo of silesians) | F | Myrmecological collections in Czech, Moravian and Silesian museums ? t | Czech, Moravian and Silesian museums |
| 19 | 3624075490498158251 | silesian (typo of silesians) | F | Constructivism in the teaching of mathematics - open educational resou | Silesian Metropolitan Network (organisation) |
| 20 | 6662750568871915489 | silesian (typo of silesians) | F | Woman in the Silesian burgher society of the late-Baroque period | Silesian burgher society, Baroque (historical region) |
| 21 | 11361989620864760203 | silesian (typo of silesians) | F | Computer compilation of the Silesian collection in the library of the  | Silesian Museum / Silesian collection |
| 22 | 2749298667637011069 | silesian (typo of silesians) | F | Boosting the scientific excellence and innovation capacity in organic  | Silesian University of Technology |
| 23 | 3514874778465912488 | silesian (typo of silesians) | F | Stability and alteration mechanisms of monazite as a function of tempe | Moravo-Silesian Culm Basin (geology) |
| 24 | 3030434602646776723 | silesian (typo of silesians) | F | Digital platform supporting remote laboratory classes in electrical en | Silesian University of Technology |
| 25 | 6839752712876462373 | silesian (typo of silesians) | F | Methane and carbon dioxide sorption on coals: Effect of pressure, temp | Upper Silesian Basin (geology) |