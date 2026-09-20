# D27 text cleaning: before / after on the real cluster dry-run data

Rows = values with the pattern. `&amp;` = contains `&amp;`; `&ent;` = any `&[a-zA-Z#0-9]+;`; `<tag` = `<[a-zA-Z/]`; `&lt;..` = `&lt;`, `&gt;` or `&quot;`.

| field | rows | `&amp;` before | after | `&ent;` before | after | `<tag` before | after | `&lt;..` before | after | changed rows | s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| works.title | 6,001,159 | 20,924 | 1 | 32,627 | 15 | 248,020 | 404 | 9,311 | 0 | 279,330 | 6.3 |
| works.publisher | 5,387,214 | 570 | 0 | 1,367 | 0 | 3 | 1 | 755 | 0 | 1,369 | 0.4 |
| works.container_name | 4,805,136 | 234,014 | 0 | 234,112 | 0 | 22 | 25 | 32 | 0 | 238,178 | 0.9 |
| works.authors[] | 32,408,557 | 1,664 | 0 | 2,479 | 5 | 41 | 41 | 4 | 0 | 13,379 | 6.0 |
| projects.title | 3,893,065 | 6,437 | 0 | 8,670 | 35 | 139 | 53 | 1,374 | 0 | 9,271 | 2.2 |
| projects.summary | 533,148 | 17,622 | 0 | 68,183 | 68 | 1,833 | 725 | 42,589 | 0 | 203,779 | 23.5 |
| projects.keywords | 507,115 | 7,755 | 0 | 8,067 | 0 | 0 | 0 | 62 | 0 | 8,067 | 0.4 |
| projects.acronym | 120,629 | 6 | 0 | 7 | 0 | 1 | 1 | 0 | 0 | 7 | 0.1 |
| projects.subjects[] | 414,756 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.8 |
| organisations.legalName | 494,099 | 2,161 | 0 | 2,186 | 0 | 2 | 3 | 26 | 0 | 2,186 | 0.5 |
| organisations.legalShortName | 475,410 | 2,109 | 0 | 2,116 | 0 | 2 | 3 | 7 | 0 | 2,116 | 0.3 |
| organisations.alternativeNames[] | 272,732 | 165 | 0 | 165 | 0 | 0 | 0 | 0 | 0 | 165 | 0.5 |
| organisations.address_street | 69,450 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.1 |
| organisations.address_city | 69,754 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.0 |
| grants.description | 6,074 | 201 | 0 | 203 | 0 | 0 | 0 | 2 | 0 | 206 | 0.0 |
| grants.id | 6,074 | 156 | 0 | 158 | 0 | 0 | 0 | 2 | 0 | 158 | 0.0 |

## Residual patterns after cleaning (498,240 values still contain `&` or `<`; plain `&` / `<` in text are fine)

### Top 20 residual entity-like patterns (`&[a-zA-Z#0-9]+;`)

| pattern | rows | example field |
|---|---|---|
| `&D;` | 46 | projects.title |
| `&I;` | 16 | projects.summary |
| `&955;` | 6 | projects.title |
| `&COGNITION;` | 4 | projects.title |
| `&Va00;` | 4 | projects.summary |
| `&A;` | 4 | projects.summary |
| `&E;` | 3 | works.title |
| `&IMMUNITY;` | 3 | projects.title |
| `&RETINA;` | 3 | projects.title |
| `&ELDERLY;` | 3 | projects.title |
| `&bgr;` | 2 | works.title |
| `&Ch05;` | 2 | projects.summary |
| `&al2022;` | 2 | projects.summary |
| `&empowerment;` | 2 | projects.summary |
| `&al2019;` | 2 | projects.summary |
| `&945;` | 2 | projects.title |
| `&8217;` | 2 | works.title |
| `&T;` | 2 | projects.summary |
| `&NA;` | 2 | works.authors[] |
| `&#;` | 2 | works.authors[] |

### Top 20 residual tag-like patterns (`<[a-zA-Z/][^>]{0,25}>?`)

| pattern | rows | example field |
|---|---|---|
| `<Background >` | 440 | projects.summary |
| `<This text has been machine` | 279 | projects.summary |
| `<z` | 140 | projects.title |
| `<Objectives>` | 89 | projects.summary |
| `<Objectives >` | 72 | projects.summary |
| `<Background>` | 29 | projects.summary |
| `<p` | 28 | works.title |
| `<x` | 27 | works.title |
| `<sup>` | 25 | works.authors[] |
| `<it>` | 20 | works.authors[] |
| `<Implementation >` | 17 | projects.summary |
| `</it>` | 17 | works.authors[] |
| `<IMPACT>` | 15 | works.title |
| `</sup>` | 15 | works.authors[] |
| `<Results >` | 14 | projects.summary |
| `<alpha>` | 11 | projects.title |
| `</msg>` | 11 | works.container_name |
| `<Goals >` | 9 | projects.summary |
| `</xhtml:span>` | 7 | works.title |
| `<xhtml:span xmlns:xhtml="ht` | 7 | works.title |

## 15 before / after examples

| field | before | after |
|---|---|---|
| works.title | `PENGEMBANGAN MODEL OPTIMASI DISTRIBUSI LIMBAH DENGAN LINEAR PROGRAMMING: CONTOH KASUS STATELINE SHIPPING &amp;amp; TRANSPORT COMPANY` | `PENGEMBANGAN MODEL OPTIMASI DISTRIBUSI LIMBAH DENGAN LINEAR PROGRAMMING: CONTOH KASUS STATELINE SHIPPING & TRANSPORT COMPANY` |
| works.title | `Stable isotope analysis of atmospheric CO <sub>2</sub> using a Gasbench II‐Cold Trap‐IRMS setting` | `Stable isotope analysis of atmospheric CO 2 using a Gasbench II‐Cold Trap‐IRMS setting` |
| works.title | `The lymphoid lineage?&#8364;&#8220;specific actin-uncapping protein Rltpr is essential for costimulation via CD28 and the development of regulatory T cells` | `The lymphoid lineage?€“specific actin-uncapping protein Rltpr is essential for costimulation via CD28 and the development of regulatory T cells` |
| works.title | `Improved thermonuclear rate of<sup>42</sup>Ti(<i>p</i>,<i>γ</i>)<sup>43</sup>V and its astrophysical implication in the<i>rp</i>process` | `Improved thermonuclear rate of42Ti(p,γ)43V and its astrophysical implication in therpprocess` |
| works.container_name | `Agriculture, Ecosystems &amp; Environment` | `Agriculture, Ecosystems & Environment` |
| works.publisher | `Lippincott Williams &amp; Wilkins Ltd.` | `Lippincott Williams & Wilkins Ltd.` |
| works.authors[] | `Palle Rasmussen &amp;` | `Palle Rasmussen &` |
| projects.title | `Identification of ligands for &#947;&#948; T cell receptors: a genomics/ transcriptomics approach.` | `Identification of ligands for γδ T cell receptors: a genomics/ transcriptomics approach.` |
| projects.summary | `Purpose and goal:
<p>The project aims to commercialize a biosensor platform for use as a point-of-care tool for the healthcare system.</p>
Expected results and effects:
Approach and implementation:` | `Purpose and goal: The project aims to commercialize a biosensor platform for use as a point-of-care tool for the healthcare system. Expected results and effects: Approach and implementation:` |
| projects.summary | `The aim of this project is to investigate the role of selectins and their ligands in allograft rejection of parenchymatous organ in a model of &quot;gene-knock-out&quot; mouse.` | `The aim of this project is to investigate the role of selectins and their ligands in allograft rejection of parenchymatous organ in a model of "gene-knock-out" mouse.` |
| organisations.legalName | `Plant Systems Biology, Bioinformatics &amp; Evolutionary Genomics` | `Plant Systems Biology, Bioinformatics & Evolutionary Genomics` |
| grants.id | `NWO::NWA L2 - Thema 2018 - Ecologie &amp; Noordzee - Call NWA Ecologie &amp; Noordzee` | `NWO::NWA L2 - Thema 2018 - Ecologie & Noordzee - Call NWA Ecologie & Noordzee` |
| works.title | `Intravital imaging of &lt;i&gt;in vivo&lt;/i&gt; pharmacological actions of molecular targeted drugs` | `Intravital imaging of in vivo pharmacological actions of molecular targeted drugs` |
| works.title | `Buffer Standards for Physiological pH of the Buffer N-(2-Acetamido)-2-aminoethanesulfonic Acid from 5&amp;#176;C to 55&amp;#176;C` | `Buffer Standards for Physiological pH of the Buffer N-(2-Acetamido)-2-aminoethanesulfonic Acid from 5°C to 55°C` |

## Publisher variants: 65,918 distinct raw values -> 65,873 after cleaning (in the works dry-run sample; variants such as `A &amp; B` and `A & B` merge).
