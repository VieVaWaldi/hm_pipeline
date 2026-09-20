# minorities - Last updated: 19-09-2026

- **Path:** `/work/lu72hip/data/duckdb/sources/minorities_raw.duckdb`
- **Size:** 0.5 MB
- **Tables:** 1

## TL;DR

| Table | Rows | Columns |
|---|---|---|
| minorities_raw | 278 | 16 |

## **minorities_raw** - 278 rows

| Column | Type | NotNull | Sample |
|---|---|---|---|
| qid | VARCHAR | 278 (100%) | Q7325 |
| merged_qids | VARCHAR[] | 278 (100%) | ["Q7325"] |
| group_name_en | VARCHAR | 278 (100%) | Jewish people |
| countries | VARCHAR[] | 278 (100%) | ["(global \u2014 see subgroups in dataset)"] |
| source_class | VARCHAR[] | 278 (100%) | ["manual_seed"] |
| population | DOUBLE | 77 (28%) | 14606000.0 |
| religions | VARCHAR[] | 82 (29%) | ["Judaism"] |
| native_languages | VARCHAR[] | 66 (24%) | ["Abaza"] |
| part_of | VARCHAR[] | 78 (28%) | ["Georgians"] |
| subclass_of | VARCHAR[] | 96 (35%) | ["Semitic people"] |
| diaspora | VARCHAR[] | 0 (0%) |  |
| ancestral_home | VARCHAR[] | 15 (5%) | ["Ladinia"] |
| admin_territory | VARCHAR[] | 21 (8%) | ["Kabardino-Balkaria", "Karachay-Cherkessia", "Moscow", "Stavropol Krai"] |
| has_parts | VARCHAR[] | 20 (7%) | ["Ashkenazi Jews", "Iranian", "Musta'arabi Jews", "Romaniote Jews", "Sephardi Jews", "Yemenite Jews"] |
| known_subgroups | STRUCT("name" VARCHAR, qid VARCHAR)[] | 12 (4%) | [{"name": "Ashkenazi Jews", "qid": "Q34069"}, {"name": "Sephardi Jews", "qid": "Q102251"}] |
| search_keywords | VARCHAR[] | 278 (100%) | ["Hebrajczyk", "Jew", "Jewish", "Jewish community", "Jewish nation", "Jewish people", "Jewish person", "Jewry", "Jews", "Jud", "Judinja", "The Jewish people", "The Jews", "evreic\u0103", "evreu", "heb… |

