"""
Reproducible synthetic OpenAire staging-v4 fixture for local end-to-end runs of core_v4.

There is no representative local sample of `openaire_staging_v4.duckdb` (the 1000-row
`openaire_staging_2.duckdb` has no doi/countries columns and its relations do not line up), so this builds
one. Output (git-ignored, under data/duckdb/sample/):

    openaire_staging_v4_sample.duckdb   tables organization, project, work, relation with EXACTLY the
                                        staging-v4 schema of src/sources/dumps/openaire/staging.py --target v4
    sample_truth.duckdb                 what was planted where (minority qids, pillar bitmask, org variants,
                                        DOI-only projects, ...), so a run can be checked against it

Everything is derived from the seed and the local reference dbs (ROR, Cordis heritage subset, minorities,
core_v2 geolocations), so the same seed gives the same fixture.

Planted so every stage has something to do:
  * organizations: real ROR ids (ROR tier), PICs of Cordis institutions (PIC match), Cordis names without a
    PIC / with a wrong PIC (name + country fallback) / without a country (must NOT match), PICs of Cordis
    institutions whose project is not in the sample (org-level pass 2), core_v2 names (legacy tier), bare orgs;
  * projects: grantIds equal to Cordis id_original, DOI-only matches (doi in mixed formats), others;
    project -produces-> works, project -hasParticipant-> orgs, some relations to projects/orgs that do not exist;
  * works: tier 0 (project-produced), tier 1 (author institutions only), unlinked (dropped); dates from
    1990 to today, ~1.5% in the future (incl. 2999), ~3% NULL; ~2% without title, ~25% without description;
    English plus some German/French;
  * texts with minority keywords (minorities_raw), pillar stems (inclusi*, sustainab*, resilien*, innovat*,
    global*) and heritage / tourism / economy vocabulary so topics and themes come out non-empty.

Usage:
    uv run python -m pipelines.core_v4.build_sample_fixture [--projects 4000] [--orgs 7500] [--works 80000]
                                                            [--seed 42] [--out-dir data/duckdb/sample]
"""

import argparse
import datetime as dt
import hashlib
import logging
import random
import re
from pathlib import Path

import duckdb
import pyarrow as pa

from common.config.dumps import get_dumps_paths
from common.config.source_paths import resolve_data_path

CORDIS_HERITAGE_DB = "data/duckdb/sources/cordis_heritage_subset_with_pdfs_raw.duckdb"
CORE_V2_GEO_DB = "data/pile/core_v2_geolocation/core_v2_geolocations.duckdb"
CORE_V2_PIC_DB = "data/pile/core_v2_geolocation/core_v2_institution_pic.duckdb"

# ---------------------------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------------------------
# (phrase to plant, qid it must produce). Checked against enrichment.minority_matching.matcher.
MINORITY_PHRASES = [
    ("the Ladin communities of the Dolomites", "Q1799968"),
    ("Sami reindeer herding and joik traditions", "Q48199"),
    ("Abkhazians living in exile", "Q171795"),
    ("the Adjarians of Georgia", "Q26291"),
    ("Jewish heritage and Yiddish theatre", "Q7325"),
]
# pillar stems: the pillars enrichment matches whole words STARTING with these (case-insensitive), bit order low -> high
PILLAR_STEMS = ["inclusi", "sustainab", "resilien", "innovat", "global"]
PILLAR_SENTENCES = [
    "The approach is designed to be inclusive of all communities.",
    "It promotes sustainable use of local resources.",
    "The results show how resilient regions respond to shocks.",
    "A novel and innovative method is proposed.",
    "The findings are relevant for the global context.",
]

HERITAGE = [
    "digital preservation of cultural heritage collections",
    "museum collections, archives and library catalogues",
    "archaeological excavation, conservation and restoration of historic buildings",
    "intangible cultural heritage, oral traditions and folklore",
    "digitisation and 3D scanning of monuments and artefacts",
    "the history of medieval manuscripts and archival records",
]
TOURISM = [
    "cultural tourism and visitor management in heritage destinations",
    "hospitality, hotels and sustainable travel behaviour of tourists",
    "leisure, recreation and tourism marketing in coastal regions",
    "wine tourism, culinary culture and rural tourism",
    "tourism destinations, visitor experience and hospitality education",
]
ECONOMY = [
    "regional economic development, employment and labour markets",
    "monetary policy, inflation and financial markets",
    "economic growth, international trade and productivity",
    "local economic development and planning in post-communist economies",
    "agricultural economics, farm income and rural policy",
]
BACKGROUND = [
    "deep learning for medical image classification",
    "clinical trial of a new cancer therapy",
    "quantum materials and superconducting devices",
    "soil microbiome and plant growth in arable land",
    "protein structure prediction and drug design",
    "wireless sensor networks and edge computing",
    "gravitational wave detection with laser interferometers",
    "climate models and ocean circulation",
]
CONNECTORS = [
    "This study investigates", "We present results on", "The paper examines", "The project develops methods for",
    "A case study of", "The article discusses", "New evidence is presented on", "Researchers analyse",
]
GERMAN = [
    "Die Studie untersucht das kulturelle Erbe und die Museen der Region.",
    "Ein Projekt zur Digitalisierung von Archiven und historischen Dokumenten.",
    "Tourismus und Gastgewerbe im ländlichen Raum.",
    "Wirtschaftliche Entwicklung und Arbeitsmarkt in Europa.",
]
FRENCH = [
    "Cette étude porte sur le patrimoine culturel et les musées de la région.",
    "Un projet de numérisation des archives et des documents historiques.",
    "Le tourisme et l'hôtellerie en zone rurale.",
    "Développement économique et marché du travail en Europe.",
]
JOURNALS = [
    "Journal of Cultural Heritage", "Tourism Management", "Economic Policy Review", "Museum Studies Quarterly",
    "Archaeological Science Reports", "Digital Humanities Journal", "Annals of Regional Science", "Kulturjournal",
]
KEYWORDS = ["heritage", "museum", "tourism", "economy", "archive", "conservation", "digital", "regional", "culture", "policy"]
WORK_COUNTRIES = ["DE", "UK", "EL", "FR", "IT", "ES", "NL", "SE", "PL", "EU", "ZZ", "XK", "US", "YU", "FI", "AT", "de", "gb"]
LANGS = {"en": ("eng", "English"), "de": ("deu/ger", "German"), "fr": ("fra/fre", "French")}
FUNDER_STREAMS = ["EC::H2020::RIA", "EC::HE::HORIZON-RIA", "EC::FP7::CP", "EC::H2020::MSCA-ITN"]

TODAY = dt.date.today()


def _rand_date(rng: random.Random) -> dt.date | None:
    r = rng.random()
    if r < 0.03:
        return None
    if r < 0.045:  # in the future, some absurd
        return dt.date(2999, 1, 1) if rng.random() < 0.3 else dt.date(rng.randint(TODAY.year + 1, TODAY.year + 6), rng.randint(1, 12), rng.randint(1, 28))
    year = int(1990 + (TODAY.year - 1990) * (rng.random() ** 0.55))  # skewed towards recent years
    day = dt.date(year, rng.randint(1, 12), rng.randint(1, 28))
    return day if day <= TODAY else dt.date(TODAY.year, 1, 1)


def _sentence(rng: random.Random, pool: list[str]) -> str:
    return f"{rng.choice(CONNECTORS)} {rng.choice(pool)}."


def _body(rng: random.Random, lang: str, n_sentences: int) -> str:
    """Body text from one dominant theme (heritage / tourism / economy / background) plus a second one."""
    if lang == "de":
        return " ".join(rng.sample(GERMAN, min(n_sentences, len(GERMAN))))
    if lang == "fr":
        return " ".join(rng.sample(FRENCH, min(n_sentences, len(FRENCH))))
    pools = [HERITAGE, TOURISM, ECONOMY, BACKGROUND]
    main = rng.choices(pools, weights=[3, 3, 3, 3])[0]
    out = [_sentence(rng, main) for _ in range(max(1, n_sentences - 1))]
    if n_sentences > 1:
        out.append(_sentence(rng, rng.choice(pools)))
    return " ".join(out)


def _plant(rng: random.Random, text: str, p_minority: float, p_pillar: float) -> tuple[str, list[str]]:
    """Appends planted sentences; returns (text, qids planted)."""
    qids = []
    if rng.random() < p_minority:
        for phrase, qid in rng.sample(MINORITY_PHRASES, rng.choice([1, 1, 1, 2])):
            text += f" The work also covers {phrase}."
            qids.append(qid)
    for sentence in PILLAR_SENTENCES:
        if rng.random() < p_pillar:
            text += " " + sentence
    return text, sorted(set(qids))


def _pillar_mask(*texts: str | None) -> int:
    blob = " ".join(t for t in texts if t)
    mask = 0
    for bit, stem in enumerate(PILLAR_STEMS):
        if re.search(rf"\b{stem}\w*", blob, flags=re.IGNORECASE):
            mask |= 1 << bit
    return mask


def _h(*parts) -> int:
    return int(hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def _oid(kind: str, key: str) -> str:
    """OpenAire-looking id (hash of it becomes the staging id)."""
    prefix = {"org": "20|openorgs____::", "project": "40|corda__h2020::", "work": "50|doi_dedup___::"}[kind]
    return prefix + hashlib.md5(f"{kind}:{key}".encode()).hexdigest()


# ---------------------------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------------------------
def _load_reference():
    ror = duckdb.connect(get_dumps_paths()["ror_dump"]["path_duck"], read_only=True)
    ror_rows = ror.execute(
        """SELECT id, list_filter(names, x -> list_contains(x.types, 'ror_display'))[1].value AS name,
                  locations[1].geonames_details.country_code AS cc,
                  locations[1].geonames_details.lat IS NOT NULL AS has_geo
           FROM organizations WHERE locations[1].geonames_details.country_code IS NOT NULL ORDER BY id"""
    ).fetchall()
    ror.close()
    cordis = duckdb.connect(resolve_data_path(CORDIS_HERITAGE_DB), read_only=True)
    projects = cordis.execute(
        "SELECT id, id_original, doi, title, objective FROM project WHERE title IS NOT NULL ORDER BY id"
    ).fetchall()
    trip = cordis.execute(
        """SELECT j.project_id, i.id, nullif(trim(j.organization_id), '') AS pic, i.legal_name, i.country
           FROM j_project_institution j JOIN institution i ON i.id = j.institution_id
           WHERE i.legal_name IS NOT NULL AND i.legal_name <> 'N/A' ORDER BY j.project_id, i.id"""
    ).fetchall()
    cordis.close()
    v2 = duckdb.connect(resolve_data_path(CORE_V2_GEO_DB), read_only=True)
    v2.execute(f"ATTACH '{resolve_data_path(CORE_V2_PIC_DB)}' AS pc (READ_ONLY)")
    # usable PIC rows (standard, unambiguous) vs institutions that have none (those get the name + country fallback)
    v2_pic_rows = v2.execute(
        """SELECT i.legal_name, i.country, p.pic FROM institution i
           JOIN pc.institution_pic p ON p.institution_id = i.id
           WHERE p.pic_is_standard AND NOT p.pic_has_multiple_institutions AND i.country IS NOT NULL ORDER BY i.id"""
    ).fetchall()
    v2_name_rows = v2.execute(
        """SELECT legal_name, country FROM institution
           WHERE legal_name IS NOT NULL AND country IS NOT NULL
             AND id NOT IN (SELECT institution_id FROM pc.institution_pic WHERE pic_is_standard AND NOT pic_has_multiple_institutions)
           ORDER BY id"""
    ).fetchall()
    v2.close()
    return ror_rows, projects, trip, (v2_name_rows, v2_pic_rows)


# ---------------------------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------------------------
STAGING_DDL = """
CREATE TABLE organization (
    id UBIGINT, openaireId VARCHAR, legalName VARCHAR, legalShortName VARCHAR, websiteUrl VARCHAR,
    alternativeNames VARCHAR[], countryCode VARCHAR, rorId VARCHAR, wikiId VARCHAR,
    pids STRUCT(scheme VARCHAR, "value" VARCHAR)[]);
CREATE TABLE project (
    id UBIGINT, openaireId VARCHAR, grantId VARCHAR, title VARCHAR, acronym VARCHAR, websiteUrl VARCHAR,
    startDate DATE, endDate DATE, callIdentifier VARCHAR, keywords VARCHAR,
    openAccessMandateForPublications BOOLEAN, openAccessMandateForDataset BOOLEAN, subjects VARCHAR[],
    fundings STRUCT(fundingStream STRUCT(description VARCHAR, id VARCHAR), jurisdiction VARCHAR, "name" VARCHAR, shortName VARCHAR)[],
    frameworkProgrammes VARCHAR[], summary VARCHAR, doi VARCHAR,
    granted STRUCT(currency VARCHAR, fundedAmount DOUBLE, totalCost DOUBLE));
CREATE TABLE work (
    id UBIGINT, openaireId VARCHAR, title VARCHAR, publicationDate DATE, publisher VARCHAR, openAccessColor VARCHAR,
    isGreen BOOLEAN, isInDiamondJournal BOOLEAN, publiclyFunded BOOLEAN,
    language STRUCT(code VARCHAR, "label" VARCHAR),
    bestAccessRight STRUCT(code VARCHAR, "label" VARCHAR, scheme VARCHAR),
    authors STRUCT(fullName VARCHAR, "name" VARCHAR,
                   pid STRUCT(id STRUCT(scheme VARCHAR, "value" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR)),
                   rank BIGINT, surname VARCHAR)[],
    subjects STRUCT(provenance STRUCT(provenance VARCHAR, trust VARCHAR), subject STRUCT(scheme VARCHAR, "value" VARCHAR))[],
    descriptions VARCHAR[], pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], sources VARCHAR[], formats VARCHAR[],
    instances STRUCT(accessRight STRUCT(code VARCHAR, "label" VARCHAR, openAccessRoute VARCHAR, scheme VARCHAR),
                     alternateIdentifiers STRUCT(scheme VARCHAR, "value" VARCHAR)[],
                     articleProcessingCharge STRUCT(amount VARCHAR, currency VARCHAR), license VARCHAR,
                     pids STRUCT(scheme VARCHAR, "value" VARCHAR)[], publicationDate DATE, refereed VARCHAR,
                     "type" VARCHAR, urls VARCHAR[])[],
    citationCount DOUBLE, influence DOUBLE, views BIGINT, countries VARCHAR[],
    container STRUCT(edition JSON, ep VARCHAR, iss VARCHAR, issnLinking VARCHAR, issnOnline VARCHAR, issnPrinted VARCHAR,
                     "name" VARCHAR, sp VARCHAR, vol VARCHAR));
CREATE TABLE relation (
    source UBIGINT, sourceType VARCHAR, target UBIGINT, targetType VARCHAR,
    relType STRUCT("name" VARCHAR, "type" VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR), validated BOOLEAN);
"""


def build(n_projects: int, n_orgs: int, n_works: int, seed: int, out_dir: Path) -> tuple[Path, Path]:
    rng = random.Random(seed)
    ror_rows, cordis_projects, trip, (v2_name_rows, v2_pic_rows) = _load_reference()
    out_dir.mkdir(parents=True, exist_ok=True)
    out, truth_out = out_dir / "openaire_staging_v4_sample.duckdb", out_dir / "sample_truth.duckdb"
    for p in (out, truth_out):
        for f in (p, p.with_name(p.name + ".wal")):
            f.unlink(missing_ok=True)

    trip_by_project: dict[int, list] = {}
    for pid, inst, pic, name, cc in trip:
        trip_by_project.setdefault(pid, []).append((inst, pic, name, cc))
    cordis_with_inst = [p for p in cordis_projects if p[0] in trip_by_project]
    rng.shuffle(cordis_with_inst)

    # ------------------------------------------------------------------ organizations
    orgs: dict[str, dict] = {}  # key -> org
    ror_pool = ror_rows[:]
    rng.shuffle(ror_pool)
    ror_iter = iter(ror_pool)

    def new_org(key, name, cc, variant, pic=None, ror=None, extra_pids=()):
        pids = []
        if ror:
            pids.append(("ROR", ror))
        if pic:
            pids.append(("PIC", pic))
        pids.extend(extra_pids)
        orgs[key] = dict(key=key, name=name, cc=cc, variant=variant, pic=pic, ror=ror, pids=pids)
        return key

    # A. Cordis-derived orgs for the projects that will carry a Cordis grantId
    n_cordis_projects = min(int(n_projects * 0.30), len(cordis_with_inst))
    cordis_sel = cordis_with_inst[:n_cordis_projects]
    used_pics = set()
    project_orgs: dict[int, list[str]] = {}  # cordis project id -> org keys (hasParticipant)
    for cp in cordis_sel:
        keys = []
        for inst, pic, name, cc in trip_by_project[cp[0]]:
            key = f"org_pic_{pic}" if pic else f"org_inst_{inst}"
            if key not in orgs:
                r = _h("variant", key) % 100
                ror = next(ror_iter)[0] if _h("ror", key) % 100 < 30 else None
                if pic and r < 62:
                    new_org(key, name, cc, "pic", pic, ror)
                    used_pics.add(pic)
                elif r < 77 or not pic:
                    new_org(key, name.title() if r % 2 else name, cc, "name_country", None, ror)
                elif r < 89:
                    new_org(key, name, cc, "wrong_pic", f"9{_h('wp', key) % 10**8:08d}", ror)
                else:
                    new_org(key, name, None, "name_no_country", None, ror)
            if rng.random() < 0.88:  # some Cordis participants are not in OpenAire's hasParticipant
                keys.append(key)
        project_orgs[cp[0]] = keys

    # B. ROR-only orgs
    n_ror = int(n_orgs * 0.32)
    for _ in range(n_ror):
        rid, name, cc, has_geo = next(ror_iter)
        new_org(f"org_ror_{rid}", name, cc if rng.random() > 0.05 else None, "ror_only", None, rid)

    # C. pass-2 orgs: PIC of Cordis institutions whose project is not in the sample
    sel_ids = {p[0] for p in cordis_sel}
    unused = [t for pid, ts in trip_by_project.items() if pid not in sel_ids for t in ts if t[1] and t[1] not in used_pics]
    rng.shuffle(unused)
    for inst, pic, name, cc in unused[: int(n_orgs * 0.08)]:
        new_org(f"org_pass2_{pic}", name, cc, "pass2_pic", pic)

    # D. core_v2 legacy orgs. Only core_v2 can place them: their names / PICs are not in the Cordis subset.
    #    name + country only works for institutions without a usable PIC row; a PIC works for any institution.
    cordis_names = {t[2].strip().lower() for ts in trip_by_project.values() for t in ts}
    cordis_pics = {t[1] for ts in trip_by_project.values() for t in ts if t[1]}
    v2_name_pool = [(n, c) for n, c in v2_name_rows if n.strip().lower() not in cordis_names]
    rng.shuffle(v2_name_pool)
    for i, (name, cc) in enumerate(v2_name_pool[: int(n_orgs * 0.04)]):
        new_org(f"org_v2n_{i}", name, cc, "core_v2_name")
    v2_pic_pool = [(n, c, p) for n, c, p in v2_pic_rows if p not in cordis_pics and n.strip().lower() not in cordis_names]
    rng.shuffle(v2_pic_pool)
    for i, (name, cc, pic) in enumerate(v2_pic_pool[: int(n_orgs * 0.04)]):
        new_org(f"org_v2p_{i}", f"{name} (renamed)", None, "core_v2_pic", pic)

    # E. bare orgs, until n_orgs
    i = 0
    while len(orgs) < n_orgs:
        cc = rng.choice(["DE", "FR", "IT", "ES", "PL", "UK", "EL", None])
        new_org(f"org_bare_{i}", f"Sample Institute {i} for {rng.choice(KEYWORDS)}", cc, "bare")
        i += 1
    org_keys = list(orgs)
    non_cordis_org_keys = [k for k in org_keys if not k.startswith(("org_pic_", "org_inst_"))]

    # ------------------------------------------------------------------ projects
    projects, relations = [], []  # relations: (src_key, src_type, tgt_key, tgt_type, relname, reltype)
    truth_projects = []
    sel_iter = iter(cordis_sel)
    doi_only = 0
    for i in range(n_projects):
        cordis = next(sel_iter, None)
        key = f"p{i}"
        if cordis:
            cid, id_original, cdoi, ctitle, objective = cordis
            grant, doi, title = id_original, None, ctitle
            summary = (objective or "")[:1500] or None
            variant = "cordis_grant"
            if cdoi and rng.random() < 0.35:  # DOI-only match: the grantId is not Cordis'
                grant = f"OA-{id_original}"
                doi = rng.choice([cdoi, cdoi.upper(), f"https://doi.org/{cdoi}"])
                variant = "cordis_doi"
                doi_only += 1
            orgs_of = project_orgs[cid]
        else:
            cid = None
            grant, doi, variant = f"XP-{i:06d}-{rng.randint(10, 99)}", None, "plain"
            lang = "en" if rng.random() < 0.92 else rng.choice(["de", "fr"])
            title = _sentence(rng, rng.choice([HERITAGE, TOURISM, ECONOMY, BACKGROUND])).rstrip(".") if lang == "en" else _body(rng, lang, 1)
            summary = _body(rng, lang, rng.randint(2, 5)) if rng.random() > 0.05 else None
            orgs_of = rng.sample(org_keys, rng.choice([0, 1, 2, 2, 3, 4, 5])) if rng.random() > 0.03 else []
        summary_new, qids = _plant(rng, summary, 0.06, 0.12) if summary else (None, [])
        acronym = f"SP{i % 977}" if rng.random() > 0.1 else None
        keywords = ", ".join(rng.sample(KEYWORDS, rng.randint(1, 3))) if rng.random() > 0.2 else None
        subjects = rng.sample(KEYWORDS, rng.randint(0, 3))
        start = dt.date(rng.randint(2002, 2024), rng.randint(1, 12), 1)
        projects.append(
            dict(key=key, grant=grant, title=title, acronym=acronym, keywords=keywords, subjects=subjects, summary=summary_new,
                 doi=doi, start=start, end=start + dt.timedelta(days=rng.randint(300, 1500)),
                 stream=rng.choice(FUNDER_STREAMS), amount=float(rng.randint(50, 5000) * 1000))
        )
        truth_projects.append((_oid("project", key), variant, qids, _pillar_mask(title, summary_new, acronym, keywords, " ".join(subjects))))
        for ok in dict.fromkeys(orgs_of):
            relations.append((key, "project", ok, "organization", "hasParticipant", "participation"))
        if rng.random() < 0.01:  # relation to an org that does not exist in organization
            relations.append((key, "project", f"ghost_org_{i}", "organization", "hasParticipant", "participation"))
    # relations of projects that are not in `project` (staging drops untitled / "unidentified" ones, relations stay)
    for j in range(max(5, n_projects // 200)):
        relations.append((f"ghost_p{j}", "project", rng.choice(org_keys), "organization", "hasParticipant", "participation"))

    # ------------------------------------------------------------------ works
    n_prod = int(n_works * 0.10)
    n_org_only = int(n_works * 0.72)
    works, truth_works = [], []
    proj_keys = [p["key"] for p in projects]
    producers = rng.sample(proj_keys, int(len(proj_keys) * 0.42))
    for i in range(n_works):
        key = f"w{i}"
        kind = "tier0" if i < n_prod else "tier1" if i < n_prod + n_org_only else "unlinked"
        lang = "en" if rng.random() < 0.90 else rng.choice(["de", "fr"])
        r = rng.random()
        title = None if r < 0.02 else _body(rng, lang, 1)[:150].rstrip(".")
        if r >= 0.02 and r < 0.30:
            desc_list = [] if rng.random() < 0.6 else None  # both an empty list and NULL occur in the real staging
            desc = None
        else:
            desc = _body(rng, lang, rng.randint(2, 6))
            desc_list = None
        text_for_plant = desc if desc is not None else (title or "")
        planted, qids = _plant(rng, text_for_plant, 0.05, 0.10)
        if desc is not None:
            desc = planted
        elif title is not None:
            title = planted  # no description: the phrase goes into the title
        descriptions = [desc] if desc is not None else desc_list
        subjects = rng.sample(KEYWORDS, rng.randint(0, 3))
        container = rng.choice(JOURNALS) if rng.random() < 0.6 else None
        works.append(
            dict(key=key, title=title, date=_rand_date(rng), descriptions=descriptions, subjects=subjects, container=container,
                 lang=lang, countries=None if rng.random() < 0.1 else rng.sample(WORK_COUNTRIES, rng.randint(0, 3)),
                 n_authors=rng.randint(1, 4), citations=float(rng.randint(0, 300)))
        )
        truth_works.append((_oid("work", key), kind, qids, _pillar_mask(title, desc, " ".join(subjects))))
        if kind == "tier0":
            for pk in rng.sample(producers, 2 if rng.random() < 0.05 else 1):
                relations.append((pk, "project", key, "product", "produces", "outcome"))
        if kind in ("tier0", "tier1") and not (kind == "tier0" and rng.random() < 0.4):
            for ok in rng.sample(org_keys, rng.choice([1, 1, 2, 3])):
                relations.append((key, "product", ok, "organization", "hasAuthorInstitution", "affiliation"))
        if kind == "tier1" and not any(r_[0] == key for r_ in relations[-3:]):
            relations.append((key, "product", rng.choice(org_keys), "organization", "hasAuthorInstitution", "affiliation"))
    # a few relations of a type the pipeline does not use (must be dropped by the seed)
    for j in range(20):
        a, b = rng.sample(org_keys, 2)
        relations.append((a, "organization", b, "organization", "isParentOf", "hierarchy"))

    # ------------------------------------------------------------------ write staging
    con = duckdb.connect(str(out))
    con.execute(STAGING_DDL)

    def arrow(rows: list[dict], schema: dict) -> pa.Table:
        return pa.table({k: pa.array([r[k] for r in rows], type=t) for k, t in schema.items()})

    pid_t = pa.list_(pa.struct([("scheme", pa.string()), ("value", pa.string())]))
    org_t = arrow(
        [
            dict(oid=_oid("org", o["key"]), name=o["name"], short=None, url=None, alt=[o["name"].lower()] if rng.random() < 0.2 else [],
                 cc=o["cc"], ror=o["ror"], wiki=None, pids=[dict(scheme=s, value=v) for s, v in o["pids"]] + ([dict(scheme="ISNI", value="0000000123")] if rng.random() < 0.1 else []))
            for o in orgs.values()
        ],
        dict(oid=pa.string(), name=pa.string(), short=pa.string(), url=pa.string(), alt=pa.list_(pa.string()), cc=pa.string(),
             ror=pa.string(), wiki=pa.string(), pids=pid_t),
    )
    con.register("org_t", org_t)
    con.execute(
        "INSERT INTO organization SELECT hash(oid), oid, name, short, url, alt, cc, ror, wiki, pids FROM org_t"
    )

    fund_t = pa.list_(pa.struct([("fundingStream", pa.struct([("description", pa.string()), ("id", pa.string())])),
                                 ("jurisdiction", pa.string()), ("name", pa.string()), ("shortName", pa.string())]))
    proj_t = arrow(
        [
            dict(oid=_oid("project", p["key"]), grant=p["grant"], title=p["title"], acronym=p["acronym"], start=p["start"], end=p["end"],
                 keywords=p["keywords"], subjects=p["subjects"], summary=p["summary"], doi=p["doi"],
                 fundings=[dict(fundingStream=dict(description="sample", id=p["stream"] + "::x"), jurisdiction="EU",
                                name="European Commission", shortName="EC")],
                 fps=[p["stream"].split("::")[1]], granted=dict(currency="EUR", fundedAmount=p["amount"], totalCost=p["amount"] * 1.1))
            for p in projects
        ],
        dict(oid=pa.string(), grant=pa.string(), title=pa.string(), acronym=pa.string(), start=pa.date32(), end=pa.date32(),
             keywords=pa.string(), subjects=pa.list_(pa.string()), summary=pa.string(), doi=pa.string(), fundings=fund_t,
             fps=pa.list_(pa.string()), granted=pa.struct([("currency", pa.string()), ("fundedAmount", pa.float64()), ("totalCost", pa.float64())])),
    )
    con.register("proj_t", proj_t)
    con.execute(
        """INSERT INTO project SELECT hash(oid), oid, grant, title, acronym, NULL, start, "end", NULL, keywords, true, false, subjects,
                  fundings, fps, summary, doi, granted FROM proj_t"""
    )

    # works: nested columns built in SQL from flat arrow columns (keeps the arrow schema simple)
    work_t = arrow(
        [
            dict(oid=_oid("work", w["key"]), title=w["title"], date=w["date"], descriptions=w["descriptions"], subjects=w["subjects"],
                 container=w["container"], lang=w["lang"], countries=w["countries"], n_authors=w["n_authors"], citations=w["citations"],
                 doi=f"10.5555/sample.{w['key']}")
            for w in works
        ],
        dict(oid=pa.string(), title=pa.string(), date=pa.date32(), descriptions=pa.list_(pa.string()), subjects=pa.list_(pa.string()),
             container=pa.string(), lang=pa.string(), countries=pa.list_(pa.string()), n_authors=pa.int64(), citations=pa.float64(),
             doi=pa.string()),
    )
    con.register("work_t", work_t)
    con.execute(
        """INSERT INTO work
           SELECT hash(oid), oid, title, date, 'Sample Publisher', 'gold', false, false, true,
                  CASE lang WHEN 'de' THEN {'code': 'deu/ger', 'label': 'German'} WHEN 'fr' THEN {'code': 'fra/fre', 'label': 'French'}
                            ELSE {'code': 'eng', 'label': 'English'} END,
                  {'code': 'c_abf2', 'label': 'OPEN', 'scheme': 'http://vocabularies.coar-repositories.org/documentation/access_rights/'},
                  [{'fullName': 'Author, ' || i::VARCHAR, 'name': 'A', 'pid': NULL, 'rank': i, 'surname': 'Author'} for i in range(1, n_authors + 1)],
                  [{'provenance': {'provenance': 'Harvested', 'trust': '0.9'}, 'subject': {'scheme': 'keyword', 'value': s}} for s in subjects],
                  descriptions, [{'scheme': 'doi', 'value': doi}], ['sample source'], NULL, [],
                  citations, citations / 10, CAST(citations * 3 AS BIGINT), countries,
                  CASE WHEN container IS NULL THEN NULL
                       ELSE {'edition': NULL, 'ep': NULL, 'iss': NULL, 'issnLinking': NULL, 'issnOnline': NULL, 'issnPrinted': NULL,
                             'name': container, 'sp': NULL, 'vol': NULL} END
           FROM work_t"""
    )

    rel_t = pa.table(
        {
            "s": [_oid({"project": "project", "product": "work", "organization": "org"}[r[1]], r[0]) for r in relations],
            "st": [r[1] for r in relations],
            "t": [_oid({"project": "project", "product": "work", "organization": "org"}[r[3]], r[2]) for r in relations],
            "tt": [r[3] for r in relations],
            "rn": [r[4] for r in relations],
            "rt": [r[5] for r in relations],
        }
    )
    con.register("rel_t", rel_t)
    con.execute(
        """INSERT INTO relation SELECT hash(s), st, hash(t), tt, {'name': rn, 'type': rt},
                  {'provenance': 'Inferred by OpenAIRE', 'trust': '0.9'}, false FROM rel_t"""
    )
    counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("organization", "project", "work", "relation")}
    con.close()

    # ------------------------------------------------------------------ truth
    tc = duckdb.connect(str(truth_out))
    tc.execute("CREATE TABLE org_truth (openaireId VARCHAR, variant VARCHAR, name VARCHAR, cc VARCHAR, pic VARCHAR, ror VARCHAR)")
    tc.executemany("INSERT INTO org_truth VALUES (?, ?, ?, ?, ?, ?)", [(_oid("org", o["key"]), o["variant"], o["name"], o["cc"], o["pic"], o["ror"]) for o in orgs.values()])
    tc.execute("CREATE TABLE project_truth (openaireId VARCHAR, variant VARCHAR, minority_qid VARCHAR[], pillars UTINYINT)")
    tc.executemany("INSERT INTO project_truth VALUES (?, ?, ?, ?)", truth_projects)
    tc.execute("CREATE TABLE work_truth (openaireId VARCHAR, kind VARCHAR, minority_qid VARCHAR[], pillars UTINYINT)")
    tc.executemany("INSERT INTO work_truth VALUES (?, ?, ?, ?)", truth_works)
    tc.close()

    logging.info(f"fixture {out}: {counts}; DOI-only projects: {doi_only}; org variants: "
                 f"{ {v: sum(1 for o in orgs.values() if o['variant'] == v) for v in sorted({o['variant'] for o in orgs.values()})} }")
    return out, truth_out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--projects", type=int, default=4000)
    parser.add_argument("--orgs", type=int, default=7500)
    parser.add_argument("--works", type=int, default=80000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="data/duckdb/sample")
    args = parser.parse_args()
    out, truth = build(args.projects, args.orgs, args.works, args.seed, Path(resolve_data_path(args.out_dir)))
    print(f"staging fixture: {out}\ntruth:           {truth}")


if __name__ == "__main__":
    main()
