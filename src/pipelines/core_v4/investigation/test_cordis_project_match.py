"""Tests for cordis_project_match.py against (a) a tiny synthetic Cordis + OpenAire pair with known
answers and (b) the real local Cordis heritage subset with a hand-built OpenAire fixture (the local
OpenAire staging sample has no project.doi column)."""

import json
import runpy
from pathlib import Path

import duckdb
import pytest

SCRIPT = Path(__file__).with_name("cordis_project_match.py")
REPO = Path(__file__).resolve().parents[4]
REAL_CORDIS = REPO / "data" / "duckdb" / "sources" / "cordis_heritage_subset_with_pdfs_raw.duckdb"


def load():
    return runpy.run_path(str(SCRIPT), run_name="cordis_project_match_under_test")


def make_openaire(path, rows, with_doi=True):
    """rows: (id, openaireId, grantId, doi)"""
    con = duckdb.connect(str(path))
    con.execute(f"CREATE TABLE project (id UBIGINT, openaireId VARCHAR, grantId VARCHAR{', doi VARCHAR' if with_doi else ''})")
    for r in rows:
        con.execute("INSERT INTO project VALUES (?, ?, ?" + (", ?)" if with_doi else ")"), list(r) if with_doi else list(r[:3]))
    con.close()


def make_cordis(path, projects, programmes):
    """projects: (id, id_original, doi, start_date); programmes: {project id: framework_programme}"""
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE project (id INTEGER, id_original VARCHAR, doi VARCHAR, start_date DATE)")
    con.execute("CREATE TABLE fundingprogramme (id INTEGER, framework_programme VARCHAR)")
    con.execute("CREATE TABLE j_project_fundingprogramme (project_id INTEGER, fundingprogramme_id INTEGER)")
    for p in projects:
        con.execute("INSERT INTO project VALUES (?, ?, ?, ?)", list(p))
    fps = sorted(set(programmes.values()))
    for i, fp in enumerate(fps, 1):
        con.execute("INSERT INTO fundingprogramme VALUES (?, ?)", [i, fp])
    for pid, fp in programmes.items():
        con.execute("INSERT INTO j_project_fundingprogramme VALUES (?, ?)", [pid, fps.index(fp) + 1])
    con.close()


def run(tmp_path, capsys, oa, cordis):
    out = tmp_path / "out.json"
    load()["main"](["--staging-db", str(oa), "--cordis-db", str(cordis), "--out", str(out), "--temp-dir", str(tmp_path / "spill"), "--mem-mb", "6000", "--threads", "2"])
    return capsys.readouterr().out, json.loads(out.read_text())


def test_synthetic_rules_and_causes(tmp_path, capsys):
    cordis = tmp_path / "cordis.duckdb"
    make_cordis(
        cordis,
        [
            (1, "100001", None, "1999-05-01"),  # exact
            (2, "HPMF-CT-2001-01503", None, "2001-01-01"),  # OA grantId without hyphens: alnum
            (3, "200002", None, "2015-01-01"),  # OA grantId '0200002': leading zeros
            (4, "300003", None, "2016-01-01"),  # OA grantId 'GA 300003': prefix
            (5, "400004", "10.3030/400004", "2016-01-01"),  # unmatched by id, DOI upper-case in OA
            (6, "500005", "10.3030/999999", "2017-01-01"),  # only the DOI suffix (=grantId 999999) finds it
            (7, "600006", None, "2018-01-01"),  # nothing matches
            (8, "700007", None, "2018-01-01"),  # exact, twice, both EC: same datasource
            (9, "800008", None, None),  # exact, EC + NSF: collision
            (10, "900009", None, "2019-01-01"),  # exact, NSF only
        ],
        {1: "FP5", 2: "FP5", 3: "H2020", 4: "H2020", 5: "H2020", 6: "H2020", 7: "H2020", 8: "H2020", 9: "FP7"},
    )
    oa = tmp_path / "oa.duckdb"
    make_openaire(
        oa,
        [
            (1, "corda_______::a", "100001", None),
            (2, "corda_______::b", "HPMFCT200101503", None),
            (3, "corda__h2020::c", "0200002", None),
            (4, "corda__h2020::d", "GA 300003", None),
            (5, "corda__h2020::e", "X-1", "10.3030/400004".upper()),
            (6, "corda__h2020::f", "999999", None),
            (7, "corda__h2020::g", "700007", None),
            (8, "corda__h2020::h", "700007", None),
            (9, "corda__h2020::i", "800008", None),
            (10, "nsf_________::j", "800008", None),
            (11, "nsf_________::k", "900009", None),
            (12, "nih_________::l", "unrelated-1", None),
        ],
    )
    out, js = run(tmp_path, capsys, oa, cordis)

    base = js["baseline"]
    assert base["cordis_projects"] == 10 and base["cordis_matched_exact"] == 4 and base["cordis_with_doi"] == 2
    assert base["openaire_ec_records"] == 9

    rec = {r["rule"]: r for r in js["recommended_rule"]}
    assert [rec[k]["cumulative_projects"] for k in ["exact", "lower", "alnum", "zeros", "prefix", "doi", "doi_suffix"]] == [4, 4, 5, 6, 7, 8, 9]
    assert rec["doi"]["new_projects"] == 1 and rec["doi_suffix"]["new_projects"] == 1
    assert rec["exact"]["cumulative_projects_with_multiple_matches"] == 2  # 700007 and 800008

    un = js["unmatched"]
    assert (un["unmatched"], un["with_doi"]) == (6, 2)
    assert un["routes"]["doi"]["recovered_unmatched"] == 1 and un["routes"]["prefix"]["recovered_unmatched"] == 3
    assert un["routes"]["alnum"]["recovered_unmatched"] == 1  # only the hyphens; zeros/prefix are supersets

    causes = {c["cause"]: c for c in js["multiple_matches"]["causes"]}
    assert causes["EC duplicates (same EC datasource)"]["projects"] == 1  # 700007
    assert causes["EC + non-EC (grantId collision with another funder)"]["projects"] == 1
    assert js["multiple_matches"]["total"] == 2
    assert js["multiple_matches"]["exact_matched_only_non_ec"] == 1  # project 10

    years = {r["key"]: r for r in js["by_start_year"]}
    assert years["1995-1999"]["exact"] == 1 and years["no date"]["projects"] == 1 and years["2015-2019"]["unmatched"] == 5
    progs = {r["key"]: r for r in js["by_programme"]}
    assert progs["FP5"]["projects"] == 2 and progs["none"]["projects"] == 1 and progs["H2020"]["exact"] == 1 and progs["FP7"]["exact"] == 1

    for heading in ["Matched vs unmatched by Cordis start year", "by funding programme", "Recommended match rule", "more than one OpenAire project"]:
        assert heading in out
    assert "| **all** | 10 | 4 |" in out


def test_without_doi_column_the_doi_rules_match_nothing(tmp_path, capsys):
    cordis = tmp_path / "cordis.duckdb"
    make_cordis(cordis, [(1, "100001", "10.3030/100001", "2010-01-01"), (2, "200002", None, "2010-01-01")], {1: "FP7", 2: "FP7"})
    oa = tmp_path / "oa.duckdb"
    make_openaire(oa, [(1, "corda_______::a", "100001", None)], with_doi=False)
    out, js = run(tmp_path, capsys, oa, cordis)
    assert "no `doi` column" in out and js["meta"]["openaire_project_has_doi"] is False
    assert {r["rule"]: r["new_projects"] for r in js["recommended_rule"]}["doi"] == 0


@pytest.mark.skipif(not REAL_CORDIS.exists(), reason="local Cordis heritage subset not present")
def test_real_cordis_subset_with_fake_openaire(tmp_path, capsys):
    con = duckdb.connect(str(REAL_CORDIS), read_only=True)
    n_total = con.execute("SELECT count(*) FROM project").fetchone()[0]
    digits = [r[0] for r in con.execute("SELECT id_original FROM project WHERE id_original ~ '^[0-9]{6}$' AND doi IS NULL ORDER BY id LIMIT 8").fetchall()]
    hyphen = [r[0] for r in con.execute("SELECT id_original FROM project WHERE id_original LIKE '%-%' ORDER BY id LIMIT 3").fetchall()]
    doi = [r for r in con.execute("SELECT id_original, doi FROM project WHERE doi IS NOT NULL ORDER BY id LIMIT 2").fetchall()]
    n_with_doi = con.execute("SELECT count(*) FROM project WHERE doi IS NOT NULL").fetchone()[0]
    con.close()
    assert len(digits) == 8 and len(hyphen) == 3 and len(doi) == 2

    rows = [(i, "corda__h2020::x", g, None) for i, g in enumerate(digits[:4], 1)]  # exact
    rows += [(10 + i, "corda_______::y", g.replace("-", ""), None) for i, g in enumerate(hyphen)]  # alnum
    rows += [(20 + i, "corda__h2020::z", "GA" + g, None) for i, g in enumerate(digits[4:6])]  # prefix
    rows += [(30 + i, "nsf_________::n", "no-match-%d" % i, d.upper()) for i, (_, d) in enumerate(doi)]  # doi only
    oa = tmp_path / "oa.duckdb"
    make_openaire(oa, rows)
    out, js = run(tmp_path, capsys, oa, REAL_CORDIS)

    assert js["baseline"]["cordis_projects"] == n_total == 9267
    assert js["baseline"]["cordis_with_doi"] == n_with_doi
    assert js["baseline"]["cordis_matched_exact"] == 4
    rec = {r["rule"]: r["cumulative_projects"] for r in js["recommended_rule"]}
    assert rec["exact"] == 4 and rec["alnum"] == 7 and rec["prefix"] == 9 and rec["doi"] == 9 + len(doi)
    assert sum(r["projects"] for r in js["by_programme"] if r["key"] != "**all**") == n_total
    assert sum(r["projects"] for r in js["by_start_year"] if r["key"] != "**all**") == n_total
    # the DOI-only OpenAire records are non-EC, so the heuristic must not recommend the DOI rule blindly
    doi_rule = [r for r in js["recommended_rule"] if r["rule"] == "doi"][0]
    assert doi_rule["new_pairs_non_ec"] == doi_rule["new_pairs"] == len(doi) and doi_rule["recommended"] is False
    assert "Recommended match rule" in out
