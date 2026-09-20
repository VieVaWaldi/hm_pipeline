"""Corrected / cleaned project -> minority tags for the serving export, without a pipeline re-run (the 132 GB file is untouched).

Why: the stored `project.minority_qid` comes from the keyword matcher (src/enrichment/minority_matching/matcher.py). The precision review
(agent_job/MINORITY_REVIEW.md) found false positives that come from the matcher itself: edit-distance-1 typo variants of multi-word
keywords ("manx people" -> "many people", "the jews" -> "the news"), the Swedish word "same" for Sámi, country adjectives used as keywords
("russian", "turkish", "silesian"). All fixes are SUBTRACTIVE, so it is enough to re-evaluate the ~9.3k projects that are tagged today.

    .venv/bin/python src/pipelines/core_v4/serving/export/minority_override.py            # needs spaCy + pyahocorasick: the main .venv, not .venv-serving
        [--profile obvious|strict]       # obvious (DEFAULT) = R1 + R2 + R5(institution) only; strict = also R3 + R4 (the old full-replacement behaviour)
        [--projects agent_job/out/projects_full.parquet | path/to/core_v4.duckdb]      # parquet (all columns) or a DuckDB file with a `project` table
        [--minority-db data/duckdb/core/core_v4_noworkenrichment-min.duckdb]            # source of the 278-group `minority` table (read-only)
        [--out PATH] [--report PATH]     # obvious: minority_exclusions.csv + agent_job/MINORITY_EXCLUSIONS.md;  strict: minority_override.parquet + agent_job/MINORITY_OVERRIDE.md
        [--hebrew institution|context|drop|keep] [--single-typo second-signal|keep|drop] [--signal narrow|broad] [--window 4]   # override single profile settings

Outputs (chosen by the suffix of --out):
  * `.csv`     EXCLUSION LIST (default for profile obvious): one row per (project_id, minority_qid) pair to REMOVE = stored tags minus kept tags. Columns:
               project_id, minority_qid, group_name_en, rule (R1/R2/R5...), matched_text (the variant that matched), title (first 100 chars). Human-reviewable,
               git-trackable (~3k rows). export.py reads only the first two columns: `export.py --minority-exclude minority_exclusions.csv` (SUBTRACTIVE: every
               other tag stays, all 278 groups stay in the minorities index even if they end with 0 projects).
  * `.parquet` full replacement (default for profile strict): (project_id VARCHAR, minority_qid VARCHAR[]) = the tags to KEEP for tagged projects; used with
               `export.py --minority-override` (a project not in the file has NO minority). `*.parquet` is git-ignored: a local artifact unless force-added.
By decision of 2026-09-20 the export ships the stored tags minus the small `obvious` deny-list (D32); the strict profile is only documentation.

Rules (each removes a (project, group) tag only if NO other match still supports it; `same`, typo and weak-keyword rules are per match):
  R1 typo variants of MULTI-word keywords never count ("many people", "do people", "the news", "same people", "laz people" -> "lay people").
  R2 the keyword `same` (Sámi) never counts ("Same-sex", "the same"): the rest of the Sámi keywords stay.
  R3 weak keywords need a second signal (default `narrow`, within --window words of the hit): Russians {russian, russians, rus},
     Turkish {turkish, turks}, Silesians {silesian(s)}. Their multi-word forms ("russian people", "ethnic russians") and foreign-language
     keywords stay unconditional. `--signal broad` = any of minority|community|heritage|language|culture|people|history... anywhere in the text
     (weaker: those words are in nearly every text, kept only to show the difference in the report).
  R4 single-word typo variants (silesian(s), azorean(s), aragonese, franconian...: adjective/noun forms, not typos) need the same second signal
     (--single-typo second-signal, default; `keep` = unchanged, `drop` = remove).
  R5 `hebrew` (Jewish): default `institution` = drop only if every occurrence of "Hebrew" is part of an institution name (Hebrew University,
     Hebrew Union College...). `context` = keep only if a Jewish-context word (genizah, talmud, rabbi, judaeo, ...) is in the text, `drop`, `keep`.
Profile obvious = R1 + R2 + R5 with `hebrew=institution` (R3 and R4 off); profile strict = all five. Everything else is unchanged; the stock matcher run on the stored tags is asserted to reproduce them exactly (sanity check for the environment).
"""
import argparse
import hashlib
import random
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]   # HERE=export; parents: [0] serving, [1] core_v4, [2] pipelines, [3] src, [4] repo root
sys.path.insert(0, str(REPO / "src"))
from enrichment.minority_matching.matcher import (  # noqa: E402
    MinorityMatcher, load_groups, load_rules, normalize, read_term_overrides, read_titular_majority)

SOURCE_DIR = REPO / "src" / "sources" / "dumps" / "minorities"
DEFAULT_PROJECTS = HERE.parent / "agent_job" / "out" / "projects_full.parquet"
DEFAULT_MINORITY_DB = REPO / "data" / "duckdb" / "core" / "core_v4_noworkenrichment-min.duckdb"
DEFAULT_OUT = HERE / "minority_override.parquet"
DEFAULT_REPORT = HERE.parent / "agent_job" / "MINORITY_OVERRIDE.md"

SAMI, JEWISH, RUSSIANS, TURKISH, SILESIANS = "Q48199", "Q7325", "Q49542", "Q84072", "Q140472"
BIG = [("Q125564", "Manx"), ("Q2656122", "Dom"), (RUSSIANS, "Russians"), (TURKISH, "Turkish"), (SILESIANS, "Silesians"), (JEWISH, "Jewish"), (SAMI, "Sámi")]
WEAK = {RUSSIANS: {"russian", "russians", "rus"}, TURKISH: {"turkish", "turks"}, SILESIANS: {"silesians", "silesian"}}

# second signal near the hit (narrow = the review's recommendation: no community/heritage/origin/refugee, those are everywhere)
NARROW = (r"minority|minorities|ethnic|ethnicity|diaspora|immigrants?|emigrants?|migrants?|settlers?|newcomers?|descent|descendants?|"
          r"mother tongue|second generation|guest workers?|speaking|indigenous|native speakers?")
EXTRA = {TURKISH: r"|cypriots?", SILESIANS: r"|language|dialect|nation|national"}
BROAD_RX = re.compile(r"\b(minority|minorities|community|communities|indigenous|diaspora|heritage|ethnic|language|culture|cultural|people|folk|traditions|history)\b")
INST_RX = re.compile(r"\bhebrew (university|union college|univ|teachers college|college|institute of technology)\b|\bacademy of the hebrew language\b|\bhebrew senior ?life\b")
JEWISH_CONTEXT = re.compile(r"\b(genizah|geniza|judaeo|judeo|talmud\w*|torah|rabbi\w*|midrash\w*|mishnah|masoretic|synagogue\w*|kabbal\w*|sephardi\w*|ashkenaz\w*|"
                            r"ladino|israelites?|hasidi\w*|zionis\w*|holocaust|shoah|dead sea scrolls?)\b")


@dataclass(frozen=True)
class Cfg:
    hebrew: str = "institution"
    single_typo: str = "second-signal"
    signal: str = "narrow"
    window: int = 4
    weak: bool = True   # R3 (weak adjective keywords need a second signal) on/off


PROFILES = {
    "obvious": Cfg(hebrew="institution", single_typo="keep", signal="narrow", window=4, weak=False),   # R1 + R2 + R5 only
    "strict": Cfg(),                                                                                   # R1-R5 (the previous defaults)
}


def build_matcher(minority_db: Path):
    con = duckdb.connect(str(minority_db), read_only=True)
    try:
        rows = con.execute("SELECT qid, merged_qids, group_name_en, search_keywords FROM minority").fetchall()
    finally:
        con.close()
    groups = load_groups(rows, read_term_overrides(SOURCE_DIR / "manual_term_overrides.csv"), read_titular_majority(SOURCE_DIR / "titular_majority_overrides.csv"))
    return MinorityMatcher(groups, load_rules()), {r[0]: r[2] for r in rows}


def load_tagged(projects: Path):
    """(id, title, text, stored minority_qid) of every tagged project. text = the matcher's input: title, summary, keywords, subjects."""
    con = duckdb.connect()
    src = f"read_parquet('{projects}')" if projects.suffix == ".parquet" else None
    if src is None:
        con.execute(f"ATTACH '{projects}' AS s (READ_ONLY)")
        src = "s.project"
    return con.execute(f"""SELECT id::VARCHAR, title, concat_ws(' ', title, summary, keywords, list_aggregate(subjects, 'string_agg', ' ')), minority_qid
                           FROM {src} WHERE minority_qid IS NOT NULL AND len(minority_qid) > 0 ORDER BY id::VARCHAR""").fetchall()


def norm_for(m, norm_l: str, norm_c: str) -> str:
    return norm_c if m.strict else norm_l


def has_signal(rx: re.Pattern, norm: str, m, window: int) -> bool:
    left, right = norm[: m.start].lower().split()[-window:], norm[m.end:].lower().split()[:window]
    return bool(rx.search(" ".join(left) + " · " + " ".join(right)))


def hebrew_verdict(norm_l: str, mode: str) -> str | None:
    """None = the `hebrew` match still supports the Jewish tag, else the drop reason."""
    if mode == "keep":
        return None
    if mode == "drop":
        return "hebrew_dropped"
    if mode == "context":
        return None if JEWISH_CONTEXT.search(norm_l) else "hebrew_no_jewish_context"
    inst = [m.span() for m in INST_RX.finditer(norm_l)]
    occ = [m.span() for m in re.finditer(r"\bhebrew\b", norm_l)]
    return "hebrew_institution_only" if occ and all(any(a <= s and e <= b for a, b in inst) for s, e in occ) else None


def drop_reason(m, q: str, norm_l: str, norm_c: str, cfg: Cfg) -> str | None:
    """Why the match `m` does NOT support tag `q` (None = it supports it)."""
    kw, multi = m.keyword, " " in m.keyword
    if m.typo and multi:
        return "R1_typo_multiword"
    if kw == "same" and q == SAMI:
        return "R2_same"
    if kw == "hebrew" and q == JEWISH:
        r = hebrew_verdict(norm_l, cfg.hebrew)
        return f"R5_{r}" if r else None
    single_typo = m.typo and not multi
    if single_typo and cfg.single_typo == "drop":
        return "R4_single_typo_dropped"
    if (cfg.weak and kw in WEAK.get(q, ())) or (single_typo and cfg.single_typo == "second-signal"):
        if cfg.signal == "broad":
            ok = bool(BROAD_RX.search(norm_l))
        else:
            ok = has_signal(re.compile(r"\b(" + NARROW + EXTRA.get(q, "") + r")\b"), norm_for(m, norm_l, norm_c), m, cfg.window)
        if not ok:
            return "R3_weak_keyword_no_signal" if (cfg.weak and kw in WEAK.get(q, ())) else "R4_single_typo_no_signal"
    return None


def evaluate(records, cfg: Cfg):
    """records: [(pid, title, matches, norm_l, norm_c, stored)] -> ({pid: kept qids}, {(pid, q): (reason, match)}, {(pid, q): supporting match}), all deterministic."""
    kept, dropped, support = {}, {}, {}
    for pid, _t, matches, norm_l, norm_c, _s in records:
        by_q = defaultdict(list)
        for m in matches:
            for q in m.qids:
                by_q[q].append(m)
        keep = []
        for q in sorted(by_q):
            reasons = [(drop_reason(m, q, norm_l, norm_c, cfg), m) for m in by_q[q]]
            ok = [m for r, m in reasons if r is None]
            if ok:
                keep.append(q)
                support[(pid, q)] = ok[0]
            else:
                reason = Counter(r for r, _ in reasons).most_common(1)[0][0]
                dropped[(pid, q)] = (reason, next(m for r, m in reasons if r == reason))
        kept[pid] = keep
    return kept, dropped, support


def snippet(norm: str, m, width: int = 55) -> str:
    return (norm[max(0, m.start - width): m.start] + "[[" + norm[m.start: m.end] + "]]" + norm[m.end: m.end + width]).replace("|", "/")


def write_override(kept: dict, out: Path) -> int:
    rows = [(pid, qs) for pid, qs in kept.items() if qs]
    table = pa.table({"project_id": pa.array([r[0] for r in rows], pa.string()), "minority_qid": pa.array([r[1] for r in rows], pa.list_(pa.string()))})
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    return len(rows)


def exclusion_rows(records, kept, dropped, names):
    """Stored tags minus kept tags, as sorted CSV rows. Every excluded pair must come from a dropped match (asserted)."""
    rows = []
    rec = {r[0]: r for r in records}
    for pid, _t, _m, norm_l, norm_c, stored in records:
        for q in sorted(stored):
            if q in kept[pid]:
                continue
            reason, m = dropped[(pid, q)]   # KeyError = a stored tag without any match: the matcher differs from the one that built the data
            rows.append((pid, q, names.get(q, ""), reason.split("_")[0], norm_for(m, norm_l, norm_c)[m.start: m.end], " ".join((rec[pid][1] or "").split())[:100]))
    return sorted(rows, key=lambda r: (int(r[0]), r[1]))


def write_exclusions(rows, out: Path) -> None:
    import csv
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["project_id", "minority_qid", "group_name_en", "rule", "matched_text", "title"])
        w.writerows(rows)


def write_exclusion_report(path: Path, ctx: dict) -> None:
    names, before, cfg, profile = ctx["names"], ctx["before"], ctx["cfg"], ctx["profile"]
    rows, kept, dropped, recs = ctx["rows"], ctx["kept"], ctx["dropped"], ctx["records"]
    excl = defaultdict(set)
    for r in rows:
        excl[r[0]].add(r[1])
    after = Counter(q for pid, _t, _m, _l, _c, stored in recs for q in stored if q not in excl[pid])
    tags_b, tags_a = sum(before.values()), sum(after.values())
    lost_all = sum(1 for pid, _t, _m, _l, _c, stored in recs if all(q in excl[pid] for q in stored))
    with_min_after = len(recs) - lost_all
    to_zero = [q for q in before if after.get(q, 0) == 0]
    changed = [q for q in before if after.get(q, 0) != before[q]]
    by_rule = Counter(r[3] for r in rows)
    L = ["# Minority exclusions: a deny-list of obviously wrong (project, minority) pairs", "",
         f"Generated by `serving/export/minority_override.py --profile {profile}` from `{ctx['projects']}` ({len(recs):,} tagged projects). "
         "**By decision of 2026-09-20 the export ships the stored `project.minority_qid` minus this list** (`export.py --minority-exclude serving/export/minority_exclusions.csv`); "
         "adjective-style keyword hits (\"Russian\", \"Turkish\", \"Silesian\") are accepted and stay. All 278 minority groups stay in the `minorities` index, also those that end with 0 projects.", "",
         f"Rules removed in this profile: **R1** typo variants of multi-word keywords (\"many people\" -> \"manx people\", \"the news\" -> \"the jews\", \"do people\" -> \"dom people\"), "
         "**R2** the word `same` for Sámi (\"Same-sex\", \"the same\"), **R5** `hebrew` only inside an institution name (Hebrew University ...) for Jewish people. "
         f"Not applied: R3 (weak adjectives) and R4 (single-word typo variants). Config: `{cfg}`.", "",
         f"Sanity: the stock matcher reproduces the stored tags of **{ctx['baseline_ok']:,} of {len(recs):,}** projects exactly (mismatches: {len(recs) - ctx['baseline_ok']}). "
         "The CSV is exactly stored tags minus kept tags.", "",
         "## Totals", "", "| | before | after | removed |", "|---|---|---|---|",
         f"| **projects with >= 1 minority** (goes into the cluster verification table) | {len(recs):,} | **{with_min_after:,}** | {lost_all:,} ({100 * lost_all / max(1, len(recs)):.1f}%) lose ALL tags |",
         f"| project-group tags | {tags_b:,} | {tags_a:,} | {len(rows):,} pairs ({100 * len(rows) / max(1, tags_b):.1f}%) |",
         f"| groups with >= 1 project | {len(before)} | {len(before) - len(to_zero)} | {len(to_zero)} groups go to 0 projects |",
         f"| groups in the minorities index | 278 | 278 | none removed |", "",
         f"Groups that change: {len(changed)}. Groups that end with 0 projects: {', '.join(names.get(q, q) for q in sorted(to_zero, key=lambda q: -before[q])) or 'none'}. "
         f"Groups without any project before (unchanged): {278 - len(before)}.", "",
         "### Pairs removed per rule", "", "| rule | pairs | what it is |", "|---|---|---|"]
    what = {"R1": "typo variant of a multi-word keyword", "R2": "`same` (Sámi)", "R5": "`hebrew` inside an institution name only (Jewish)"}
    for r, n in sorted(by_rule.items()):
        L.append(f"| {r} | {n:,} | {what.get(r, '')} |")
    hi = {"Q125564", "Q2656122", "Q7325", "Q48199", "Q208551", "Q846578", "Q415693"}
    L += ["", "## Before / after per group (only groups that change; Manx, Dom, Jewish, Sámi, Laz, Svan, Akan in bold)", "",
          "| group | qid | before | after | removed | % removed |", "|---|---|---|---|---|---|"]
    for q in sorted(changed, key=lambda q: -(before[q] - after.get(q, 0))):
        a, b = after.get(q, 0), before[q]
        nm = f"**{names.get(q, q)}**" if q in hi else names.get(q, q)
        L.append(f"| {nm} | {q} | {b:,} | {a:,} | {b - a:,} | {100 * (b - a) / b:.0f}% |")
    L += ["", "## 10 random examples per rule (fixed seed)", "", "Context = normalised text around the hit (`[[..]]`).", ""]
    rec = {r[0]: r for r in recs}
    for rule in sorted(by_rule):
        cand = sorted((r for r in rows if r[3] == rule), key=lambda r: (int(r[0]), r[1]))
        L += [f"### {rule}: {what.get(rule, '')} ({len(cand):,} pairs)", "", "| project id | group | title | matched text | context |", "|---|---|---|---|---|"]
        for r in random.Random(rule).sample(cand, min(10, len(cand))):
            _p, _t, _m, norm_l, norm_c, _s = rec[r[0]]
            _reason, m = dropped[(r[0], r[1])]
            L.append(f"| {r[0]} | {r[2]} | {r[5][:80].replace('|', '/')} | {r[4]} | {snippet(norm_for(m, norm_l, norm_c), m)} |")
        L.append("")
    L += ["## Files and use", "",
          "- `serving/export/minority_exclusions.csv`: this list (project_id, minority_qid, group_name_en, rule, matched_text, title). **Commit it** (csv, ~3k rows, human-reviewable).",
          "- `python export.py ... --minority-exclude serving/export/minority_exclusions.csv` (added to `export.sbatch`). Subtractive: projects not listed keep their stored tags; "
          "a project that loses all tags gets an empty list; pairs that are not stored tags or projects that do not exist abort the export (the file belongs to this database).",
          f"- Regenerate: `.venv/bin/python src/pipelines/core_v4/serving/export/minority_override.py` (profile {profile} is the default, ~15 s).",
          f"- **Verification number for the cluster export: projects with a non-empty `minority_qids` = {with_min_after:,}** (stored: {len(recs):,}).", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(L))


def count_by_group(kept: dict) -> Counter:
    return Counter(q for qs in kept.values() for q in qs)


def write_report(path: Path, ctx: dict) -> None:
    names, before, cfg = ctx["names"], ctx["before"], ctx["cfg"]
    kept, dropped, support, recs, alts = ctx["kept"], ctx["dropped"], ctx["support"], ctx["records"], ctx["alts"]
    after = count_by_group(kept)
    tags_b, tags_a = sum(before.values()), sum(after.values())
    lost_all = sum(1 for pid, _t, _m, _l, _c, stored in recs if not kept[pid])
    L = ["# Minority override: corrected project -> minority tags (Option B)", "",
         f"Generated by `serving/export/minority_override.py` from `{ctx['projects']}` ({len(recs):,} tagged projects). Config: `{cfg}`. "
         "All rules are subtractive re-evaluations of the stored tags (rules R1-R5 in the script docstring). Full-replacement semantics: a project not in "
         "`minority_override.parquet` has no minority when the override is active.", "",
         f"Sanity: the stock matcher reproduces the stored tags of **{ctx['baseline_ok']:,} of {len(recs):,}** projects exactly "
         f"(mismatches: {len(recs) - ctx['baseline_ok']}).", "",
         "## Totals", "", "| | before | after | removed |", "|---|---|---|---|",
         f"| tagged projects | {len(recs):,} | {len(recs) - lost_all:,} | {lost_all:,} ({100 * lost_all / max(1, len(recs)):.1f}%) |",
         f"| project-group tags | {tags_b:,} | {tags_a:,} | {tags_b - tags_a:,} ({100 * (tags_b - tags_a) / max(1, tags_b):.1f}%) |",
         f"| groups with >= 1 project | {len(before)} | {len(after)} | {len(before) - len(after)} |", "",
         "### Removed tags by rule (a tag is counted under its most frequent drop reason)", "", "| rule | tags |", "|---|---|"]
    for r, n in Counter(v[0] for v in dropped.values()).most_common():
        L.append(f"| {r} | {n:,} |")
    L += ["", "## Before / after per group (all groups with projects; the seven reviewed groups in bold)", "",
          "| group | qid | before | after | removed | % removed |", "|---|---|---|---|---|---|"]
    big = {q for q, _ in BIG}
    for q, b in sorted(before.items(), key=lambda kv: -kv[1]):
        a = after.get(q, 0)
        nm = f"**{names.get(q, q)}**" if q in big else names.get(q, q)
        L.append(f"| {nm} | {q} | {b:,} | {a:,} | {b - a:,} | {100 * (b - a) / b:.0f}% |")
    # ---- alternatives ----
    L += ["", "## Effect of the alternative settings (projects per group after the rule)", ""]
    for title, keys in (("Second signal: narrow proximity (default) vs the broad same-text list", ("signal", ("narrow", "broad"), [RUSSIANS, TURKISH, SILESIANS])),
                        ("Single-word typo variants (silesian, azorean, aragonese, ...)", ("single_typo", ("second-signal", "keep", "drop"), ["Q140472", "Q115175925", "Q2706746", "Q15474747", "Q1970302", "Q140420", "Q147540", "Q179248"])),
                        ("`hebrew` handling (Jewish people)", ("hebrew", ("institution", "context", "drop", "keep"), [JEWISH]))):
        field, values, qids = keys
        L += [f"### {title}", "", "| group | before | " + " | ".join(f"{field}={v}" for v in values) + " |", "|---|---|" + "---|" * len(values)]
        for q in qids:
            L.append(f"| {names.get(q, q)} | {before.get(q, 0):,} | " + " | ".join(f"{count_by_group(alts[(field, v)]).get(q, 0):,}" for v in values) + " |")
        L.append("")
    # ---- hebrew ----
    heb = [(pid, t, m, l) for pid, t, m, l, c, s in recs if JEWISH in s and any(x.keyword == "hebrew" for x in m)
           and all(x.keyword == "hebrew" for x in m if JEWISH in x.qids and not (x.typo and " " in x.keyword))]
    inst = [h for h in heb if hebrew_verdict(h[3], "institution")]
    ctxk = [h for h in heb if not hebrew_verdict(h[3], "institution") and JEWISH_CONTEXT.search(h[3])]
    biblang = [h for h in heb if not hebrew_verdict(h[3], "institution") and not JEWISH_CONTEXT.search(h[3])]
    L += ["## `hebrew`: what the projects that match Jewish ONLY through `hebrew` are", "",
          f"{len(heb)} projects match Jewish people only via `hebrew` (after R1). Classified by the script: **{len(inst)} institution names only** (Hebrew University etc.: dropped), "
          f"**{len(ctxk)} with an explicit Jewish-context word** (genizah, talmud, rabbi, ...: kept in every mode but `drop`), **{len(biblang)} other** (Hebrew as a language in a list of "
          "languages, Hebrew Bible / manuscripts philology, NLP on Hebrew, medieval translations from Hebrew: kept by the default, but most of them are about the language, not about Jewish people; "
          "`--hebrew context` would drop them).", ""]
    rnd = random.Random("hebrew")
    for label, lst in (("Dropped (institution name only)", inst), ("Kept (Jewish-context word)", ctxk), ("Kept (other: language / Bible / manuscripts)", biblang)):
        L += [f"### {label}: 15 random examples", "", "| project id | title | context |", "|---|---|---|"]
        for pid, t, m, l in rnd.sample(lst, min(15, len(lst))):
            hit = next(x for x in m if x.keyword == "hebrew")
            L.append(f"| {pid} | {(t or '')[:90].replace('|', '/')} | {snippet(l, hit)} |")
        L.append("")
    # ---- evidence ----
    L += ["## Evidence: 10 random removed and 10 random kept projects per big group", "",
          "Snippets are the normalised text around the hit (`[[..]]`), reproducible (fixed seed).", ""]
    tit = {r[0]: r[1] for r in recs}
    rec = {r[0]: r for r in recs}
    for q, short in BIG:
        rem = sorted(pid for (pid, qq) in dropped if qq == q)
        kp = sorted(pid for pid, qs in kept.items() if q in qs)
        L += [f"### {names.get(q, short)} ({q}): {before.get(q, 0):,} before, {after.get(q, 0):,} after", ""]
        for label, lst, srcmap in (("Removed", rem, "drop"), ("Kept", kp, "keep")):
            r2 = random.Random(f"{q}-{label}")
            pick = r2.sample(lst, min(10, len(lst)))
            L += [f"**{label}** ({len(lst):,} total)" + ("" if pick else ": none"), ""]
            if pick:
                L += ["| project id | title | " + ("rule | " if srcmap == "drop" else "") + "matched keyword | context |", "|---|---|" + ("---|" if srcmap == "drop" else "") + "---|---|"]
            for pid in pick:
                _p, _t, _m, norm_l, norm_c, _s = rec[pid]
                if srcmap == "drop":
                    reason, m = dropped[(pid, q)]
                else:
                    reason, m = None, support[(pid, q)]
                kw = m.keyword + (" (typo variant)" if m.typo else "")
                L.append(f"| {pid} | {(tit[pid] or '')[:80].replace('|', '/')} | " + (f"{reason} | " if reason else "") + f"{kw} | {snippet(norm_for(m, norm_l, norm_c), m)} |")
            L.append("")
    L += ["## Regenerate", "", "```bash", ".venv/bin/python src/pipelines/core_v4/serving/export/minority_override.py", "```",
          "OPTIONAL: by decision of 2026-09-20 the export ships the stored tags. To use the corrected tags anyway, copy `serving/export/minority_override.parquet` (git-ignored, ~30 kB) to the machine that runs the export and pass `export.py ... --minority-override <file>`; `export.sbatch` does not use it.", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(L))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--profile", default="obvious", choices=sorted(PROFILES))
    ap.add_argument("--projects", type=Path, default=DEFAULT_PROJECTS)
    ap.add_argument("--minority-db", type=Path, default=DEFAULT_MINORITY_DB)
    ap.add_argument("--out", type=Path, default=None, help="obvious: minority_exclusions.csv; strict: minority_override.parquet (suffix decides the format)")
    ap.add_argument("--report", type=Path, default=None)
    ap.add_argument("--hebrew", default=None, choices=["institution", "context", "drop", "keep"])
    ap.add_argument("--single-typo", default=None, choices=["second-signal", "keep", "drop"])
    ap.add_argument("--signal", default=None, choices=["narrow", "broad"])
    ap.add_argument("--window", type=int, default=None)
    a = ap.parse_args()
    base = PROFILES[a.profile]
    cfg = Cfg(hebrew=a.hebrew or base.hebrew, single_typo=a.single_typo or base.single_typo, signal=a.signal or base.signal,
              window=a.window or base.window, weak=base.weak)
    out = a.out or (HERE / ("minority_exclusions.csv" if a.profile == "obvious" else "minority_override.parquet"))
    report = a.report or (HERE.parent / "agent_job" / ("MINORITY_EXCLUSIONS.md" if a.profile == "obvious" else "MINORITY_OVERRIDE.md"))

    matcher, names = build_matcher(a.minority_db)
    tagged = load_tagged(a.projects)
    records, baseline_ok = [], 0
    for pid, title, text, stored in tagged:
        matches = matcher.find(text or "")
        baseline_ok += sorted({q for m in matches for q in m.qids}) == sorted(stored)
        records.append((pid, title, matches, normalize(text or ""), normalize(text or "", lower=False), stored))
    if baseline_ok != len(records):
        print(f"WARNING: stock matcher reproduces only {baseline_ok}/{len(records)} stored tag lists (different spaCy word list / keyword table?)", file=sys.stderr)

    kept, dropped, support = evaluate(records, cfg)
    before = Counter(q for r in records for q in r[5])
    ctx = dict(names=names, before=before, cfg=cfg, profile=a.profile, kept=kept, dropped=dropped, support=support, records=records,
               baseline_ok=baseline_ok, projects=a.projects)
    if out.suffix == ".csv":
        rows = exclusion_rows(records, kept, dropped, names)
        write_exclusions(rows, out)
        if report.name.startswith("MINORITY_EXCLUSIONS") or a.report:
            write_exclusion_report(report, {**ctx, "rows": rows})
        n = len(rows)
        print(f"profile {a.profile}: {len(records):,} tagged projects, {n:,} (project, group) pairs to exclude ({100 * n / max(1, sum(before.values())):.1f}% of {sum(before.values()):,}); "
              f"wrote {out} and {report}")
    else:
        alts = {}
        for field, values in (("signal", ("narrow", "broad")), ("single_typo", ("second-signal", "keep", "drop")), ("hebrew", ("institution", "context", "drop", "keep"))):
            for v in values:
                alts[(field, v)] = evaluate(records, Cfg(**{**cfg.__dict__, field: v}))[0]
        n = write_override(kept, out)
        write_report(report, {**ctx, "alts": alts})
        after = count_by_group(kept)
        sha = hashlib.sha256(out.read_bytes()).hexdigest()[:12]
        print(f"profile {a.profile}: {len(records):,} tagged projects -> {n:,} keep >= 1 tag; tags {sum(before.values()):,} -> {sum(after.values()):,}; wrote {out} "
              f"({out.stat().st_size / 1e3:.0f} kB, sha256 {sha}) and {report}")


if __name__ == "__main__":
    main()
