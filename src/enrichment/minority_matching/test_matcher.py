import pytest

from enrichment.minority_matching.matcher import (
    Group,
    MinorityMatcher,
    Rules,
    load_groups,
    load_rules,
    normalize,
)

GROUPS = [
    Group("Q7325", ["Jewish", "Jews", "Jew", "yiddish", "Jewish people"], "Jewish people"),
    Group("Q48199", ["sami", "Sámi", "Saami", "same", "joik", "Sámi people"], "Sámi people"),
    Group("Q1799968", ["ladin", "gardenese"], "Ladins"),
    Group("Q185461", ["Suraye", "Assyrians"], "Assyrians"),
    Group("Q377085", ["Suraye", "Arameans"], "Arameans"),
    Group("Q1", ["Kashubians", "Kaszëbi", "Silesians"], "Kashubians"),
    Group("Q2", ["Eli", "Watter"], "Eli"),
]
RULES = Rules(common_words={"same"}, blocklist={"watter", "eli"})


@pytest.fixture(scope="module")
def m():
    return MinorityMatcher(GROUPS, RULES)


def test_normalize():
    assert normalize("  Sámi   Ładin\tßeta ") == "sami ladin sseta"
    assert normalize("Vad’d’a") == "vad'd'a"
    assert normalize("Sámi", lower=False) == "Sami"


def test_accents_and_case_insensitive_for_long_keywords(m):
    assert m.qids("Music of the SÁMI people, from Sápmi") == ["Q48199"]  # 'sámi people' is long: any case
    assert m.qids("kaszebi and SILESIANS") == ["Q1"]
    assert m.qids("SAAMI culture") == ["Q48199"]


def test_whole_word_boundaries(m):
    assert m.qids("Jewishness and Jewelry") == []  # Jewish/Jew inside longer words
    assert m.qids("ladins, ladin.") == ["Q1799968"]
    assert m.qids("ladinsky") == []
    assert m.qids("Jewish-American history") == ["Q7325"]  # punctuation is a boundary
    assert m.qids("XJews") == []


def test_short_keywords_need_exact_capitalisation(m):
    assert m.qids("Sami music") == ["Q48199"]
    assert m.qids("Sámi music") == ["Q48199"]  # accent stripped, case kept
    assert m.qids("sami music") == []
    assert m.qids("SAMI music") == []
    assert m.qids("Jews and Jew") == ["Q7325"]
    assert m.qids("jews") == []
    assert m.qids("Joik is a song") == ["Q48199"]


def test_common_english_word_capitalised_only(m):
    assert m.qids("the same result") == []
    assert m.qids("The same result") == []
    assert m.qids("Same as before") == ["Q48199"]  # accepted cost of the capitalised-only rule


def test_blocklist_never_matches(m):
    assert m.qids("Eli Watter watter WATTER") == []
    assert "watter" in m.blocked and "eli" in m.blocked


def test_multi_qid_keyword_returns_all_groups(m):
    assert m.qids("The Suraye of Turabdin") == ["Q185461", "Q377085"]


def test_typo_tolerance_only_for_long_keywords(m):
    assert m.qids("Gardenese valley") == ["Q1799968"]
    assert m.qids("Gardenes valley") == ["Q1799968"]  # deletion
    assert m.qids("Gardenesse valley") == ["Q1799968"]  # insertion
    assert m.qids("Gardenese") == ["Q1799968"]
    assert m.qids("Gradenese") == ["Q1799968"]  # transposition
    assert m.qids("Kashubiams") == ["Q1"]  # substitution
    assert m.qids("Yidish") == ["Q7325"]
    assert m.qids("Jewishh people") == ["Q7325"]
    # short / ambiguous keywords get no variants
    assert m.qids("Jow") == []
    assert m.qids("Samj") == []
    assert m.qids("ladan") == []  # 'ladin' is 5 chars: below the typo minimum
    # first letter is never varied
    assert m.qids("Xardenese") == []


def test_typo_matches_are_flagged_and_variant_never_shadows_exact():
    matcher = MinorityMatcher([Group("A", ["hebrews"]), Group("B", ["hebrew"])], Rules())
    # 'hebrew' is an exact keyword of B and also a deletion-variant of A's 'hebrews': stays B
    assert matcher.qids("hebrew") == ["B"]
    assert matcher.qids("hebrews") == ["A"]
    (hit,) = matcher.find("hebrews")
    assert not hit.typo
    (hit,) = MinorityMatcher([Group("A", ["hebrews"])], Rules()).find("hebrewz")
    assert hit.typo and hit.keyword == "hebrews"


def test_variant_shared_by_two_groups_is_dropped():
    matcher = MinorityMatcher([Group("A", ["abcdefg"]), Group("B", ["abcdefh"])], Rules())
    assert matcher.qids("abcdefg") == ["A"]
    assert matcher.qids("abcdefx") == []  # one substitution away from both


def test_no_typo_switch():
    assert MinorityMatcher(GROUPS, RULES, typo=False).qids("Gardenes") == []


def test_empty_and_non_text(m):
    assert m.qids("") == []
    assert m.find("") == []


def test_load_groups_overrides_and_titular():
    rows = [
        ("Q7325", ["Q7325", "Q999"], "Jewish people", ["Jewish"]),
        ("Q42884", ["Q42884"], "Germans", ["Germans"]),
        ("Q5", None, "Zed people", None),
    ]
    groups = load_groups(rows, term_overrides={"Q999": ["hebrew"]}, titular_majority={"Q42884"})
    assert [g.qid for g in groups] == ["Q7325", "Q5"]
    assert "hebrew" in groups[0].keywords  # override keyed by a merged qid is found
    assert MinorityMatcher(groups, Rules()).qids("Hebrew poems") == ["Q7325"]
    assert MinorityMatcher(groups, Rules()).qids("Zed people") == ["Q5"]  # the name itself is a keyword


def test_default_rules_file_loads():
    rules = load_rules()
    assert "same" in rules.common_words and "eli" in rules.blocklist


def test_always_match_exempts_short_distinctive_keywords():
    g = [Group("Q48199", ["joik", "sami"])]
    strict = MinorityMatcher(g, Rules())
    relaxed = MinorityMatcher(g, Rules(always_match={"joik"}))
    assert strict.qids("a joik song") == []
    assert relaxed.qids("a joik song") == ["Q48199"]
    assert relaxed.qids("a sami song") == []  # not exempt
