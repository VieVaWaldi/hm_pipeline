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
    assert m.qids("Jewishh people") == ["Q7325"]
    # short / ambiguous keywords get no variants
    assert m.qids("Jow") == []
    assert m.qids("Samj") == []
    assert m.qids("ladan") == []  # 'ladin' is 5 chars: below the typo minimum
    assert m.qids("Yidish") == []  # 'yiddish' is 7 chars: below MIN_TYPO_LEN (8), typo variants only from 8 characters on
    # first letter is never varied
    assert m.qids("Xardenese") == []


def test_typo_matches_are_flagged_and_variant_never_shadows_exact():
    matcher = MinorityMatcher([Group("A", ["hebrewish"]), Group("B", ["hebrewis"])], Rules(), known_words=[])
    # 'hebrewis' is an exact keyword of B and also a deletion-variant of A's 'hebrewish': stays B
    assert matcher.qids("hebrewis") == ["B"]
    assert matcher.qids("hebrewish") == ["A"]
    (hit,) = matcher.find("hebrewish")
    assert not hit.typo
    (hit,) = MinorityMatcher([Group("A", ["hebrewish"])], Rules(), known_words=[]).find("hebrewishz")
    assert hit.typo and hit.keyword == "hebrewish"


def test_variant_shared_by_two_groups_is_dropped():
    matcher = MinorityMatcher([Group("A", ["abcdefgh"]), Group("B", ["abcdefgi"])], Rules(), known_words=[])
    assert matcher.qids("abcdefgh") == ["A"]
    assert matcher.qids("abcdefgx") == []  # one substitution away from both


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


# ---- typo false positives on real text (Phase 3F) ----------------------------------------------
from enrichment.minority_matching import matcher as matcher_module  # noqa: E402
from enrichment.minority_matching.matcher import MIN_TYPO_LEN, english_words, is_common_word  # noqa: E402


def test_typo_length_floor_is_a_named_constant_and_applies_from_that_length():
    assert MIN_TYPO_LEN == 8
    seven, eight = "abcdefg", "abcdefgh"
    assert MinorityMatcher([Group("A", [seven])], Rules(), known_words=[]).qids("abcdefx") == []  # 7 chars: exact only
    assert MinorityMatcher([Group("A", [seven])], Rules(), known_words=[]).qids(seven) == ["A"]
    assert MinorityMatcher([Group("A", [eight])], Rules(), known_words=[]).qids("abcdefgx") == ["A"]  # 8 chars: variants


def test_hazard_does_not_match_hazara_and_poland_does_not_match_polans():
    # regression: real Cordis text hit "hazard" 19 times (Hazara / Hazaras) and "plans" / "Poland" 27 times (Polans)
    m = MinorityMatcher([Group("Q1", ["Hazara", "Hazaras"], "Hazaras"), Group("Q2", ["Polans"], "Polans")], Rules())
    for text in ("hazard", "hazards", "risk hazard assessment", "plans", "Poland", "the plans for Poland"):
        assert m.qids(text) == [], text
    assert m.qids("The Hazara people") == ["Q1"]  # exact matches are untouched
    assert m.qids("hazaras") == ["Q1"] and m.qids("The Polans were a Slavic tribe") == ["Q2"]


def test_variant_that_is_a_common_english_word_is_never_generated():
    groups = [Group("Q1", ["Egyptians"]), Group("Q2", ["Balkarians"])]
    assert MinorityMatcher(groups, Rules(), known_words=[]).qids("Egyptian art") == ["Q1"]  # the word list off: it matches
    m = MinorityMatcher(groups, Rules())
    assert m.qids("Egyptian art") == []  # 'egyptian' is an English word: not a typo of Egyptians
    assert m.qids("Egyptians and Balkarians") == ["Q1", "Q2"]  # exact matches still work
    assert m.qids("Balkarian") == ["Q2"]  # not an English word: a real typo of a long keyword is still found
    assert m.qids("Balkarianz") == ["Q2"]
    assert MinorityMatcher(groups, Rules(), known_words=["balkarianz"]).qids("Balkarianz") == []  # the list is pluggable


def test_variant_equal_to_another_keyword_is_still_dropped_with_the_word_list():
    m = MinorityMatcher([Group("A", ["hebrewish"]), Group("B", ["hebrewis"])], Rules())
    assert m.qids("hebrewis") == ["B"]


def test_english_word_list_is_offline_and_inflection_aware():
    words = english_words()
    assert len(words) > 50_000
    assert {"hazard", "poland", "plan", "the", "egyptian"} <= words  # base forms, place names and stop words
    assert is_common_word("plans", words) and is_common_word("hazards", words)
    assert is_common_word("gardens", frozenset({"garden"})) and is_common_word("cities", frozenset({"city"}))
    assert is_common_word("boxes", frozenset({"box"})) and is_common_word("hoped", frozenset({"hope"}))
    assert is_common_word("running", frozenset({"run"})) is False  # only the plain rules, no consonant doubling
    assert not is_common_word("gardenes", frozenset({"garden"}))  # 'es' only after s/x/z/ch/sh
    assert english_words() is english_words()  # cached


def test_word_list_degrades_when_spacy_lookups_are_missing(monkeypatch, caplog):
    matcher_module.english_words.cache_clear()
    monkeypatch.setattr("spacy.util.get_package_path", lambda name: (_ for _ in ()).throw(OSError("no model")))
    try:
        with caplog.at_level("WARNING"):
            words = english_words()
        assert "the" in words and "hazard" not in words  # stop words only
        assert "lemma tables unavailable" in caplog.text
    finally:
        matcher_module.english_words.cache_clear()
