import pytest

from enrichment.nllb_translator import download
from enrichment.nllb_translator.language_id import LanguageIdentifier, is_shouting, clean_for_lid, latin_share, script_consistent, should_translate
from enrichment.nllb_translator.translator import NllbTranslator, split_sentences, truncate_text

needs_models = pytest.mark.skipif(not (download.hf_dir("600M") / download.SPM_FILE).exists(), reason="run download on the login node")


def test_split_sentences():
    assert split_sentences("Erster Satz. Zweiter Satz! Dritter?") == ["Erster Satz.", "Zweiter Satz!", "Dritter?"]
    assert split_sentences("これは文です。次の文です。") == ["これは文です。", "次の文です。"]
    assert split_sentences("  ") == []
    assert split_sentences("no terminator") == ["no terminator"]


def test_truncate_prefers_sentence_end():
    text = "Aaa bbb. " * 30
    cut = truncate_text(text, 100)
    assert len(cut) <= 100 and cut.endswith(".")
    assert truncate_text("short", 100) == "short"
    words = "word " * 100
    assert len(truncate_text(words, 50)) <= 50
    assert not truncate_text("x" * 5_000_000, 1500)[1500:]  # a 5M-character description is cut, not processed


def test_lid_helpers():
    assert clean_for_lid("a\n\nb\t c") == "a b c"
    assert clean_for_lid(None) == ""
    assert should_translate("deu_Latn", 0.9)
    assert not should_translate("eng_Latn", 0.99)
    assert not should_translate("deu_Latn", 0.3)
    assert not should_translate(None, 0.0)


def test_download_guard_message(tmp_path, monkeypatch):
    monkeypatch.setattr(download, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(download, "LID_PATH", tmp_path / "lid218e.bin")
    with pytest.raises(download.ModelNotDownloadedError, match="login node"):
        download.require_lid()
    with pytest.raises(download.ModelNotDownloadedError, match="login node"):
        download.require_model("600M", "ctranslate2")
    with pytest.raises(ValueError):
        download.require_model("nope", "ctranslate2")


class FakeBackend:
    """Uppercases the decoded text; records how it was called."""

    name = "fake"

    def __init__(self):
        self.calls = []

    def translate(self, tokenizer, batch, src_langs):
        self.calls.append((sorted(set(src_langs)), [len(b) for b in batch]))
        return ["EN(" + tokenizer.decode(b) + ")" for b in batch]


@needs_models
def make_translator(max_chunk_tokens=200):
    tr = NllbTranslator.__new__(NllbTranslator)
    from enrichment.nllb_translator.translator import TranslationStats, _Tokenizer

    tr.model_key, tr.max_chunk_tokens, tr.max_chars, tr.group_by_language = "600M", max_chunk_tokens, 1500, False
    tr.tokenizer = _Tokenizer("600M")
    tr.backend = FakeBackend()
    tr.supported_languages = {"deu_Latn", "fra_Latn"}
    tr.stats = TranslationStats()
    return tr


@needs_models
def test_chunks_respect_token_limit_and_reassemble():
    tr = make_translator(max_chunk_tokens=20)
    sentence = "Dies ist ein ziemlich langer deutscher Satz über die digitale Kultur. "
    out = tr.translate([sentence * 10], ["deu_Latn"])
    assert len(tr.backend.calls) == 1
    lengths = tr.backend.calls[0][1]
    assert len(lengths) > 1 and max(lengths) <= 20
    assert lengths == sorted(lengths, reverse=True)  # longest first
    assert out[0].count("EN(") == len(lengths)


@needs_models
def test_single_unpunctuated_run_is_cut_on_tokens():
    tr = make_translator(max_chunk_tokens=10)
    tr.translate(["wort " * 200], ["deu_Latn"])
    assert max(tr.backend.calls[0][1]) <= 10


@needs_models
def test_groups_by_language_and_keeps_alignment():
    tr = make_translator()
    tr.group_by_language = True
    out = tr.translate(["Ein Satz.", "Une phrase.", "Noch ein Satz."], ["deu_Latn", "fra_Latn", "deu_Latn"])
    assert sorted(c[0][0] for c in tr.backend.calls) == ["deu_Latn", "fra_Latn"]
    assert len(tr.backend.calls) == 2  # one homogeneous batch per language
    assert [o.startswith("EN(") for o in out] == [True] * 3
    assert "Une phrase" in out[1] and "Noch ein Satz" in out[2]
    with pytest.raises(ValueError, match="unsupported"):
        tr.translate(["Hello."], ["xxx_Latn"])


def test_caps_are_lowercased_before_lid():
    assert clean_for_lid("AI-DRIVEN WASTE MANAGEMENT SYSTEMS") == "ai-driven waste management systems"
    assert clean_for_lid("Mixed Case Title Stays") == "Mixed Case Title Stays"
    assert clean_for_lid("UN NGO") == "UN NGO"  # too few letters to judge


def test_script_consistency():
    assert latin_share("hello wörld") == 1.0
    assert script_consistent("deu_Latn", "Die Zukunft")
    assert not script_consistent("kor_Hang", "Classification of MRI Using SVM")
    assert script_consistent("kor_Hang", "한국어 문장입니다")
    assert script_consistent("rus_Cyrl", "Цифровое наследие")
    assert not script_consistent("deu_Latn", "Цифровое наследие")


@pytest.mark.skipif(not download.LID_PATH.exists(), reason="run download on the login node")
def test_lid_real_model():
    lid = LanguageIdentifier()
    assert lid.predict("Die Zukunft der Museen im digitalen Zeitalter ist ungewiss.")[0] == "deu_Latn"
    assert lid.predict("AI-DRIVEN WASTE MANAGEMENT SYSTEMS: A COMPARATIVE REVIEW OF INNOVATIONS")[0] == "eng_Latn"
    assert lid.predict("short")[0] is None


@needs_models
def test_default_mixes_languages_in_one_batch():
    tr = make_translator()
    out = tr.translate(["Ein Satz.", "Une phrase.", "Noch ein Satz."], ["deu_Latn", "fra_Latn", "deu_Latn"])
    assert len(tr.backend.calls) == 1 and tr.backend.calls[0][0] == ["deu_Latn", "fra_Latn"]
    assert "Une phrase" in out[1] and "Noch ein Satz" in out[2]


def test_is_shouting():
    assert is_shouting("PERAN MOTIVASI BERWIRAUSAHA")
    assert not is_shouting("Peran Motivasi Berwirausaha")
    assert not is_shouting("DNA")  # too few letters


@needs_models
def test_caps_lowercased_before_translation():
    tr = make_translator()
    out = tr.translate(["PERAN MOTIVASI BERWIRAUSAHA MEMODERASI PENGARUH"], ["deu_Latn"])
    assert out[0] == "EN(peran motivasi berwirausaha memoderasi pengaruh)"
