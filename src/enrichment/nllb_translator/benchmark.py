"""
Measurements for README.md: language mix of the sample, LID-threshold tuning, model/backend speed and
VRAM, and a GPU-hour estimate. Input is the parquet written by sample_works.py.

    stage lid    (CPU)  language mix + LID probability distribution + agreement with OpenAire's label
    stage speed  (GPU)  each --models x --backends on the same texts: chunks/s, source tokens/s, VRAM

Results go to --out (json) and are printed; nothing here writes to the pipeline's side outputs.

    uv run python -m enrichment.nllb_translator.benchmark --stage lid
    uv run python -m enrichment.nllb_translator.benchmark --stage speed --models 600M 1.3B \
        --backends ctranslate2 transformers
"""

import argparse
import json
import logging
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path

import duckdb

from common.file_handling.path_utils import get_project_root_path
from enrichment.nllb_translator import download
from enrichment.nllb_translator.language_id import ENGLISH, LanguageIdentifier

SAMPLE = get_project_root_path() / "data/enrichment/nllb_sample/works_2018_org_only.parquet"
OUT_DIR = get_project_root_path() / "data/enrichment/nllb_sample"
THRESHOLDS = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
FULL_SET_WORKS = 50_000_000  # the core_v4 cap
DESC_CHARS = 1500


def _dir_size_gb(path: Path) -> float:
    return round(sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e9, 2)


def _load(sample: Path, limit=None):
    q = f"SELECT id, oa_lang, title, description FROM read_parquet('{sample}')" + (f" LIMIT {limit}" if limit else "")
    return duckdb.sql(q).fetchall()


def stage_lid(args) -> dict:
    rows = _load(Path(args.sample))
    lid = LanguageIdentifier()
    t0 = time.perf_counter()
    preds = {"title": [], "description": []}
    for _, _, title, description in rows:
        preds["title"].append(lid.predict(title))
        preds["description"].append(lid.predict(description))
    elapsed = time.perf_counter() - t0
    out = {"n_works": len(rows), "lid_texts_per_second": round(2 * len(rows) / elapsed)}
    for field, ps in preds.items():
        texts = sum(1 for label, _ in ps if label is not None)
        non_en = [(l, p) for l, p in ps if l is not None and l != ENGLISH]
        out[field] = {
            "identifiable_texts (>=20 chars)": texts,
            "share_of_works_identifiable": round(texts / len(rows), 4),
            "non_english_share_by_min_prob": {str(th): round(sum(1 for _, p in non_en if p >= th) / len(rows), 4) for th in THRESHOLDS},
            "non_english_share_of_identifiable_by_min_prob": {
                str(th): round(sum(1 for _, p in non_en if p >= th) / max(texts, 1), 4) for th in THRESHOLDS
            },
            "top_languages_prob>=0.5": Counter(l for l, p in non_en if p >= 0.5).most_common(15),
            "prob_histogram_non_english": {f"{lo:.1f}-{lo + 0.1:.1f}": sum(1 for _, p in non_en if lo <= p < lo + 0.1) for lo in [i / 10 for i in range(10)]},
        }
    # any field non-English (a work is "affected" if title or description is translated)
    for th in THRESHOLDS:
        affected = sum(
            1
            for i in range(len(rows))
            if any(preds[f][i][0] not in (None, ENGLISH) and preds[f][i][1] >= th for f in preds)
        )
        out.setdefault("works_with_any_non_english_field_by_min_prob", {})[str(th)] = round(affected / len(rows), 4)
    # agreement with OpenAire's label (for the README: why we ignore it)
    und = sum(1 for r in rows if r[1] in (None, "und"))
    out["openaire_label_und_or_null_share"] = round(und / len(rows), 4)
    out["openaire_label_eng_but_lid_non_english_title_p>=0.8"] = sum(
        1 for r, (l, p) in zip(rows, preds["title"]) if r[1] == "eng" and l not in (None, ENGLISH) and p >= 0.8
    )
    out["openaire_label_und_but_lid_non_english_title_p>=0.8"] = sum(
        1 for r, (l, p) in zip(rows, preds["title"]) if r[1] in (None, "und") and l not in (None, ENGLISH) and p >= 0.8
    )
    out["openaire_label_non_eng_but_lid_english_title_p>=0.8"] = sum(
        1 for r, (l, p) in zip(rows, preds["title"]) if r[1] not in (None, "und", "eng") and l == ENGLISH and p >= 0.8
    )
    # token statistics of the translatable texts, for the GPU-hour estimate
    from enrichment.nllb_translator.translator import _Tokenizer, truncate_text

    tok = _Tokenizer("600M")
    tokens_per_work = []
    for i, (_, _, title, description) in enumerate(rows):
        n = 0
        for field, text, limit in (("title", title, 1500), ("description", description, DESC_CHARS)):
            label, prob = preds[field][i]
            if label not in (None, ENGLISH) and prob >= args.min_prob and text:
                n += sum(len(c) for c in tok.chunk(truncate_text(text, limit), 200))
        tokens_per_work.append(n)
    out["mean_source_tokens_per_work_at_min_prob"] = {"min_prob": args.min_prob, "mean": round(sum(tokens_per_work) / len(rows), 2)}
    return out


class _VramPoller(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.peak = 0
        self.baseline = None  # memory already in use before this run (leftovers of the previous one)
        self.stop = threading.Event()

    def run(self):
        while not self.stop.is_set():
            try:
                used = subprocess.run(
                    ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"], capture_output=True, text=True
                ).stdout.split()[0]
                self.baseline = int(used) if self.baseline is None else self.baseline
                self.peak = max(self.peak, int(used))
            except Exception:
                pass
            time.sleep(0.5)


def _speed_texts(args, n_texts):
    """n_texts non-English texts (titles and descriptions mixed) chosen by LID from the sample."""
    lid = LanguageIdentifier()
    texts, langs = [], []
    for _, _, title, description in _load(Path(args.sample)):
        for text in (title, description):
            label, prob = lid.predict(text)
            if label not in (None, ENGLISH) and prob >= args.min_prob and label in args.supported:
                texts.append(text)
                langs.append(label)
        if len(texts) >= n_texts:
            break
    return texts[:n_texts], langs[:n_texts]


def stage_speed(args) -> dict:
    from enrichment.nllb_translator.translator import NllbTranslator

    from enrichment.nllb_translator.translator import TranslationStats

    cfg = json.loads((download.hf_dir(args.models[0]) / "special_tokens_map.json").read_text())
    args.supported = set(cfg["additional_special_tokens"])
    texts, langs = _speed_texts(args, args.n_texts)
    logging.info(f"{len(texts)} texts, {len(set(langs))} languages")
    results = {"n_texts": len(texts), "languages": Counter(langs).most_common(10), "runs": []}
    examples_idx = list(range(0, len(texts), max(1, len(texts) // 8)))[:8]
    examples = {"source": [texts[i][:300] for i in examples_idx], "lang": [langs[i] for i in examples_idx]}
    for model in args.models:
        for backend in args.backends:
            run = {"model": model, "backend": backend, "quantization": args.quantization, "beam_size": args.beam_size, "batch_tokens": args.batch_tokens, "grouped_by_language": not args.ungrouped, "inter_threads": args.inter_threads}
            try:
                poller = _VramPoller()
                poller.start()
                t0 = time.perf_counter()
                tr = NllbTranslator(model, backend=backend, quantization=args.quantization, beam_size=args.beam_size, batch_tokens=args.batch_tokens, group_by_language=not args.ungrouped, **({'inter_threads': args.inter_threads} if backend == 'ctranslate2' else {}))
                run["load_seconds"] = round(time.perf_counter() - t0, 1)
                tr.translate(texts[:32], langs[:32])  # warm-up (CUDA kernels, allocator)
                tr.stats = TranslationStats()
                out = tr.translate(texts, langs)
                s = tr.stats
                run.update(
                    chunks=s.chunks,
                    source_tokens=s.source_tokens,
                    seconds=round(s.seconds, 1),
                    sentences_per_second=round(s.chunks_per_second, 1),
                    tokens_per_second=round(s.tokens_per_second),
                )
                time.sleep(1)
                poller.stop.set()
                run["peak_vram_mb_incl_model"] = poller.peak - (poller.baseline or 0)
                examples[f"{model}/{backend}"] = [out[i][:300] for i in examples_idx]
                if args.dump:  # all outputs, for chrF against a reference configuration (score_outputs.py)
                    dump = Path(args.dump)
                    dump.parent.mkdir(parents=True, exist_ok=True)
                    dump.write_text("\n".join(json.dumps({"lang": l, "src": t, "out": o}, ensure_ascii=False) for t, l, o in zip(texts, langs, out)))
                del tr
            except Exception as e:  # e.g. CTranslate2 vs the driver: record it, keep going
                run["error"] = repr(e)
                logging.exception(f"{model}/{backend} failed")
            try:
                import gc

                gc.collect()
                import torch

                torch.cuda.empty_cache()
            except Exception:
                pass
            results["runs"].append(run)
            logging.info(run)
    results["examples"] = examples
    results["disk_gb"] = {
        f"{m}/{kind}": _dir_size_gb(p)
        for m in args.models
        for kind, p in (("hf", download.hf_dir(m)), (f"ct2-{args.quantization}", download.ct2_dir(m, args.quantization)))
        if p.exists()
    }
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage", choices=["lid", "speed"], required=True)
    parser.add_argument("--sample", default=str(SAMPLE))
    parser.add_argument("--out", default=None)
    parser.add_argument("--min-prob", type=float, default=0.5)
    parser.add_argument("--models", nargs="+", default=["600M", "1.3B"])
    parser.add_argument("--backends", nargs="+", default=["ctranslate2", "transformers"])
    parser.add_argument("--quantization", default="float16")
    parser.add_argument("--n-texts", type=int, default=3000)
    parser.add_argument("--beam-size", type=int, default=1)
    parser.add_argument("--batch-tokens", type=int, default=32768)
    parser.add_argument("--dump", default=None, help="write every translation to this jsonl")
    parser.add_argument("--inter-threads", type=int, default=1, help="CTranslate2 parallel batches on one GPU")
    parser.add_argument("--ungrouped", action="store_true", help="mix source languages within a batch")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    result = stage_lid(args) if args.stage == "lid" else stage_speed(args)
    out = Path(args.out or OUT_DIR / f"benchmark_{args.stage}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=str, ensure_ascii=False))
    print(json.dumps(result, indent=1, default=str, ensure_ascii=False))
    logging.info(f"written {out}")


if __name__ == "__main__":
    main()
