"""
Fetches everything NLLB needs into data/models/nllb/. Compute nodes have no internet:
run this on the login node, once. Every other module only *reads* these paths and raises
ModelNotDownloadedError (with the command to run) when something is missing.

Layout (MODELS_DIR = <project root>/data/models/nllb):
    lid218e.bin                         fastText LID-218 (FLORES codes, CC-BY-NC 4.0)
    <key>/hf/                           HF snapshot: tokenizer, sentencepiece.bpe.model, weights
    <key>/ct2-<quantization>/           CTranslate2 conversion of <key>/hf (+ sentencepiece.bpe.model)
    (<key> is "600M" or "1.3B": the distilled NLLB-200 checkpoints)

Usage (login node):
    uv run python -m enrichment.nllb_translator.download                      # LID + 600M + 1.3B, float16
    uv run python -m enrichment.nllb_translator.download --models 600M --quantization int8_float16
    uv run python -m enrichment.nllb_translator.download --skip-ct2           # HF weights only
"""

import argparse
import logging
import shutil
import urllib.request
from pathlib import Path
from typing import Optional

from common.file_handling.path_utils import get_project_root_path

MODELS_DIR = get_project_root_path() / "data/models/nllb"

LID_URL = "https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin"
LID_PATH = MODELS_DIR / "lid218e.bin"

# Both distilled; "1.3B" is facebook/nllb-200-distilled-1.3B (the non-distilled 1.3B is not compared).
NLLB_REPOS = {
    "600M": "facebook/nllb-200-distilled-600M",
    "1.3B": "facebook/nllb-200-distilled-1.3B",
}
DEFAULT_QUANTIZATION = "float16"
SPM_FILE = "sentencepiece.bpe.model"

DOWNLOAD_HINT = (
    "Compute nodes have no internet access. Download once on the login node:\n"
    "    uv run python -m enrichment.nllb_translator.download"
)


class ModelNotDownloadedError(FileNotFoundError):
    pass


def hf_dir(key: str) -> Path:
    return MODELS_DIR / key / "hf"


def ct2_dir(key: str, quantization: str = DEFAULT_QUANTIZATION) -> Path:
    return MODELS_DIR / key / f"ct2-{quantization}"


def _require(path: Path, what: str) -> Path:
    if not path.exists():
        raise ModelNotDownloadedError(f"{what} not found at {path}.\n{DOWNLOAD_HINT}")
    return path


def require_lid() -> Path:
    return _require(LID_PATH, "fastText LID-218 model")


def require_model(key: str, backend: str, quantization: str = DEFAULT_QUANTIZATION) -> Path:
    """Path of the model dir for `backend` ("ctranslate2" or "transformers")."""
    if key not in NLLB_REPOS:
        raise ValueError(f"unknown NLLB model {key!r}; choose from {sorted(NLLB_REPOS)}")
    if backend == "ctranslate2":
        return _require(ct2_dir(key, quantization) / "model.bin", f"CTranslate2 {key} ({quantization}) model").parent
    if backend == "transformers":
        return _require(hf_dir(key) / "config.json", f"Transformers {key} model").parent
    raise ValueError(f"unknown backend {backend!r}")


def spm_path(key: str) -> Path:
    return _require(hf_dir(key) / SPM_FILE, f"NLLB sentencepiece model of {key}")


def download_lid() -> Path:
    if LID_PATH.exists():
        return LID_PATH
    LID_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LID_PATH.with_suffix(".tmp")
    logging.info(f"Downloading LID-218 from {LID_URL} ...")
    with urllib.request.urlopen(LID_URL) as resp, open(tmp, "wb") as f:
        shutil.copyfileobj(resp, f, 1 << 20)
    tmp.rename(LID_PATH)
    return LID_PATH


def download_hf(key: str) -> Path:
    from huggingface_hub import snapshot_download

    target = hf_dir(key)
    logging.info(f"Downloading {NLLB_REPOS[key]} -> {target}")
    # Weights as safetensors/bin only once; skip the TF/flax/onnx duplicates.
    snapshot_download(
        NLLB_REPOS[key],
        local_dir=str(target),
        allow_patterns=["*.json", "*.model", "*.txt", "*.safetensors", "pytorch_model*.bin"],
    )
    return target


def convert_ct2(key: str, quantization: str = DEFAULT_QUANTIZATION) -> Path:
    from ctranslate2.converters import TransformersConverter

    target = ct2_dir(key, quantization)
    if (target / "model.bin").exists():
        return target
    logging.info(f"Converting {key} to CTranslate2 ({quantization}) -> {target}")
    TransformersConverter(str(hf_dir(key))).convert(str(target), quantization=quantization, force=True)
    shutil.copy(hf_dir(key) / SPM_FILE, target / SPM_FILE)
    return target


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--models", nargs="+", default=list(NLLB_REPOS), choices=list(NLLB_REPOS))
    parser.add_argument("--quantization", default=DEFAULT_QUANTIZATION, help="CTranslate2 weight type")
    parser.add_argument("--skip-ct2", action="store_true")
    parser.add_argument("--skip-lid", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if not args.skip_lid:
        download_lid()
    for key in args.models:
        download_hf(key)
        if not args.skip_ct2:
            convert_ct2(key, args.quantization)
    logging.info("done")


if __name__ == "__main__":
    main()
