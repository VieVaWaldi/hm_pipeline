import gc
import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import PyPDF2

from common.sanitizers.parse_text import parse_content


def pdf_to_text(pdf_path: Path) -> Optional[str]:
    try:
        chunks = []

        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            page_count = len(reader.pages)

            for page_num in range(page_count):
                try:
                    page = reader.pages[page_num]
                    text = page.extract_text()
                    text = sanitize_pdf_text(text)
                    text = parse_content(text)

                    if text:
                        chunks.append(text)

                except Exception as page_error:
                    logging.warning(
                        f"Error processing page {page_num} in {pdf_path}: {page_error}"
                    )
                    continue

        gc.collect()
        full_text = "\n\n".join(chunks)
        return full_text

    except Exception as e:
        logging.warning(f"Failed to OCR pdf from: {pdf_path},\n{e}")
        gc.collect()  # Ensure memory is freed even after an error
        return None


def sanitize_pdf_text(text: Optional[str]) -> Optional[str]:
    """
    Sanitizes text extracted from PDFs to ensure clean UTF-8 output.

    Handles illegal characters, non-standard encodings, and special symbols
    commonly found in academic publications. Preserves meaningful content
    while removing problematic characters that could cause encoding errors.
    """
    if not text:
        return None

    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    replacements = {
        "\ufeff": "",
        "\u2028": "\n",
        "\u2029": "\n",
        "\xad": "-",
        "–": "-",
        "—": "-",
        '"': '"',
        "…": "...",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = "".join(c for c in text if c.isprintable() or c in "\n\t")
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip() or None


if __name__ == "__main__":
    file_path = Path(
        "/Users/wehrenberger/Downloads/Exported Items/files/14/D4.1_Requirements_Analysis_v1.0.pdf"
    )
    print(pdf_to_text(file_path))
