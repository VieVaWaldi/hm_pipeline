from pathlib import Path
from typing import Optional, List


def count_files(path: Path, file_extension: str = ".xml") -> int:
    if not file_extension.startswith("."):
        file_extension = f".{file_extension}"

    return len(list(path.rglob(f"*{file_extension}")))


def find_pdfs_in_directory(file_path: Path) -> Optional[List[Path]]:
    """
    Given a Path to a file, finds all PDF files in the same directory.

    Args:
        file_path (Path): Path to a file

    Returns:
        Optional[List[Path]]: List of Paths to PDF files found in the directory,
                            or None if no PDFs are found or path is invalid
    """
    try:
        directory = file_path.parent

        if not file_path.is_file():
            return None

        pdf_files = list(directory.glob("*.pdf"))
        return pdf_files if pdf_files else None

    except Exception:
        return None
