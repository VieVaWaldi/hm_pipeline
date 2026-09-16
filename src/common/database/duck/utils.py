from pathlib import Path


def get_size_log(source: Path, db: Path) -> str:
    db_size_mb = db.stat().st_size / (1024**2)
    source_size_mb = source.stat().st_size / (1024**2)
    compression_ratio = source_size_mb / db_size_mb
    return f"Compression: {source_size_mb:.1f} MB -> {db_size_mb:.1f} MB ({compression_ratio:.2f}x)"
