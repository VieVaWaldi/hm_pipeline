from pathlib import Path


def get_project_root_path() -> Path:
    """Return the absolute path to the project root directory."""
    return Path(__file__).resolve().parents[3]


def get_source_data_path(source_name: str, query_id: str | None) -> Path:
    """
    Returns the path to the raw source data.
    Source data is saved under /data/pile/{source_name}-{query_id}
    In dev mode source data is saved in project dir.
    In prod mode source data is saved on another place on disk, see config.
    """
    from common.config.queries import get_query_settings
    from common.config.settings import get_settings

    assert source_name in get_query_settings(), f"Faulty source name: {source_name}"

    settings = get_settings()
    path = (
        Path(settings.data_path) / f"{source_name}-query_id-{query_id}"
        if query_id is not None  # for data without queries, e.g. meta_heritage
        else Path(settings.data_path) / source_name
    )

    if settings.env == "dev":
        return get_project_root_path() / path
    return path
