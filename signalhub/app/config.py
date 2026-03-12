from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VIRTUALS_ENDPOINT = "https://api2.virtuals.io/api/virtuals"
DEFAULT_VIRTUALS_APP_BASE_URL = "https://app.virtuals.io"


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _to_headers(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def _to_csv_list(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str
    db_path: Path
    virtuals_endpoint: str | None
    virtuals_app_base_url: str
    poll_interval_seconds: int
    request_timeout_seconds: int
    source_name: str
    source_type: str
    source_enabled: bool
    sample_mode: bool
    sample_data_path: Path
    dashboard_path: Path
    virtuals_mode: str
    virtuals_page_size: int
    virtuals_pages: int
    virtuals_detail_refresh_limit: int
    virtuals_sort: str
    virtuals_populate: tuple[str, ...]
    virtuals_statuses: tuple[str, ...]
    virtuals_categories: tuple[str, ...]
    virtuals_chain: str | None
    virtuals_headers: dict[str, str]


def load_settings() -> Settings:
    virtuals_endpoint = os.getenv("VIRTUALS_ENDPOINT", DEFAULT_VIRTUALS_ENDPOINT).strip() or None
    virtuals_mode = os.getenv("VIRTUALS_MODE", "upcoming_launches").strip().lower()
    default_sort = "launchedAt:asc" if virtuals_mode == "upcoming_launches" else "createdAt:desc"

    return Settings(
        app_name=os.getenv("APP_NAME", "SignalHub - Virtuals Monitor"),
        db_path=Path(os.getenv("SIGNALHUB_DB_PATH", PROJECT_ROOT / "signalhub.db")),
        virtuals_endpoint=virtuals_endpoint,
        virtuals_app_base_url=os.getenv("VIRTUALS_APP_BASE_URL", DEFAULT_VIRTUALS_APP_BASE_URL),
        poll_interval_seconds=max(int(os.getenv("POLL_INTERVAL_SECONDS", "30")), 5),
        request_timeout_seconds=max(int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15")), 3),
        source_name=os.getenv("SOURCE_NAME", "virtuals_projects"),
        source_type=os.getenv("SOURCE_TYPE", "http_polling"),
        source_enabled=_to_bool(os.getenv("SOURCE_ENABLED"), True),
        sample_mode=_to_bool(os.getenv("VIRTUALS_SAMPLE_MODE"), False),
        sample_data_path=Path(
            os.getenv(
                "SAMPLE_DATA_PATH",
                PROJECT_ROOT / "sample_data" / "virtuals_projects.json",
            )
        ),
        dashboard_path=Path(
            os.getenv(
                "DASHBOARD_PATH",
                PROJECT_ROOT / "signalhub" / "ui" / "dashboard" / "index.html",
            )
        ),
        virtuals_mode=virtuals_mode,
        virtuals_page_size=max(int(os.getenv("VIRTUALS_PAGE_SIZE", "100")), 1),
        virtuals_pages=max(int(os.getenv("VIRTUALS_PAGES", "2")), 1),
        virtuals_detail_refresh_limit=max(int(os.getenv("VIRTUALS_DETAIL_REFRESH_LIMIT", "50")), 1),
        virtuals_sort=os.getenv("VIRTUALS_SORT", default_sort),
        virtuals_populate=_to_csv_list(os.getenv("VIRTUALS_POPULATE", "image,creator")),
        virtuals_statuses=_to_csv_list(os.getenv("VIRTUALS_STATUSES")),
        virtuals_categories=_to_csv_list(os.getenv("VIRTUALS_CATEGORIES")),
        virtuals_chain=os.getenv("VIRTUALS_CHAIN"),
        virtuals_headers=_to_headers(os.getenv("VIRTUALS_HEADERS")),
    )
