from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


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


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str
    db_path: Path
    virtuals_endpoint: str | None
    poll_interval_seconds: int
    request_timeout_seconds: int
    source_name: str
    source_type: str
    source_enabled: bool
    sample_mode: bool
    sample_data_path: Path
    dashboard_path: Path
    virtuals_headers: dict[str, str]


def load_settings() -> Settings:
    virtuals_endpoint = os.getenv("VIRTUALS_ENDPOINT")
    default_sample_mode = virtuals_endpoint is None

    return Settings(
        app_name=os.getenv("APP_NAME", "SignalHub - Virtuals Monitor"),
        db_path=Path(os.getenv("SIGNALHUB_DB_PATH", PROJECT_ROOT / "signalhub.db")),
        virtuals_endpoint=virtuals_endpoint,
        poll_interval_seconds=max(int(os.getenv("POLL_INTERVAL_SECONDS", "30")), 5),
        request_timeout_seconds=max(int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15")), 3),
        source_name=os.getenv("SOURCE_NAME", "virtuals_projects"),
        source_type=os.getenv("SOURCE_TYPE", "http_polling"),
        source_enabled=_to_bool(os.getenv("SOURCE_ENABLED"), True),
        sample_mode=_to_bool(os.getenv("VIRTUALS_SAMPLE_MODE"), default_sample_mode),
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
        virtuals_headers=_to_headers(os.getenv("VIRTUALS_HEADERS")),
    )
