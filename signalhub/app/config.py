from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VIRTUALS_ENDPOINT = "https://api2.virtuals.io/api/virtuals"
DEFAULT_VIRTUALS_APP_BASE_URL = "https://app.virtuals.io"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue

        env_value = value.strip()
        if (
            len(env_value) >= 2
            and env_value[0] == env_value[-1]
            and env_value[0] in {"'", '"'}
        ):
            env_value = env_value[1:-1]

        os.environ[env_key] = env_value


_load_env_file(PROJECT_ROOT / ".env")


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
    token_pool_export_path: Path
    virtuals_endpoint: str | None
    virtuals_app_base_url: str
    chainstack_base_https_url: str | None
    chainstack_base_wss_url: str | None
    chainstack_subscription_enabled: bool
    chainstack_subscription_refresh_seconds: int
    chainstack_trace_backfill_enabled: bool
    chainstack_trace_backfill_batch_size: int
    chainstack_trace_backfill_cooldown_seconds: int
    chainstack_log_chunk_size: int
    chainstack_earliest_scan_window_blocks: int
    chainstack_earliest_batch_size: int
    chainstack_pattern_log_limit: int
    chainstack_prelaunch_statuses: tuple[str, ...]
    etherscan_api_key: str | None
    etherscan_base_api_url: str
    base_chain_id: str
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
    etherscan_api_key = (
        os.getenv("BASESCAN_API_KEY")
        or os.getenv("ETHERSCAN_API_KEY")
        or ""
    ).strip() or None
    chainstack_base_https_url = (os.getenv("CHAINSTACK_BASE_HTTPS_URL") or "").strip() or None
    chainstack_base_wss_url = (os.getenv("CHAINSTACK_BASE_WSS_URL") or "").strip() or None

    return Settings(
        app_name=os.getenv("APP_NAME", "SignalHub - Virtuals Monitor"),
        db_path=Path(os.getenv("SIGNALHUB_DB_PATH", PROJECT_ROOT / "signalhub.db")),
        token_pool_export_path=Path(
            os.getenv(
                "TOKEN_POOL_EXPORT_PATH",
                PROJECT_ROOT / "exports" / "token-pools.json",
            )
        ),
        virtuals_endpoint=virtuals_endpoint,
        virtuals_app_base_url=os.getenv("VIRTUALS_APP_BASE_URL", DEFAULT_VIRTUALS_APP_BASE_URL),
        chainstack_base_https_url=chainstack_base_https_url,
        chainstack_base_wss_url=chainstack_base_wss_url,
        chainstack_subscription_enabled=_to_bool(os.getenv("CHAINSTACK_SUBSCRIPTION_ENABLED"), True),
        chainstack_subscription_refresh_seconds=max(
            int(os.getenv("CHAINSTACK_SUBSCRIPTION_REFRESH_SECONDS", "30")),
            5,
        ),
        chainstack_trace_backfill_enabled=_to_bool(
            os.getenv("CHAINSTACK_TRACE_BACKFILL_ENABLED"),
            True,
        ),
        chainstack_trace_backfill_batch_size=max(
            int(os.getenv("CHAINSTACK_TRACE_BACKFILL_BATCH_SIZE", "6")),
            1,
        ),
        chainstack_trace_backfill_cooldown_seconds=max(
            int(os.getenv("CHAINSTACK_TRACE_BACKFILL_COOLDOWN_SECONDS", "180")),
            5,
        ),
        chainstack_log_chunk_size=max(
            int(os.getenv("CHAINSTACK_LOG_CHUNK_SIZE", "100")),
            1,
        ),
        chainstack_earliest_scan_window_blocks=max(
            int(os.getenv("CHAINSTACK_EARLIEST_SCAN_WINDOW_BLOCKS", "3000")),
            100,
        ),
        chainstack_earliest_batch_size=max(
            int(os.getenv("CHAINSTACK_EARLIEST_BATCH_SIZE", "12")),
            1,
        ),
        chainstack_pattern_log_limit=max(
            int(os.getenv("CHAINSTACK_PATTERN_LOG_LIMIT", "3")),
            1,
        ),
        chainstack_prelaunch_statuses=_to_csv_list(
            os.getenv("CHAINSTACK_PRELAUNCH_STATUSES", "PRE LAUNCH,PRE_LAUNCH,PRELAUNCH")
        ),
        etherscan_api_key=etherscan_api_key,
        etherscan_base_api_url=os.getenv("ETHERSCAN_BASE_API_URL", "https://api.etherscan.io/v2/api").strip(),
        base_chain_id=os.getenv("BASE_CHAIN_ID", "8453").strip() or "8453",
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
