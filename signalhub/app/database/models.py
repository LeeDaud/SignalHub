from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_raw_hash(name: str, status: str, description: str) -> str:
    raw = f"{name.strip()}|{status.strip()}|{description.strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()[:24]


def build_event_id() -> str:
    return f"evt_{uuid4().hex[:16]}"


@dataclass(slots=True)
class ProjectEntity:
    project_id: str
    name: str
    symbol: str
    url: str
    contract_address: str
    status: str
    description: str
    creator: str
    created_time: str | None
    launch_time: str | None
    last_seen: str
    raw_hash: str

    def as_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_row(cls, row: Any) -> "ProjectEntity":
        return cls(
            project_id=row["project_id"],
            name=row["name"],
            symbol=row["symbol"],
            url=row["url"],
            contract_address=row["contract_address"],
            status=row["status"],
            description=row["description"],
            creator=row["creator"],
            created_time=row["created_time"],
            launch_time=row["launch_time"],
            last_seen=row["last_seen"],
            raw_hash=row["raw_hash"],
        )


@dataclass(slots=True)
class SignalEvent:
    event_id: str
    source: str
    type: str
    target: str
    time: str
    payload: dict[str, Any]
