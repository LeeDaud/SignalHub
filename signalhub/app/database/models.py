from __future__ import annotations

import hashlib
import json
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


def links_to_json(links: list[str] | tuple[str, ...]) -> str:
    normalized = sorted({item.strip() for item in links if item and item.strip()})
    return json.dumps(normalized, ensure_ascii=False)


def links_from_json(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item).strip() for item in payload if str(item).strip()]


@dataclass(slots=True)
class ProjectEntity:
    project_id: str
    name: str
    symbol: str
    url: str
    token_address: str
    contract_address: str
    internal_market_address: str
    status: str
    description: str
    creator: str
    team: str
    links_json: str
    tokenomics: str
    created_time: str | None
    launch_time: str | None
    last_seen: str
    raw_hash: str
    lifecycle_stage: str = "detected"
    project_score: int = 0
    risk_level: str = "high"
    watchlist: bool = False

    def as_record(self) -> dict[str, Any]:
        return asdict(self)

    def links(self) -> list[str]:
        return links_from_json(self.links_json)

    @classmethod
    def from_row(cls, row: Any) -> "ProjectEntity":
        return cls(
            project_id=row["project_id"],
            name=row["name"],
            symbol=row["symbol"],
            url=row["url"],
            token_address=row["token_address"],
            contract_address=row["contract_address"],
            internal_market_address=row["internal_market_address"],
            status=row["status"],
            description=row["description"],
            creator=row["creator"],
            team=row["team"],
            links_json=row["links_json"],
            tokenomics=row["tokenomics"],
            created_time=row["created_time"],
            launch_time=row["launch_time"],
            last_seen=row["last_seen"],
            raw_hash=row["raw_hash"],
            lifecycle_stage=row["lifecycle_stage"],
            project_score=int(row["project_score"]),
            risk_level=row["risk_level"],
            watchlist=bool(row["watchlist"]),
        )


@dataclass(slots=True)
class ProjectSnapshot:
    project_id: str
    snapshot_time: str
    description: str
    team: str
    links_json: str
    tokenomics: str
    launch_time: str | None
    raw_data: dict[str, Any]

    def links(self) -> list[str]:
        return links_from_json(self.links_json)


@dataclass(slots=True)
class ProjectChange:
    project_id: str
    change_type: str
    field: str
    old_value: str | None
    new_value: str | None
    time: str


@dataclass(slots=True)
class ProjectAddress:
    project_id: str
    address: str
    address_type: str
    chain: str
    first_seen: str


@dataclass(slots=True)
class ProjectLaunchTrace:
    project_id: str
    token_contract: str
    launch_tx_hash: str
    launch_block_number: int | None
    internal_market_address: str
    intermediate_address: str
    mint_recipient: str
    launch_sender: str
    launch_target: str
    confidence: str
    evidence: str
    subscription_status: str
    first_seen: str
    last_seen: str
    last_error: str = ""
    notes_json: str = "[]"

    def notes(self) -> list[str]:
        return links_from_json(self.notes_json)

    @classmethod
    def from_row(cls, row: Any) -> "ProjectLaunchTrace":
        return cls(
            project_id=row["project_id"],
            token_contract=row["token_contract"],
            launch_tx_hash=row["launch_tx_hash"],
            launch_block_number=row["launch_block_number"],
            internal_market_address=row["internal_market_address"],
            intermediate_address=row["intermediate_address"],
            mint_recipient=row["mint_recipient"],
            launch_sender=row["launch_sender"],
            launch_target=row["launch_target"],
            confidence=row["confidence"],
            evidence=row["evidence"],
            subscription_status=row["subscription_status"],
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            last_error=row["last_error"],
            notes_json=row["notes_json"],
        )


@dataclass(slots=True)
class ParsedProject:
    entity: ProjectEntity
    snapshot: ProjectSnapshot
    addresses: list[ProjectAddress]
    raw_data: dict[str, Any]


@dataclass(slots=True)
class SignalEvent:
    event_id: str
    source: str
    type: str
    target: str
    time: str
    payload: dict[str, Any]
