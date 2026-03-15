from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from signalhub.app.database.models import (
    ProjectAddress,
    ProjectChange,
    ProjectEntity,
    ProjectLaunchTrace,
    ProjectSnapshot,
    SignalEvent,
    links_from_json,
)


TABLES_SQL = """
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    token_address TEXT NOT NULL DEFAULT '',
    contract_address TEXT NOT NULL DEFAULT '',
    internal_market_address TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unknown',
    description TEXT NOT NULL DEFAULT '',
    creator TEXT NOT NULL DEFAULT '',
    team TEXT NOT NULL DEFAULT '',
    links_json TEXT NOT NULL DEFAULT '[]',
    tokenomics TEXT NOT NULL DEFAULT '',
    created_time TEXT,
    launch_time TEXT,
    last_seen TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    lifecycle_stage TEXT NOT NULL DEFAULT 'detected',
    project_score INTEGER NOT NULL DEFAULT 0,
    risk_level TEXT NOT NULL DEFAULT 'high',
    watchlist INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    target TEXT NOT NULL,
    time TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    endpoint TEXT,
    interval INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_run TEXT
);

CREATE TABLE IF NOT EXISTS project_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    snapshot_time TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    team TEXT NOT NULL DEFAULT '',
    links_json TEXT NOT NULL DEFAULT '[]',
    tokenomics TEXT NOT NULL DEFAULT '',
    launch_time TEXT,
    raw_data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    change_type TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    address TEXT NOT NULL,
    address_type TEXT NOT NULL,
    chain TEXT NOT NULL DEFAULT 'BASE',
    first_seen TEXT NOT NULL,
    UNIQUE(project_id, address, address_type, chain)
);

CREATE TABLE IF NOT EXISTS project_launch_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL UNIQUE,
    token_contract TEXT NOT NULL DEFAULT '',
    launch_tx_hash TEXT NOT NULL DEFAULT '',
    launch_block_number INTEGER,
    internal_market_address TEXT NOT NULL DEFAULT '',
    intermediate_address TEXT NOT NULL DEFAULT '',
    mint_recipient TEXT NOT NULL DEFAULT '',
    launch_sender TEXT NOT NULL DEFAULT '',
    launch_target TEXT NOT NULL DEFAULT '',
    confidence TEXT NOT NULL DEFAULT '',
    evidence TEXT NOT NULL DEFAULT '',
    subscription_status TEXT NOT NULL DEFAULT 'pending',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    notes_json TEXT NOT NULL DEFAULT '[]'
);
"""

INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_entities_last_seen ON entities(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_entities_launch_time ON entities(launch_time ASC);
CREATE INDEX IF NOT EXISTS idx_entities_token_address ON entities(token_address);
CREATE INDEX IF NOT EXISTS idx_entities_contract_address ON entities(contract_address);
CREATE INDEX IF NOT EXISTS idx_entities_stage ON entities(lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_entities_score ON entities(project_score DESC);
CREATE INDEX IF NOT EXISTS idx_entities_watchlist ON entities(watchlist);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(time DESC);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_target ON events(target);
CREATE INDEX IF NOT EXISTS idx_sources_name ON sources(name);
CREATE INDEX IF NOT EXISTS idx_snapshots_project_time ON project_snapshots(project_id, snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_changes_project_time ON project_changes(project_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_addresses_project ON project_addresses(project_id);
CREATE INDEX IF NOT EXISTS idx_launch_traces_internal_market ON project_launch_traces(internal_market_address);
CREATE INDEX IF NOT EXISTS idx_launch_traces_subscription_status ON project_launch_traces(subscription_status);
"""


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init_db(self) -> None:
        with self._connect() as connection:
            connection.executescript(TABLES_SQL)
            self._ensure_entities_columns(connection)
            self._ensure_launch_trace_columns(connection)
            self._sync_internal_market_addresses(connection)
            connection.executescript(INDEXES_SQL)
            connection.commit()

    def _ensure_entities_columns(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(entities)").fetchall()
        }
        if "launch_time" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN launch_time TEXT")
        if "token_address" not in existing_columns:
            connection.execute(
                "ALTER TABLE entities ADD COLUMN token_address TEXT NOT NULL DEFAULT ''"
            )
        if "contract_address" not in existing_columns:
            connection.execute(
                "ALTER TABLE entities ADD COLUMN contract_address TEXT NOT NULL DEFAULT ''"
            )
        if "internal_market_address" not in existing_columns:
            connection.execute(
                "ALTER TABLE entities ADD COLUMN internal_market_address TEXT NOT NULL DEFAULT ''"
            )
        if "team" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN team TEXT NOT NULL DEFAULT ''")
        if "links_json" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN links_json TEXT NOT NULL DEFAULT '[]'")
        if "tokenomics" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN tokenomics TEXT NOT NULL DEFAULT ''")
        if "lifecycle_stage" not in existing_columns:
            connection.execute(
                "ALTER TABLE entities ADD COLUMN lifecycle_stage TEXT NOT NULL DEFAULT 'detected'"
            )
        if "project_score" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN project_score INTEGER NOT NULL DEFAULT 0")
        if "risk_level" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN risk_level TEXT NOT NULL DEFAULT 'high'")
        if "watchlist" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN watchlist INTEGER NOT NULL DEFAULT 0")
        connection.execute(
            """
            UPDATE entities
            SET token_address = contract_address
            WHERE token_address = ''
              AND contract_address <> ''
            """
        )

    def _ensure_launch_trace_columns(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(project_launch_traces)").fetchall()
        }
        if "intermediate_address" not in existing_columns:
            connection.execute(
                "ALTER TABLE project_launch_traces ADD COLUMN intermediate_address TEXT NOT NULL DEFAULT ''"
            )

    def _sync_internal_market_addresses(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            UPDATE entities
            SET internal_market_address = (
                SELECT project_launch_traces.internal_market_address
                FROM project_launch_traces
                WHERE project_launch_traces.project_id = entities.project_id
                LIMIT 1
            )
            WHERE internal_market_address = ''
              AND EXISTS (
                  SELECT 1
                  FROM project_launch_traces
                  WHERE project_launch_traces.project_id = entities.project_id
                    AND project_launch_traces.internal_market_address <> ''
              )
            """
        )

    def upsert_source(
        self,
        *,
        name: str,
        source_type: str,
        endpoint: str | None,
        interval_seconds: int,
        enabled: bool,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO sources(name, type, endpoint, interval, enabled)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    type = excluded.type,
                    endpoint = excluded.endpoint,
                    interval = excluded.interval,
                    enabled = excluded.enabled
                """,
                (name, source_type, endpoint, interval_seconds, int(enabled)),
            )
            connection.commit()

    def update_source_last_run(self, name: str, last_run: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE sources SET last_run = ? WHERE name = ?",
                (last_run, name),
            )
            connection.commit()

    def get_source(self, name: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sources WHERE name = ?",
                (name,),
            ).fetchone()

        if row is None:
            return None

        return {
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "endpoint": row["endpoint"],
            "interval": row["interval"],
            "enabled": bool(row["enabled"]),
            "last_run": row["last_run"],
        }

    def get_entity(self, project_id: str) -> ProjectEntity | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE project_id = ?",
                (project_id,),
            ).fetchone()

        if row is None:
            return None
        return ProjectEntity.from_row(row)

    def upsert_entity(self, entity: ProjectEntity) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO entities(
                    project_id, name, symbol, url, token_address, contract_address, internal_market_address, status, description,
                    creator, team, links_json, tokenomics, created_time, launch_time,
                    last_seen, raw_hash, lifecycle_stage, project_score, risk_level, watchlist
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    name = excluded.name,
                    symbol = excluded.symbol,
                    url = excluded.url,
                    token_address = excluded.token_address,
                    contract_address = excluded.contract_address,
                    internal_market_address = excluded.internal_market_address,
                    status = excluded.status,
                    description = excluded.description,
                    creator = excluded.creator,
                    team = excluded.team,
                    links_json = excluded.links_json,
                    tokenomics = excluded.tokenomics,
                    created_time = excluded.created_time,
                    launch_time = excluded.launch_time,
                    last_seen = excluded.last_seen,
                    raw_hash = excluded.raw_hash,
                    lifecycle_stage = excluded.lifecycle_stage,
                    project_score = excluded.project_score,
                    risk_level = excluded.risk_level,
                    watchlist = excluded.watchlist
                """,
                (
                    entity.project_id,
                    entity.name,
                    entity.symbol,
                    entity.url,
                    entity.token_address,
                    entity.contract_address,
                    entity.internal_market_address,
                    entity.status,
                    entity.description,
                    entity.creator,
                    entity.team,
                    entity.links_json,
                    entity.tokenomics,
                    entity.created_time,
                    entity.launch_time,
                    entity.last_seen,
                    entity.raw_hash,
                    entity.lifecycle_stage,
                    entity.project_score,
                    entity.risk_level,
                    int(entity.watchlist),
                ),
            )
            connection.commit()

    def set_internal_market_address(self, project_id: str, internal_market_address: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE entities
                SET internal_market_address = ?
                WHERE project_id = ?
                """,
                (internal_market_address.strip(), project_id),
            )
            connection.commit()
        return cursor.rowcount > 0

    def create_event(self, event: SignalEvent) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO events(event_id, source, type, target, time, payload)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.source,
                    event.type,
                    event.target,
                    event.time,
                    json.dumps(event.payload, ensure_ascii=False),
                ),
            )
            connection.commit()

    def list_events(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        event_type: str | None = None,
        source: str | None = None,
        target: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        if event_type:
            clauses.append("type = ?")
            params.append(event_type)
        if source:
            clauses.append("source = ?")
            params.append(source)
        if target:
            clauses.append("target = ?")
            params.append(target)

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        params.extend([limit, offset])
        query = f"""
            SELECT event_id, source, type, target, time, payload
            FROM events
            {where_sql}
            ORDER BY time DESC, id DESC
            LIMIT ? OFFSET ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [self._event_row_to_dict(row) for row in rows]

    def count_events(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM events").fetchone()
        return int(row["count"])

    def list_entities(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        stage: str | None = None,
        upcoming_only: bool = False,
        watchlist_only: bool = False,
        order_by: str = "last_seen_desc",
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        clauses: list[str] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if stage:
            clauses.append("lifecycle_stage = ?")
            params.append(stage)

        if upcoming_only:
            clauses.append("launch_time IS NOT NULL")
            clauses.append("datetime(launch_time) > datetime('now')")
        if watchlist_only:
            clauses.append("watchlist = 1")

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        order_sql = "ORDER BY last_seen DESC, id DESC"
        if order_by == "launch_time_asc":
            order_sql = "ORDER BY launch_time ASC, id DESC"
        elif order_by == "created_time_desc":
            order_sql = "ORDER BY created_time DESC, id DESC"
        elif order_by == "score_desc":
            order_sql = "ORDER BY project_score DESC, last_seen DESC, id DESC"

        params.extend([limit, offset])
        query = f"""
            SELECT *
            FROM entities
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [self._entity_row_to_dict(row) for row in rows]

    def count_entities(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM entities").fetchone()
        return int(row["count"])

    def count_upcoming_launches(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM entities
                WHERE launch_time IS NOT NULL
                  AND datetime(launch_time) > datetime('now')
                """
            ).fetchone()
        return int(row["count"])

    def count_watchlist(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM entities WHERE watchlist = 1").fetchone()
        return int(row["count"])

    def get_entity_detail(self, project_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE project_id = ?",
                (project_id,),
            ).fetchone()

        if row is None:
            return None

        return self._entity_row_to_dict(row)

    def list_recent_new_projects(self, limit: int = 5) -> list[dict[str, Any]]:
        query = """
            SELECT e.*
            FROM entities AS e
            INNER JOIN (
                SELECT target, MAX(time) AS last_event_time
                FROM events
                WHERE type = 'new_project_detected'
                GROUP BY target
            ) AS latest
            ON latest.target = e.project_id
            ORDER BY latest.last_event_time DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()

        return [self._entity_row_to_dict(row) for row in rows]

    def list_upcoming_launches(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM entities
            WHERE launch_time IS NOT NULL
              AND datetime(launch_time) > datetime('now')
            ORDER BY launch_time ASC, id DESC
            LIMIT ? OFFSET ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit, offset)).fetchall()

        return [self._entity_row_to_dict(row) for row in rows]

    def list_upcoming_launches_for_feed(
        self,
        *,
        limit: int = 50,
        within_hours: int | None = None,
        contract_ready_only: bool = False,
    ) -> list[dict[str, Any]]:
        clauses = [
            "launch_time IS NOT NULL",
            "datetime(launch_time) > datetime('now')",
        ]
        params: list[Any] = []

        if within_hours is not None:
            clauses.append("datetime(launch_time) <= datetime('now', ?)")
            params.append(f"+{int(within_hours)} hours")

        if contract_ready_only:
            clauses.append("COALESCE(NULLIF(token_address, ''), NULLIF(contract_address, ''), '') <> ''")

        params.append(limit)
        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT *
            FROM entities
            WHERE {where_sql}
            ORDER BY launch_time ASC, id DESC
            LIMIT ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [self._entity_row_to_dict(row) for row in rows]

    def list_events_for_feed(
        self,
        *,
        limit: int = 100,
        since: str | None = None,
        event_types: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        if since:
            clauses.append("time > ?")
            params.append(since)

        if event_types:
            placeholders = ", ".join("?" for _ in event_types)
            clauses.append(f"type IN ({placeholders})")
            params.extend(event_types)

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        params.append(limit)
        query = f"""
            SELECT event_id, source, type, target, time, payload
            FROM events
            {where_sql}
            ORDER BY time DESC, id DESC
            LIMIT ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [self._event_row_to_dict(row) for row in rows]

    def list_projects_for_detail_refresh(self, limit: int = 50) -> list[str]:
        query = """
            SELECT project_id
            FROM entities
            WHERE launch_time IS NOT NULL
              AND datetime(launch_time) > datetime('now', '-2 days')
            ORDER BY
                CASE WHEN contract_address = '' THEN 0 ELSE 1 END,
                launch_time ASC,
                id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [str(row["project_id"]) for row in rows]

    def list_top_scored_projects(self, limit: int = 10) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM entities
            ORDER BY project_score DESC, last_seen DESC, id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [self._entity_row_to_dict(row) for row in rows]

    def list_watchlist(self, limit: int = 50) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM entities
            WHERE watchlist = 1
            ORDER BY project_score DESC, launch_time ASC, last_seen DESC, id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [self._entity_row_to_dict(row) for row in rows]

    def set_watchlist(self, project_id: str, enabled: bool) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE entities SET watchlist = ? WHERE project_id = ?",
                (int(enabled), project_id),
            )
            connection.commit()
        return cursor.rowcount > 0

    def count_lifecycle_stages(self) -> list[dict[str, Any]]:
        query = """
            SELECT lifecycle_stage, COUNT(*) AS count
            FROM entities
            GROUP BY lifecycle_stage
            ORDER BY count DESC, lifecycle_stage ASC
        """
        with self._connect() as connection:
            rows = connection.execute(query).fetchall()
        return [
            {"stage": row["lifecycle_stage"] or "detected", "count": int(row["count"])}
            for row in rows
        ]

    def get_latest_snapshot(self, project_id: str) -> ProjectSnapshot | None:
        query = """
            SELECT project_id, snapshot_time, description, team, links_json, tokenomics, launch_time, raw_data
            FROM project_snapshots
            WHERE project_id = ?
            ORDER BY snapshot_time DESC, id DESC
            LIMIT 1
        """
        with self._connect() as connection:
            row = connection.execute(query, (project_id,)).fetchone()
        if row is None:
            return None
        return ProjectSnapshot(
            project_id=row["project_id"],
            snapshot_time=row["snapshot_time"],
            description=row["description"],
            team=row["team"],
            links_json=row["links_json"],
            tokenomics=row["tokenomics"],
            launch_time=row["launch_time"],
            raw_data=json.loads(row["raw_data"]),
        )

    def create_snapshot(self, snapshot: ProjectSnapshot) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO project_snapshots(
                    project_id, snapshot_time, description, team, links_json, tokenomics, launch_time, raw_data
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.project_id,
                    snapshot.snapshot_time,
                    snapshot.description,
                    snapshot.team,
                    snapshot.links_json,
                    snapshot.tokenomics,
                    snapshot.launch_time,
                    json.dumps(snapshot.raw_data, ensure_ascii=False),
                ),
            )
            connection.commit()

    def create_changes(self, changes: list[ProjectChange]) -> None:
        if not changes:
            return
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO project_changes(project_id, change_type, field, old_value, new_value, time)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        change.project_id,
                        change.change_type,
                        change.field,
                        change.old_value,
                        change.new_value,
                        change.time,
                    )
                    for change in changes
                ],
            )
            connection.commit()

    def list_project_changes(self, project_id: str, limit: int = 50) -> list[dict[str, Any]]:
        query = """
            SELECT project_id, change_type, field, old_value, new_value, time
            FROM project_changes
            WHERE project_id = ?
            ORDER BY time DESC, id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (project_id, limit)).fetchall()
        return [
            {
                "project_id": row["project_id"],
                "change_type": row["change_type"],
                "field": row["field"],
                "old_value": row["old_value"],
                "new_value": row["new_value"],
                "time": row["time"],
            }
            for row in rows
        ]

    def upsert_project_addresses(self, addresses: list[ProjectAddress]) -> None:
        if not addresses:
            return
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO project_addresses(project_id, address, address_type, chain, first_seen)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(project_id, address, address_type, chain) DO NOTHING
                """,
                [
                    (
                        address.project_id,
                        address.address,
                        address.address_type,
                        address.chain,
                        address.first_seen,
                    )
                    for address in addresses
                ],
            )
            connection.commit()

    def list_project_addresses(self, project_id: str) -> list[dict[str, Any]]:
        query = """
            SELECT project_id, address, address_type, chain, first_seen
            FROM project_addresses
            WHERE project_id = ?
            ORDER BY address_type ASC, first_seen ASC, id ASC
        """
        with self._connect() as connection:
            rows = connection.execute(query, (project_id,)).fetchall()
        return [
            {
                "project_id": row["project_id"],
                "address": row["address"],
                "address_type": row["address_type"],
                "chain": row["chain"],
                "first_seen": row["first_seen"],
            }
            for row in rows
        ]

    def has_project_address(
        self,
        project_id: str,
        address: str,
        address_type: str,
        *,
        chain: str = "BASE",
    ) -> bool:
        query = """
            SELECT 1
            FROM project_addresses
            WHERE project_id = ? AND lower(address) = lower(?) AND address_type = ? AND upper(chain) = upper(?)
            LIMIT 1
        """
        with self._connect() as connection:
            row = connection.execute(query, (project_id, address, address_type, chain)).fetchone()
        return row is not None

    def get_project_launch_trace(self, project_id: str) -> dict[str, Any] | None:
        query = """
            SELECT *
            FROM project_launch_traces
            WHERE project_id = ?
            LIMIT 1
        """
        with self._connect() as connection:
            row = connection.execute(query, (project_id,)).fetchone()
        if row is None:
            return None
        return self._launch_trace_row_to_dict(row)

    def upsert_project_launch_trace(self, trace: ProjectLaunchTrace) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO project_launch_traces(
                    project_id, token_contract, launch_tx_hash, launch_block_number,
                    internal_market_address, intermediate_address, mint_recipient, launch_sender, launch_target,
                    confidence, evidence, subscription_status, first_seen, last_seen,
                    last_error, notes_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    token_contract = excluded.token_contract,
                    launch_tx_hash = excluded.launch_tx_hash,
                    launch_block_number = excluded.launch_block_number,
                    internal_market_address = excluded.internal_market_address,
                    intermediate_address = excluded.intermediate_address,
                    mint_recipient = excluded.mint_recipient,
                    launch_sender = excluded.launch_sender,
                    launch_target = excluded.launch_target,
                    confidence = excluded.confidence,
                    evidence = excluded.evidence,
                    subscription_status = excluded.subscription_status,
                    first_seen = COALESCE(NULLIF(project_launch_traces.first_seen, ''), excluded.first_seen),
                    last_seen = excluded.last_seen,
                    last_error = excluded.last_error,
                    notes_json = excluded.notes_json
                """,
                (
                    trace.project_id,
                    trace.token_contract,
                    trace.launch_tx_hash,
                    trace.launch_block_number,
                    trace.internal_market_address,
                    trace.intermediate_address,
                    trace.mint_recipient,
                    trace.launch_sender,
                    trace.launch_target,
                    trace.confidence,
                    trace.evidence,
                    trace.subscription_status,
                    trace.first_seen,
                    trace.last_seen,
                    trace.last_error,
                    trace.notes_json,
                ),
            )
            connection.commit()

    def list_projects_for_chainstack_subscription(self, limit: int = 500) -> list[dict[str, Any]]:
        query = """
            SELECT
                e.project_id,
                e.name,
                e.symbol,
                e.token_address,
                e.contract_address,
                e.created_time,
                e.launch_time,
                e.status,
                COALESCE(t.subscription_status, 'pending') AS subscription_status,
                COALESCE(t.internal_market_address, '') AS internal_market_address,
                COALESCE(t.launch_tx_hash, '') AS launch_tx_hash,
                COALESCE(t.last_error, '') AS last_error
            FROM entities AS e
            LEFT JOIN project_launch_traces AS t
              ON t.project_id = e.project_id
            WHERE COALESCE(NULLIF(e.token_address, ''), NULLIF(e.contract_address, ''), '') <> ''
              AND e.internal_market_address = ''
              AND (e.launch_time IS NULL OR datetime(e.launch_time) > datetime('now', '-3 days'))
              AND COALESCE(t.internal_market_address, '') = ''
            ORDER BY
                CASE WHEN e.launch_time IS NULL THEN 1 ELSE 0 END,
                e.launch_time ASC,
                e.id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "token_address": row["token_address"],
                "contract_address": row["contract_address"],
                "created_time": row["created_time"],
                "launch_time": row["launch_time"],
                "status": row["status"],
                "subscription_status": row["subscription_status"],
                "internal_market_address": row["internal_market_address"],
                "launch_tx_hash": row["launch_tx_hash"],
                "last_error": row["last_error"],
            }
            for row in rows
        ]

    def get_internal_market_detail(self, project_id: str) -> dict[str, Any] | None:
        query = """
            SELECT
                e.project_id,
                e.name,
                e.symbol,
                e.url,
                e.token_address,
                e.contract_address,
                e.internal_market_address AS entity_internal_market_address,
                e.status,
                e.launch_time,
                t.token_contract,
                t.launch_tx_hash,
                t.launch_block_number,
                COALESCE(NULLIF(e.internal_market_address, ''), t.internal_market_address, '') AS internal_market_address,
                t.intermediate_address,
                t.mint_recipient,
                t.launch_sender,
                t.launch_target,
                t.confidence,
                t.evidence,
                t.subscription_status,
                t.first_seen,
                t.last_seen,
                t.last_error,
                t.notes_json
            FROM entities AS e
            LEFT JOIN project_launch_traces AS t
              ON t.project_id = e.project_id
            WHERE e.project_id = ?
            LIMIT 1
        """
        with self._connect() as connection:
            row = connection.execute(query, (project_id,)).fetchone()
        if row is None:
            return None
        return self._internal_market_row_to_dict(row)

    def list_internal_market_details(self, limit: int = 100) -> list[dict[str, Any]]:
        query = """
            SELECT
                e.project_id,
                e.name,
                e.symbol,
                e.url,
                e.token_address,
                e.contract_address,
                e.internal_market_address AS entity_internal_market_address,
                e.status,
                e.launch_time,
                t.token_contract,
                t.launch_tx_hash,
                t.launch_block_number,
                COALESCE(NULLIF(e.internal_market_address, ''), t.internal_market_address, '') AS internal_market_address,
                t.intermediate_address,
                t.mint_recipient,
                t.launch_sender,
                t.launch_target,
                t.confidence,
                t.evidence,
                t.subscription_status,
                t.first_seen,
                t.last_seen,
                t.last_error,
                t.notes_json
            FROM project_launch_traces AS t
            INNER JOIN entities AS e
              ON e.project_id = t.project_id
            WHERE COALESCE(NULLIF(e.internal_market_address, ''), t.internal_market_address, '') <> ''
            ORDER BY t.last_seen DESC, e.id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [self._internal_market_row_to_dict(row) for row in rows]

    def list_token_pool_export_items(self, limit: int = 1000) -> list[dict[str, Any]]:
        query = """
            SELECT
                e.project_id,
                e.name,
                e.symbol,
                e.url,
                e.token_address,
                e.contract_address,
                e.internal_market_address,
                e.status,
                e.launch_time,
                e.last_seen,
                e.lifecycle_stage,
                e.project_score,
                e.risk_level,
                COALESCE(t.launch_tx_hash, '') AS launch_tx_hash,
                COALESCE(t.launch_block_number, NULL) AS launch_block_number,
                COALESCE(t.intermediate_address, '') AS intermediate_address,
                COALESCE(t.confidence, '') AS confidence,
                COALESCE(t.evidence, '') AS evidence
            FROM entities AS e
            LEFT JOIN project_launch_traces AS t
              ON t.project_id = e.project_id
            WHERE COALESCE(NULLIF(e.token_address, ''), NULLIF(e.contract_address, ''), '') <> ''
            ORDER BY
                CASE WHEN e.launch_time IS NULL THEN 1 ELSE 0 END,
                e.launch_time ASC,
                e.project_score DESC,
                e.last_seen DESC,
                e.id DESC
            LIMIT ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit,)).fetchall()
        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "url": row["url"],
                "token_address": row["token_address"],
                "contract_address": row["contract_address"],
                "pool_address": row["internal_market_address"],
                "internal_market_address": row["internal_market_address"],
                "status": row["status"],
                "launch_time": row["launch_time"],
                "last_seen": row["last_seen"],
                "lifecycle_stage": row["lifecycle_stage"],
                "project_score": int(row["project_score"]),
                "risk_level": row["risk_level"],
                "launch_tx_hash": row["launch_tx_hash"],
                "launch_block_number": row["launch_block_number"],
                "intermediate_address": row["intermediate_address"],
                "confidence": row["confidence"],
                "evidence": row["evidence"],
            }
            for row in rows
        ]

    def _entity_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "project_id": row["project_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "url": row["url"],
            "token_address": row["token_address"],
            "contract_address": row["contract_address"],
            "internal_market_address": row["internal_market_address"],
            "pool_address": row["internal_market_address"],
            "status": row["status"],
            "description": row["description"],
            "creator": row["creator"],
            "team": row["team"],
            "links_json": row["links_json"],
            "links": links_from_json(row["links_json"]),
            "tokenomics": row["tokenomics"],
            "created_time": row["created_time"],
            "launch_time": row["launch_time"],
            "last_seen": row["last_seen"],
            "raw_hash": row["raw_hash"],
            "lifecycle_stage": row["lifecycle_stage"],
            "project_score": int(row["project_score"]),
            "risk_level": row["risk_level"],
            "watchlist": bool(row["watchlist"]),
        }

    def _event_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["event_id"],
            "source": row["source"],
            "type": row["type"],
            "target": row["target"],
            "time": row["time"],
            "payload": json.loads(row["payload"]),
        }

    def _launch_trace_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "project_id": row["project_id"],
            "token_contract": row["token_contract"],
            "launch_tx_hash": row["launch_tx_hash"],
            "launch_block_number": row["launch_block_number"],
            "internal_market_address": row["internal_market_address"],
            "pool_address": row["internal_market_address"],
            "intermediate_address": row["intermediate_address"],
            "mint_recipient": row["mint_recipient"],
            "launch_sender": row["launch_sender"],
            "launch_target": row["launch_target"],
            "confidence": row["confidence"],
            "evidence": row["evidence"],
            "subscription_status": row["subscription_status"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "last_error": row["last_error"],
            "notes": links_from_json(row["notes_json"]),
        }

    def _internal_market_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "project_id": row["project_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "url": row["url"],
            "token_address": row["token_address"],
            "contract_address": row["contract_address"],
            "status": row["status"],
            "launch_time": row["launch_time"],
            "token_contract": row["token_contract"],
            "launch_tx_hash": row["launch_tx_hash"],
            "launch_block_number": row["launch_block_number"],
            "internal_market_address": row["internal_market_address"],
            "pool_address": row["internal_market_address"],
            "intermediate_address": row["intermediate_address"],
            "mint_recipient": row["mint_recipient"],
            "launch_sender": row["launch_sender"],
            "launch_target": row["launch_target"],
            "confidence": row["confidence"],
            "evidence": row["evidence"],
            "subscription_status": row["subscription_status"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "last_error": row["last_error"],
            "notes": links_from_json(row["notes_json"]),
        }
