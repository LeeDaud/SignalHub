from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from signalhub.app.database.models import ProjectEntity, SignalEvent


TABLES_SQL = """
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    contract_address TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unknown',
    description TEXT NOT NULL DEFAULT '',
    creator TEXT NOT NULL DEFAULT '',
    created_time TEXT,
    launch_time TEXT,
    last_seen TEXT NOT NULL,
    raw_hash TEXT NOT NULL
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
"""

INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_entities_last_seen ON entities(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_entities_launch_time ON entities(launch_time ASC);
CREATE INDEX IF NOT EXISTS idx_entities_contract_address ON entities(contract_address);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(time DESC);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_sources_name ON sources(name);
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
            connection.executescript(INDEXES_SQL)
            connection.commit()

    def _ensure_entities_columns(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(entities)").fetchall()
        }
        if "launch_time" not in existing_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN launch_time TEXT")
        if "contract_address" not in existing_columns:
            connection.execute(
                "ALTER TABLE entities ADD COLUMN contract_address TEXT NOT NULL DEFAULT ''"
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
                    project_id, name, symbol, url, status, description,
                    contract_address, creator, created_time, launch_time, last_seen, raw_hash
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    name = excluded.name,
                    symbol = excluded.symbol,
                    url = excluded.url,
                    contract_address = excluded.contract_address,
                    status = excluded.status,
                    description = excluded.description,
                    creator = excluded.creator,
                    created_time = excluded.created_time,
                    launch_time = excluded.launch_time,
                    last_seen = excluded.last_seen,
                    raw_hash = excluded.raw_hash
                """,
                (
                    entity.project_id,
                    entity.name,
                    entity.symbol,
                    entity.url,
                    entity.status,
                    entity.description,
                    entity.contract_address,
                    entity.creator,
                    entity.created_time,
                    entity.launch_time,
                    entity.last_seen,
                    entity.raw_hash,
                ),
            )
            connection.commit()

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
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        if event_type:
            clauses.append("type = ?")
            params.append(event_type)
        if source:
            clauses.append("source = ?")
            params.append(source)

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

        return [
            {
                "id": row["event_id"],
                "source": row["source"],
                "type": row["type"],
                "target": row["target"],
                "time": row["time"],
                "payload": json.loads(row["payload"]),
            }
            for row in rows
        ]

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
        upcoming_only: bool = False,
        order_by: str = "last_seen_desc",
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        clauses: list[str] = []
        if status:
            clauses.append("status = ?")
            params.append(status)

        if upcoming_only:
            clauses.append("launch_time IS NOT NULL")
            clauses.append("datetime(launch_time) > datetime('now')")

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " AND ".join(clauses)

        order_sql = "ORDER BY last_seen DESC, id DESC"
        if order_by == "launch_time_asc":
            order_sql = "ORDER BY launch_time ASC, id DESC"
        elif order_by == "created_time_desc":
            order_sql = "ORDER BY created_time DESC, id DESC"

        params.extend([limit, offset])
        query = f"""
            SELECT
                project_id, name, symbol, url, contract_address, status, description,
                creator, created_time, launch_time, last_seen, raw_hash
            FROM entities
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "url": row["url"],
                "contract_address": row["contract_address"],
                "status": row["status"],
                "description": row["description"],
                "creator": row["creator"],
                "created_time": row["created_time"],
                "launch_time": row["launch_time"],
                "last_seen": row["last_seen"],
                "raw_hash": row["raw_hash"],
            }
            for row in rows
        ]

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

    def get_entity_detail(self, project_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    project_id, name, symbol, url, contract_address, status, description,
                    creator, created_time, launch_time, last_seen, raw_hash
                FROM entities
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "project_id": row["project_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "url": row["url"],
            "contract_address": row["contract_address"],
            "status": row["status"],
            "description": row["description"],
            "creator": row["creator"],
            "created_time": row["created_time"],
            "launch_time": row["launch_time"],
            "last_seen": row["last_seen"],
            "raw_hash": row["raw_hash"],
        }

    def list_recent_new_projects(self, limit: int = 5) -> list[dict[str, Any]]:
        query = """
            SELECT e.project_id, e.name, e.symbol, e.url, e.contract_address, e.status, e.launch_time, e.last_seen
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

        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "url": row["url"],
                "contract_address": row["contract_address"],
                "status": row["status"],
                "launch_time": row["launch_time"],
                "last_seen": row["last_seen"],
            }
            for row in rows
        ]

    def list_upcoming_launches(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        query = """
            SELECT
                project_id, name, symbol, url, contract_address, status, description,
                creator, created_time, launch_time, last_seen, raw_hash
            FROM entities
            WHERE launch_time IS NOT NULL
              AND datetime(launch_time) > datetime('now')
            ORDER BY launch_time ASC, id DESC
            LIMIT ? OFFSET ?
        """
        with self._connect() as connection:
            rows = connection.execute(query, (limit, offset)).fetchall()

        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "url": row["url"],
                "contract_address": row["contract_address"],
                "status": row["status"],
                "description": row["description"],
                "creator": row["creator"],
                "created_time": row["created_time"],
                "launch_time": row["launch_time"],
                "last_seen": row["last_seen"],
                "raw_hash": row["raw_hash"],
            }
            for row in rows
        ]

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
            clauses.append("contract_address <> ''")

        params.append(limit)
        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT
                project_id, name, symbol, url, contract_address, status, description,
                creator, created_time, launch_time, last_seen, raw_hash
            FROM entities
            WHERE {where_sql}
            ORDER BY launch_time ASC, id DESC
            LIMIT ?
        """

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            {
                "project_id": row["project_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "url": row["url"],
                "contract_address": row["contract_address"],
                "status": row["status"],
                "description": row["description"],
                "creator": row["creator"],
                "created_time": row["created_time"],
                "launch_time": row["launch_time"],
                "last_seen": row["last_seen"],
                "raw_hash": row["raw_hash"],
            }
            for row in rows
        ]

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

        return [
            {
                "id": row["event_id"],
                "source": row["source"],
                "type": row["type"],
                "target": row["target"],
                "time": row["time"],
                "payload": json.loads(row["payload"]),
            }
            for row in rows
        ]

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
