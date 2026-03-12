from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from signalhub.app.database.models import ProjectEntity, SignalEvent


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unknown',
    description TEXT NOT NULL DEFAULT '',
    creator TEXT NOT NULL DEFAULT '',
    created_time TEXT,
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

CREATE INDEX IF NOT EXISTS idx_entities_last_seen ON entities(last_seen DESC);
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
            connection.executescript(SCHEMA_SQL)
            connection.commit()

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
                    creator, created_time, last_seen, raw_hash
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    name = excluded.name,
                    symbol = excluded.symbol,
                    url = excluded.url,
                    status = excluded.status,
                    description = excluded.description,
                    creator = excluded.creator,
                    created_time = excluded.created_time,
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
                    entity.creator,
                    entity.created_time,
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
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where_sql = ""
        if status:
            where_sql = "WHERE status = ?"
            params.append(status)

        params.extend([limit, offset])
        query = f"""
            SELECT
                project_id, name, symbol, url, status, description,
                creator, created_time, last_seen, raw_hash
            FROM entities
            {where_sql}
            ORDER BY last_seen DESC, id DESC
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
                "status": row["status"],
                "description": row["description"],
                "creator": row["creator"],
                "created_time": row["created_time"],
                "last_seen": row["last_seen"],
                "raw_hash": row["raw_hash"],
            }
            for row in rows
        ]

    def count_entities(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM entities").fetchone()
        return int(row["count"])

    def get_entity_detail(self, project_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    project_id, name, symbol, url, status, description,
                    creator, created_time, last_seen, raw_hash
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
            "status": row["status"],
            "description": row["description"],
            "creator": row["creator"],
            "created_time": row["created_time"],
            "last_seen": row["last_seen"],
            "raw_hash": row["raw_hash"],
        }

    def list_recent_new_projects(self, limit: int = 5) -> list[dict[str, Any]]:
        query = """
            SELECT e.project_id, e.name, e.symbol, e.url, e.status, e.last_seen
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
                "status": row["status"],
                "last_seen": row["last_seen"],
            }
            for row in rows
        ]
