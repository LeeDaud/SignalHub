from __future__ import annotations

from typing import Any

from signalhub.app.database.db import Database
from signalhub.app.database.models import ProjectEntity, SignalEvent, build_event_id, utc_now_iso


class EventProcessor:
    def __init__(self, database: Database, *, source_name: str = "virtuals") -> None:
        self.database = database
        self.source_name = source_name

    def process_projects(self, projects: list[ProjectEntity]) -> dict[str, int]:
        summary = {
            "received": len(projects),
            "new_projects": 0,
            "updated_projects": 0,
            "status_changes": 0,
        }

        for project in projects:
            existing = self.database.get_entity(project.project_id)
            if existing is None:
                self.database.upsert_entity(project)
                self.database.create_event(self._build_new_project_event(project))
                summary["new_projects"] += 1
                continue

            for event in self._build_update_events(existing, project):
                self.database.create_event(event)
                if event.type == "project_status_changed":
                    summary["status_changes"] += 1
                if event.type == "project_updated":
                    summary["updated_projects"] += 1

            self.database.upsert_entity(project)

        return summary

    def _build_new_project_event(self, project: ProjectEntity) -> SignalEvent:
        return SignalEvent(
            event_id=build_event_id(),
            source=self.source_name,
            type="new_project_detected",
            target=project.project_id,
            time=utc_now_iso(),
            payload={
                "name": project.name,
                "symbol": project.symbol,
                "url": project.url,
                "contract_address": project.contract_address,
                "status": project.status or "detected",
                "launch_time": project.launch_time,
            },
        )

    def _build_update_events(
        self,
        existing: ProjectEntity,
        incoming: ProjectEntity,
    ) -> list[SignalEvent]:
        events: list[SignalEvent] = []
        field_changes = self._collect_field_changes(existing, incoming)

        status_changed = existing.status != incoming.status
        non_status_changes = {
            key: value for key, value in field_changes.items() if key != "status"
        }

        if status_changed:
            events.append(
                SignalEvent(
                    event_id=build_event_id(),
                    source=self.source_name,
                    type="project_status_changed",
                    target=incoming.project_id,
                    time=utc_now_iso(),
                    payload={
                        "name": incoming.name,
                        "symbol": incoming.symbol,
                        "url": incoming.url,
                        "contract_address": incoming.contract_address,
                        "old_status": existing.status,
                        "new_status": incoming.status,
                        "launch_time": incoming.launch_time,
                    },
                )
            )

        if non_status_changes:
            events.append(
                SignalEvent(
                    event_id=build_event_id(),
                    source=self.source_name,
                    type="project_updated",
                    target=incoming.project_id,
                    time=utc_now_iso(),
                    payload={
                        "name": incoming.name,
                        "symbol": incoming.symbol,
                        "url": incoming.url,
                        "contract_address": incoming.contract_address,
                        "status": incoming.status,
                        "launch_time": incoming.launch_time,
                        "changes": non_status_changes,
                        "raw_hash_changed": existing.raw_hash != incoming.raw_hash,
                    },
                )
            )

        return events

    def _collect_field_changes(
        self,
        existing: ProjectEntity,
        incoming: ProjectEntity,
    ) -> dict[str, dict[str, Any]]:
        changes: dict[str, dict[str, Any]] = {}
        watched_fields = (
            "name",
            "symbol",
            "url",
            "contract_address",
            "status",
            "description",
            "creator",
            "created_time",
            "launch_time",
        )

        for field in watched_fields:
            before = getattr(existing, field)
            after = getattr(incoming, field)
            if before == after:
                continue
            changes[field] = {"before": before, "after": after}

        return changes
