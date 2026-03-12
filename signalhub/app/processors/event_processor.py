from __future__ import annotations

from typing import Any

from signalhub.app.address.address_analyzer import AddressAnalyzer
from signalhub.app.database.db import Database
from signalhub.app.database.models import (
    ParsedProject,
    ProjectAddress,
    ProjectEntity,
    ProjectSnapshot,
    SignalEvent,
    build_event_id,
    build_raw_hash,
    utc_now_iso,
)
from signalhub.app.diff.diff_engine import DiffEngine
from signalhub.app.lifecycle.lifecycle_engine import LifecycleEngine
from signalhub.app.scoring.score_engine import ScoreEngine


class EventProcessor:
    def __init__(self, database: Database, *, source_name: str = "virtuals") -> None:
        self.database = database
        self.source_name = source_name
        self.lifecycle_engine = LifecycleEngine()
        self.diff_engine = DiffEngine()
        self.score_engine = ScoreEngine()
        self.address_analyzer = AddressAnalyzer()

    def process_projects(self, projects: list[ParsedProject]) -> dict[str, int]:
        summary = {
            "received": len(projects),
            "new_projects": 0,
            "updated_projects": 0,
            "status_changes": 0,
            "stage_changes": 0,
            "content_changes": 0,
        }

        for parsed in projects:
            existing = self.database.get_entity(parsed.entity.project_id)
            previous_snapshot = self.database.get_latest_snapshot(parsed.entity.project_id)

            entity = self._merge_entity(existing, parsed.entity)
            snapshot = self._build_snapshot(entity, parsed.snapshot)
            content_changes = self.diff_engine.compare(previous_snapshot, snapshot)

            score, risk_level, score_flags = self.score_engine.evaluate(entity)
            entity.project_score = score
            entity.risk_level = risk_level
            entity.lifecycle_stage = self.lifecycle_engine.resolve_stage(
                existing,
                entity,
                raw_data=parsed.raw_data,
                content_changed=bool(content_changes),
            )

            if existing is None:
                self.database.upsert_entity(entity)
                self.database.create_snapshot(snapshot)
                self.database.upsert_project_addresses(
                    self._collect_addresses(entity, parsed)
                )
                self.database.create_event(self._build_new_project_event(entity, score_flags))
                summary["new_projects"] += 1
                continue

            stage_changed = existing.lifecycle_stage != entity.lifecycle_stage
            field_changes = self._collect_field_changes(existing, entity)
            status_changed = existing.status != entity.status
            non_status_changes = {
                key: value for key, value in field_changes.items() if key != "status"
            }

            if content_changes:
                self.database.create_changes(content_changes)
                self.database.create_snapshot(snapshot)
                self.database.create_event(self._build_content_updated_event(entity, content_changes))
                summary["content_changes"] += 1

            if status_changed:
                self.database.create_event(self._build_status_changed_event(existing, entity))
                summary["status_changes"] += 1

            if stage_changed:
                self.database.create_event(self._build_stage_changed_event(existing, entity))
                summary["stage_changes"] += 1

            if non_status_changes:
                self.database.create_event(self._build_project_updated_event(existing, entity, non_status_changes))
                summary["updated_projects"] += 1

            self.database.upsert_entity(entity)
            self.database.upsert_project_addresses(self._collect_addresses(entity, parsed))

        return summary

    def _merge_entity(
        self,
        existing: ProjectEntity | None,
        incoming: ProjectEntity,
    ) -> ProjectEntity:
        if existing is None:
            return incoming

        merged = ProjectEntity(
            project_id=incoming.project_id,
            name=incoming.name or existing.name,
            symbol=incoming.symbol or existing.symbol,
            url=incoming.url or existing.url,
            contract_address=incoming.contract_address or existing.contract_address,
            status=incoming.status or existing.status,
            description=incoming.description or existing.description,
            creator=incoming.creator or existing.creator,
            team=incoming.team or existing.team,
            links_json=incoming.links_json if incoming.links_json != "[]" else existing.links_json,
            tokenomics=incoming.tokenomics or existing.tokenomics,
            created_time=incoming.created_time or existing.created_time,
            launch_time=incoming.launch_time or existing.launch_time,
            last_seen=incoming.last_seen,
            raw_hash=build_raw_hash(
                incoming.name or existing.name,
                incoming.status or existing.status,
                incoming.description or existing.description,
            ),
            lifecycle_stage=existing.lifecycle_stage,
            project_score=existing.project_score,
            risk_level=existing.risk_level,
            watchlist=existing.watchlist,
        )
        return merged

    def _build_snapshot(self, entity: ProjectEntity, incoming: ProjectSnapshot) -> ProjectSnapshot:
        return ProjectSnapshot(
            project_id=entity.project_id,
            snapshot_time=incoming.snapshot_time,
            description=entity.description,
            team=entity.team,
            links_json=entity.links_json,
            tokenomics=entity.tokenomics,
            launch_time=entity.launch_time,
            raw_data=incoming.raw_data,
        )

    def _build_new_project_event(
        self,
        project: ProjectEntity,
        score_flags: dict[str, bool],
    ) -> SignalEvent:
        score_grade = self.score_engine.grade(project.project_score)
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
                "lifecycle_stage": project.lifecycle_stage,
                "project_score": project.project_score,
                "score_grade": score_grade,
                "risk_level": project.risk_level,
                "watchlist": project.watchlist,
                "score_flags": score_flags,
            },
        )

    def _build_status_changed_event(
        self,
        existing: ProjectEntity,
        incoming: ProjectEntity,
    ) -> SignalEvent:
        score_grade = self.score_engine.grade(incoming.project_score)
        return SignalEvent(
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
                "lifecycle_stage": incoming.lifecycle_stage,
                "project_score": incoming.project_score,
                "score_grade": score_grade,
                "risk_level": incoming.risk_level,
            },
        )

    def _build_stage_changed_event(
        self,
        existing: ProjectEntity,
        incoming: ProjectEntity,
    ) -> SignalEvent:
        score_grade = self.score_engine.grade(incoming.project_score)
        return SignalEvent(
            event_id=build_event_id(),
            source=self.source_name,
            type="project_lifecycle_changed",
            target=incoming.project_id,
            time=utc_now_iso(),
            payload={
                "name": incoming.name,
                "symbol": incoming.symbol,
                "old_stage": existing.lifecycle_stage,
                "new_stage": incoming.lifecycle_stage,
                "status": incoming.status,
                "launch_time": incoming.launch_time,
                "project_score": incoming.project_score,
                "score_grade": score_grade,
                "risk_level": incoming.risk_level,
            },
        )

    def _build_content_updated_event(
        self,
        project: ProjectEntity,
        changes: list[Any],
    ) -> SignalEvent:
        score_grade = self.score_engine.grade(project.project_score)
        return SignalEvent(
            event_id=build_event_id(),
            source=self.source_name,
            type="project_content_updated",
            target=project.project_id,
            time=utc_now_iso(),
            payload={
                "name": project.name,
                "symbol": project.symbol,
                "url": project.url,
                "status": project.status,
                "lifecycle_stage": project.lifecycle_stage,
                "project_score": project.project_score,
                "score_grade": score_grade,
                "risk_level": project.risk_level,
                "changes": [
                    {
                        "field": change.field,
                        "change_type": change.change_type,
                        "old_value": change.old_value,
                        "new_value": change.new_value,
                    }
                    for change in changes
                ],
            },
        )

    def _build_project_updated_event(
        self,
        existing: ProjectEntity,
        incoming: ProjectEntity,
        changes: dict[str, dict[str, Any]],
    ) -> SignalEvent:
        score_grade = self.score_engine.grade(incoming.project_score)
        return SignalEvent(
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
                "lifecycle_stage": incoming.lifecycle_stage,
                "project_score": incoming.project_score,
                "score_grade": score_grade,
                "risk_level": incoming.risk_level,
                "changes": changes,
                "raw_hash_changed": existing.raw_hash != incoming.raw_hash,
            },
        )

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
            "creator",
            "created_time",
            "launch_time",
            "lifecycle_stage",
            "project_score",
            "risk_level",
        )

        for field in watched_fields:
            before = getattr(existing, field)
            after = getattr(incoming, field)
            if before == after:
                continue
            changes[field] = {"before": before, "after": after}

        return changes

    def _collect_addresses(
        self,
        entity: ProjectEntity,
        parsed: ParsedProject,
    ) -> list[ProjectAddress]:
        combined = parsed.addresses + self.address_analyzer.analyze(entity, parsed.raw_data)
        deduped: dict[tuple[str, str, str], ProjectAddress] = {}
        for item in combined:
            key = (item.address.lower(), item.address_type, item.chain.upper())
            deduped.setdefault(key, item)
        return list(deduped.values())
