from __future__ import annotations

from signalhub.app.database.models import ProjectChange, ProjectSnapshot, utc_now_iso


class DiffEngine:
    TRACKED_FIELDS = (
        "description",
        "team",
        "links_json",
        "tokenomics",
        "launch_time",
    )

    def compare(
        self,
        previous: ProjectSnapshot | None,
        current: ProjectSnapshot,
    ) -> list[ProjectChange]:
        if previous is None:
            return []

        changes: list[ProjectChange] = []
        change_time = utc_now_iso()
        for field in self.TRACKED_FIELDS:
            before = self._normalize(getattr(previous, field))
            after = self._normalize(getattr(current, field))
            if before == after:
                continue
            changes.append(
                ProjectChange(
                    project_id=current.project_id,
                    change_type=self._change_type(before, after),
                    field="links" if field == "links_json" else field,
                    old_value=before or None,
                    new_value=after or None,
                    time=change_time,
                )
            )
        return changes

    def has_snapshot_delta(
        self,
        previous: ProjectSnapshot | None,
        current: ProjectSnapshot,
    ) -> bool:
        return previous is None or bool(self.compare(previous, current))

    def _change_type(self, before: str, after: str) -> str:
        if not before and after:
            return "added"
        if before and not after:
            return "removed"
        return "updated"

    def _normalize(self, value: str | None) -> str:
        return (value or "").strip()
