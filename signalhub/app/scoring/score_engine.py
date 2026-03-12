from __future__ import annotations

from signalhub.app.database.models import ProjectEntity


class ScoreEngine:
    SCORE_WEIGHTS = {
        "team_present": 16,
        "description_complete": 18,
        "tokenomics_present": 16,
        "external_links_present": 14,
        "github_present": 12,
        "creator_present": 10,
        "launch_time_announced": 8,
        "symbol_present": 6,
    }
    GRADE_THRESHOLDS = (
        ("A", 84),
        ("B", 68),
        ("C", 52),
        ("D", 36),
        ("E", 20),
    )

    def evaluate(self, project: ProjectEntity) -> tuple[int, str, dict[str, bool]]:
        links = project.links()
        description = project.description.strip()
        flags = {
            "team_present": bool(project.team.strip()),
            "description_complete": len(description) >= 96,
            "tokenomics_present": bool(project.tokenomics.strip()),
            "external_links_present": bool(links),
            "github_present": any("github.com" in link.lower() for link in links),
            "creator_present": bool(project.creator.strip()),
            "launch_time_announced": bool(project.launch_time),
            "symbol_present": bool(project.symbol.strip()),
        }
        score = sum(
            weight for flag, weight in self.SCORE_WEIGHTS.items() if flags.get(flag)
        )
        return score, self._risk_level(score), flags

    @classmethod
    def normalize_score(cls, score: int | str | None) -> int:
        try:
            raw_score = int(score or 0)
        except (TypeError, ValueError):
            return 0
        if raw_score <= 5:
            return max(0, min(raw_score, 5)) * 20
        return max(0, min(raw_score, 100))

    @classmethod
    def grade(cls, score: int | str | None) -> str:
        normalized = cls.normalize_score(score)
        for grade, threshold in cls.GRADE_THRESHOLDS:
            if normalized >= threshold:
                return grade
        return "F"

    @classmethod
    def risk_level(cls, score: int | str | None) -> str:
        grade = cls.grade(score)
        if grade in {"A", "B"}:
            return "low"
        if grade in {"C", "D"}:
            return "medium"
        return "high"

    def _risk_level(self, score: int) -> str:
        return self.risk_level(score)
