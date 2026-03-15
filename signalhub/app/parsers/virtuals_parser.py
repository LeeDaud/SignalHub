from __future__ import annotations

import json
import re
from typing import Any

from signalhub.app.database.models import (
    ParsedProject,
    ProjectAddress,
    ProjectEntity,
    ProjectSnapshot,
    build_raw_hash,
    build_url_hash,
    links_to_json,
    utc_now_iso,
)


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
PROTOTYPE_ADDRESS_RE = re.compile(r"/prototypes/(0x[a-fA-F0-9]{40})")


class VirtualsParser:
    def __init__(self, app_base_url: str = "https://app.virtuals.io") -> None:
        self.app_base_url = app_base_url.rstrip("/")

    def parse_projects(self, payload: Any) -> list[ParsedProject]:
        rows = self._extract_items(payload)
        last_seen = utc_now_iso()
        projects: list[ParsedProject] = []
        seen_project_ids: set[str] = set()

        for row in rows:
            if not isinstance(row, dict):
                continue
            project = self._parse_project(row, last_seen)
            if project is None or project.entity.project_id in seen_project_ids:
                continue
            projects.append(project)
            seen_project_ids.add(project.entity.project_id)

        return projects

    def _extract_items(self, payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            for key in ("projects", "data", "items", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value

        return []

    def _parse_project(self, row: dict[str, Any], last_seen: str) -> ParsedProject | None:
        name = self._as_text(row.get("name") or row.get("title"))
        symbol = self._as_text(row.get("symbol") or row.get("ticker"))
        url = self._resolve_project_url(row)
        status = self._as_text(row.get("status"), default="unknown")
        description = self._extract_description(row)
        creator = self._extract_creator(row.get("creator") or row.get("owner"))
        created_time = self._extract_time(row)
        launch_time = self._extract_launch_time(row)
        team = self._extract_team(row)
        links = self._extract_links(row, project_url=url)
        tokenomics = self._extract_tokenomics(row)

        project_id = self._resolve_project_id(row, url, name, symbol)
        if not project_id:
            return None

        token_address = self._extract_token_address(row, url)
        contract_address = self._extract_contract_address(row, token_address)
        entity = ProjectEntity(
            project_id=project_id,
            name=name or project_id,
            symbol=symbol,
            url=url,
            token_address=token_address,
            contract_address=contract_address,
            internal_market_address="",
            status=status,
            description=description,
            creator=creator,
            team=team,
            links_json=links_to_json(links),
            tokenomics=tokenomics,
            created_time=created_time,
            launch_time=launch_time,
            last_seen=last_seen,
            raw_hash=build_raw_hash(name or project_id, status, description),
        )
        snapshot = ProjectSnapshot(
            project_id=project_id,
            snapshot_time=last_seen,
            description=description,
            team=team,
            links_json=links_to_json(links),
            tokenomics=tokenomics,
            launch_time=launch_time,
            raw_data=row,
        )
        return ParsedProject(
            entity=entity,
            snapshot=snapshot,
            addresses=self._extract_addresses(project_id, creator, token_address, contract_address, row),
            raw_data=row,
        )

    def _resolve_project_id(
        self,
        row: dict[str, Any],
        url: str,
        name: str,
        symbol: str,
    ) -> str:
        candidate_keys = (
            "project_id",
            "projectId",
            "virtualId",
            "id",
            "uid",
            "slug",
            "tokenAddress",
            "preToken",
            "contract_address",
            "contractAddress",
            "token_address",
        )

        for key in candidate_keys:
            value = self._as_text(row.get(key))
            if value:
                return value

        if url:
            return f"url_{build_url_hash(url)}"

        fallback = f"{name}|{symbol}".strip("|")
        if fallback:
            return f"generated_{build_url_hash(fallback)}"

        return ""

    def _extract_description(self, row: dict[str, Any]) -> str:
        return self._as_text(
            row.get("description")
            or row.get("aidesc")
            or row.get("summary")
            or row.get("firstMessage")
        )

    def _extract_time(self, row: dict[str, Any]) -> str | None:
        for key in (
            "created_time",
            "createdAt",
            "created_at",
            "published_at",
            "launch_time",
            "timestamp",
        ):
            value = self._as_text(row.get(key))
            if value:
                return value
        return None

    def _extract_launch_time(self, row: dict[str, Any]) -> str | None:
        for key in (
            "launch_time",
            "launchedAt",
            "launched_at",
        ):
            value = self._as_text(row.get(key))
            if value:
                return value
        return None

    def _extract_token_address(self, row: dict[str, Any], url: str) -> str:
        for key in (
            "tokenAddress",
            "token_address",
            "preToken",
            "pre_token",
            "prototypeAddress",
            "prototype_address",
        ):
            value = self._as_text(row.get(key))
            if ADDRESS_RE.match(value):
                return value

        matched = PROTOTYPE_ADDRESS_RE.search(url)
        if matched:
            return matched.group(1)

        return ""

    def _extract_contract_address(self, row: dict[str, Any], token_address: str) -> str:
        for key in (
            "contract_address",
            "contractAddress",
            "migrateTokenAddress",
            "migrate_token_address",
            "launchTokenAddress",
            "launch_token_address",
        ):
            value = self._as_text(row.get(key))
            if ADDRESS_RE.match(value):
                return value
        return token_address

    def _extract_creator(self, value: Any) -> str:
        if isinstance(value, dict):
            for key in ("walletAddress", "displayName", "name", "handle", "username", "id"):
                text = self._as_text(value.get(key))
                if text:
                    return text
            user_socials = value.get("userSocials")
            if isinstance(user_socials, list):
                for item in user_socials:
                    if not isinstance(item, dict):
                        continue
                    text = self._as_text(item.get("walletAddress"))
                    if text:
                        return text
            return ""
        return self._as_text(value)

    def _extract_team(self, row: dict[str, Any]) -> str:
        for key in ("team", "teamInfo", "aboutTeam", "founders"):
            text = self._compact_value(row.get(key))
            if text:
                return text
        return ""

    def _extract_links(self, row: dict[str, Any], *, project_url: str) -> list[str]:
        links: list[str] = []

        for key in ("links", "socialLinks", "socials", "projectLinks"):
            links.extend(self._extract_link_values(row.get(key)))

        for key in ("website", "twitter", "telegram", "discord", "github", "docs"):
            value = self._as_text(row.get(key))
            if value:
                links.append(value)

        creator = row.get("creator")
        if isinstance(creator, dict):
            for key in ("website", "twitter", "github"):
                value = self._as_text(creator.get(key))
                if value:
                    links.append(value)

        normalized = []
        seen: set[str] = set()
        for link in links:
            candidate = link.strip()
            if not candidate or candidate == project_url:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
        return normalized

    def _extract_link_values(self, value: Any) -> list[str]:
        if isinstance(value, list):
            links: list[str] = []
            for item in value:
                links.extend(self._extract_link_values(item))
            return links

        if isinstance(value, dict):
            links: list[str] = []
            for key in ("url", "href", "link", "website", "twitter", "github", "telegram", "discord"):
                text = self._as_text(value.get(key))
                if text:
                    links.append(text)
            return links

        text = self._as_text(value)
        return [text] if text else []

    def _extract_tokenomics(self, row: dict[str, Any]) -> str:
        for key in (
            "tokenomics",
            "tokenomicsInfo",
            "economics",
            "allocation",
            "tokenSupply",
            "supply",
        ):
            text = self._compact_value(row.get(key))
            if text:
                return text
        return ""

    def _extract_addresses(
        self,
        project_id: str,
        creator: str,
        token_address: str,
        contract_address: str,
        row: dict[str, Any],
    ) -> list[ProjectAddress]:
        chain = self._as_text(row.get("chain"), default="BASE").upper()
        first_seen = utc_now_iso()
        addresses: list[ProjectAddress] = []
        for value, address_type in (
            (creator, "creator"),
            (token_address, "token_address"),
            (contract_address, "token_contract"),
            (self._as_text(row.get("treasuryWallet") or row.get("treasuryAddress")), "treasury"),
            (self._as_text(row.get("liquidityPool") or row.get("poolAddress")), "liquidity_pool"),
        ):
            if not ADDRESS_RE.match(value):
                continue
            addresses.append(
                ProjectAddress(
                    project_id=project_id,
                    address=value,
                    address_type=address_type,
                    chain=chain,
                    first_seen=first_seen,
                )
            )
        return addresses

    def _resolve_project_url(self, row: dict[str, Any]) -> str:
        explicit_url = self._as_text(row.get("url") or row.get("link"))
        if explicit_url:
            return explicit_url

        project_id = self._as_text(row.get("id"))
        status = self._as_text(row.get("status")).upper()
        token_or_pre_token = self._as_text(row.get("tokenAddress") or row.get("preToken"))

        if status in {"UNDERGRAD", "INITIALIZED"} and token_or_pre_token:
            return f"{self.app_base_url}/prototypes/{token_or_pre_token}"

        if project_id:
            return f"{self.app_base_url}/virtuals/{project_id}"

        return ""

    def _compact_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            parts = [self._compact_value(item) for item in value]
            return "; ".join(part for part in parts if part)
        if isinstance(value, dict):
            preferred_keys = ("name", "title", "description", "summary", "value", "text", "content")
            parts: list[str] = []
            for key in preferred_keys:
                text = self._as_text(value.get(key))
                if text:
                    parts.append(text)
            if parts:
                return " | ".join(parts)
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value).strip()

    def _as_text(self, value: Any, default: str = "") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text or default
