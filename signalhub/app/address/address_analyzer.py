from __future__ import annotations

import re
from typing import Any

from signalhub.app.database.models import ProjectAddress, ProjectEntity, utc_now_iso


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class AddressAnalyzer:
    def analyze(self, project: ProjectEntity, raw_data: dict[str, Any]) -> list[ProjectAddress]:
        chain = str(raw_data.get("chain") or "BASE").strip().upper()
        first_seen = utc_now_iso()
        candidates = [
            (project.creator, "creator"),
            (project.contract_address, "token_contract"),
            (raw_data.get("treasuryWallet") or raw_data.get("treasuryAddress"), "treasury"),
            (raw_data.get("liquidityPool") or raw_data.get("poolAddress"), "liquidity_pool"),
        ]

        addresses: list[ProjectAddress] = []
        seen: set[tuple[str, str]] = set()
        for value, address_type in candidates:
            address = str(value or "").strip()
            if not ADDRESS_RE.match(address):
                continue
            key = (address.lower(), address_type)
            if key in seen:
                continue
            seen.add(key)
            addresses.append(
                ProjectAddress(
                    project_id=project.project_id,
                    address=address,
                    address_type=address_type,
                    chain=chain,
                    first_seen=first_seen,
                )
            )

        return addresses
