"""Runtime consumption of live allocation plans for capital gating."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from modules.routing_policy import RoutingPolicy, routing_sources


@dataclass
class RuntimeAllocationGate:
    configured: bool = False
    allow_entries: bool = True
    plan_path: str = ""
    current_regime: str = "unknown"
    deployable_capital_usd: float = 0.0
    allocated_capital_usd: float = 0.0
    enabled_strategies: int = 0
    per_slot_margin_usd: float = 0.0
    enabled_strategy_ids: List[str] = field(default_factory=list)
    allowed_entry_sources: List[str] = field(default_factory=list)
    routing_rules: List[Dict[str, Any]] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RuntimeAllocationLoader:
    """Load a live allocation plan and convert it into WOLF budget controls."""

    def load(
        self,
        plan_path: str,
        total_budget_usd: float,
        max_slots: int,
    ) -> RuntimeAllocationGate:
        path = Path(plan_path)
        if not path.exists():
            return RuntimeAllocationGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                reasons=["allocation_plan_missing"],
            )

        data = json.loads(path.read_text())
        allocated = float(data.get("allocated_capital_usd", 0.0) or 0.0)
        deployable = float(data.get("deployable_capital_usd", 0.0) or 0.0)
        enabled_count = int(data.get("enabled_strategies", 0) or 0)
        current_regime = str((data.get("current_regime") or {}).get("label", "unknown") or "unknown")
        enabled_decisions = [
            item for item in data.get("decisions", [])
            if bool(item.get("enabled", False)) and float(item.get("capital_usd", 0.0) or 0.0) > 0
        ]
        enabled_strategy_ids = [str(item.get("strategy_id", "")) for item in enabled_decisions]
        routing_policy = RoutingPolicy.from_dict(data.get("routing_policy", {}))
        allowed_entry_sources = routing_sources(routing_policy)

        capped_budget = min(total_budget_usd, allocated) if allocated > 0 else 0.0
        reasons: List[str] = []
        if enabled_count <= 0 or capped_budget <= 0:
            reasons.append("no_validated_edge")
        elif not allowed_entry_sources:
            reasons.append("no_wolf_compatible_edge")
        if current_regime:
            reasons.append(f"regime:{current_regime}")

        return RuntimeAllocationGate(
            configured=True,
            allow_entries=enabled_count > 0 and capped_budget > 0 and bool(allowed_entry_sources),
            plan_path=str(path),
            current_regime=current_regime,
            deployable_capital_usd=deployable,
            allocated_capital_usd=capped_budget,
            enabled_strategies=enabled_count,
            per_slot_margin_usd=(capped_budget / max(max_slots, 1)) if capped_budget > 0 else 0.0,
            enabled_strategy_ids=enabled_strategy_ids,
            allowed_entry_sources=allowed_entry_sources,
            routing_rules=[rule.to_dict() for rule in routing_policy.rules],
            reasons=reasons,
        )
