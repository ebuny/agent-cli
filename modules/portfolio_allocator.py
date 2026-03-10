"""Portfolio-level allocator to cap concurrent strategy budgets."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from parent.store import StateDB


@dataclass
class PortfolioGate:
    configured: bool = False
    allow_entries: bool = True
    plan_path: str = ""
    strategy_id: str = ""
    instrument: str = ""
    total_capital_usd: float = 0.0
    strategy_capital_usd: float = 0.0
    approved_capital_usd: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioAllocationRecord:
    runner_id: str
    strategy_id: str
    instrument: str
    approved_capital_usd: float
    requested_capital_usd: float
    updated_at_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PortfolioAllocator:
    """Coordinate shared capital allocation across multiple runner processes."""

    def __init__(self, state_db_path: str = "data/portfolio/state.db", ttl_ms: int = 600_000):
        self.state_db = StateDB(path=state_db_path)
        self.ttl_ms = max(int(ttl_ms), 10_000)

    def refresh(
        self,
        plan_path: str,
        runner_id: str,
        strategy_id: str,
        instrument: str,
        requested_capital_usd: float,
    ) -> PortfolioGate:
        path = Path(plan_path)
        if not path.exists():
            return PortfolioGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                strategy_id=strategy_id,
                instrument=instrument,
                reasons=["portfolio_plan_missing"],
            )

        plan = json.loads(path.read_text())
        total_cap = float(plan.get("allocated_capital_usd", 0.0) or 0.0)
        if total_cap <= 0:
            return PortfolioGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                strategy_id=strategy_id,
                instrument=instrument,
                total_capital_usd=total_cap,
                reasons=["portfolio_cap_empty"],
            )

        strategy_cap = self._strategy_cap(plan, strategy_id)
        if strategy_id != "*" and strategy_cap <= 0:
            return PortfolioGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                strategy_id=strategy_id,
                instrument=instrument,
                total_capital_usd=total_cap,
                strategy_capital_usd=strategy_cap,
                reasons=["strategy_not_enabled"],
            )

        allocations = self._load_allocations()
        now_ms = int(time.time() * 1000)
        allocations = self._prune_stale(allocations, now_ms)

        current_total = sum(
            item.approved_capital_usd for item in allocations.values()
            if item.runner_id != runner_id
        )
        current_strategy = 0.0
        if strategy_id != "*":
            current_strategy = sum(
                item.approved_capital_usd for item in allocations.values()
                if item.runner_id != runner_id and item.strategy_id == strategy_id
            )

        available_total = max(total_cap - current_total, 0.0)
        available_strategy = available_total
        if strategy_id != "*":
            available_strategy = max(strategy_cap - current_strategy, 0.0)

        requested = float(requested_capital_usd or 0.0)
        approved = min(requested, available_total, available_strategy)

        reasons: List[str] = []
        allow_entries = approved > 0
        if not allow_entries:
            if available_total <= 0:
                reasons.append("portfolio_cap_exhausted")
            if strategy_id != "*" and available_strategy <= 0:
                reasons.append("strategy_cap_exhausted")

        allocations[runner_id] = PortfolioAllocationRecord(
            runner_id=runner_id,
            strategy_id=strategy_id,
            instrument=instrument,
            approved_capital_usd=round(approved, 2),
            requested_capital_usd=round(requested, 2),
            updated_at_ms=now_ms,
        )
        self._save_allocations(allocations)

        return PortfolioGate(
            configured=True,
            allow_entries=allow_entries,
            plan_path=str(path),
            strategy_id=strategy_id,
            instrument=instrument,
            total_capital_usd=round(total_cap, 2),
            strategy_capital_usd=round(strategy_cap if strategy_id != "*" else total_cap, 2),
            approved_capital_usd=round(approved, 2),
            reasons=reasons,
        )

    def _load_allocations(self) -> Dict[str, PortfolioAllocationRecord]:
        raw = self.state_db.get("portfolio_allocations") or {}
        allocations: Dict[str, PortfolioAllocationRecord] = {}
        for runner_id, payload in raw.items():
            try:
                allocations[runner_id] = PortfolioAllocationRecord(**payload)
            except TypeError:
                continue
        return allocations

    def _save_allocations(self, allocations: Dict[str, PortfolioAllocationRecord]) -> None:
        payload = {rid: item.to_dict() for rid, item in allocations.items()}
        self.state_db.put("portfolio_allocations", payload)

    def _prune_stale(
        self,
        allocations: Dict[str, PortfolioAllocationRecord],
        now_ms: int,
    ) -> Dict[str, PortfolioAllocationRecord]:
        fresh: Dict[str, PortfolioAllocationRecord] = {}
        for runner_id, item in allocations.items():
            if now_ms - int(item.updated_at_ms) <= self.ttl_ms:
                fresh[runner_id] = item
        return fresh

    @staticmethod
    def _strategy_cap(plan: Dict[str, Any], strategy_id: str) -> float:
        if strategy_id == "*":
            return float(plan.get("allocated_capital_usd", 0.0) or 0.0)
        for item in plan.get("decisions", []):
            if str(item.get("strategy_id", "")) == strategy_id:
                return float(item.get("capital_usd", 0.0) or 0.0)
        return 0.0
