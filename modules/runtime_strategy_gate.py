"""Runtime allocation gating for single-strategy engine runs."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RuntimeStrategyGate:
    configured: bool = False
    allow_entries: bool = True
    plan_path: str = ""
    strategy_id: str = ""
    instrument: str = ""
    current_regime: str = "unknown"
    capital_usd: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RuntimeStrategyGateLoader:
    """Load a live allocation plan and decide if a strategy is allowed to trade."""

    def load(
        self,
        plan_path: str,
        strategy_id: str,
        instrument: str,
    ) -> RuntimeStrategyGate:
        path = Path(plan_path)
        if not path.exists():
            return RuntimeStrategyGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                strategy_id=strategy_id,
                instrument=instrument,
                reasons=["allocation_plan_missing"],
            )

        data = json.loads(path.read_text())
        current_regime = str((data.get("current_regime") or {}).get("label", "unknown") or "unknown")
        decision = self._find_decision(data, strategy_id)
        if not decision:
            return RuntimeStrategyGate(
                configured=True,
                allow_entries=False,
                plan_path=str(path),
                strategy_id=strategy_id,
                instrument=instrument,
                current_regime=current_regime,
                reasons=["strategy_not_enabled"],
            )

        enabled = bool(decision.get("enabled", False))
        capital = float(decision.get("capital_usd", 0.0) or 0.0)
        reasons = list(decision.get("reasons", []) or [])
        allow_entries = enabled and capital > 0.0
        if not allow_entries:
            reasons.append("strategy_not_enabled")

        if allow_entries:
            rules = self._strategy_rules(data, strategy_id)
            if rules and not self._instrument_allowed(rules, instrument):
                allow_entries = False
                reasons.append("instrument_not_allowed")

        return RuntimeStrategyGate(
            configured=True,
            allow_entries=allow_entries,
            plan_path=str(path),
            strategy_id=strategy_id,
            instrument=instrument,
            current_regime=current_regime,
            capital_usd=capital,
            reasons=reasons,
        )

    @staticmethod
    def _find_decision(data: Dict[str, Any], strategy_id: str) -> Optional[Dict[str, Any]]:
        for item in data.get("decisions", []):
            if str(item.get("strategy_id", "")) == strategy_id:
                return item
        return None

    @staticmethod
    def _strategy_rules(data: Dict[str, Any], strategy_id: str) -> List[Dict[str, Any]]:
        rules = []
        for rule in (data.get("routing_policy") or {}).get("rules", []):
            if str(rule.get("strategy_id", "")) == strategy_id:
                rules.append(rule)
        return rules

    @staticmethod
    def _instrument_allowed(rules: List[Dict[str, Any]], instrument: str) -> bool:
        for rule in rules:
            instruments = list(rule.get("allowed_instruments", []) or [])
            if not instruments or instrument in instruments:
                return True
        return False
