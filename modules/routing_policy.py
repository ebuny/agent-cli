"""Build and apply explicit runtime routing policy artifacts from research."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence


_STRATEGY_SOURCE_PROFILES: Dict[str, List[str]] = {
    "momentum_breakout": ["scanner", "movers_signal"],
    "mean_reversion": ["scanner"],
    "aggressive_taker": ["scanner", "movers_signal", "movers_immediate"],
    "liquidation_mm": ["movers_immediate", "movers_signal"],
    "claude_agent": ["scanner", "movers_signal", "movers_immediate", "smart_money"],
}


@dataclass
class RoutingRule:
    strategy_id: str
    source: str
    allowed_regimes: List[str] = field(default_factory=list)
    allowed_instruments: List[str] = field(default_factory=list)
    min_signal_score: float = 0.0
    capital_usd: float = 0.0
    weight: float = 0.0
    score: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RoutingPolicy:
    generated_at: str = ""
    rules: List[RoutingRule] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "rules": [rule.to_dict() for rule in self.rules],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RoutingPolicy":
        return cls(
            generated_at=str(data.get("generated_at", "")),
            rules=[RoutingRule(**item) for item in data.get("rules", [])],
        )


class ResearchRoutingPolicyBuilder:
    """Build runtime routing rules from research results and allocations."""

    def build(
        self,
        generated_at: str,
        strategy_results: Sequence[Any],
        allocations: Sequence[Dict[str, Any]],
        edge_report: Optional[Dict[str, Any]] = None,
    ) -> RoutingPolicy:
        allocation_by_strategy = {
            str(item.get("strategy_id", "")): item
            for item in allocations
        }
        threshold_map = self._build_threshold_map(edge_report or {})
        rules: List[RoutingRule] = []

        for result in strategy_results:
            scorecard = result.scorecard
            allocation = allocation_by_strategy.get(result.strategy_id, {})
            if scorecard is None:
                continue
            if not bool(allocation.get("enabled", False)) or float(allocation.get("capital_usd", 0.0) or 0.0) <= 0:
                continue

            sources = _STRATEGY_SOURCE_PROFILES.get(result.strategy_id, [])
            if not sources:
                continue

            allowed_instruments = self._derive_allowed_instruments(result)
            allowed_regimes = list(scorecard.supported_regimes or ([scorecard.dominant_regime] if scorecard.dominant_regime else []))

            for source in sources:
                instruments = allowed_instruments or [""]
                for instrument in instruments:
                    threshold = self._lookup_threshold(
                        threshold_map=threshold_map,
                        source=source,
                        instrument=instrument or "*",
                    )
                    rule_reasons = list(allocation.get("reasons", []) or [])
                    if threshold > 0:
                        rule_reasons.append("empirical_min_signal_score")
                    rules.append(RoutingRule(
                        strategy_id=result.strategy_id,
                        source=source,
                        allowed_regimes=allowed_regimes,
                        allowed_instruments=[instrument] if instrument else [],
                        min_signal_score=threshold,
                        capital_usd=float(allocation.get("capital_usd", 0.0) or 0.0),
                        weight=float(allocation.get("weight", 0.0) or 0.0),
                        score=float(scorecard.score or 0.0),
                        reasons=rule_reasons,
                    ))

        rules.sort(key=lambda item: (-item.capital_usd, -item.score, item.source, item.strategy_id))
        return RoutingPolicy(generated_at=generated_at, rules=rules)

    @staticmethod
    def _derive_allowed_instruments(result: Any) -> List[str]:
        instruments = []
        for item in result.dataset_breakdown:
            scorecard = item.scorecard
            if scorecard and scorecard.enabled and scorecard.score > 0:
                instruments.append(item.instrument)
        if not instruments:
            instruments = [item.instrument for item in result.dataset_breakdown if item.instrument]
        return sorted(set(instruments))

    @staticmethod
    def _build_threshold_map(edge_report: Dict[str, Any]) -> Dict[tuple[str, str], float]:
        threshold_map: Dict[tuple[str, str], float] = {}
        for item in edge_report.get("thresholds", []) or []:
            source = _normalize_source(str(item.get("source", "")))
            instrument = str(item.get("instrument", "*") or "*")
            if not source:
                continue
            threshold_map[(source, instrument)] = float(item.get("min_signal_score", 0.0) or 0.0)
        return threshold_map

    @staticmethod
    def _lookup_threshold(
        threshold_map: Dict[tuple[str, str], float],
        source: str,
        instrument: str,
    ) -> float:
        normalized_source = _normalize_source(source)
        if (normalized_source, instrument) in threshold_map:
            return threshold_map[(normalized_source, instrument)]
        return threshold_map.get((normalized_source, "*"), 0.0)


def filter_routing_policy(
    policy: RoutingPolicy,
    current_regime: str,
    enabled_strategy_ids: Optional[Sequence[str]] = None,
) -> RoutingPolicy:
    enabled_set = set(enabled_strategy_ids or [])
    filtered = []
    for rule in policy.rules:
        if enabled_set and rule.strategy_id not in enabled_set:
            continue
        if rule.allowed_regimes and current_regime and current_regime != "unknown":
            if current_regime not in rule.allowed_regimes:
                continue
        filtered.append(rule)
    return RoutingPolicy(generated_at=policy.generated_at, rules=filtered)


def routing_sources(policy: RoutingPolicy) -> List[str]:
    return sorted({rule.source for rule in policy.rules})


def routing_allows(
    policy: RoutingPolicy,
    source: str,
    instrument: str,
    signal_score: float = 0.0,
) -> bool:
    normalized = source.split(":", 1)[0] if source.startswith("smart_money:") else source
    for rule in policy.rules:
        if rule.source != normalized:
            continue
        if rule.allowed_instruments and instrument not in rule.allowed_instruments:
            continue
        if signal_score < float(rule.min_signal_score or 0.0):
            continue
        return True
    return False


def _normalize_source(source: str) -> str:
    if source.startswith("smart_money:"):
        return "smart_money"
    return source
