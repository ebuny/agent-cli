"""Live allocator that filters research allocations by current market regime."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from modules.allocator_feedback import AllocatorFeedbackEngine, FeedbackAdjustment
from modules.market_regime import MarketRegimeClassifier, RegimeProfile
from modules.research_pipeline import HistoricalDatasetLoader
from modules.routing_policy import RoutingPolicy, filter_routing_policy


@dataclass
class LiveAllocationDecision:
    strategy_id: str
    enabled: bool
    capital_usd: float
    weight: float
    current_regime: str
    supported_regimes: List[str] = field(default_factory=list)
    dominant_regime: str = ""
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LiveAllocationPlan:
    snapshot_path: str
    current_regime: RegimeProfile
    deployable_capital_usd: float
    allocated_capital_usd: float
    enabled_strategies: int
    decisions: List[LiveAllocationDecision] = field(default_factory=list)
    routing_policy: RoutingPolicy = field(default_factory=RoutingPolicy)
    feedback: Optional[FeedbackAdjustment] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "snapshot_path": self.snapshot_path,
            "current_regime": self.current_regime.to_dict(),
            "deployable_capital_usd": round(self.deployable_capital_usd, 2),
            "allocated_capital_usd": round(self.allocated_capital_usd, 2),
            "enabled_strategies": self.enabled_strategies,
            "decisions": [item.to_dict() for item in self.decisions],
            "routing_policy": self.routing_policy.to_dict(),
        }
        if self.feedback:
            payload["feedback"] = self.feedback.to_dict()
        return payload


class LiveAllocator:
    """Convert research snapshot outputs into a regime-aware live deployment plan."""

    def __init__(self):
        self.dataset_loader = HistoricalDatasetLoader()
        self.regime_classifier = MarketRegimeClassifier()

    def plan(
        self,
        snapshot_path: str,
        dataset_path: Optional[str] = None,
        regime_override: Optional[str] = None,
        recent_window: int = 12,
        feedback_enable: bool = False,
        feedback_trades_path: Optional[str] = None,
        feedback_judge_path: Optional[str] = None,
        feedback_min_round_trips: int = 5,
        feedback_fp_block_threshold: float = 60.0,
        feedback_fp_reduce_threshold: float = 40.0,
    ) -> LiveAllocationPlan:
        payload = json.loads(Path(snapshot_path).read_text())
        current_regime = self._resolve_regime(
            dataset_path=dataset_path,
            regime_override=regime_override,
            recent_window=recent_window,
        )

        decisions: List[LiveAllocationDecision] = []
        for item in payload.get("strategies", []):
            strategy_id = str(item.get("strategy_id", ""))
            scorecard = item.get("scorecard") or {}
            allocation = item.get("allocation") or {}
            supported_regimes = list(scorecard.get("supported_regimes", []) or [])
            dominant_regime = str(scorecard.get("dominant_regime", "") or "")
            enabled = bool(allocation.get("enabled", False))
            reasons = list(allocation.get("reasons", []) or [])

            regime_allowed = self._regime_allowed(
                current=current_regime.label,
                supported=supported_regimes,
                dominant=dominant_regime,
            )
            capital = float(allocation.get("capital_usd", 0.0) or 0.0) if enabled and regime_allowed else 0.0
            decision_enabled = enabled and regime_allowed and capital > 0
            if enabled and not regime_allowed:
                reasons = [r for r in reasons if r != "eligible_for_allocation"]
                reasons.append(f"regime_mismatch:{current_regime.label}")
            if not enabled and not reasons:
                reasons.append("not_enabled_in_research")

            decisions.append(LiveAllocationDecision(
                strategy_id=strategy_id,
                enabled=decision_enabled,
                capital_usd=round(capital, 2),
                weight=float(allocation.get("weight", 0.0) or 0.0) if decision_enabled else 0.0,
                current_regime=current_regime.label,
                supported_regimes=supported_regimes,
                dominant_regime=dominant_regime,
                reasons=reasons,
            ))

        decisions.sort(key=lambda item: (-item.capital_usd, item.strategy_id))
        deployable = float((payload.get("summary") or {}).get("deployable_capital_usd", 0.0) or 0.0)
        enabled_ids = [item.strategy_id for item in decisions if item.enabled]
        routing_policy = filter_routing_policy(
            RoutingPolicy.from_dict(payload.get("routing_policy", {})),
            current_regime=current_regime.label,
            enabled_strategy_ids=enabled_ids,
        )
        feedback: Optional[FeedbackAdjustment] = None
        if feedback_enable:
            engine = AllocatorFeedbackEngine(
                min_round_trips=feedback_min_round_trips,
                fp_block_threshold=feedback_fp_block_threshold,
                fp_reduce_threshold=feedback_fp_reduce_threshold,
            )
            feedback = engine.evaluate(
                trades_path=feedback_trades_path,
                judge_report_path=feedback_judge_path,
                data_dir=str(Path(feedback_trades_path).parent) if feedback_trades_path else None,
            )
            decisions, routing_policy = self._apply_feedback(decisions, routing_policy, feedback)
            enabled_ids = [item.strategy_id for item in decisions if item.enabled]
            routing_policy = filter_routing_policy(
                routing_policy,
                current_regime=current_regime.label,
                enabled_strategy_ids=enabled_ids,
            )

        decisions.sort(key=lambda item: (-item.capital_usd, item.strategy_id))
        allocated_capital = sum(item.capital_usd for item in decisions)
        enabled_count = sum(1 for item in decisions if item.enabled)
        return LiveAllocationPlan(
            snapshot_path=snapshot_path,
            current_regime=current_regime,
            deployable_capital_usd=deployable,
            allocated_capital_usd=allocated_capital,
            enabled_strategies=enabled_count,
            decisions=decisions,
            routing_policy=routing_policy,
            feedback=feedback,
        )

    def save_plan(self, plan: LiveAllocationPlan, output_path: str) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(plan.to_dict(), indent=2))
        return path

    def _resolve_regime(
        self,
        dataset_path: Optional[str],
        regime_override: Optional[str],
        recent_window: int,
    ) -> RegimeProfile:
        if regime_override:
            return RegimeProfile(label=regime_override)
        if dataset_path:
            dataset = self.dataset_loader.load(dataset_path)
            snapshots = dataset.snapshots[-recent_window:] if recent_window > 0 else dataset.snapshots
            return self.regime_classifier.classify(snapshots)
        return RegimeProfile()

    @staticmethod
    def _regime_allowed(current: str, supported: Sequence[str], dominant: str) -> bool:
        if not current or current == "unknown":
            return True
        if current in supported:
            return True
        if not supported and dominant:
            return current == dominant
        return False

    @staticmethod
    def _apply_feedback(
        decisions: List[LiveAllocationDecision],
        routing_policy: RoutingPolicy,
        feedback: FeedbackAdjustment,
    ) -> Tuple[List[LiveAllocationDecision], RoutingPolicy]:
        if not feedback:
            return decisions, routing_policy

        pre_rule_strategies = {rule.strategy_id for rule in routing_policy.rules}
        if feedback.blocked_sources:
            blocked = set(feedback.blocked_sources)
            filtered_rules = [rule for rule in routing_policy.rules if rule.source not in blocked]
            routing_policy = RoutingPolicy(generated_at=routing_policy.generated_at, rules=filtered_rules)
        else:
            blocked = set()

        post_rule_strategies = {rule.strategy_id for rule in routing_policy.rules}
        removed_strategies = pre_rule_strategies - post_rule_strategies
        if removed_strategies and blocked:
            blocked_summary = ",".join(sorted(blocked))
            for decision in decisions:
                if decision.strategy_id in removed_strategies and decision.enabled:
                    decision.enabled = False
                    decision.capital_usd = 0.0
                    decision.weight = 0.0
                    decision.reasons.append(f"feedback_blocked_sources:{blocked_summary}")

        if feedback.capital_multiplier < 1.0:
            for decision in decisions:
                if decision.enabled and decision.capital_usd > 0:
                    decision.capital_usd = round(decision.capital_usd * feedback.capital_multiplier, 2)
                    decision.reasons.append(
                        f"feedback_capital_multiplier:{feedback.capital_multiplier:.2f}"
                    )
                    if decision.capital_usd <= 0:
                        decision.enabled = False
                        decision.reasons.append("feedback_capital_blocked")

        total_enabled = sum(d.capital_usd for d in decisions if d.enabled and d.capital_usd > 0)
        for decision in decisions:
            if decision.enabled and decision.capital_usd > 0 and total_enabled > 0:
                decision.weight = round(decision.capital_usd / total_enabled, 4)
            else:
                decision.weight = 0.0

        return decisions, routing_policy
