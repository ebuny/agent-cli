"""Historical replay and walk-forward evaluation utilities."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Dict, List, Optional, Sequence

from common.models import MarketSnapshot
from modules.howl_engine import HowlEngine, HowlMetrics, TradeRecord
from modules.market_regime import MarketRegimeClassifier
from parent.position_tracker import PositionTracker
from sdk.strategy_sdk.base import BaseStrategy, StrategyContext


ZERO = Decimal("0")


@dataclass
class ReplayTrade:
    tick: int
    instrument: str
    side: str
    price: float
    quantity: float
    timestamp_ms: int
    meta: str = ""
    strategy: str = ""
    fee: float = 0.0

    def to_dict(self) -> Dict[str, object]:
        return {
            "tick": self.tick,
            "oid": f"replay-{self.tick}-{self.instrument}-{self.side}",
            "instrument": self.instrument,
            "side": self.side,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "timestamp_ms": self.timestamp_ms,
            "fee": str(self.fee),
            "strategy": self.strategy,
            "meta": self.meta,
        }


@dataclass
class ReplayResult:
    ticks: int = 0
    trades: List[ReplayTrade] = field(default_factory=list)
    metrics: HowlMetrics = field(default_factory=HowlMetrics)
    ending_position_qty: float = 0.0
    ending_mark_price: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0


@dataclass
class WalkForwardFold:
    fold: int
    train_start: int
    train_end: int
    validation_start: int
    validation_end: int


@dataclass
class WalkForwardFoldResult:
    fold: WalkForwardFold
    train: ReplayResult
    validation: ReplayResult
    validation_regime: str = "unknown"


class StrategyReplayEngine:
    """Replay market snapshots through a strategy with deterministic fills."""

    def __init__(
        self,
        strategy: BaseStrategy,
        instrument: str = "ETH-PERP",
        close_open_positions: bool = True,
    ):
        self.strategy = strategy
        self.instrument = instrument
        self.close_open_positions = close_open_positions

    def run(self, snapshots: Sequence[MarketSnapshot]) -> ReplayResult:
        tracker = PositionTracker()
        trades: List[ReplayTrade] = []
        peak_pnl = 0.0
        max_drawdown = 0.0
        last_snapshot = None

        for tick, snapshot in enumerate(snapshots, start=1):
            last_snapshot = snapshot
            pos = tracker.get_agent_position(self.strategy.strategy_id, self.instrument)
            mark_price = Decimal(str(snapshot.mid_price))
            total_pnl = float(pos.total_pnl(mark_price))
            peak_pnl = max(peak_pnl, total_pnl)
            max_drawdown = max(max_drawdown, peak_pnl - total_pnl)

            ctx = StrategyContext(
                snapshot=snapshot,
                position_qty=float(pos.net_qty),
                position_notional=float(pos.notional),
                unrealized_pnl=float(pos.unrealized_pnl(mark_price)),
                realized_pnl=float(pos.realized_pnl),
                round_number=tick,
            )

            decisions = self.strategy.on_tick(snapshot, context=ctx)
            for decision in decisions:
                if decision.action != "place_order" or decision.size <= 0:
                    continue

                fill_price = snapshot.ask if decision.side == "buy" else snapshot.bid
                if fill_price <= 0:
                    fill_price = decision.limit_price
                if fill_price <= 0:
                    continue

                tracker.apply_fill(
                    self.strategy.strategy_id,
                    decision.instrument or self.instrument,
                    decision.side,
                    Decimal(str(decision.size)),
                    Decimal(str(fill_price)),
                )
                trades.append(ReplayTrade(
                    tick=tick,
                    instrument=decision.instrument or self.instrument,
                    side=decision.side,
                    price=fill_price,
                    quantity=decision.size,
                    timestamp_ms=snapshot.timestamp_ms or tick,
                    meta=str(decision.meta.get("signal", decision.meta.get("reason", ""))),
                    strategy=self.strategy.strategy_id,
                ))

        if self.close_open_positions and last_snapshot is not None:
            pos = tracker.get_agent_position(self.strategy.strategy_id, self.instrument)
            if pos.net_qty != ZERO:
                side = "sell" if pos.net_qty > ZERO else "buy"
                fill_price = last_snapshot.bid if side == "sell" else last_snapshot.ask
                if fill_price > 0:
                    qty = float(abs(pos.net_qty))
                    tracker.apply_fill(
                        self.strategy.strategy_id,
                        self.instrument,
                        side,
                        Decimal(str(qty)),
                        Decimal(str(fill_price)),
                    )
                    trades.append(ReplayTrade(
                        tick=len(snapshots) + 1,
                        instrument=self.instrument,
                        side=side,
                        price=fill_price,
                        quantity=qty,
                        timestamp_ms=(last_snapshot.timestamp_ms or len(snapshots)) + 1,
                        meta="replay_close",
                        strategy=self.strategy.strategy_id,
                    ))

        final_pos = tracker.get_agent_position(self.strategy.strategy_id, self.instrument)
        ending_mark = Decimal(str(last_snapshot.mid_price)) if last_snapshot else ZERO
        realized_pnl = float(final_pos.realized_pnl)
        unrealized_pnl = float(final_pos.unrealized_pnl(ending_mark))
        total_pnl = float(final_pos.total_pnl(ending_mark))
        metrics = HowlEngine().compute([TradeRecord.from_dict(t.to_dict()) for t in trades])

        return ReplayResult(
            ticks=len(snapshots),
            trades=trades,
            metrics=metrics,
            ending_position_qty=float(final_pos.net_qty),
            ending_mark_price=float(ending_mark),
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            total_pnl=total_pnl,
            max_drawdown=max_drawdown,
        )


def build_walk_forward_folds(
    snapshots: Sequence[MarketSnapshot],
    train_size: int,
    validation_size: int,
    step_size: Optional[int] = None,
) -> List[WalkForwardFold]:
    """Split snapshots into sequential walk-forward folds."""
    if train_size <= 0 or validation_size <= 0:
        raise ValueError("train_size and validation_size must be positive")

    folds: List[WalkForwardFold] = []
    step = step_size or validation_size
    start = 0
    fold_idx = 1
    total = len(snapshots)

    while start + train_size + validation_size <= total:
        train_start = start
        train_end = start + train_size
        val_start = train_end
        val_end = val_start + validation_size
        folds.append(WalkForwardFold(
            fold=fold_idx,
            train_start=train_start,
            train_end=train_end,
            validation_start=val_start,
            validation_end=val_end,
        ))
        start += step
        fold_idx += 1

    return folds


class WalkForwardEvaluator:
    """Evaluate a strategy factory across walk-forward folds."""

    def __init__(
        self,
        strategy_factory: Callable[[], BaseStrategy],
        instrument: str = "ETH-PERP",
    ):
        self.strategy_factory = strategy_factory
        self.instrument = instrument
        self.regime_classifier = MarketRegimeClassifier()

    def evaluate(
        self,
        snapshots: Sequence[MarketSnapshot],
        train_size: int,
        validation_size: int,
        step_size: Optional[int] = None,
    ) -> List[WalkForwardFoldResult]:
        folds = build_walk_forward_folds(
            snapshots,
            train_size=train_size,
            validation_size=validation_size,
            step_size=step_size,
        )

        results: List[WalkForwardFoldResult] = []
        for fold in folds:
            train_engine = StrategyReplayEngine(
                self.strategy_factory(),
                instrument=self.instrument,
            )
            validation_engine = StrategyReplayEngine(
                self.strategy_factory(),
                instrument=self.instrument,
            )
            train_result = train_engine.run(snapshots[fold.train_start:fold.train_end])
            validation_result = validation_engine.run(
                snapshots[fold.validation_start:fold.validation_end]
            )
            results.append(WalkForwardFoldResult(
                fold=fold,
                train=train_result,
                validation=validation_result,
                validation_regime=self.regime_classifier.classify(
                    snapshots[fold.validation_start:fold.validation_end]
                ).label,
            ))
        return results
