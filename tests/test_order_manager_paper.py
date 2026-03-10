"""Tests for paper-mode order execution."""
from __future__ import annotations

from decimal import Decimal

from cli.order_manager import OrderManager
from common.models import MarketSnapshot, StrategyDecision


class _StubHL:
    def get_open_orders(self, instrument: str):
        return []

    def cancel_order(self, instrument: str, oid: str) -> bool:
        return True


def test_order_manager_paper_fills_orders():
    snapshot = MarketSnapshot(
        instrument="ETH-PERP",
        mid_price=100.0,
        bid=99.5,
        ask=100.5,
        timestamp_ms=123,
    )
    decision = StrategyDecision(
        action="place_order",
        instrument="ETH-PERP",
        side="buy",
        size=0.5,
        limit_price=100.25,
    )

    manager = OrderManager(_StubHL(), instrument="ETH-PERP", paper=True)
    fills = manager.update([decision], snapshot)

    assert len(fills) == 1
    fill = fills[0]
    assert fill.instrument == "ETH-PERP"
    assert fill.side == "buy"
    assert fill.quantity == Decimal("0.5")
    assert fill.price == Decimal("100.25")
