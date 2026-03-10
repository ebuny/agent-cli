"""Minimal quoting engine for the packaged strategies."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from statistics import pstdev
from typing import Any, Callable, Dict, List, Optional

from quoting_engine.config import MarketConfig


@dataclass
class QuoteLevel:
    level: int
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float


@dataclass
class EngineResult:
    halted: bool = False
    reduce_only: bool = False
    vol_bin: str = "I_low"
    m_vol: float = 1.0
    m_dd: float = 1.0
    fv_raw: float = 0.0
    fv_skewed: float = 0.0
    half_spread: float = 0.0
    sigma_price: float = 0.0
    levels: List[QuoteLevel] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


class QuotingEngine:
    """Small, deterministic quoting engine used by strategy wrappers."""

    def __init__(
        self,
        config: MarketConfig,
        toxicity_scorer=None,
        event_schedule=None,
        oracle_monitor=None,
        microprice_calc=None,
        funding_feed=None,
    ):
        self.config = config
        self.toxicity_scorer = toxicity_scorer
        self.event_schedule = event_schedule
        self.oracle_monitor = oracle_monitor
        self.microprice_calc = microprice_calc
        self.funding_feed = funding_feed
        self._vol_bin_classify: Callable[[float], tuple[float, str]] = lambda sigma: (1.0, "I_low")
        self._dd_multiplier: Callable[[float], tuple[float, str]] = lambda dd: (1.0, "green")
        self._mid_history: List[float] = []
        self._prev_oi: Optional[float] = None
        self._liq_cooldown_remaining: int = 0

    def set_risk_classifiers(
        self,
        vol_bin_classify: Callable[[float], tuple[float, str]],
        dd_multiplier: Callable[[float], tuple[float, str]],
    ) -> None:
        self._vol_bin_classify = vol_bin_classify
        self._dd_multiplier = dd_multiplier

    def tick(
        self,
        mid: float,
        bid: float,
        ask: float,
        inventory: float = 0.0,
        daily_drawdown_pct: float = 0.0,
        reduce_only: bool = False,
        timestamp_ms: int = 0,
        open_interest: float = 0.0,
        external_ref: float = 0.0,
    ) -> EngineResult:
        if mid <= 0:
            return EngineResult(halted=True)

        self._mid_history.append(mid)
        self._mid_history = self._mid_history[-32:]

        sigma_log_std = self._sigma_log_std()
        sigma_price = mid * sigma_log_std
        m_vol, vol_bin = self._vol_bin_classify(sigma_log_std)
        m_dd, _ = self._dd_multiplier(daily_drawdown_pct)

        fv_raw = external_ref if external_ref > 0 else self._fair_value(mid, bid, ask)
        inv_skew_bps = inventory * 1.5
        fv_skewed = fv_raw - (mid * inv_skew_bps / 10_000)

        book_spread_bps = ((ask - bid) / mid * 10_000) if ask > 0 and bid > 0 and mid > 0 else 0.0
        spread_bps = max(self.config.spread.min_spread_bps, book_spread_bps)
        spread_bps *= max(m_vol, 1.0) * min(max(m_dd, 1.0), 3.0)

        liq_triggered, in_cascade = self._detect_liquidation(open_interest)
        if in_cascade:
            spread_bps *= self.config.liquidation_detector.spread_mult

        spread_bps = min(spread_bps, self.config.spread.max_spread_bps)
        half_spread = spread_bps / 2

        levels: List[QuoteLevel] = []
        for level in range(self.config.ladder.num_levels):
            size = self.config.ladder.s0 * (self.config.ladder.size_decay ** level)
            offset_bps = half_spread + level * self.config.spread.level_spacing_bps
            offset_px = mid * offset_bps / 10_000
            levels.append(QuoteLevel(
                level=level,
                bid_price=round(fv_skewed - offset_px, 6),
                ask_price=round(fv_skewed + offset_px, 6),
                bid_size=round(max(size, 0.0), 6),
                ask_size=round(max(size, 0.0), 6),
            ))

        return EngineResult(
            halted=False,
            reduce_only=reduce_only,
            vol_bin=vol_bin,
            m_vol=round(m_vol, 4),
            m_dd=round(m_dd, 4),
            fv_raw=fv_raw,
            fv_skewed=fv_skewed,
            half_spread=half_spread,
            sigma_price=sigma_price,
            levels=levels,
            meta={
                "liq_triggered": liq_triggered,
                "liq_cooldown_remaining": self._liq_cooldown_remaining,
            },
        )

    def _fair_value(self, mid: float, bid: float, ask: float) -> float:
        if self.microprice_calc is not None and bid > 0 and ask > 0:
            try:
                micro = self.microprice_calc.compute(bid, ask)
                return (mid + micro) / 2
            except Exception:
                return mid
        return mid

    def _sigma_log_std(self) -> float:
        if len(self._mid_history) < 3:
            return 0.0
        log_returns = []
        for prev, curr in zip(self._mid_history[:-1], self._mid_history[1:]):
            if prev > 0 and curr > 0:
                log_returns.append(math.log(curr / prev))
        if len(log_returns) < 2:
            return 0.0
        return pstdev(log_returns)

    def _detect_liquidation(self, open_interest: float) -> tuple[bool, bool]:
        cfg = self.config.liquidation_detector
        if not cfg.enabled or open_interest <= 0:
            self._prev_oi = open_interest or self._prev_oi
            return False, False

        liq_triggered = False
        if self._prev_oi and self._prev_oi > 0:
            oi_drop_pct = (self._prev_oi - open_interest) / self._prev_oi * 100
            if oi_drop_pct >= cfg.oi_drop_threshold_pct:
                liq_triggered = True
                self._liq_cooldown_remaining = cfg.cooldown_ticks

        self._prev_oi = open_interest

        in_cascade = liq_triggered or self._liq_cooldown_remaining > 0
        if self._liq_cooldown_remaining > 0:
            self._liq_cooldown_remaining -= 1

        return liq_triggered, in_cascade

