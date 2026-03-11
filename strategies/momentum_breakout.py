"""Momentum breakout strategy ??" enter on volume + price breakout above/below N-period range."""
from __future__ import annotations

from collections import deque
from typing import List, Optional

from common.models import MarketSnapshot, StrategyDecision
from sdk.strategy_sdk.base import BaseStrategy, StrategyContext


class MomentumBreakoutStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str = "momentum_breakout",
        lookback: int = 36,  # Increased from 18 to 36. A longer lookback period helps define a more robust price range (e.g., 36 hours for 1-hour candles), reducing noise and potentially identifying stronger, more sustained breakouts. This aims to reduce false signals.
        breakout_threshold_bps: float = 300.0,  # Increased from 200.0. A higher threshold requires a stronger price movement for a breakout, aiming to filter out weaker, potentially false, signals. This should lead to fewer, but higher-conviction, trades.
        volume_surge_mult: float = 2.0,  # Retained at 2.0. This multiplier ensures breakouts are confirmed by significant trading activity.
        stop_loss_bps: float = 500.0,  # Increased from 300.0. A significantly wider stop loss gives trades more room to develop and reduces premature stop-outs, especially for breakout strategies that might experience initial pullbacks or retests of the breakout level.
        take_profit_mult: float = 3.0,  # Retained at 3.0. Aims for a 3:1 Reward:Risk ratio with the new stop_loss_bps (1500 bps TP). This maintains the strategy's goal to seek substantial profitability from winning trades despite a potentially low win rate, by letting winners run.
        size: float = 1.0,
    ):
        super().__init__(strategy_id=strategy_id)
        self.lookback = lookback
        self.breakout_threshold_bps = breakout_threshold_bps
        self.volume_surge_mult = volume_surge_mult
        self.stop_loss_bps = stop_loss_bps
        self.take_profit_mult = take_profit_mult
        self.size = size
        self.entry_price = 0.0

        self.highs: deque = deque(maxlen=lookback)
        self.lows: deque = deque(maxlen=lookback)
        self.volumes: deque = deque(maxlen=lookback)  # Stores per-period volume DELTAS
        self._prev_vol_24h: float = 0.0  # Track previous tick's rolling 24h volume

    def on_tick(self, snapshot: MarketSnapshot,
                context: Optional[StrategyContext] = None) -> List[StrategyDecision]:
        mid = snapshot.mid_price
        if mid <= 0:
            return []

        # Use ask as proxy high, bid as proxy low for the current tick
        high = snapshot.ask if snapshot.ask > 0 else mid
        low = snapshot.bid if snapshot.bid > 0 else mid
        
        # Compute per-period volume as the DELTA of the rolling 24h aggregate.
        # volume_24h is cumulative, so the difference between ticks gives us
        # how much new volume was traded in this period.
        raw_vol_24h = getattr(snapshot, "volume_24h", 0) or 0
        if self._prev_vol_24h > 0 and raw_vol_24h >= self._prev_vol_24h:
            current_vol = raw_vol_24h - self._prev_vol_24h
        else:
            # First tick or 24h window rolled over (reset) -- use raw as fallback
            # Or if volume decreases (should not happen for cumulative volume)
            current_vol = 0.0 
        self._prev_vol_24h = raw_vol_24h

        # Initialize history if not enough data
        if len(self.highs) < self.lookback:
            self.highs.append(high)
            self.lows.append(low)
            self.volumes.append(current_vol)
            return []

        # Compute range from PREVIOUS 'lookback' periods before updating with current tick
        # This means the current high/low and volume will be added *after* calculations for this tick.
        # So period_high/low represent the range *up to the previous tick*.
        period_high = max(self.highs)
        period_low = min(self.lows)
        
        # Calculate average volume from historical deltas, excluding zero volumes
        valid_volumes = [v for v in self.volumes if v > 0]
        avg_vol = sum(valid_volumes) / len(valid_volumes) if valid_volumes else 1.0

        # Update history with current tick's data for the *next* iteration
        self.highs.append(high)
        self.lows.append(low)
        self.volumes.append(current_vol)

        # Check for volume surge: current period delta must be positive and exceed average by multiplier
        vol_surge = current_vol > avg_vol * self.volume_surge_mult if avg_vol > 0 and current_vol > 0 else False

        # Calculate breakout levels in basis points
        upside_bps = (mid - period_high) / period_high * 10_000 if period_high > 0 else 0
        downside_bps = (period_low - mid) / period_low * 10_000 if period_low > 0 else 0

        ctx = context or StrategyContext()
        orders: List[StrategyDecision] = []

        # Exit logic for existing position (Take Profit / Stop Loss)
        # Calculate TP and SL prices based on entry_price and defined bps
        take_profit_bps_abs = self.stop_loss_bps * self.take_profit_mult 
        
        if ctx.position_qty != 0 and self.entry_price > 0:
            if ctx.position_qty > 0: # Long position
                tp_price = self.entry_price * (1 + take_profit_bps_abs / 10_000)
                sl_price = self.entry_price * (1 - self.stop_loss_bps / 10_000)
                
                # Check for Take Profit or Stop Loss hit for long positions
                # Use bid for selling (exit long)
                if snapshot.bid >= tp_price:
                    orders.append(StrategyDecision(
                        action="place_order",
                        instrument=snapshot.instrument,
                        side="sell",
                        size=abs(ctx.position_qty),
                        limit_price=round(snapshot.bid, 2), # Exit at current bid
                        order_type="Ioc",
                        meta={"signal": "exit_long_tp"},
                    ))
                    self.entry_price = 0.0 # Reset entry price AFTER placing order
                elif snapshot.bid <= sl_price:
                    orders.append(StrategyDecision(
                        action="place_order",
                        instrument=snapshot.instrument,
                        side="sell",
                        size=abs(ctx.position_qty),
                        limit_price=round(snapshot.bid, 2), # Exit at current bid
                        order_type="Ioc",
                        meta={"signal": "exit_long_sl"},
                    ))
                    self.entry_price = 0.0 # Reset entry price AFTER placing order
            else: # Short position
                tp_price = self.entry_price * (1 - take_profit_bps_abs / 10_000)
                sl_price = self.entry_price * (1 + self.stop_loss_bps / 10_000)
                
                # Check for Take Profit or Stop Loss hit for short positions
                # Use ask for buying (exit short)
                if snapshot.ask <= tp_price:
                    orders.append(StrategyDecision(
                        action="place_order",
                        instrument=snapshot.instrument,
                        side="buy",
                        size=abs(ctx.position_qty),
                        limit_price=round(snapshot.ask, 2), # Exit at current ask
                        order_type="Ioc",
                        meta={"signal": "exit_short_tp"},
                    ))
                    self.entry_price = 0.0 # Reset entry price AFTER placing order
                elif snapshot.ask >= sl_price:
                    orders.append(StrategyDecision(
                        action="place_order",
                        instrument=snapshot.instrument,
                        side="buy",
                        size=abs(ctx.position_qty),
                        limit_price=round(snapshot.ask, 2), # Exit at current ask
                        order_type="Ioc",
                        meta={"signal": "exit_short_sl"},
                    ))
                    self.entry_price = 0.0 # Reset entry price AFTER placing order
            
            # If an exit order was placed, return immediately to prevent new entry in the same tick
            # This ensures only one action (entry or exit) per tick.
            if orders:
                return orders

        # Breakout entry (only if no existing position)
        if ctx.position_qty == 0:
            if upside_bps > self.breakout_threshold_bps and vol_surge:
                orders.append(StrategyDecision(
                    action="place_order",
                    instrument=snapshot.instrument,
                    side="buy",
                    size=self.size,
                    limit_price=round(snapshot.ask, 2), # Enter at current ask
                    meta={
                        "signal": "breakout_long",
                        "breakout_bps": round(upside_bps, 2),
                        "volume_surge": True,
                    },
                    order_type="Ioc",
                ))
                self.entry_price = snapshot.ask # Record entry price AFTER placing order
            elif downside_bps > self.breakout_threshold_bps and vol_surge:
                orders.append(StrategyDecision(
                    action="place_order",
                    instrument=snapshot.instrument,
                    side="sell",
                    size=self.size,
                    limit_price=round(snapshot.bid, 2), # Enter at current bid
                    meta={
                        "signal": "breakout_short",
                        "breakout_bps": round(downside_bps, 2),
                        "volume_surge": True,
                    },
                    order_type="Ioc",
                ))
                self.entry_price = snapshot.bid # Record entry price AFTER placing order

        return orders
