"""WOLF standalone runner — multi-slot orchestrator tick loop.

Composes scanner + movers + DSL + HOWL into a single autonomous strategy.
Each tick: fetch prices → update ROEs → check DSL → run movers → evaluate.
Periodic: HOWL performance review → auto-adjust config parameters.
Scheduled: daily PnL reset, comprehensive HOWL reports.
"""
from __future__ import annotations

import skills._bootstrap  # noqa: F401 — auto-setup sys.path

import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.dsl_config import DSLConfig, PRESETS as DSL_PRESETS
from modules.dsl_guard import DSLGuard
from modules.dsl_state import DSLState, DSLStateStore
from modules.execution_health import ExecutionHealthGate, ExecutionHealthTracker
from modules.howl_adapter import adapt, apply_adjustments
from modules.howl_engine import HowlEngine, TradeRecord
from modules.howl_reporter import HowlReporter
from modules.journal_engine import JournalEngine
from modules.journal_guard import JournalGuard
from modules.judge_guard import JudgeGuard
from modules.memory_engine import MemoryEngine
from modules.memory_guard import MemoryGuard
from modules.movers_guard import MoversGuard
from modules.portfolio_allocator import PortfolioAllocator, PortfolioGate
from modules.runtime_allocation import RuntimeAllocationGate, RuntimeAllocationLoader
from modules.scanner_guard import ScannerGuard
from modules.wolf_config import WolfConfig
from modules.wolf_engine import WolfAction, WolfEngine
from modules.wolf_state import WolfSlot, WolfState, WolfStateStore
from modules.telegram_reporter import TelegramReporter
from execution.parent_order import ParentOrder
from execution.portfolio_risk import PortfolioRiskManager, PortfolioRiskConfig
from execution.twap import TWAPExecutor
from parent.store import JSONLStore

log = logging.getLogger("wolf_runner")


class WolfRunner:
    """Autonomous WOLF strategy tick loop.

    Tick schedule (60s base):
      Every tick:      Fetch prices → update ROEs → check DSL → run movers → evaluate
      Every 5 ticks:   Watchdog health check
      Every 15 ticks:  Run scanner → queue high-score opportunities
    """

    def __init__(
        self,
        hl,
        config: Optional[WolfConfig] = None,
        tick_interval: float = 60.0,
        json_output: bool = False,
        data_dir: str = "data/wolf",
        builder: Optional[dict] = None,
        resume: bool = True,
    ):
        self.hl = hl
        self.config = config or WolfConfig()
        self.tick_interval = tick_interval
        self.json_output = json_output
        self.data_dir = data_dir
        self.builder = builder
        self._base_total_budget = self.config.total_budget
        self._base_margin_per_slot = self.config.margin_per_slot

        # Core engine (pure, zero I/O)
        self.engine = WolfEngine(self.config)

        # State + persistence
        self.state_store = WolfStateStore(path=f"{data_dir}/state.json")
        if resume:
            self.state = self.state_store.load() or WolfState.new(self.config.max_slots)
        else:
            self.state = WolfState.new(self.config.max_slots)

        # Sub-guards
        self.movers_guard = MoversGuard()
        self.scanner_guard = ScannerGuard()
        self.scanner_guard.history.path = f"{data_dir}/scanner-history.json"

        # DSL guards per slot (created on entry, removed on exit)
        self.dsl_guards: Dict[int, DSLGuard] = {}
        self._restore_dsl_guards()
        self._twap_executors: Dict[int, TWAPExecutor] = {}
        self._twap_orders: Dict[int, ParentOrder] = {}
        self._restore_pending_entries()

        # Trade logging for HOWL
        self.trade_log = JSONLStore(path=f"{data_dir}/trades.jsonl")

        # Self-improvement subsystems
        self.memory_engine = MemoryEngine()
        self.memory_guard = MemoryGuard(data_dir=f"{data_dir}/memory")
        self.journal_engine = JournalEngine()
        self.journal_guard = JournalGuard(data_dir=data_dir)
        self.judge_guard = JudgeGuard(data_dir=data_dir)

        # Obsidian integration (optional)
        self._obsidian_writer = None
        self._obsidian_reader = None
        self._obsidian_context = None
        if self.config.obsidian_vault_path:
            try:
                from modules.obsidian_reader import ObsidianReader
                from modules.obsidian_writer import ObsidianWriter
                self._obsidian_reader = ObsidianReader(self.config.obsidian_vault_path)
                self._obsidian_writer = ObsidianWriter(self.config.obsidian_vault_path)
                if self._obsidian_reader.available:
                    self._obsidian_context = self._obsidian_reader.read_trading_context()
                    log.info("Obsidian vault loaded: %d watchlist, %d theses",
                             len(self._obsidian_context.watchlist),
                             len(self._obsidian_context.market_theses))
            except Exception as e:
                log.warning("Obsidian integration failed: %s", e)

        # Portfolio risk manager
        self.portfolio_risk = PortfolioRiskManager(PortfolioRiskConfig(
            max_correlated_positions=self.config.portfolio_max_correlated,
            max_same_direction_total=self.config.portfolio_max_same_direction,
            margin_utilization_warn=self.config.portfolio_margin_warn,
            margin_utilization_block=self.config.portfolio_margin_block,
            enabled=self.config.portfolio_risk_enabled,
        ))

        # Smart money tracker (optional)
        self.smart_money_tracker = None
        if self.config.smart_money_enabled and self.config.smart_money_addresses:
            from modules.smart_money.tracker import SmartMoneyTracker
            from modules.smart_money.config import SmartMoneyConfig
            sm_cfg = SmartMoneyConfig(
                watch_addresses=self.config.smart_money_addresses,
                min_position_usd=self.config.smart_money_min_position_usd,
                conviction_threshold=self.config.smart_money_conviction_threshold,
                poll_interval_ticks=self.config.smart_money_poll_interval_ticks,
            )
            self.smart_money_tracker = SmartMoneyTracker(sm_cfg)
            log.info("Smart money tracker: watching %d addresses", len(sm_cfg.watch_addresses))

        # Scheduled task tracking (UTC hour -> last executed date string)
        self._last_scheduled: Dict[str, str] = {}
        self._allocation_loader = RuntimeAllocationLoader()
        self._allocation_gate = RuntimeAllocationGate()
        self._refresh_allocation_gate(force=True)
        self.execution_health = ExecutionHealthTracker(
            path=f"{data_dir}/execution-health.jsonl",
            max_events=self.config.execution_health_max_events,
        )
        self._execution_gate = ExecutionHealthGate()
        self._execution_edge_cooldowns: Dict[tuple[str, str], int] = {}
        self._refresh_execution_gate(force=True)
        self._portfolio_allocator = PortfolioAllocator(
            state_db_path=self.config.portfolio_state_db_path,
            ttl_ms=int(self.config.portfolio_ttl_ticks * self.tick_interval * 1000),
        )
        self._portfolio_gate = PortfolioGate()
        self._portfolio_runner_id = f"wolf-{os.getpid()}"
        self._refresh_portfolio_gate(force=True)

        self.telegram = TelegramReporter()
        self._running = False

    def _restore_dsl_guards(self) -> None:
        """Restore DSL guards for active slots from persisted state."""
        dsl_store = DSLStateStore(data_dir=f"{self.data_dir}/dsl")
        for slot in self.state.active_slots():
            pos_id = f"wolf-slot-{slot.slot_id}"
            guard = DSLGuard.from_store(pos_id, store=dsl_store)
            if guard and guard.is_active:
                self.dsl_guards[slot.slot_id] = guard
                log.info("Restored DSL guard for slot %d (%s)", slot.slot_id, slot.instrument)

    def _restore_pending_entries(self) -> None:
        """Restore in-flight TWAP entries from persisted state."""
        restored_queue: List[Dict[str, Any]] = []
        for item in self.state.entry_queue:
            slot_id = int(item.get("slot_id", -1))
            slot = next((s for s in self.state.slots if s.slot_id == slot_id), None)
            parent_data = item.get("parent_order", {})
            if slot is None or slot_id < 0 or not parent_data:
                continue

            try:
                parent = ParentOrder(**parent_data)
            except TypeError:
                continue

            if parent.status != "active":
                continue

            exe = TWAPExecutor()
            exe.submit(parent)
            self._twap_executors[slot_id] = exe
            self._twap_orders[slot_id] = parent
            slot.status = "entering"
            restored_queue.append(self._serialize_pending_entry(slot_id, item))
            log.info("Restored TWAP entry for slot %d (%s)", slot_id, slot.instrument)

        self.state.entry_queue = restored_queue

    def run(self, max_ticks: int = 0) -> None:
        """Main loop. Blocks until max_ticks reached or SIGINT."""
        self._running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        log.info("WOLF started: slots=%d leverage=%.0fx budget=$%.0f tick=%ds",
                 self.config.max_slots, self.config.leverage,
                 self.config.total_budget, self.tick_interval)

        # Log session start to memory
        try:
            event = self.memory_engine.create_session_event(
                event_type="session_start",
                tick_count=self.state.tick_count,
                total_pnl=self.state.total_pnl,
                active_slots=len(self.state.active_slots()),
                total_trades=self.state.total_trades,
            )
            self.memory_guard.log_event(event)
        except Exception:
            pass  # Memory logging should never break the runner

        while self._running:
            if max_ticks > 0 and self.state.tick_count >= max_ticks:
                log.info("Reached max ticks (%d), stopping", max_ticks)
                break

            try:
                self._tick()
            except Exception as e:
                log.error("Tick %d failed: %s", self.state.tick_count, e, exc_info=True)

            if self._running and self.tick_interval > 0 and (max_ticks == 0 or self.state.tick_count < max_ticks):
                time.sleep(self.tick_interval)

        self._print_summary()
        log.info("WOLF stopped after %d ticks", self.state.tick_count)

    def run_once(self) -> List[WolfAction]:
        """Single tick pass — no loop."""
        actions = self._tick()
        self._print_status()
        return actions

    def _tick(self) -> List[WolfAction]:
        """Execute a single WOLF tick cycle."""
        self.state.tick_count += 1
        tick = self.state.tick_count
        now_ms = int(time.time() * 1000)

        log.info("--- WOLF tick %d ---", tick)

        # 1. Fetch current prices for active slots
        slot_prices = self._fetch_slot_prices()

        # 1b. Process in-flight TWAP entries before evaluating new actions
        self._process_pending_entries()
        slot_prices.update(self._fetch_slot_prices())

        # 2. Run DSL checks for active slots
        slot_dsl_results = self._run_dsl_checks(slot_prices)

        # 3. Run movers (every tick)
        movers_signals = self._run_movers()

        # 3b. Run smart money tracker
        smart_money_signals = []
        if self.smart_money_tracker:
            try:
                smart_money_signals = self.smart_money_tracker.scan(self.hl)
            except Exception as e:
                log.warning("Smart money scan failed: %s", e)

        # 4. Run scanner (every N ticks)
        scanner_opps = []
        if tick % self.config.scanner_interval_ticks == 0:
            scanner_opps = self._run_scanner()

        # 5. Watchdog (every N ticks)
        if tick % self.config.watchdog_interval_ticks == 0:
            self._watchdog()

        # 5b. HOWL self-improvement (every N ticks)
        if tick % self.config.howl_interval_ticks == 0:
            self._run_howl()

        # 5c. Scheduled tasks (time-based)
        self._check_scheduled_tasks(now_ms)

        # 5d. Runtime capital gate from research/live allocation plan
        self._refresh_allocation_gate()
        self._refresh_execution_gate()
        self._refresh_portfolio_gate()
        if (self._allocation_gate.configured and not self._allocation_gate.allow_entries
                or self._execution_gate.configured and not self._execution_gate.allow_entries
                or self._portfolio_gate.configured and not self._portfolio_gate.allow_entries):
            movers_signals = []
            scanner_opps = []
            smart_money_signals = []
        else:
            movers_signals, scanner_opps, smart_money_signals = self._apply_source_routing(
                movers_signals=movers_signals,
                scanner_opps=scanner_opps,
                smart_money_signals=smart_money_signals,
            )

        # 6. Engine evaluation
        actions = self.engine.evaluate(
            state=self.state,
            movers_signals=movers_signals,
            scanner_opps=scanner_opps,
            slot_prices=slot_prices,
            slot_dsl_results=slot_dsl_results,
            now_ms=now_ms,
            smart_money_signals=smart_money_signals,
        )

        # 7. Execute actions
        for action in actions:
            if action.action == "enter" and not self._is_entry_allowed(
                action.source,
                action.instrument,
                signal_score=action.signal_score,
            ):
                log.info(
                    "Allocation routing blocked %s entry for %s score=%.2f",
                    action.source,
                    action.instrument,
                    action.signal_score,
                )
                continue
            self._execute_action(action)

        # 8. Persist state
        self.state_store.save(self.state)

        self._print_status()
        return actions

    def _fetch_slot_prices(self) -> Dict[int, float]:
        """Fetch current prices for all active slot instruments."""
        prices: Dict[int, float] = {}
        active = self.state.active_slots()
        if not active:
            return prices

        try:
            all_mids = self.hl.get_all_mids()
        except Exception as e:
            log.warning("Failed to fetch mids: %s", e)
            return prices

        for slot in active:
            coin = slot.instrument.replace("-PERP", "")
            mid = all_mids.get(coin)
            if mid:
                prices[slot.slot_id] = float(mid)

        return prices

    def _run_dsl_checks(self, slot_prices: Dict[int, float]) -> Dict[int, Dict[str, Any]]:
        """Run DSL guard checks for each active slot with a DSL guard."""
        results: Dict[int, Dict[str, Any]] = {}

        for slot in self.state.active_slots():
            guard = self.dsl_guards.get(slot.slot_id)
            if guard is None or not guard.is_active:
                continue

            price = slot_prices.get(slot.slot_id, 0)
            if price <= 0:
                continue

            try:
                dsl_result = guard.check(price)
                if dsl_result.action.value == "CLOSE":
                    results[slot.slot_id] = {
                        "action": "close",
                        "reason": dsl_result.reason,
                    }
                else:
                    results[slot.slot_id] = {
                        "action": dsl_result.action.value.lower(),
                        "roe_pct": dsl_result.roe_pct,
                    }
            except Exception as e:
                log.warning("DSL check failed for slot %d: %s", slot.slot_id, e)

        return results

    def _run_movers(self) -> List[Dict[str, Any]]:
        """Run movers scan and return signal dicts for the engine."""
        try:
            all_markets = self.hl.get_all_markets()

            # Fetch 4h candles for qualifying assets so volume surge detection works
            asset_candles: Dict[str, Dict[str, List[Dict]]] = {}
            if len(all_markets) >= 2:
                universe = all_markets[0].get("universe", [])
                ctxs = all_markets[1]
                for i, ctx in enumerate(ctxs):
                    if i >= len(universe):
                        break
                    try:
                        name = universe[i].get("name", "")
                    except (IndexError, AttributeError):
                        continue
                    vol = float(ctx.get("dayNtlVlm", 0))
                    if vol >= self.movers_guard.config.volume_min_24h and name:
                        try:
                            c4h = self.hl.get_candles(name, "4h", 7 * 24 * 3600 * 1000)
                            c1h = self.hl.get_candles(name, "1h", 48 * 3600 * 1000)
                            asset_candles[name] = {"4h": c4h, "1h": c1h}
                            time.sleep(0.05)  # Rate limit: ~20 req/s to avoid HL 429s
                        except Exception:
                            pass

            result = self.movers_guard.scan(all_markets=all_markets, asset_candles=asset_candles)
            return [
                {
                    "asset": sig.asset,
                    "signal_type": sig.signal_type,
                    "direction": sig.direction,
                    "confidence": sig.confidence,
                }
                for sig in result.signals
            ]
        except Exception as e:
            log.warning("Movers scan failed: %s", e)
            return []

    def _run_scanner(self) -> List[Dict[str, Any]]:
        """Run scanner and return opportunity dicts for the engine."""
        try:
            all_markets = self.hl.get_all_markets()

            # Fetch BTC candles
            btc_4h = self.hl.get_candles("BTC", "4h", 7 * 24 * 3600 * 1000)
            btc_1h = self.hl.get_candles("BTC", "1h", 48 * 3600 * 1000)
            asset_candles = self._fetch_scanner_asset_candles(all_markets)

            result = self.scanner_guard.scan(
                all_markets=all_markets,
                btc_candles_4h=btc_4h,
                btc_candles_1h=btc_1h,
                asset_candles=asset_candles,
            )

            return [
                {
                    "asset": opp.asset,
                    "direction": opp.direction,
                    "final_score": opp.final_score,
                }
                for opp in result.opportunities
            ]
        except Exception as e:
            log.warning("Scanner failed: %s", e)
            return []

    def _fetch_scanner_asset_candles(
        self,
        all_markets: List[Any],
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Fetch the candle sets required by the full scanner pipeline."""
        from modules.scanner_engine import OpportunityScannerEngine

        cfg = self.scanner_guard.config
        temp_engine = OpportunityScannerEngine(cfg)
        assets = temp_engine._bulk_screen(all_markets)
        top_assets = temp_engine._select_top(assets)
        asset_names = [asset.name for asset in top_assets]

        asset_candles: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {}
            for name in asset_names:
                for interval, lookback in [
                    ("4h", cfg.lookback_4h_ms),
                    ("1h", cfg.lookback_1h_ms),
                    ("15m", cfg.lookback_15m_ms),
                ]:
                    futures[pool.submit(
                        self.hl.get_candles, name, interval, lookback,
                    )] = (name, interval)

            for future in as_completed(futures):
                asset_name, timeframe = futures[future]
                try:
                    candles = future.result()
                except Exception as e:
                    log.warning("Scanner candles failed for %s %s: %s",
                                asset_name, timeframe, e)
                    continue

                asset_candles.setdefault(asset_name, {})[timeframe] = candles

        return asset_candles

    def _watchdog(self) -> None:
        """Health check — verify positions match exchange state."""
        active = self.state.active_slots()
        if not active:
            return

        try:
            account = self.hl.get_account_state()
            positions = account.get("assetPositions", [])
            exchange_instruments = set()
            for pos in positions:
                p = pos.get("position", {})
                if float(p.get("szi", "0")) != 0:
                    coin = p.get("coin", "")
                    exchange_instruments.add(f"{coin}-PERP")

            for slot in active:
                if slot.instrument not in exchange_instruments:
                    log.warning("Watchdog: slot %d (%s) has no exchange position — marking closed",
                                slot.slot_id, slot.instrument)
                    self._close_slot(slot, reason="watchdog_no_position", pnl=0)
        except Exception as e:
            log.warning("Watchdog check failed: %s", e)

    def _execute_action(self, action: WolfAction) -> None:
        """Execute a single WolfAction (enter or exit)."""
        if action.action == "enter":
            self._execute_enter(action)
        elif action.action == "exit":
            self._execute_exit(action)

    def _execute_enter(self, action: WolfAction) -> None:
        """Execute an entry order."""
        slot = next((s for s in self.state.slots if s.slot_id == action.slot_id), None)
        if slot is None:
            return

        if self._allocation_gate.configured and not self._allocation_gate.allow_entries:
            log.info("Allocation gate blocked entry for %s (%s)",
                     action.instrument, ", ".join(self._allocation_gate.reasons))
            slot.status = "empty"
            slot.instrument = ""
            return
        if self._execution_gate.configured and not self._execution_gate.allow_entries:
            log.info("Execution gate blocked entry for %s (%s)",
                     action.instrument, ", ".join(self._execution_gate.reasons))
            slot.status = "empty"
            slot.instrument = ""
            return
        if self._portfolio_gate.configured and not self._portfolio_gate.allow_entries:
            log.info("Portfolio gate blocked entry for %s (%s)",
                     action.instrument, ", ".join(self._portfolio_gate.reasons))
            slot.status = "empty"
            slot.instrument = ""
            return
        if self._execution_gate.configured and self._is_execution_edge_blocked(action.source, action.instrument):
            log.info("Execution gate blocked %s entry for %s", action.source, action.instrument)
            slot.status = "empty"
            slot.instrument = ""
            return

        coin = action.instrument.replace("-PERP", "")
        try:
            # Get current price for size calculation
            mids = self.hl.get_all_mids()
            mid = float(mids.get(coin, "0"))
            if mid <= 0:
                log.warning("Cannot enter %s: no mid price", action.instrument)
                slot.status = "empty"
                slot.instrument = ""
                return

            entry_margin = self._compute_entry_margin(action.instrument)
            if entry_margin <= 0:
                log.info("Allocation budget exhausted; skipping entry for %s", action.instrument)
                slot.status = "empty"
                slot.instrument = ""
                return

            # Portfolio risk check
            current_positions = {}
            for s in self.state.active_slots():
                if s.is_active():
                    current_positions[s.instrument] = {
                        "direction": s.direction,
                        "notional": s.margin_allocated * self.config.leverage,
                    }

            ok, reason = self.portfolio_risk.check_entry(
                action.instrument, action.direction, current_positions)
            if not ok:
                log.warning("Portfolio risk blocked entry for %s: %s",
                            action.instrument, reason)
                slot.status = "empty"
                slot.instrument = ""
                return

            size = (entry_margin * self.config.leverage) / mid
            side = "buy" if action.direction == "long" else "sell"

            slot.direction = action.direction
            slot.entry_source = action.source
            slot.entry_signal_score = action.signal_score
            slot.margin_allocated = entry_margin
            slot.entry_ts = int(time.time() * 1000)
            slot.last_progress_ts = slot.entry_ts
            slot.last_signal_seen_ts = slot.entry_ts

            if action.execution_algo == "twap":
                parent = ParentOrder(
                    instrument=action.instrument,
                    side=side,
                    target_qty=round(size, 4),
                    algo="twap",
                    duration_ticks=self.config.twap_duration_ticks,
                    urgency=self.config.twap_urgency,
                    created_at_ms=slot.entry_ts,
                )
                exe = TWAPExecutor()
                exe.submit(parent)
                self._twap_executors[slot.slot_id] = exe
                self._twap_orders[slot.slot_id] = parent
                slot.status = "entering"
                self._upsert_pending_entry(slot.slot_id, {
                    "slot_id": slot.slot_id,
                    "instrument": action.instrument,
                    "direction": action.direction,
                    "source": action.source,
                    "signal_score": action.signal_score,
                    "decision_reason": action.reason,
                    "execution_algo": action.execution_algo,
                    "parent_order": self._serialize_parent_order(parent),
                })
                self.state_store.save(self.state)
                log.info("TWAP queued for slot %d: %s %s target=%.4f over %d ticks",
                         slot.slot_id, action.direction, action.instrument,
                         parent.target_qty, parent.duration_ticks)
                return

            try:
                fill = self.hl.place_order(
                    instrument=action.instrument,
                    side=side,
                    size=round(size, 4),
                    price=mid,
                    tif="Ioc",
                    builder=self.builder,
                )
            except Exception as e:
                self._record_order_attempt(
                    instrument=action.instrument,
                    source=action.source,
                    side=side,
                    requested_qty=round(size, 4),
                    success=False,
                    error=str(e),
                )
                raise
            self._record_order_attempt(
                instrument=action.instrument,
                source=action.source,
                side=side,
                requested_qty=round(size, 4),
                success=bool(fill),
            )

            if fill:
                self._record_order_fill(
                    instrument=action.instrument,
                    source=action.source,
                    side=side,
                    requested_qty=round(size, 4),
                    filled_qty=float(fill.quantity),
                    fill_price=float(fill.price),
                    mid_price=self._resolve_mid_price(mid),
                )
                slot.status = "active"
                slot.entry_price = float(fill.price)
                slot.entry_size = float(fill.quantity)
                slot.high_water_roe = 0.0
                slot.current_roe = 0.0

                # Create DSL guard for this slot
                self._create_dsl_guard(slot)

                self.state.total_trades += 1
                self._log_trade(
                    tick=self.state.tick_count, instrument=action.instrument,
                    side=side, price=float(fill.price),
                    quantity=float(fill.quantity), fee=float(getattr(fill, "fee", 0)),
                    meta=f"entry:{action.source}",
                    entry_source=action.source,
                    entry_signal_score=action.signal_score,
                    execution_algo=action.execution_algo,
                    decision_reason=action.reason,
                )
                log.info("ENTERED slot %d: %s %s @ %.4f size=%.4f (%s)",
                         slot.slot_id, action.direction, action.instrument,
                         float(fill.price), float(fill.quantity), action.reason)
                
                self.telegram.send_message(
                    f"🟢 <b>WOLF ENTRY</b>\n"
                    f"<b>{action.instrument}</b> {action.direction.upper()}\n"
                    f"Price: {float(fill.price):.4f}\n"
                    f"Size: {float(fill.quantity):.4f}\n"
                    f"Reason: <i>{action.reason}</i>"
                )
                
                self.telegram.send_message(
                    f"🟢 <b>WOLF ENTRY</b>\n"
                    f"<b>{action.instrument}</b> {action.direction.upper()}\n"
                    f"Price: {float(fill.price):.4f}\n"
                    f"Size: {float(fill.quantity):.4f}\n"
                    f"Reason: <i>{action.reason}</i>"
                )
            else:
                log.warning("Entry fill failed for %s", action.instrument)
                slot.status = "empty"
                slot.instrument = ""

        except Exception as e:
            log.error("Entry failed for %s: %s", action.instrument, e)
            slot.status = "empty"
            slot.instrument = ""

    def _execute_exit(self, action: WolfAction) -> None:
        """Execute an exit order."""
        slot = next((s for s in self.state.slots if s.slot_id == action.slot_id), None)
        if slot is None or not slot.is_active():
            return

        coin = action.instrument.replace("-PERP", "")
        try:
            mids = self.hl.get_all_mids()
            mid = float(mids.get(coin, "0"))
            side = "sell" if slot.direction == "long" else "buy"

            try:
                fill = self.hl.place_order(
                    instrument=action.instrument,
                    side=side,
                    size=slot.entry_size,
                    price=mid if mid > 0 else slot.current_price,
                    tif="Ioc",
                    builder=self.builder,
                )
            except Exception as e:
                self._record_order_attempt(
                    instrument=action.instrument,
                    source="exit",
                    side=side,
                    requested_qty=slot.entry_size,
                    success=False,
                    error=str(e),
                )
                raise

            self._record_order_attempt(
                instrument=action.instrument,
                source="exit",
                side=side,
                requested_qty=slot.entry_size,
                success=bool(fill),
            )
            if fill:
                self._record_order_fill(
                    instrument=action.instrument,
                    source="exit",
                    side=side,
                    requested_qty=slot.entry_size,
                    filled_qty=float(fill.quantity),
                    fill_price=float(fill.price),
                    mid_price=self._resolve_mid_price(mid),
                )

            exit_price = fill.price if fill else mid
            pnl = 0.0
            if slot.entry_price > 0 and exit_price > 0:
                if slot.direction == "long":
                    pnl = (exit_price - slot.entry_price) / slot.entry_price * slot.margin_allocated * self.config.leverage
                else:
                    pnl = (slot.entry_price - exit_price) / slot.entry_price * slot.margin_allocated * self.config.leverage

            self._close_slot(slot, reason=action.reason, pnl=pnl)
            self._log_trade(
                tick=self.state.tick_count, instrument=action.instrument,
                side=side, price=float(exit_price),
                quantity=slot.entry_size, fee=float(getattr(fill, "fee", 0)) if fill else 0,
                meta=action.reason,
                entry_source=slot.entry_source,
                decision_reason=action.reason,
            )
            log.info("EXITED slot %d: %s %s @ %.4f PnL=$%.2f (%s)",
                     slot.slot_id, slot.direction, action.instrument,
                     exit_price, pnl, action.reason)
            
            icon = "🔥" if pnl > 0 else "🩸"
            self.telegram.send_message(
                f"{icon} <b>WOLF EXIT</b>\n"
                f"<b>{action.instrument}</b> {slot.direction.upper()}\n"
                f"PnL: <b>${pnl:.2f}</b>\n"
                f"Reason: <i>{action.reason}</i>"
            )
            
            icon = "🔥" if pnl > 0 else "🩸"
            self.telegram.send_message(
                f"{icon} <b>WOLF EXIT</b>\n"
                f"<b>{action.instrument}</b> {slot.direction.upper()}\n"
                f"Price: {exit_price:.4f}\n"
                f"PnL: <b>${pnl:.2f}</b> ({slot.current_roe:+.1f}%)\n"
                f"Reason: <i>{action.reason}</i>"
            )

        except Exception as e:
            log.error("Exit failed for slot %d (%s): %s", slot.slot_id, action.instrument, e)

    def _close_slot(self, slot: WolfSlot, reason: str, pnl: float) -> None:
        """Reset a slot to empty and update PnL tracking."""
        self._cancel_pending_entry(slot.slot_id, reason=reason, persist=False)

        # Close DSL guard
        guard = self.dsl_guards.pop(slot.slot_id, None)
        if guard:
            guard.mark_closed(slot.current_price, reason)

        # Update PnL
        self.state.daily_pnl += pnl
        self.state.total_pnl += pnl

        if self.state.daily_pnl <= -self.config.daily_loss_limit:
            self.state.daily_loss_triggered = True
            log.warning("DAILY LOSS LIMIT triggered: $%.2f", self.state.daily_pnl)

        # Log to trade journal
        close_ts = int(time.time() * 1000)
        try:
            journal_entry = self.journal_engine.create_entry(
                instrument=slot.instrument,
                direction=slot.direction,
                entry_price=slot.entry_price,
                exit_price=slot.current_price,
                pnl=pnl,
                roe_pct=slot.current_roe,
                entry_source=slot.entry_source,
                entry_signal_score=slot.entry_signal_score,
                close_reason=reason,
                entry_ts=slot.entry_ts,
                close_ts=close_ts,
            )
            self.journal_guard.log_entry(journal_entry)

            # Notable trade -> memory + obsidian
            notable_threshold = max(slot.margin_allocated, self._effective_margin_per_slot()) * 0.1
            if abs(pnl) > notable_threshold:
                mem_event = self.memory_engine.create_notable_trade_event(
                    instrument=slot.instrument,
                    direction=slot.direction,
                    pnl=pnl,
                    roe_pct=slot.current_roe,
                    entry_source=slot.entry_source,
                    close_reason=reason,
                )
                self.memory_guard.log_event(mem_event)

                if self._obsidian_writer:
                    self._obsidian_writer.write_notable_trade(journal_entry.to_dict())
        except Exception as e:
            log.debug("Journal/memory logging failed: %s", e)

        self.state.closed_slots_buffer.append({
            "slot_id": slot.slot_id,
            "instrument": slot.instrument,
            "direction": slot.direction,
            "entry_source": slot.entry_source,
            "entry_signal_score": slot.entry_signal_score,
            "entry_price": slot.entry_price,
            "entry_size": slot.entry_size,
            "entry_ts": slot.entry_ts,
            "close_ts": close_ts,
            "close_reason": reason,
            "close_pnl": pnl,
            "current_price": slot.current_price,
            "current_roe": slot.current_roe,
            "high_water_roe": slot.high_water_roe,
        })

        # Reset slot
        slot.close_ts = close_ts
        slot.close_reason = reason
        slot.close_pnl = pnl
        slot.status = "empty"
        slot.instrument = ""
        slot.direction = ""
        slot.entry_source = ""
        slot.entry_signal_score = 0.0
        slot.entry_price = 0.0
        slot.entry_size = 0.0
        slot.margin_allocated = 0.0
        slot.current_price = 0.0
        slot.current_roe = 0.0
        slot.high_water_roe = 0.0

    def _create_dsl_guard(self, slot: WolfSlot) -> None:
        """Create a DSL guard for a newly entered slot."""
        preset_name = self.config.dsl_preset
        dsl_config = DSL_PRESETS.get(preset_name, DSL_PRESETS.get("tight", DSLConfig()))
        dsl_config = DSLConfig.from_dict(dsl_config.to_dict())  # copy
        dsl_config.direction = slot.direction
        dsl_config.leverage = self.config.dsl_leverage_override or self.config.leverage

        dsl_state = DSLState.new(
            instrument=slot.instrument,
            entry_price=slot.entry_price,
            position_size=slot.entry_size,
            direction=slot.direction,
            position_id=f"wolf-slot-{slot.slot_id}",
        )

        dsl_store = DSLStateStore(data_dir=f"{self.data_dir}/dsl")
        guard = DSLGuard(config=dsl_config, state=dsl_state, store=dsl_store)
        self.dsl_guards[slot.slot_id] = guard

    def _log_trade(self, tick: int, instrument: str, side: str,
                   price: float, quantity: float, fee: float = 0,
                   meta: str = "", **extra_fields: Any) -> None:
        """Append a trade record to the JSONL log."""
        record = {
            "tick": tick,
            "oid": f"wolf-{tick}-{instrument}",
            "instrument": instrument,
            "side": side,
            "price": str(price),
            "quantity": str(quantity),
            "timestamp_ms": int(time.time() * 1000),
            "fee": str(fee),
            "strategy": "wolf",
            "meta": meta,
        }
        record.update(extra_fields)
        self.trade_log.append(record)

    def _process_pending_entries(self) -> None:
        """Advance any in-flight TWAP entries by one slice."""
        if not self.state.entry_queue:
            return

        for item in list(self.state.entry_queue):
            slot_id = int(item.get("slot_id", -1))
            slot = next((s for s in self.state.slots if s.slot_id == slot_id), None)
            exe = self._twap_executors.get(slot_id)
            parent = self._twap_orders.get(slot_id)
            if slot is None or exe is None or parent is None:
                continue

            try:
                snapshot = self.hl.get_snapshot(slot.instrument or item.get("instrument", ""))
            except Exception as e:
                log.warning("TWAP snapshot failed for slot %d: %s", slot_id, e)
                continue

            mid_price = self._resolve_mid_price(0.0, snapshot=snapshot)

            for child in exe.on_tick(snapshot):
                try:
                    fill = self.hl.place_order(
                        instrument=child.instrument,
                        side=child.side,
                        size=child.size,
                        price=child.price,
                        tif="Ioc",
                        builder=self.builder,
                    )
                except Exception as e:
                    self._record_order_attempt(
                        instrument=child.instrument,
                        source=str(item.get("source", "")),
                        side=child.side,
                        requested_qty=child.size,
                        success=False,
                        error=str(e),
                    )
                    continue

                self._record_order_attempt(
                    instrument=child.instrument,
                    source=str(item.get("source", "")),
                    side=child.side,
                    requested_qty=child.size,
                    success=bool(fill),
                )
                if fill is None:
                    continue

                self._record_order_fill(
                    instrument=child.instrument,
                    source=str(item.get("source", "")),
                    side=child.side,
                    requested_qty=child.size,
                    filled_qty=float(fill.quantity),
                    fill_price=float(fill.price),
                    mid_price=mid_price,
                )

                exe.record_fill(parent.order_id, float(fill.quantity), float(fill.price), fill.timestamp_ms)
                self._apply_entry_fill(
                    slot=slot,
                    fill=fill,
                    source=str(item.get("source", "")),
                    signal_score=float(item.get("signal_score", 0.0)),
                    decision_reason=str(item.get("decision_reason", "")),
                    execution_algo=str(item.get("execution_algo", "twap")),
                )

            if parent.status == "complete":
                self._finalize_twap_entry(slot, parent)
            else:
                self._upsert_pending_entry(slot_id, {
                    **item,
                    "parent_order": self._serialize_parent_order(parent),
                })

        self.state_store.save(self.state)

    def _apply_entry_fill(
        self,
        slot: WolfSlot,
        fill,
        source: str,
        signal_score: float,
        decision_reason: str,
        execution_algo: str,
    ) -> None:
        """Accumulate a TWAP child fill into the slot entry state."""
        fill_qty = float(fill.quantity)
        fill_price = float(fill.price)
        prev_qty = slot.entry_size
        new_qty = prev_qty + fill_qty

        if new_qty > 0:
            if prev_qty > 0:
                slot.entry_price = ((slot.entry_price * prev_qty) + (fill_price * fill_qty)) / new_qty
            else:
                slot.entry_price = fill_price
        slot.entry_size = new_qty
        slot.current_price = fill_price

        self._log_trade(
            tick=self.state.tick_count,
            instrument=slot.instrument,
            side=fill.side,
            price=fill_price,
            quantity=fill_qty,
            fee=float(getattr(fill, "fee", 0)),
            meta=f"entry:{source}",
            entry_source=source,
            entry_signal_score=signal_score,
            execution_algo=execution_algo,
            decision_reason=decision_reason,
            parent_order_id=self._twap_orders.get(slot.slot_id).order_id if slot.slot_id in self._twap_orders else "",
        )

    def _finalize_twap_entry(self, slot: WolfSlot, parent: ParentOrder) -> None:
        """Promote a fully-filled TWAP entry into an active slot."""
        slot.status = "active"
        slot.high_water_roe = 0.0
        slot.current_roe = 0.0
        if slot.slot_id not in self.dsl_guards and slot.entry_size > 0:
            self._create_dsl_guard(slot)
        self.state.total_trades += 1
        self._cancel_pending_entry(slot.slot_id, reason="twap_complete", persist=False)
        log.info("TWAP completed for slot %d: %s size=%.4f avg=%.4f",
                 slot.slot_id, slot.instrument, slot.entry_size, slot.entry_price)

    def _cancel_pending_entry(self, slot_id: int, reason: str, persist: bool = True) -> None:
        """Remove any in-flight TWAP state for a slot."""
        self._twap_executors.pop(slot_id, None)
        parent = self._twap_orders.pop(slot_id, None)
        if parent is not None and parent.status == "active":
            parent.status = "cancelled"
        self.state.entry_queue = [
            item for item in self.state.entry_queue
            if int(item.get("slot_id", -1)) != slot_id
        ]
        if persist:
            self.state_store.save(self.state)

    def _upsert_pending_entry(self, slot_id: int, data: Dict[str, Any]) -> None:
        """Insert or replace a serialized pending entry."""
        serialized = self._serialize_pending_entry(slot_id, data)
        updated = False
        for idx, item in enumerate(self.state.entry_queue):
            if int(item.get("slot_id", -1)) == slot_id:
                self.state.entry_queue[idx] = serialized
                updated = True
                break
        if not updated:
            self.state.entry_queue.append(serialized)

    @staticmethod
    def _serialize_parent_order(parent: ParentOrder) -> Dict[str, Any]:
        return {
            "instrument": parent.instrument,
            "side": parent.side,
            "target_qty": parent.target_qty,
            "algo": parent.algo,
            "duration_ticks": parent.duration_ticks,
            "urgency": parent.urgency,
            "filled_qty": parent.filled_qty,
            "child_fills": list(parent.child_fills),
            "status": parent.status,
            "ticks_elapsed": parent.ticks_elapsed,
            "created_at_ms": parent.created_at_ms,
            "order_id": parent.order_id,
        }

    def _serialize_pending_entry(self, slot_id: int, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "slot_id": slot_id,
            "instrument": item.get("instrument", ""),
            "direction": item.get("direction", ""),
            "source": item.get("source", ""),
            "signal_score": item.get("signal_score", 0.0),
            "decision_reason": item.get("decision_reason", ""),
            "execution_algo": item.get("execution_algo", "twap"),
            "parent_order": dict(item.get("parent_order", {})),
        }

    def _run_howl(self) -> None:
        """Run HOWL performance review and optionally auto-adjust config."""
        try:
            closed_slots = list(self.state.closed_slots_buffer)
            raw_trades = self.trade_log.read_all()
            if not raw_trades:
                log.info("HOWL: no trades logged yet, skipping")
                return

            trades = [TradeRecord.from_dict(t) for t in raw_trades]
            metrics = HowlEngine().compute(trades)

            # Log distilled summary
            summary = HowlReporter().distill(metrics)
            log.info(summary)
            self.telegram.send_message(f"🐺 <b>HOWL REPORT</b>\n<pre>{summary}</pre>")

            # Save report
            howl_dir = Path(self.data_dir) / "howl"
            howl_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
            report = HowlReporter().generate(metrics, date=ts)
            (howl_dir / f"{ts}.md").write_text(report)

            # Log HOWL review to memory
            try:
                howl_event = self.memory_engine.create_howl_event(
                    win_rate=metrics.win_rate,
                    net_pnl=metrics.net_pnl,
                    fdr=metrics.fdr,
                    round_trips=metrics.total_round_trips,
                    distilled=summary,
                )
                self.memory_guard.log_event(howl_event)
            except Exception:
                pass

            # Write HOWL report to Obsidian
            if self._obsidian_writer:
                try:
                    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    self._obsidian_writer.write_howl_report(
                        briefing_md=report, date=date,
                        win_rate=metrics.win_rate, net_pnl=metrics.net_pnl,
                        fdr=metrics.fdr, round_trips=metrics.total_round_trips,
                    )
                except Exception:
                    pass

            # Auto-adjust if enabled and enough data
            if (self.config.howl_auto_adjust
                    and metrics.total_round_trips >= self.config.howl_min_round_trips):
                adjustments, adj_log = adapt(metrics, self.config)
                if adjustments:
                    apply_adjustments(adjustments, self.config)
                    log.info(adj_log)
                    # Re-sync engine with updated config
                    self.engine = WolfEngine(self.config)

                    # Log param changes to memory
                    try:
                        pc_event = self.memory_engine.create_param_change_event(
                            adjustments, metrics_summary=summary,
                        )
                        self.memory_guard.log_event(pc_event)
                    except Exception:
                        pass
                else:
                    log.info("HOWL: no adjustments needed")

            # Run Judge evaluation
            try:
                judge_report = self.judge_guard.run_evaluation(
                    self.trade_log, closed_slots=closed_slots,
                )
                if judge_report.round_trips_evaluated > 0 or judge_report.findings:
                    self.judge_guard.save_report(judge_report)
                    self.judge_guard.apply_to_memory(judge_report, self.memory_guard)
                    if judge_report.config_recommendations:
                        recs = "; ".join(r.get("summary", "") for r in judge_report.config_recommendations)
                        log.info("Judge recommendations: %s", recs)

                    # Write Judge report to Obsidian
                    if self._obsidian_writer:
                        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        self._obsidian_writer.write_judge_report(
                            judge_report.to_dict(), date=date,
                        )
                if closed_slots:
                    self.state.closed_slots_buffer = []
                    self.state_store.save(self.state)
            except Exception as e:
                log.debug("Judge evaluation failed: %s", e)

        except Exception as e:
            log.warning("HOWL review failed: %s", e)

    def _check_scheduled_tasks(self, now_ms: int) -> None:
        """Run time-based scheduled tasks (daily reset, HOWL reports)."""
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        today = now.strftime("%Y-%m-%d")
        current_hour = now.hour

        # Daily PnL reset
        if (current_hour == self.config.daily_reset_hour
                and self._last_scheduled.get("daily_reset") != today):
            self._last_scheduled["daily_reset"] = today
            old_pnl = self.state.daily_pnl
            self.state.daily_pnl = 0.0
            self.state.daily_loss_triggered = False
            log.info("Daily PnL reset (was $%.2f)", old_pnl)

        # Scheduled HOWL comprehensive report
        if (current_hour == self.config.howl_report_hour
                and self._last_scheduled.get("howl_report") != today):
            self._last_scheduled["howl_report"] = today
            log.info("Scheduled HOWL report (UTC %02d:00)", current_hour)
            self._run_howl()

        # Nightly review (today vs 7-day average)
        if (self.config.nightly_review_enabled
                and current_hour == self.config.nightly_review_hour
                and self._last_scheduled.get("nightly_review") != today):
            self._last_scheduled["nightly_review"] = today
            log.info("Running nightly review (UTC %02d:00)", current_hour)
            self._run_nightly_review(today)

        # Obsidian context refresh
        if (self._obsidian_reader
                and self.state.tick_count % self.config.obsidian_scan_interval_ticks == 0
                and self.state.tick_count > 0):
            try:
                self._obsidian_context = self._obsidian_reader.read_trading_context()
            except Exception:
                pass

    def _refresh_allocation_gate(self, force: bool = False) -> None:
        """Refresh runtime capital controls from a saved live allocation plan."""
        if not self.config.allocation_enforce or not self.config.allocation_plan_path:
            self._allocation_gate = RuntimeAllocationGate()
            return

        refresh_ticks = max(int(self.config.allocation_refresh_ticks or 0), 1)
        if not force and self.state.tick_count > 0 and self.state.tick_count % refresh_ticks != 0:
            return

        try:
            gate = self._allocation_loader.load(
                plan_path=self.config.allocation_plan_path,
                total_budget_usd=self._base_total_budget,
                max_slots=self.config.max_slots,
            )
        except Exception as e:
            gate = RuntimeAllocationGate(
                configured=True,
                allow_entries=False,
                plan_path=self.config.allocation_plan_path,
                reasons=[f"allocation_plan_error:{e}"],
            )

        previous = self._allocation_gate.allow_entries if self._allocation_gate.configured else None
        self._allocation_gate = gate
        if previous != gate.allow_entries:
            status = "enabled" if gate.allow_entries else "disabled"
            log.info(
                "Allocation gate %s entries: regime=%s budget=$%.2f strategies=%d sources=%s",
                status,
                gate.current_regime,
                gate.allocated_capital_usd,
                gate.enabled_strategies,
                ",".join(gate.allowed_entry_sources) or "-",
            )

    def _refresh_execution_gate(self, force: bool = False) -> None:
        """Refresh execution health kill-switch state."""
        if not self.config.execution_health_enabled:
            self._execution_gate = ExecutionHealthGate()
            return

        refresh_ticks = max(int(self.config.execution_health_refresh_ticks or 0), 1)
        if not force and self.state.tick_count > 0 and self.state.tick_count % refresh_ticks != 0:
            return

        metrics = self.execution_health.compute_metrics()
        reasons: List[str] = []
        allow_entries = True
        min_attempts = int(self.config.execution_health_min_attempts or 0)

        if metrics.attempts < min_attempts:
            reasons.append("execution_health_insufficient_samples")
        else:
            if metrics.fill_ratio < self.config.execution_health_min_fill_ratio:
                reasons.append("execution_health_low_fill_ratio")
            if metrics.api_error_rate > self.config.execution_health_max_api_error_rate:
                reasons.append("execution_health_api_error_rate")
            if metrics.avg_slippage_bps > self.config.execution_health_max_avg_slippage_bps:
                reasons.append("execution_health_avg_slippage")
            if metrics.p95_slippage_bps > self.config.execution_health_max_p95_slippage_bps:
                reasons.append("execution_health_p95_slippage")

        if metrics.attempts >= min_attempts and reasons:
            allow_entries = False

        blocked_edges = self._evaluate_execution_edges(min_attempts)
        if allow_entries and blocked_edges and self._all_edges_blocked(blocked_edges):
            allow_entries = False
            reasons.append("execution_health_blocked_all_edges")

        self._execution_gate = ExecutionHealthGate(
            configured=True,
            allow_entries=allow_entries,
            metrics=metrics,
            blocked_edges=blocked_edges,
            reasons=reasons,
        )

    def _refresh_portfolio_gate(self, force: bool = False) -> None:
        if not self.config.portfolio_enforce:
            self._portfolio_gate = PortfolioGate()
            return

        refresh_ticks = max(int(self.config.portfolio_refresh_ticks or 0), 1)
        if not force and self.state.tick_count > 0 and self.state.tick_count % refresh_ticks != 0:
            return

        plan_path = self.config.portfolio_plan_path or self.config.allocation_plan_path
        if not plan_path:
            self._portfolio_gate = PortfolioGate(
                configured=True,
                allow_entries=False,
                strategy_id=self.config.portfolio_strategy_id,
                reasons=["portfolio_plan_missing"],
            )
            return

        requested = self._base_total_budget
        if self._allocation_gate.configured and self._allocation_gate.allocated_capital_usd > 0:
            requested = min(requested, self._allocation_gate.allocated_capital_usd)

        try:
            self._portfolio_gate = self._portfolio_allocator.refresh(
                plan_path=plan_path,
                runner_id=self._portfolio_runner_id,
                strategy_id=self.config.portfolio_strategy_id or "*",
                instrument="*",
                requested_capital_usd=requested,
            )
        except Exception as e:
            self._portfolio_gate = PortfolioGate(
                configured=True,
                allow_entries=False,
                plan_path=plan_path,
                strategy_id=self.config.portfolio_strategy_id,
                reasons=[f"portfolio_plan_error:{e}"],
            )

    def _evaluate_execution_edges(self, min_attempts: int) -> List[Any]:
        edges = self._current_allowed_edges()
        blocked = []
        cooldown_ticks = int(self.config.execution_health_cooldown_ticks or 0)

        for source, instrument in sorted(edges):
            edge_key = (source, instrument)
            cooldown_until = self._execution_edge_cooldowns.get(edge_key)
            if cooldown_until and self.state.tick_count < cooldown_until:
                blocked.append(self._build_edge_block(source, instrument, ["execution_health_cooldown"]))
                continue

            metrics = self.execution_health.compute_metrics(source=source, instrument=instrument)
            if metrics.attempts < min_attempts:
                continue

            edge_reasons: List[str] = []
            if metrics.fill_ratio < self.config.execution_health_min_fill_ratio:
                edge_reasons.append("execution_health_low_fill_ratio")
            if metrics.api_error_rate > self.config.execution_health_max_api_error_rate:
                edge_reasons.append("execution_health_api_error_rate")
            if metrics.avg_slippage_bps > self.config.execution_health_max_avg_slippage_bps:
                edge_reasons.append("execution_health_avg_slippage")
            if metrics.p95_slippage_bps > self.config.execution_health_max_p95_slippage_bps:
                edge_reasons.append("execution_health_p95_slippage")

            if edge_reasons:
                if cooldown_ticks > 0:
                    self._execution_edge_cooldowns[edge_key] = self.state.tick_count + cooldown_ticks
                blocked.append(self._build_edge_block(source, instrument, edge_reasons, metrics))
            else:
                self._execution_edge_cooldowns.pop(edge_key, None)

        return blocked

    def _build_edge_block(
        self,
        source: str,
        instrument: str,
        reasons: List[str],
        metrics: Optional[Any] = None,
    ) -> Any:
        from modules.execution_health import ExecutionHealthEdge, ExecutionHealthMetrics

        return ExecutionHealthEdge(
            source=source,
            instrument=instrument,
            metrics=metrics or ExecutionHealthMetrics(),
            reasons=reasons,
        )

    def _all_edges_blocked(self, blocked_edges: List[Any]) -> bool:
        edges = self._current_allowed_edges()
        if not edges:
            return False
        blocked_set = {(edge.source, edge.instrument) for edge in blocked_edges}
        return all(edge in blocked_set for edge in edges)

    def _current_allowed_edges(self) -> List[tuple[str, str]]:
        edges: set[tuple[str, str]] = set()
        if self._allocation_gate.routing_rules:
            for rule in self._allocation_gate.routing_rules:
                source = str(rule.get("source", "") or "")
                instruments = list(rule.get("allowed_instruments", []) or []) or ["*"]
                for instrument in instruments:
                    edges.add((source, instrument))
        elif self._allocation_gate.allowed_entry_sources:
            for source in self._allocation_gate.allowed_entry_sources:
                edges.add((source, "*"))
        return list(edges)

    def _is_execution_edge_blocked(self, source: str, instrument: str) -> bool:
        normalized = self._normalize_source(source)
        for edge in self._execution_gate.blocked_edges:
            if edge.source != normalized:
                continue
            if edge.instrument != "*" and edge.instrument != instrument:
                continue
            return True
        return False

    def _record_order_attempt(
        self,
        instrument: str,
        source: str,
        side: str,
        requested_qty: float,
        success: bool,
        error: str = "",
    ) -> None:
        if not self.config.execution_health_enabled:
            return
        self.execution_health.record_attempt(
            instrument=instrument,
            source=self._normalize_source(source),
            side=side,
            requested_qty=requested_qty,
            success=success,
            error=error,
        )

    def _record_order_fill(
        self,
        instrument: str,
        source: str,
        side: str,
        requested_qty: float,
        filled_qty: float,
        fill_price: float,
        mid_price: float,
    ) -> None:
        if not self.config.execution_health_enabled:
            return
        self.execution_health.record_fill(
            instrument=instrument,
            source=self._normalize_source(source),
            side=side,
            requested_qty=requested_qty,
            filled_qty=filled_qty,
            fill_price=fill_price,
            mid_price=mid_price,
        )

    @staticmethod
    def _resolve_mid_price(mid: float, snapshot: Optional[Any] = None) -> float:
        if mid and mid > 0:
            return float(mid)
        if snapshot is None:
            return 0.0
        try:
            resolved_mid = float(getattr(snapshot, "mid_price", 0) or 0)
        except Exception:
            resolved_mid = 0.0
        if resolved_mid > 0:
            return resolved_mid
        bid = float(getattr(snapshot, "bid", 0) or 0)
        ask = float(getattr(snapshot, "ask", 0) or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return 0.0

    @staticmethod
    def _normalize_source(source: str) -> str:
        if source.startswith("smart_money:"):
            return "smart_money"
        return source

    def _apply_source_routing(
        self,
        movers_signals: List[Dict[str, Any]],
        scanner_opps: List[Dict[str, Any]],
        smart_money_signals: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Filter entry sources according to the live allocation routing policy."""
        if not self._allocation_gate.configured or not self.config.allocation_enforce:
            return movers_signals, scanner_opps, smart_money_signals
        if not self._allocation_gate.allowed_entry_sources:
            return [], [], []

        filtered_movers = []
        for signal in movers_signals:
            source = "movers_immediate" if signal.get("signal_type") == "IMMEDIATE_MOVER" else "movers_signal"
            instrument = f"{signal.get('asset', '')}-PERP"
            if self._is_entry_allowed(
                source,
                instrument,
                signal_score=float(signal.get("confidence", 0) or 0.0),
            ):
                filtered_movers.append(signal)

        filtered_scanner = [
            item for item in scanner_opps
            if self._is_entry_allowed(
                "scanner",
                f"{item.get('asset', '')}-PERP",
                signal_score=float(item.get("final_score", 0) or 0.0),
            )
        ]
        filtered_smart_money = [
            item for item in smart_money_signals
            if self._is_entry_allowed(
                "smart_money",
                f"{item.get('asset', '')}-PERP",
                signal_score=float(item.get("confidence", 0) or 0.0),
            )
        ]
        return filtered_movers, filtered_scanner, filtered_smart_money

    def _is_entry_allowed(
        self,
        source: str,
        instrument: str,
        signal_score: float = 0.0,
    ) -> bool:
        """Check whether a source+instrument entry is currently permitted."""
        if self._execution_gate.configured:
            if not self._execution_gate.allow_entries:
                return False
            if self._is_execution_edge_blocked(source, instrument):
                return False
        if self._portfolio_gate.configured and not self._portfolio_gate.allow_entries:
            return False
        if not self._allocation_gate.configured or not self.config.allocation_enforce:
            return True
        normalized = source.split(":", 1)[0] if source.startswith("smart_money:") else source
        if not self._allocation_gate.routing_rules:
            return normalized in self._allocation_gate.allowed_entry_sources
        if not self._allocation_gate.allowed_entry_sources:
            return False
        for rule in self._allocation_gate.routing_rules:
            if rule.get("source") != normalized:
                continue
            allowed_instruments = list(rule.get("allowed_instruments", []) or [])
            if allowed_instruments and instrument not in allowed_instruments:
                continue
            min_signal_score = float(rule.get("min_signal_score", 0.0) or 0.0)
            if signal_score < min_signal_score:
                continue
            return True
        return False

    def _effective_margin_per_slot(self) -> float:
        """Per-slot margin after research/live allocation caps."""
        if not self._allocation_gate.configured or not self.config.allocation_enforce:
            base_margin = self._base_margin_per_slot
        else:
            if not self._allocation_gate.allow_entries:
                return 0.0
            if self._allocation_gate.per_slot_margin_usd > 0:
                base_margin = min(self._base_margin_per_slot, self._allocation_gate.per_slot_margin_usd)
            else:
                base_margin = self._base_margin_per_slot
        if self._portfolio_gate.configured and self._portfolio_gate.approved_capital_usd > 0:
            portfolio_per_slot = self._portfolio_gate.approved_capital_usd / max(self.config.max_slots, 1)
            return min(base_margin, portfolio_per_slot)
        return base_margin

    def _apply_volatility_sizing(self, instrument: str, base_margin: float) -> float:
        """Scale position size inversely proportional to recent hourly volatility."""
        try:
            # Fetch last 24 1h candles to compute standard deviation of returns
            candles = self.hl.get_candles(instrument.replace("-PERP", ""), "1h", 24 * 3600 * 1000)
            if not candles or len(candles) < 10:
                return base_margin
            
            import math
            returns = []
            for i in range(1, len(candles)):
                prev_close = float(candles[i-1].get("c", 0))
                curr_close = float(candles[i].get("c", 0))
                if prev_close > 0:
                    returns.append((curr_close - prev_close) / prev_close)
            
            if not returns:
                return base_margin
                
            mean_ret = sum(returns) / len(returns)
            var = sum((r - mean_ret)**2 for r in returns) / len(returns)
            std_dev_pct = math.sqrt(var) * 100.0
            
            if std_dev_pct <= 0.1:
                return base_margin
                
            # Inverse volatility multiplier
            multiplier = getattr(self.config, "volatility_target_hourly_pct", 1.5) / std_dev_pct
            multiplier = max(getattr(self.config, "volatility_min_multiplier", 0.2), 
                             min(getattr(self.config, "volatility_max_multiplier", 1.5), multiplier))
            
            adjusted = base_margin * multiplier
            log.info("Vol-Sizing %s: hourly_std=%.2f%%, target=%.2f%%, mult=%.2fx -> margin=$%.0f", 
                     instrument, std_dev_pct, getattr(self.config, "volatility_target_hourly_pct", 1.5), multiplier, adjusted)
            return adjusted
        except Exception as e:
            log.warning("Volatility sizing failed for %s: %s", instrument, e)
            return base_margin

    def _compute_entry_margin(self, instrument: str = None) -> float:
        """Return the margin budget available for the next entry."""
        effective_margin = self._effective_margin_per_slot()
        if effective_margin <= 0:
            return 0.0
        if not self._allocation_gate.configured or not self.config.allocation_enforce:
            remaining_allocation = float("inf")
        else:
            used_margin = sum(
                slot.margin_allocated for slot in self.state.engaged_slots()
                if slot.margin_allocated > 0
            )
            remaining_allocation = max(self._allocation_gate.allocated_capital_usd - used_margin, 0.0)

        remaining_portfolio = float("inf")
        if self._portfolio_gate.configured and self._portfolio_gate.approved_capital_usd > 0:
            used_margin = sum(
                slot.margin_allocated for slot in self.state.engaged_slots()
                if slot.margin_allocated > 0
            )
            remaining_portfolio = max(self._portfolio_gate.approved_capital_usd - used_margin, 0.0)

        remaining = min(remaining_allocation, remaining_portfolio)
        base_allowed = min(effective_margin, remaining)
        
        if instrument and getattr(self.config, "volatility_sizing_enabled", False):
            return self._apply_volatility_sizing(instrument, base_allowed)
            
        return base_allowed

    def _print_status(self) -> None:
        """Print current WOLF status."""
        if self.json_output:
            import json
            print(json.dumps(self.state.to_dict(), indent=2))
            return

        active = self.state.engaged_slots()
        print(f"\n{'='*60}")
        print(f"WOLF tick #{self.state.tick_count}  |  "
              f"Engaged: {len(active)}/{self.config.max_slots}  |  "
              f"Daily PnL: ${self.state.daily_pnl:+.2f}  |  "
              f"Total PnL: ${self.state.total_pnl:+.2f}")
        if self._allocation_gate.configured:
            print(f"Allocation Gate: entries={'on' if self._allocation_gate.allow_entries else 'off'}  |  "
                  f"Regime: {self._allocation_gate.current_regime}  |  "
                  f"Budget: ${self._allocation_gate.allocated_capital_usd:,.2f}")
            print(f"Allowed Sources: {', '.join(self._allocation_gate.allowed_entry_sources) or '-'}")
        if self._portfolio_gate.configured:
            print(
                f"Portfolio Gate: entries={'on' if self._portfolio_gate.allow_entries else 'off'}  |  "
                f"Approved: ${self._portfolio_gate.approved_capital_usd:,.2f}  |  "
                f"Total Cap: ${self._portfolio_gate.total_capital_usd:,.2f}"
            )
        if self._execution_gate.configured:
            metrics = self._execution_gate.metrics
            print(
                f"Execution Gate: entries={'on' if self._execution_gate.allow_entries else 'off'}  |  "
                f"Fill: {metrics.fill_ratio:.2f}  |  "
                f"Slip(avg/p95 bps): {metrics.avg_slippage_bps:.1f}/{metrics.p95_slippage_bps:.1f}  |  "
                f"Errors: {metrics.api_error_rate:.2f}"
            )
            if self._execution_gate.blocked_edges:
                blocked = ", ".join(
                    f"{edge.source}:{edge.instrument}"
                    for edge in self._execution_gate.blocked_edges
                )
                print(f"Blocked Edges: {blocked}")
        print(f"{'='*60}")

        if not active:
            print("  No active positions.")
        else:
            print(f"  {'Slot':<5} {'Stat':<8} {'Dir':<6} {'Instrument':<12} {'ROE':<8} {'HW':<8} {'Source':<16}")
            print(f"  {'-'*64}")
            for s in active:
                print(f"  {s.slot_id:<5} {s.status:<8} {s.direction:<6} {s.instrument:<12} "
                      f"{s.current_roe:+.1f}%{'':>2} {s.high_water_roe:.1f}%{'':>3} "
                      f"{s.entry_source:<16}")

        print()

    def _print_summary(self) -> None:
        """Print session summary on shutdown."""
        print(f"\n{'='*60}")
        print("WOLF SESSION SUMMARY")
        print(f"{'='*60}")
        print(f"  Ticks: {self.state.tick_count}")
        print(f"  Total trades: {self.state.total_trades}")
        print(f"  Daily PnL: ${self.state.daily_pnl:+.2f}")
        print(f"  Total PnL: ${self.state.total_pnl:+.2f}")
        if self.state.daily_loss_triggered:
            print("  ** Daily loss limit was triggered **")
        print(f"{'='*60}\n")

    def _run_nightly_review(self, today: str) -> None:
        """Run nightly review comparing today vs. 7-day rolling average."""
        try:
            raw_trades = self.trade_log.read_all()
            if not raw_trades:
                return

            now_ms = int(time.time() * 1000)
            day_ms = 86_400_000
            midnight = now_ms - (now_ms % day_ms)

            today_trades = [
                TradeRecord.from_dict(t) for t in raw_trades
                if t.get("timestamp_ms", 0) >= midnight
            ]
            week_trades = [
                TradeRecord.from_dict(t) for t in raw_trades
                if t.get("timestamp_ms", 0) >= midnight - (7 * day_ms)
            ]

            result = self.journal_engine.compute_nightly_review(
                today_trades, week_trades, date=today,
            )

            # Save briefing
            howl_dir = Path(self.data_dir) / "howl"
            howl_dir.mkdir(parents=True, exist_ok=True)
            (howl_dir / f"{today}-nightly.md").write_text(result.briefing_md)

            # Write findings to memory
            for finding in result.key_findings:
                event = self.memory_engine.create_howl_event(
                    distilled=f"Nightly: {finding}",
                )
                self.memory_guard.log_event(event)

            # Append to Obsidian daily note
            if self._obsidian_writer:
                summary_lines = [f"**{today}** — {result.round_trips_today} round trips"]
                for f in result.key_findings:
                    summary_lines.append(f"- {f}")
                self._obsidian_writer.append_to_daily(today, "\n".join(summary_lines))

            log.info("Nightly review: %d RTs today, findings: %s",
                     result.round_trips_today, "; ".join(result.key_findings))

        except Exception as e:
            log.warning("Nightly review failed: %s", e)

    def _handle_shutdown(self, signum, frame):
        log.info("Shutdown signal received")
        self._running = False

        # Log session end to memory
        try:
            event = self.memory_engine.create_session_event(
                event_type="session_end",
                tick_count=self.state.tick_count,
                total_pnl=self.state.total_pnl,
                active_slots=len(self.state.active_slots()),
                total_trades=self.state.total_trades,
            )
            self.memory_guard.log_event(event)
        except Exception:
            pass
