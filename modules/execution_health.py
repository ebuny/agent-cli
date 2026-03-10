"""Execution health tracking and kill-switch evaluation."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from parent.store import JSONLStore


@dataclass
class ExecutionHealthSample:
    event: str
    timestamp_ms: int
    instrument: str
    source: str
    side: str
    requested_qty: float
    filled_qty: float = 0.0
    fill_price: float = 0.0
    mid_price: float = 0.0
    slippage_bps: float = 0.0
    success: bool = True
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionHealthMetrics:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    requested_qty: float = 0.0
    filled_qty: float = 0.0
    fill_ratio: float = 0.0
    avg_slippage_bps: float = 0.0
    p95_slippage_bps: float = 0.0
    api_error_rate: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionHealthEdge:
    source: str
    instrument: str
    metrics: ExecutionHealthMetrics
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["metrics"] = self.metrics.to_dict()
        return payload


@dataclass
class ExecutionHealthGate:
    configured: bool = False
    allow_entries: bool = True
    metrics: ExecutionHealthMetrics = field(default_factory=ExecutionHealthMetrics)
    blocked_edges: List[ExecutionHealthEdge] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "configured": self.configured,
            "allow_entries": self.allow_entries,
            "metrics": self.metrics.to_dict(),
            "blocked_edges": [edge.to_dict() for edge in self.blocked_edges],
            "reasons": list(self.reasons),
        }


class ExecutionHealthTracker:
    """Tracks order attempts/fills and computes execution health metrics."""

    def __init__(self, path: str, max_events: int = 200):
        self.store = JSONLStore(path=path)
        self.max_events = max(max_events, 1)
        self._events: List[ExecutionHealthSample] = []
        self._load_recent()

    def record_attempt(
        self,
        instrument: str,
        source: str,
        side: str,
        requested_qty: float,
        success: bool,
        error: str = "",
    ) -> None:
        sample = ExecutionHealthSample(
            event="attempt",
            timestamp_ms=int(time.time() * 1000),
            instrument=instrument,
            source=source,
            side=side,
            requested_qty=float(requested_qty),
            success=bool(success),
            error=str(error or ""),
        )
        self._append(sample)

    def record_fill(
        self,
        instrument: str,
        source: str,
        side: str,
        requested_qty: float,
        filled_qty: float,
        fill_price: float,
        mid_price: float,
    ) -> None:
        slippage_bps = _compute_slippage_bps(
            side=side,
            fill_price=float(fill_price),
            mid_price=float(mid_price),
        )
        sample = ExecutionHealthSample(
            event="fill",
            timestamp_ms=int(time.time() * 1000),
            instrument=instrument,
            source=source,
            side=side,
            requested_qty=float(requested_qty),
            filled_qty=float(filled_qty),
            fill_price=float(fill_price),
            mid_price=float(mid_price),
            slippage_bps=slippage_bps,
            success=True,
        )
        self._append(sample)

    def compute_metrics(
        self,
        source: Optional[str] = None,
        instrument: Optional[str] = None,
    ) -> ExecutionHealthMetrics:
        events = self._filter_events(source=source, instrument=instrument)
        attempts = [e for e in events if e.event == "attempt"]
        fills = [e for e in events if e.event == "fill"]
        requested_qty = sum(e.requested_qty for e in attempts)
        filled_qty = sum(e.filled_qty for e in fills)
        failures = sum(1 for e in attempts if not e.success)
        successes = len(attempts) - failures
        slippages = [e.slippage_bps for e in fills if e.slippage_bps != 0.0]
        return ExecutionHealthMetrics(
            attempts=len(attempts),
            successes=successes,
            failures=failures,
            requested_qty=round(requested_qty, 6),
            filled_qty=round(filled_qty, 6),
            fill_ratio=_safe_ratio(filled_qty, requested_qty),
            avg_slippage_bps=round(_avg(slippages), 4),
            p95_slippage_bps=round(_percentile(slippages, 0.95), 4),
            api_error_rate=_safe_ratio(failures, len(attempts)),
        )

    def _filter_events(
        self,
        source: Optional[str],
        instrument: Optional[str],
    ) -> List[ExecutionHealthSample]:
        filtered = []
        for event in self._events:
            if source and event.source != source:
                continue
            if instrument and instrument != "*" and event.instrument != instrument:
                continue
            filtered.append(event)
        return filtered

    def _append(self, sample: ExecutionHealthSample) -> None:
        self._events.append(sample)
        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events:]
        self.store.append(sample.to_dict())

    def _load_recent(self) -> None:
        records = self.store.read_all()
        for record in records[-self.max_events:]:
            try:
                sample = ExecutionHealthSample(**record)
            except TypeError:
                continue
            self._events.append(sample)


def _compute_slippage_bps(side: str, fill_price: float, mid_price: float) -> float:
    if mid_price <= 0:
        return 0.0
    side = side.lower()
    if side == "buy":
        return (fill_price - mid_price) / mid_price * 10_000
    if side == "sell":
        return (mid_price - fill_price) / mid_price * 10_000
    return 0.0


def _avg(values: Iterable[float]) -> float:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _percentile(values: Sequence[float], quantile: float) -> float:
    vals = sorted(values)
    if not vals:
        return 0.0
    if len(vals) == 1:
        return float(vals[0])
    index = (len(vals) - 1) * max(0.0, min(quantile, 1.0))
    low = int(index)
    high = min(low + 1, len(vals) - 1)
    fraction = index - low
    return float(vals[low] + (vals[high] - vals[low]) * fraction)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)
