"""Shared setup for quoting-engine-powered strategies.

Adds the quoting_engine package paths to sys.path so it is importable
without pip install. Raises a clear error if the module is not found.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_QE_ROOT = _REPO_ROOT / "quoting_engine"

if not _QE_ROOT.is_dir():
    raise ImportError(
        f"quoting_engine not found at {_QE_ROOT}. "
        "The following strategies require it: engine_mm, regime_mm, grid_mm, "
        "liquidation_mm, funding_arb. "
        "Use simple_mm or avellaneda_mm as open-source alternatives."
    )

for p in [str(_REPO_ROOT), str(_QE_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)
