#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    PYTHON_BIN="python"
  fi
fi

PLAN_PATH="${PLAN_PATH:-data/research/live_allocation_plan.json}"
PORTFOLIO_PLAN="${PORTFOLIO_PLAN:-$PLAN_PATH}"
INSTRUMENT="${INSTRUMENT:-}"
MAINNET="${MAINNET:-false}"
DRY_RUN="${DRY_RUN:-false}"
PAPER="${PAPER:-false}"

COMMON_ARGS=(
  --allocation-plan "$PLAN_PATH"
  --allocation-enforce
  --portfolio-plan "$PORTFOLIO_PLAN"
  --portfolio-enforce
)

if [[ -n "$INSTRUMENT" ]]; then
  COMMON_ARGS+=(--instrument "$INSTRUMENT")
fi
if [[ "$MAINNET" == "true" ]]; then
  COMMON_ARGS+=(--mainnet)
fi
if [[ "$DRY_RUN" == "true" ]]; then
  COMMON_ARGS+=(--dry-run)
fi
if [[ "$PAPER" == "true" ]]; then
  COMMON_ARGS+=(--paper)
fi

"$PYTHON_BIN" -m cli.main run funding_arb --config configs/carry_micro_live/funding_arb.yaml "${COMMON_ARGS[@]}" &
"$PYTHON_BIN" -m cli.main run basis_arb --config configs/carry_micro_live/basis_arb.yaml "${COMMON_ARGS[@]}" &
"$PYTHON_BIN" -m cli.main run hedge_agent --config configs/carry_micro_live/hedge_agent.yaml "${COMMON_ARGS[@]}" &

wait
