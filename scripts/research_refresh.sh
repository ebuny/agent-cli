#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATA_DIR="${DATA_DIR:-data}"
RESEARCH_DIR="${RESEARCH_DIR:-${DATA_DIR}/research}"
NATIVE_DIR="${NATIVE_DIR:-${RESEARCH_DIR}/native}"
DATASETS_DIR="${DATASETS_DIR:-${NATIVE_DIR}/datasets}"
LIVE_PLAN_PATH="${LIVE_PLAN_PATH:-${RESEARCH_DIR}/live_allocation_plan.json}"

STRATEGIES_CSV="${STRATEGIES_CSV:-mean_reversion,momentum_breakout,basis_arb,funding_arb,liquidation_mm}"
TRAIN_SIZE="${TRAIN_SIZE:-200}"
VALIDATION_SIZE="${VALIDATION_SIZE:-50}"
STEP_SIZE="${STEP_SIZE:-50}"
CAPITAL_USD="${CAPITAL_USD:-10000}"
RESERVE_PCT="${RESERVE_PCT:-0.1}"
MAX_STRATEGY_PCT="${MAX_STRATEGY_PCT:-0.5}"
INSTRUMENT="${INSTRUMENT:-}"

SCANNER_HISTORY="${SCANNER_HISTORY:-${DATA_DIR}/scanner/scan-history.json}"
MOVERS_HISTORY="${MOVERS_HISTORY:-${DATA_DIR}/movers/scan-history.json}"
WOLF_TRADES="${WOLF_TRADES:-${DATA_DIR}/wolf/trades.jsonl}"
WOLF_EDGE_REPORT="${WOLF_EDGE_REPORT:-${NATIVE_DIR}/wolf_edge_report.json}"
FEEDBACK_TRADES="${FEEDBACK_TRADES:-${WOLF_TRADES}}"
FEEDBACK_JUDGE="${FEEDBACK_JUDGE:-}"

INGEST_ARGS=()
if [[ -f "$SCANNER_HISTORY" ]]; then
  INGEST_ARGS+=("--scanner-history" "$SCANNER_HISTORY")
fi
if [[ -f "$MOVERS_HISTORY" ]]; then
  INGEST_ARGS+=("--movers-history" "$MOVERS_HISTORY")
fi
if [[ -f "$WOLF_TRADES" ]]; then
  INGEST_ARGS+=("--wolf-trades" "$WOLF_TRADES")
fi

if [[ ${#INGEST_ARGS[@]} -gt 0 ]]; then
  python -m cli.main research ingest \
    "${INGEST_ARGS[@]}" \
    --output-dir "$NATIVE_DIR"
fi

if [[ ! -d "$DATASETS_DIR" ]]; then
  echo "No datasets directory at $DATASETS_DIR; skipping research run."
  exit 0
fi

DATASET_FILES=()
while IFS= read -r -d '' file; do
  DATASET_FILES+=("$file")
done < <(find "$DATASETS_DIR" -type f -name "*.jsonl" -print0)

if [[ ${#DATASET_FILES[@]} -eq 0 ]]; then
  echo "No datasets found in $DATASETS_DIR; skipping research run."
  exit 0
fi

STRATEGY_ARGS=()
IFS=',' read -r -a STRATEGIES <<< "$STRATEGIES_CSV"
for strat in "${STRATEGIES[@]}"; do
  if [[ -n "$strat" ]]; then
    STRATEGY_ARGS+=("--strategy" "$strat")
  fi
done

DATASET_ARGS=()
for ds in "${DATASET_FILES[@]}"; do
  DATASET_ARGS+=("--dataset" "$ds")
done

EDGE_ARGS=()
if [[ -f "$WOLF_EDGE_REPORT" ]]; then
  EDGE_ARGS+=("--wolf-edge-report" "$WOLF_EDGE_REPORT")
fi

INSTRUMENT_ARGS=()
if [[ -n "$INSTRUMENT" ]]; then
  INSTRUMENT_ARGS+=("--instrument" "$INSTRUMENT")
fi

python -m cli.main research run \
  "${STRATEGY_ARGS[@]}" \
  "${DATASET_ARGS[@]}" \
  --train-size "$TRAIN_SIZE" \
  --validation-size "$VALIDATION_SIZE" \
  --step-size "$STEP_SIZE" \
  --capital "$CAPITAL_USD" \
  --reserve-pct "$RESERVE_PCT" \
  --max-strategy-pct "$MAX_STRATEGY_PCT" \
  --data-dir "$RESEARCH_DIR" \
  "${EDGE_ARGS[@]}" \
  "${INSTRUMENT_ARGS[@]}"

python -m cli.main research allocate-live \
  --snapshot "${RESEARCH_DIR}/latest_allocation.json" \
  --dataset "${DATASET_FILES[0]}" \
  --feedback \
  --feedback-trades "$FEEDBACK_TRADES" \
  ${FEEDBACK_JUDGE:+--feedback-judge "$FEEDBACK_JUDGE"} \
  --output-path "$LIVE_PLAN_PATH"
