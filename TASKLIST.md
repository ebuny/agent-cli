# Autonomous Profit System Tasklist

## Completed

- Phase 1 hardening
- WOLF full scanner candle path
- TWAP entry execution and persistence
- Trade attribution for Judge/HOWL
- Local quoting engine packaging
- Windows-safe scanner/movers output
- Replay engine and walk-forward evaluation
- Historical dataset loader and research scoring
- Research CLI: `ingest`, `run`, `status`, `allocate-live`
- Native scanner/movers/WOLF ingestion
- Regime-aware research scoring
- Live allocation plan generation
- WOLF runtime allocation gate from live plan
- WOLF source-level routing from live allocation plan
- Research-derived signal-quality floors in live routing
- Execution health kill-switch (slippage, fill ratio, API error rate)
- Strategy-level allocation gating for `hl run`
- Portfolio allocator shared across WOLF + `hl run`
- VPS daily research refresh script + systemd unit
- Live allocation gating plumbed through WOLF CLI + MCP server
- Live allocation feedback from HOWL/Judge (capital scaling + source blocking)
- Research refresh pipeline applies feedback during live plan generation
- Phase 3 allocator wiring into live runtime controls
- Carry micro-live configs + launcher script + systemd unit
- Carry micro-live systemd env template
- Micro-live checklist for Hyperliquid deployment
- Paper mode for single-strategy runs (live data, simulated fills)
- Paper validation command (hl paper validate)
- Optional BTC carry micro-live service + env template
- BTC carry configs with reduced sizing

## In Progress

- Phase 4: promote one narrow edge to micro-live

## Next

- Enable carry micro-live on VPS with live allocation plan + portfolio caps

## Later

- Historical ingestion from richer venue/export sources
- Regime-specific allocator weights across multiple live strategies
- Strategy disable/enable orchestration from one central runtime allocator
- Revenue layer: signals/API product on top of validated edges
