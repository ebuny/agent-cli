# YEX Trading Agent CLI

Autonomous trading agent for [Hyperliquid](https://hyperliquid.xyz) perps and [YEX](https://yex.trade) yield markets. Ships with 7 built-in strategies, a Claude-powered LLM agent, and a full autonomous trading stack: Dynamic Stop Loss (DSL), Opportunity Scanner, Emerging Movers detector, and the WOLF multi-slot orchestrator.

Works as a standalone CLI, a **Claude Code skill**, or an **OpenClaw AgentSkill**.

## Quick Start

```bash
git clone https://github.com/Nunchi-trade/agent-cli.git
cd agent-cli
pip install -e .

# Set your HL private key
export HL_PRIVATE_KEY=0x...

# Mock test (no connection needed)
hl run avellaneda_mm --mock --max-ticks 10

# Live testnet
hl run avellaneda_mm -i ETH-PERP --tick 10

# Run the full WOLF autonomous strategy
hl wolf run --mock --max-ticks 10
```

## Architecture

```
cli/           CLI commands and trading engine
  commands/    Subcommand modules (run, dsl, scanner, movers, wolf, house)
  hl_adapter.py  Direct HL API adapter (live + mock)
strategies/    Trading strategy implementations
modules/       Pure logic modules (zero I/O)
  trailing_stop.py   DSL trailing stop engine
  scanner_engine.py  Opportunity scanner engine
  movers_engine.py   Emerging movers detection engine
  wolf_engine.py     WOLF decision engine
  *_config.py        Configuration + presets
  *_state.py         State models + persistence
  *_guard.py         Guard layer (engine + persistence bridge)
clearing/      Clearing types, ECIES encryption for commit-reveal
agent/         Agent client — connects to house enclave via HTTP
skills/        Agent Skills packaging (SKILL.md + runners)
sdk/           Strategy base class, loader, and model registry
common/        Shared data models and crypto utilities
parent/        HL API proxy, position tracking, risk management
tests/         Test suite
```

## Commands

```bash
# Core trading
hl run <strategy> [options]       # Start autonomous trading
hl status [--watch]               # Show positions, PnL, risk
hl trade <inst> <side> <size>     # Place a single order
hl account                        # Show HL account state
hl strategies                     # List all strategies

# DSL — Dynamic Stop Loss
hl dsl run -i ETH-PERP [options]  # Start DSL trailing stop guard
hl dsl status                     # Show active DSL guards
hl dsl presets                    # List DSL presets

# Scanner — Opportunity Scanner
hl scanner run [options]          # Start continuous scanning (15min ticks)
hl scanner once [options]         # Run a single scan
hl scanner status                 # Show last scan results
hl scanner presets                # List scanner presets

# Movers — Emerging Movers Detector
hl movers run [options]           # Start continuous movers detection (60s ticks)
hl movers once [options]          # Run a single movers scan
hl movers status                  # Show last movers results
hl movers presets                 # List movers presets

# WOLF — Autonomous Multi-Slot Strategy
hl wolf run [options]             # Start WOLF orchestrator
hl wolf once [options]            # Run a single WOLF tick
hl wolf status                    # Show WOLF state and positions
hl wolf presets                   # List WOLF presets

# House — TEE Clearing House Agent
hl house join <strategy> [--url URL]  # Join a running house enclave
hl house status [--url URL]           # Show house scoreboard
```

## Strategies

| Name | Type | Description |
|------|------|-------------|
| `simple_mm` | Market Making | Symmetric bid/ask quoting around mid |
| `avellaneda_mm` | Market Making | Inventory-aware Avellaneda-Stoikov model |
| `mean_reversion` | Statistical | Trade on SMA deviations |
| `hedge_agent` | Risk | Reduces excess exposure |
| `rfq_agent` | Liquidity | Block-size dark RFQ flow |
| `aggressive_taker` | Directional | Crosses spread with directional bias |
| `claude_agent` | LLM | Multi-model AI agent (Gemini/Claude/OpenAI) |

## Autonomous Trading Stack

### DSL — Dynamic Stop Loss

Trailing stop system with tiered profit-locking. Protects profits while letting winners run.

**Two phases:**
- **Phase 1 (Let it breathe)**: Wide retrace tolerance while position builds
- **Phase 2 (Lock the bag)**: Tiered profit floors that ratchet up as ROE grows

```bash
# Start a DSL guard on an existing position
hl dsl run -i ETH-PERP --preset tight

# Available presets: moderate, tight
hl dsl presets
```

**Presets:**

| Preset | Phase 1 Retrace | Tiers | Stagnation TP |
|--------|----------------|-------|---------------|
| `moderate` | 3% | 6 tiers (10-100% ROE) | No |
| `tight` | 5% | 4 tiers (10-75% ROE) | Yes (8% ROE, 1h) |

### Scanner — Opportunity Scanner

Multi-factor screening engine that evaluates all HL perps for trade setups. Scores assets across four pillars: market structure, technicals, funding, and BTC macro alignment.

```bash
# Run continuous scanning (every 15 min)
hl scanner run --mock

# Single scan
hl scanner once --mock
```

**Scoring pillars:**

| Pillar | Weight | Signals |
|--------|--------|---------|
| Market Structure | 35 | Volume, OI, liquidity |
| Technicals | 30 | RSI, EMA, patterns, hourly trend |
| Funding | 20 | Rate extremes, direction bias |
| BTC Macro | 15 | Trend alignment, regime filter |

### Movers — Emerging Movers Detector

Detects assets with sudden capital inflow using OI, volume, funding, and price signals. Runs every 60 seconds.

```bash
# Continuous detection
hl movers run --mock

# Single scan
hl movers once --mock
```

**Signal types:**

| Signal | Trigger | Confidence |
|--------|---------|------------|
| IMMEDIATE_MOVER | OI +15% AND volume 5x surge | 100 |
| VOLUME_SURGE | 4h volume / average > 3x | 70 |
| OI_BREAKOUT | OI jumps 8%+ above baseline | 60 |
| FUNDING_FLIP | Funding rate reverses or accelerates 50%+ | 50 |

**Direction classification** uses majority vote: funding rate sign, price breakout direction, and volume+price momentum.

### WOLF — Autonomous Multi-Slot Strategy

The top-level orchestrator. Composes Scanner + Movers + DSL into a single autonomous strategy managing 2-3 concurrent positions.

```bash
# Full autonomous mode (mock)
hl wolf run --mock --max-ticks 50

# Live testnet
hl wolf run

# With overrides
hl wolf run --budget 5000 --slots 2 --leverage 5

# Conservative preset
hl wolf run --preset conservative
```

**Tick schedule** (60s base):
- Every tick: Fetch prices, update ROEs, check DSL, run movers, evaluate entry/exit
- Every 5 ticks (5min): Watchdog health check
- Every 15 ticks (15min): Run opportunity scanner

**Entry priority:**

| Priority | Source | Condition |
|----------|--------|-----------|
| 1 | Movers IMMEDIATE | Auto-enter on compound OI+volume signal |
| 2 | Scanner | Score > 170 |
| 3 | Movers signal | Confidence > 70 |

**Exit priority:**

| Priority | Reason | Condition |
|----------|--------|-----------|
| 1 | DSL trailing stop | Tier breach / retrace exceeded |
| 2 | Hard stop | ROE < -5% |
| 3 | Conviction collapse | Signal gone + negative PnL for 30+ min |
| 4 | Stagnation TP | ROE stuck above 3% for 60+ min |

**Risk management:**
- Per-slot margin: budget / max_slots
- Daily loss limit: $500 (default) — closes all positions
- Max 2 same-direction slots
- No duplicate instruments

**Presets:**

| Preset | Slots | Leverage | Scanner Threshold | Daily Loss Limit |
|--------|-------|----------|-------------------|------------------|
| `default` | 3 | 10x | 170 | $500 |
| `conservative` | 2 | 5x | 190 | $250 |
| `aggressive` | 3 | 15x | 150 | $1,000 |

### House — TEE Clearing House Agent

This CLI implements the **agent side** of the [Nunchi / Daeji](https://github.com/Nunchi-trade/Tee-work-) architecture. Agents running market-making strategies connect to a House Enclave — a TEE-secured clearing venue — and compete for admission, flow, and revenue.

#### The 3-Layer Stack

The House sits on a Hardware / Math / Proof stack:

| Layer | Component | Purpose |
|-------|-----------|---------|
| Hardware | TEE execution + attestation | "Can't be evil" execution integrity |
| Math | Off-chain solve + on-chain verify | "Proof of optimality" cooperative clearing |
| Proof | ISFR + agent proofs + leaderboard | Durable data asset + reputational primitives |

#### Agent → House Pipeline

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌───────────────┐
│  Agent CLI   │────▶│ Strategy     │────▶│  ECIES Seal      │────▶│ House Relay    │
│  (this repo) │     │ on_tick()    │     │  to enclave      │     │ /v1/commit     │
│              │     │ → Orders     │     │  pubkey           │     │ /v1/reveal     │
└─────────────┘     └──────────────┘     └──────────────────┘     └───────┬───────┘
                                                                          │
                    ┌──────────────┐     ┌──────────────────┐     ┌───────▼───────┐
                    │  Fills +     │◀────│  KKT Certificate │◀────│ Cooperative   │
                    │  Positions   │     │  (proof of       │     │ Clearing      │
                    │  Scoreboard  │     │   optimality)    │     │ inside TEE    │
                    └──────────────┘     └──────────────────┘     └───────────────┘
```

**Step-by-step flow per round:**

1. **Register** — Agent hashes strategy source via `inspect.getsource()` (SHA-256). The House uses this for admission control and attribution.
2. **Connect** — `GET /v1/identity` fetches the enclave's secp256k1 public key and TEE attestation.
3. **Receive snapshot** — `GET /v1/snapshot` returns the current round ID, phase, and `MarketSnapshot` with mid price, bid/ask, funding rate, OI.
4. **Run strategy** — The agent's `on_tick()` produces `StrategyDecision`s, converted to `Order` objects with Decimal precision.
5. **Seal** — Orders are ECIES-encrypted (secp256k1 + AES-256-GCM) to the enclave's public key. Only the TEE can decrypt.
6. **Commit** — `POST /v1/commit` with `SHA-256(ciphertext)`. The hash locks the agent's orders before anyone reveals.
7. **Reveal** — `POST /v1/reveal` with the full ciphertext. The enclave verifies `SHA-256(ciphertext) == commitment`.
8. **Clear** — The House decrypts all bundles inside the TEE and runs cooperative clearing: supply/demand crossing that maximizes surplus. Produces fills + KKT certificates (dual-variable proof of optimality).
9. **Verify** — Agent fetches `GET /v1/result/{round_id}` with fills, clearing prices, and KKT certificates. Verification is O(n^2) checks, not O(n^3) solving.

#### Commit-Reveal Protocol

| Phase | Agent Action | Endpoint | Description |
|-------|-------------|----------|-------------|
| IDLE | Poll | `GET /v1/snapshot` | Wait for round to enter commit phase |
| COMMIT | Seal + hash | `POST /v1/commit` | Submit `SHA-256(ciphertext)` as binding commitment |
| REVEAL | Send bundle | `POST /v1/reveal` | Submit ECIES-encrypted order bundle |
| CLEARING | Wait | — | House decrypts, crosses orders, generates KKT certs |
| DONE | Fetch | `GET /v1/result/{id}` | Retrieve fills, clearing prices, certificates |

#### Why Commit-Reveal?

Without it, agents could see others' orders and front-run. The two-phase protocol ensures **execution confidentiality**: orders are encrypted to the TEE's key during commit, so no one (not even the relay operator) can read them until the reveal phase. The TEE decrypts and clears atomically.

#### Cooperative Clearing

The House doesn't just match — it **cooperatively clears**:

- **Off-chain solve**: finds the surplus-maximizing allocation across all agent orders
- **KKT certificates**: each clearing round produces dual variables (lambda, mu) that prove optimality — anyone can verify in O(n^2)
- **Fallback ladder**: if cooperative clearing fails, deterministic pruning removes lowest-priority orders and retries. If still infeasible → external hedge → safe mode.
- **Pruning rule**: orders are priority-sorted by `(priorityFee, timestamp, txHash)`. The imbalance side's lowest-priority orders are removed first.

#### House Admission ("Enter the House")

Agents don't get House access by default — they **earn it** through the leaderboard:

1. **Run as Liquidity Senate agent** — produce verifiable quotes/trades via this CLI
2. **Build reputation** — the leaderboard scores agents on Sharpe, uptime, market quality (spread, depth), risk discipline, and integrity (TEE attestation rate)
3. **Qualify** — top agents by epoch thresholds earn a `HouseSeatCertificate`
4. **Enter** — the House Enclave loads the admitted strategy via a strategy VM (bundle + signature verification)

House seats are **revocable**: immediate revocation for fraud, epoch-based renewal for uptime/quality, slashing for mandate violations.

#### Agent Roles

| Role | Description |
|------|-------------|
| **MM Agent** | Quotes two-sided markets, manages inventory bands |
| **RFQ Agent** | Responds privately to block-size RFQs; hedges externally |
| **Hedge Agent** | Reduces exposure per deterministic mandate (delta, DV01, funding) |

#### Model Registry

Strategies are versioned via source-code hashing:

```python
from sdk.strategy_sdk.registry import ModelRegistry

registry = ModelRegistry()
bundle = registry.register("strategies.avellaneda_mm:AvellanedaStoikovMM")
# bundle.source_hash = SHA-256(inspect.getsource(cls))
# bundle.strategy_id = "AvellanedaStoikovMM"

# Verify a strategy hasn't been tampered with
assert registry.verify(bundle)  # re-hashes and compares
```

The `strategy_artifact_hash` is included in every `DecisionEnvelope` for per-decision attribution and auditability.

#### Usage

```bash
# Join a house enclave with avellaneda_mm
hl house join avellaneda_mm --url http://house:8080 --agent-id my-agent

# Register strategy hash before joining (for admission control)
hl house join avellaneda_mm --url http://house:8080 --register

# Check scoreboard
hl house status --url http://house:8080

# Custom poll interval (default 2s)
hl house join simple_mm --url http://house:8080 --poll 5
```

#### API Endpoints (Agent → Relay)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/identity` | Enclave public key + TEE attestation |
| GET | `/v1/snapshot` | Current round ID, phase, market data |
| POST | `/v1/commit` | Submit commitment hash |
| POST | `/v1/reveal` | Submit encrypted order bundle |
| GET | `/v1/result/{round_id}` | Fills, clearing prices, KKT certificates |
| GET | `/v1/positions/{agent_id}` | Agent's current positions |
| GET | `/v1/scoreboard` | Leaderboard rankings |

## Custom Strategies

Create a Python file that subclasses `BaseStrategy`:

```python
# my_strategies/momentum.py
from sdk.strategy_sdk.base import BaseStrategy, StrategyContext
from common.models import MarketSnapshot, StrategyDecision

class MomentumStrategy(BaseStrategy):
    def __init__(self, strategy_id="momentum", lookback=10, threshold=0.5, size=0.1, **kwargs):
        super().__init__(strategy_id=strategy_id)
        self.lookback = lookback
        self.threshold = threshold
        self.size = size
        self._prices = []

    def on_tick(self, snapshot, context=None):
        mid = snapshot.mid_price
        if mid <= 0:
            return []

        self._prices.append(mid)
        if len(self._prices) < self.lookback:
            return []

        old = self._prices[-self.lookback]
        pct_change = (mid - old) / old * 100

        if pct_change > self.threshold:
            return [StrategyDecision(
                action="place_order",
                instrument=snapshot.instrument,
                side="buy",
                size=self.size,
                limit_price=round(snapshot.ask, 2),
            )]
        elif pct_change < -self.threshold:
            return [StrategyDecision(
                action="place_order",
                instrument=snapshot.instrument,
                side="sell",
                size=self.size,
                limit_price=round(snapshot.bid, 2),
            )]
        return []
```

Run it:

```bash
hl run my_strategies.momentum:MomentumStrategy -i ETH-PERP --tick 10
```

### Strategy Interface

Every strategy receives two objects each tick:

| Object | Fields |
|--------|--------|
| `MarketSnapshot` | `mid_price`, `bid`, `ask`, `spread_bps`, `funding_rate`, `open_interest`, `volume_24h`, `timestamp_ms` |
| `StrategyContext` | `position_qty`, `position_notional`, `unrealized_pnl`, `realized_pnl`, `reduce_only`, `safe_mode`, `round_number`, `meta` |

Return a list of `StrategyDecision`:

```python
StrategyDecision(
    action="place_order",  # or "noop"
    instrument="ETH-PERP",
    side="buy",            # or "sell"
    size=0.1,
    limit_price=2050.0,
    meta={"signal": "my_signal"},
)
```

## Run Options

| Flag | Default | Description |
|------|---------|-------------|
| `-i, --instrument` | ETH-PERP | Trading instrument |
| `-t, --tick` | 10.0 | Seconds between ticks |
| `-c, --config` | — | YAML config file |
| `--mainnet` | false | Use mainnet (default: testnet) |
| `--dry-run` | false | Run without placing orders |
| `--mock` | false | Use mock market data |
| `--max-ticks` | 0 | Stop after N ticks (0 = forever) |
| `--resume/--fresh` | resume | Resume or start fresh |
| `--model` | — | LLM model override (claude_agent) |

## YEX Markets

[YEX](https://yex.trade) (Nunchi HIP-3) yield perpetuals on Hyperliquid:

| Instrument | HL Coin | Description |
|------------|---------|-------------|
| VXX-USDYP | yex:VXX | Volatility index yield perp |
| US3M-USDYP | yex:US3M | US 3M Treasury rate yield perp |

```bash
hl run avellaneda_mm -i VXX-USDYP --tick 15
hl run claude_agent -i US3M-USDYP --tick 30
```

### Claim Testnet USDyP

```bash
curl --location 'https://api-temp.nunchi.trade/api/v1/yex/usdyp-claim' \
  --header 'x-network: testnet' \
  --header 'Content-Type: application/json' \
  --data '{"userAddress":"<YOUR_WALLET_ADDRESS>"}'
```

## LLM Agent (Multi-Model)

The `claude_agent` strategy uses structured tool/function calling to make trading decisions:

| Provider | Models | Env Variable |
|----------|--------|-------------|
| Google Gemini | `gemini-2.0-flash` (default), `gemini-2.5-pro` | `GEMINI_API_KEY` |
| Anthropic Claude | `claude-haiku-4-5-20251001`, `claude-sonnet-4-20250514` | `ANTHROPIC_API_KEY` |
| OpenAI | `gpt-4o`, `gpt-4o-mini`, `o3-mini` | `OPENAI_API_KEY` |

```bash
# Gemini (default)
export GEMINI_API_KEY=...
hl run claude_agent -i ETH-PERP --tick 15

# Claude
export ANTHROPIC_API_KEY=sk-ant-...
hl run claude_agent -i ETH-PERP --tick 15 --model claude-haiku-4-5-20251001

# OpenAI
export OPENAI_API_KEY=sk-...
hl run claude_agent -i ETH-PERP --tick 15 --model gpt-4o
```

## Configuration

```yaml
strategy: avellaneda_mm
strategy_params:
  gamma: 0.1
  k: 1.5
  base_size: 0.5

instrument: ETH-PERP
tick_interval: 10.0

max_position_qty: 5.0
max_notional_usd: 15000
max_order_size: 2.0
max_daily_drawdown_pct: 2.5

mainnet: false
dry_run: false
```

```bash
hl run avellaneda_mm --config my_config.yaml
```

## Install as a Claude Code Skill

```bash
git clone https://github.com/Nunchi-trade/agent-cli.git ~/agent-cli
cd ~/agent-cli && pip install -e .

mkdir -p ~/.claude/skills/yex-trader
cp ~/agent-cli/cli/skill.md ~/.claude/skills/yex-trader/SKILL.md
```

## Install as an OpenClaw Skill

```bash
git clone https://github.com/Nunchi-trade/agent-cli.git ~/agent-cli
cd ~/agent-cli && pip install -e .

clawhub install nunchi-trade/yex-trader
```

The skill uses the [Agent Skills](https://agentskills.io) open standard.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run specific test suites
pytest tests/test_trailing_stop.py -v     # DSL tests
pytest tests/test_scanner_engine.py -v    # Scanner tests
pytest tests/test_movers_engine.py -v     # Movers tests
pytest tests/test_wolf_engine.py -v       # WOLF tests
```

## License

MIT
