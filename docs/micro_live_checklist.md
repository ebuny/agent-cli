# Micro-Live Checklist (Hyperliquid)

Use this checklist to take the carry edge from paper to micro-live on a VPS.

**Prereqs**
- Ubuntu VPS with repo installed (example: `/opt/agent-cli` or `/root/agent-cli`).
- Python environment ready for `python -m cli.main`.
- System time synced (NTP).

**Secrets and Network**
- Export `HL_PRIVATE_KEY` or use `hl wallet import` for keystore.
- Set `HL_TESTNET=true` for testnet, `HL_TESTNET=false` for mainnet.
- If using `.env.mainnet`, source it in the systemd unit or shell.

**Data Paths**
- Ensure these directories exist and are writable.
- `data/research`
- `data/portfolio`
- `data/cli/carry`
- `data/wolf`

**Daily Research Refresh**
1. Install and enable the timer:
```bash
sudo cp deploy/systemd/research-refresh.service /etc/systemd/system/
sudo cp deploy/systemd/research-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now research-refresh.timer
systemctl status research-refresh.timer
```
2. Confirm it generates `data/research/live_allocation_plan.json`.

**Validate Live Plan (Feedback Enabled)**
```bash
python -m cli.main research allocate-live \
  --snapshot data/research/latest_allocation.json \
  --dataset data/research/native/datasets/<one_dataset>.jsonl \
  --feedback \
  --feedback-trades data/wolf/trades.jsonl
```
Confirm `Feedback:` appears and routing rules are non-zero for carry sources.

**Dry Run (Mock Data)**
```bash
PLAN_PATH=data/research/live_allocation_plan.json \
PORTFOLIO_PLAN=data/research/live_allocation_plan.json \
INSTRUMENT=ETH-PERP \
DRY_RUN=true \
scripts/run_carry_micro_live.sh
```
Watch logs for allocation blocks, routing blocks, and execution health gates.

**Paper Mode (Live Data, Simulated Fills)**
```bash
PLAN_PATH=data/research/live_allocation_plan.json \
PORTFOLIO_PLAN=data/research/live_allocation_plan.json \
INSTRUMENT=ETH-PERP \
PAPER=true \
scripts/run_carry_micro_live.sh
```

**Micro-Live Launch (Real Orders)**
```bash
PLAN_PATH=data/research/live_allocation_plan.json \
PORTFOLIO_PLAN=data/research/live_allocation_plan.json \
INSTRUMENT=ETH-PERP \
MAINNET=true \
scripts/run_carry_micro_live.sh
```

**Systemd Service**
```bash
sudo cp deploy/systemd/carry-micro-live.service /etc/systemd/system/
sudo cp deploy/systemd/carry-micro-live.env /etc/default/agent-cli-carry
sudo systemctl daemon-reload
sudo systemctl enable --now carry-micro-live.service
systemctl status carry-micro-live.service
```
If your install path is not `/opt/agent-cli`, edit `WorkingDirectory` and `PLAN_PATH` in the service file.
Edit `/etc/default/agent-cli-carry` to override plan paths or instrument.

**Optional: BTC Service (separate process)**
```bash
sudo cp deploy/systemd/carry-micro-live-btc.service /etc/systemd/system/
sudo cp deploy/systemd/carry-micro-live-btc.env /etc/default/agent-cli-carry-btc
sudo systemctl daemon-reload
sudo systemctl enable --now carry-micro-live-btc.service
systemctl status carry-micro-live-btc.service
```
BTC uses smaller sizing via `configs/carry_micro_live_btc/`.

**Monitoring**
- Check `data/cli/carry/*/trades.jsonl` for fills.
- Check `data/wolf/execution-health.jsonl` for kill-switch events.
- Confirm `data/research/live_allocation_plan.json` updates daily.
- Run HOWL summaries per strategy:
- `python -m cli.main howl run --data-dir data/cli/carry/funding_arb`
- `python -m cli.main howl run --data-dir data/cli/carry/basis_arb`
- Or run a single aggregate report:
- `python -m cli.main paper validate --data-dir data/cli/carry/funding_arb --data-dir data/cli/carry/basis_arb --data-dir data/cli/carry/hedge_agent`

**Rollback**
- Stop the service: `sudo systemctl stop carry-micro-live.service`
- Set `HL_TESTNET=true` and restart in dry run.
