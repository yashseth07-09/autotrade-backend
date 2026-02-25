# AutoTrade - Latest Implementation Summary (Split Backend/UI)

## 1) Current Folder Split (Local Machine)

The project is now split into two folders inside your `AutoTrade` parent directory:

- `AutoTrade\autotrade-core` -> backend only
- `AutoTrade\autotrade-ui` -> Expo UI only

The parent-level Python venv was intentionally kept at:

- `AutoTrade\.venv`

## 2) Runtime Architecture (Live System Design)

### Backend (`autotrade-core`)

1. `bot_engine.py` (writer / trader)
- Only process that talks to CoinDCX
- Runs the strategy (4H / 15m / 5m)
- Applies risk rules
- Executes or simulates trades
- Writes runtime state and analytics artifacts

2. `observer_api.py` (read-only FastAPI)
- Reads bot outputs only (JSON / JSONL / SQLite)
- Exposes REST endpoints and WebSocket stream
- Has no exchange keys
- Must never control the bot

### UI (`autotrade-ui`)

- Expo mobile/web observer dashboard
- Runs locally on laptop/phone
- Connects to `observer_api.py`
- Read-only monitoring layer

## 3) Data Flow (End-to-End)

1. `bot_engine.py` scans CoinDCX futures watchlist
2. Strategy evaluates 4H bias -> 15m setup/signal -> 5m execution/management
3. Bot writes to `DATA_DIR`
4. `observer_api.py` reads from `DATA_DIR`
5. Expo UI renders data from observer API

## 4) Backend Persistence (DATA_DIR)

Backend now uses a standardized `DATA_DIR` (default `./data`, env-overridable).

Persisted files:

- `latest_snapshot.json`
- `events.jsonl`
- `trades.sqlite`
- `state_resume.json`
- `command_queue.jsonl` (future phase, disabled by default)

Production (EC2) recommendation:

- Mount `DATA_DIR` to `/var/lib/autotrade/data`

## 5) Restart Continuity (Implemented)

### What now survives bot restarts better

- Daily realized PnL (rebuilt from `trades.sqlite`)
- Daily R (rebuilt from `trades.sqlite`)
- Risk session continuity (daily loss / consecutive loss context, cooldown timing best-effort)
- Top reject reasons used by UI empty states

### Runtime state persistence

- Bot writes `state_resume.json` periodically
- Includes:
  - open positions
  - management context
  - risk state
  - cycle timing / last cycle result
  - top candidates / recent rejects

### Startup recovery behavior

- `dry_run=true`: restore open positions from `state_resume.json`
- `dry_run=false`: query CoinDCX open positions and reconcile best-effort
  - imports exchange positions not in state file
  - marks file positions closed if missing on exchange (with recovery events)

## 6) Strategy / Risk (Current Behavior)

### Strategy

- `4H` bias (EMA20/EMA50 + structure)
- `15m` entry engine (breakout / pullback continuation)
- `5m` execution gate + position management

### Risk

- Small risk per trade (config-driven)
- Daily loss cap (R-based)
- Cooldown after consecutive losses
- Default leverage configurable (3x default)

### Explainability

Bot emits structured stage logs (`events.jsonl`) with:

- `stage`, `rule`, `expected`, `actual`, `passed`, `message`, `delta`, `symbol`, `ts`

Additional diagnostic/recovery events implemented:

- `DIAG_HTTP_ERROR`
- `DIAG_DATA_STALE`
- `RECOVERY_RECONCILE`

## 7) Observer API (Read-Only) - Current Endpoints

Core:

- `/health`
- `/version`
- `/snapshot`
- `/signals`
- `/positions`
- `/trades`
- `/metrics`
- `/diagnostics`
- `/events`
- `WS /stream`

### API improvements implemented

- `/version` returns build metadata (`GIT_COMMIT`, `BUILD_TIME`, `DATA_DIR`, `dry_run`)
- `/metrics` aggregates daily metrics from SQLite + snapshot context
- `/diagnostics` reports snapshot/file ages, cycle timing, WS clients, last HTTP error
- `/events` and `/trades` support filtering + pagination
- `/stream` emits heartbeat messages (snapshot age + cycle ms)
- Snapshot reads use a short cache TTL to reduce disk thrash

## 8) UI (`autotrade-ui`) - Current Status

UI remains local-only and already supports:

- connection settings panel (change API URL at runtime)
- connection test + latency
- status ribbon (API/WS/snapshot age/cycle ms/mode/risk/BTC bias)
- KPI dashboard cards
- Movers / Signals / Positions / Trades / Events / Health / Config tabs
- Events viewer with filters, severity chips, grouping, and detail modal
- actionable empty states using `snapshot.top_rejects`

API base URL is configurable and suitable for Tailscale IP usage.

## 9) Docker + EC2 Deployment (Backend only)

### Added backend ops files

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `scripts/deploy.sh`
- `.github/workflows/deploy.yml`

### Compose design

Services:

- `bot`
- `api`

Persistence:

- `/var/lib/autotrade/data` mounted into both containers

Secrets:

- `/etc/autotrade.env` on EC2 (not committed to git, not baked into image)

### Ops hardening included

- Docker log caps (`max-size`, `max-file`)
- `events.jsonl` size-based rotation (`MAX_EVENTS_MB`, `MAX_EVENTS_ROTATIONS`)
- deploy script validates `/health` and `/version`

## 10) Private API Access via Tailscale (Recommended)

Design intent:

- Do not expose port `8000` publicly
- Use Tailscale between your local UI device and EC2

Security group guidance:

- allow SSH (`22`) from your home IP only
- no public inbound `8000`

UI should use:

- `http://100.x.y.z:8000` (EC2 Tailscale IP)

## 11) Known Limitations / Remaining Gaps

1. Live CoinDCX position reconciliation is best-effort
- Exact field mappings may vary by account/API payload shape

2. UI persistence of API URL
- Web `localStorage` is implemented
- Native persistence fallback is currently in-memory in this workspace (AsyncStorage not added)

3. Bot runtime recovery is improved but not perfect
- Some advanced position management context may still be reconstructed heuristically

## 12) Practical Run Model

- Keep `bot_engine.py` running continuously
- Restart `observer_api.py` anytime
- Restart `autotrade-ui` anytime

Backend bot remains the only trading process; API/UI are read-only observers.

