# AutoTrade Core (Backend)

CoinDCX Futures intraday trading backend for personal use.

Runs on AWS EC2 (recommended) or locally:

- `bot_engine.py` = trader/writer (only process that can use CoinDCX keys)
- `observer_api.py` = read-only FastAPI observer API (no trading commands)

Expo UI runs separately in `autotrade-ui` on your laptop/phone and connects to the observer API.

## Current Local Folder Layout (Already Split)

Your machine now has:

- `C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-core` (this repo/folder)
- `C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-ui` (Expo UI folder)
- `C:\Users\yashs\OneDrive\Desktop\AutoTrade\.venv` (shared local Python venv, currently kept at parent level)

## Backend Repo Layout (`autotrade-core`)

```text
autotrade-core/
  .github/
    workflows/
      deploy.yml
  autotrade/
  data/                     # local runtime data (dev only)
  scripts/
    deploy.sh
  sql/
  tests/
  bot_engine.py
  observer_api.py
  Dockerfile
  docker-compose.yml
  config.yaml.example
  requirements.txt
  implementaion.md
  README.md
```

## What the Backend Writes (DATA_DIR)

By default the bot writes runtime artifacts into `./data` (or `DATA_DIR` if set):

- `latest_snapshot.json`
- `events.jsonl` (size-based rotation supported)
- `trades.sqlite`
- `state_resume.json`
- `command_queue.jsonl` (future phase, disabled by default)

## Runtime Settings (Env Overrides)

Supported env-backed runtime settings (`autotrade/settings.py`):

- `DATA_DIR` (default `./data`)
- `PORT` (default `8000`)
- `RUNTIME_DRY_RUN` (overrides `config.yaml` `runtime.dry_run`)
- `LOG_LEVEL` (default `INFO`)
- `MAX_EVENTS_MB` (default `128`)
- `MAX_EVENTS_ROTATIONS` (default `5`)
- `GIT_COMMIT` (for `/version`)
- `BUILD_TIME` (for `/version`)
- `AUTOTRADE_CONFIG` (config path inside Docker/container)

## Observer API Endpoints (Read-Only)

- `GET /health`
- `GET /version`
- `GET /snapshot`
- `GET /signals`
- `GET /positions`
- `GET /trades`
- `GET /metrics`
- `GET /diagnostics`
- `GET /events`
- `WS /stream`

Filtered endpoints:

- `/events?type=STAGE|ENTER|EXIT|CYCLE_ERROR|DIAG&symbol=&since=&limit=&offset=`
- `/trades?symbol=&from=&to=&limit=&offset=`

## Local Run (Current Machine, After Split)

### 1) Backend setup (one-time)

From the parent folder (where `.venv` lives):

```powershell
cd C:\Users\yashs\OneDrive\Desktop\AutoTrade
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r .\autotrade-core\requirements.txt
```

### 2) Create backend config (first time)

```powershell
cd C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-core
Copy-Item config.yaml.example config.yaml
```

### 3) Start bot engine (Terminal 1)

```powershell
cd C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-core
..\.venv\Scripts\Activate.ps1
python bot_engine.py --config config.yaml
```

### 4) Start observer API (Terminal 2)

```powershell
cd C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-core
..\.venv\Scripts\Activate.ps1
python observer_api.py --config config.yaml
```

### 5) Verify API (Terminal 2 or another shell)

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/version
Invoke-RestMethod http://localhost:8000/snapshot
Invoke-RestMethod http://localhost:8000/metrics
Invoke-RestMethod http://localhost:8000/diagnostics
```

### 6) Start UI (Terminal 3, local only)

```powershell
cd C:\Users\yashs\OneDrive\Desktop\AutoTrade\autotrade-ui
npm install
npm run start
```

Set UI API URL:

- same PC / web: `http://localhost:8000`
- Android emulator: `http://10.0.2.2:8000`
- phone on same LAN: `http://<your-pc-lan-ip>:8000`
- EC2 over Tailscale: `http://100.x.y.z:8000`

## Docker / Compose (Backend only)

Compose services:

- `bot` (trader/writer)
- `api` (read-only observer API)

Persistence and secrets are host-mounted:

- data volume: `/var/lib/autotrade/data:/var/lib/autotrade/data`
- env file: `/etc/autotrade.env`
- config file mount: `/opt/autotrade/config.yaml:/app/config.yaml:ro`

## AWS EC2 + Tailscale (Recommended)

### Security Rules

- `observer_api` stays read-only
- `bot_engine` is the only process that uses CoinDCX keys
- Do not expose port `8000` publicly (`0.0.0.0/0`)
- Access API over Tailscale private network

### Security Group (Recommended)

- Allow `22/tcp` only from your home/public IP
- Do **not** allow public inbound `8000/tcp`

### Tailscale API Access

On EC2:

```bash
tailscale ip -4
```

Use the `100.x.y.z` IP in UI:

- `http://100.x.y.z:8000`

## AWS Setup Checklist

1. Create EC2 Ubuntu instance (small size is fine for personal use)
2. Install `docker`, `docker compose`, `git`, `curl`
3. Create:
   - `/opt/autotrade`
   - `/var/lib/autotrade/data`
   - `/etc/autotrade.env`
4. Clone backend repo to `/opt/autotrade`
5. Copy and edit config:
   - `/opt/autotrade/config.yaml`
6. Add secrets + runtime env to `/etc/autotrade.env`
7. Run once:
   - `docker compose up -d --build`
8. Install Tailscale on EC2 and your device
9. Set UI API URL to EC2 Tailscale IP
10. Add GitHub secrets and test auto deploy by pushing to `master`

## Auto Deploy on Push to `master`

Workflow file:

- `.github/workflows/deploy.yml`

Remote script run on EC2:

- `/opt/autotrade/scripts/deploy.sh`

Deploy script flow:

1. `git fetch --all --prune`
2. `git reset --hard origin/master`
3. export `GIT_COMMIT` + `BUILD_TIME`
4. `docker compose up -d --build`
5. `docker image prune -f`
6. validate `http://localhost:8000/health`
7. validate `http://localhost:8000/version`

Required GitHub secrets:

- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

## Latest Implementation Highlights

- Restart-safe runtime continuity via `state_resume.json`
- Daily realized PnL / Daily R rebuilt from `trades.sqlite` on startup
- Best-effort live position reconciliation from CoinDCX open positions
- New observer endpoints: `/metrics`, `/diagnostics`, `/version`
- WebSocket heartbeat from observer API
- `events.jsonl` size-based rotation (`MAX_EVENTS_MB`)
- Dockerized backend + EC2 deploy script + GitHub Actions SSH deploy

