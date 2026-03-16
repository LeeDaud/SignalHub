# SignalHub

SignalHub is a local monitoring console for Virtuals projects.

The current project focuses on:

- tracking upcoming launches
- identifying `TA` and `PA / internal market address`
- reviewing projects in a dashboard grouped by launch phase

## Features

- FastAPI backend with SQLite storage
- dashboard at `GET /dashboard`
- auto/manual scan control
- project grouping:
  - `Pending Launch`
  - `Launch Window`
  - `External Market`
- launch-window price and FDV enrichment
- recent events and per-project analysis
- bot feeds:
  - `GET /bot/feed/unified`
  - `GET /bot/feed/upcoming`
  - `GET /bot/feed/internal-markets`
  - `GET /bot/feed/token-pools`
  - `GET /bot/feed/events`

## Launch Phase Rules

SignalHub uses `launch_time` as the phase anchor.

- `Pending Launch`: `launch_time > now`
- `Launch Window`: `launch_time <= now < launch_time + 100 minutes`
- `External Market`: `now >= launch_time + 100 minutes`

## Project Layout

- `signalhub/app/`: backend code
- `signalhub/ui/dashboard/index.html`: frontend dashboard
- `run_local.py`: local entry point
- `start_service.ps1`: start background service
- `stop_service.ps1`: stop background service
- `restart_service.ps1`: restart background service
- `.env.example`: local environment template

## Requirements

- Python 3.11+
- Windows PowerShell for service scripts
- access to the Virtuals API
- optional Base RPC access for on-chain enrichment

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Create a local environment file:

```powershell
Copy-Item .env.example .env
```

Recommended core variables:

```text
APP_NAME=SignalHub
SIGNALHUB_DB_PATH=signalhub.db
DASHBOARD_PATH=signalhub/ui/dashboard/index.html
TOKEN_POOL_EXPORT_PATH=exports/token-pools.json

SOURCE_ENABLED=true
POLL_INTERVAL_SECONDS=30
REQUEST_TIMEOUT_SECONDS=15

VIRTUALS_ENDPOINT=https://api2.virtuals.io/api/virtuals
VIRTUALS_APP_BASE_URL=https://app.virtuals.io
VIRTUALS_MODE=upcoming_launches
VIRTUALS_SAMPLE_MODE=false

CHAINSTACK_BASE_HTTPS_URL=
CHAINSTACK_BASE_WSS_URL=
CHAINSTACK_SUBSCRIPTION_ENABLED=true
```

## Run

Foreground:

```bash
python run_local.py
```

Background service:

```powershell
.\start_service.ps1 -Port 8000
.\stop_service.ps1 -Port 8000
.\restart_service.ps1 -Port 8000
```

Open the dashboard:

```text
http://127.0.0.1:8000/dashboard
```

## Useful Endpoints

- `GET /healthz`
- `GET /system/status`
- `GET /dashboard`
- `GET /projects`
- `GET /projects/{project_id}/analysis`
- `GET /bot/feed/unified`
- `GET /exports/token-pools.json`

## Notes

- Launch-window price and FDV are best-effort realtime enrichments.
- If RPC enrichment is slow or unavailable, the dashboard still loads core project data first.
- Runtime caches, logs, exports, and local temp folders are ignored by Git.
