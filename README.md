# solar-ema-monitor

Local data store, REST API, and dashboard for APsystems solar inverter systems.
Pulls data from the APsystems EMA OpenAPI, stores it in SQLite, and serves a
live dashboard over your LAN.

## Features

- **SQLite database** with 5-min system power, daily energy, per-panel readings,
  full inverter telemetry (DC/AC power, voltage, current, frequency, temperature),
  billing, and finance data
- **Daily sync** from the EMA API via systemd timer
- **REST API** (Flask) for dashboards and integrations
- **Live dashboard** with intraday power curves, per-panel overlay, heatmaps,
  billing/finance charts, and a panel configuration editor
- **Panel metadata** — assign arrays, positions, tilt/azimuth, model, capacity
- Designed for **Raspberry Pi** deployment

## Architecture

```
 ┌──────────────┐      ┌──────────────────┐      ┌──────────────┐
 │  APsystems   │      │  Local XLS/CSV   │      │   systemd    │
 │  EMA Cloud   │      │  (backfill data) │      │              │
 └──────┬───────┘      └────────┬─────────┘      │  solar-sync  │
        │  HTTPS                │                 │    .timer    │
        ▼                       ▼                 │ (daily 22:00)│
 ┌──────────────┐      ┌──────────────────┐      └──────┬───────┘
 │ema_api_pull  │◄────▶│   solar_sync.py  │◄────────────┘
 │    .py       │      │                  │    triggers --sync
 │              │      │  --backfill      │
 │  API client  │      │  --sync          │
 │  (HMAC auth) │      │  --import-*      │
 └──────────────┘      └────────┬─────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │    solar.db      │
                       │    (SQLite)      │
                       │                  │
                       │  8 data tables   │
                       │  + sync_log      │
                       └────────┬─────────┘
                                │
                                ▼
                       ┌──────────────────┐      ┌──────────────┐
                       │  solar_api.py    │      │   systemd    │
                       │  (Flask :8080)   │      │  solar-api   │
                       │                  │      │   .service   │
                       │  15 REST         │◄─────┤              │
                       │  endpoints       │      └──────────────┘
                       └────────┬─────────┘
                                │
                      ┌─────────┼─────────┐
                      ▼         ▼         ▼
               ┌──────────┐ ┌──────┐ ┌────────┐
               │Dashboard │ │Grafana│ │ Custom │
               │(built-in)│ │ etc. │ │ client │
               └──────────┘ └──────┘ └────────┘
```

**Data flow:**

1. **EMA API → DB** — `solar_sync.py --sync` calls `ema_api_pull.py` to fetch
   today's power curves, daily energy, per-panel data, and inverter telemetry,
   then upserts everything into `solar.db`.
2. **Local files → DB** — `solar_sync.py --backfill` parses historical XLS/CSV
   exports (power curves, daily energy, panel data, billing, finance) and loads
   them into the same database.
3. **DB → API → Clients** — `solar_api.py` reads from `solar.db` and serves
   JSON endpoints + the built-in dashboard over HTTP.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_USER/solar-ema-monitor.git
cd solar-ema-monitor

# 2. Install Python dependencies
pip3 install -r requirements.txt

# 3. Configure
cp .env.template .env
nano .env   # fill in your EMA API credentials and system IDs

# 4. Install systemd services and start
bash deploy/setup.sh
```

The dashboard is now at `http://<pi-ip>:8080/`.

## Configuration

Copy `.env.template` to `.env` and fill in:

| Variable | Required | Description |
|---|---|---|
| `EMA_APP_ID` | Yes | APsystems OpenAPI App ID |
| `EMA_APP_SECRET` | Yes | APsystems OpenAPI App Secret |
| `EMA_SYSTEM_ID` | Yes | Your system ID (from EMA portal) |
| `EMA_ECU_ID` | Yes | Your ECU ID (from inverter serial) |
| `SOLAR_DATA_DIR` | No | Path to XLS/CSV files for backfill |

To get API credentials, email APsystems support requesting OpenAPI access.

## Backfill Data Sources (Optional)

Historical data can be imported from local XLS/CSV files using `--backfill` or
the `--import-*` flags. Set `SOLAR_DATA_DIR` in `.env` to point at the directory
containing these files (defaults to the project directory).

### Power Curve XLS

**Path:** `$SOLAR_DATA_DIR/daily_prod_curves/*.xls`

One file per day. The date is extracted from the filename (must contain
`YYYY-MM-DD`). Exported from the EMA portal's "Detail Daily Energy Report."

| Column | Type | Description |
|---|---|---|
| `time` | `HH:MM` | 5-minute timestamp |
| `Power(W)` | number | System power in watts |

Example filename: `Power Curve for System in 2025-01-15.xls`

### Daily Energy XLS

**Path:** `$SOLAR_DATA_DIR/Daily Energy Report*.xls`

One or more files covering date ranges. Exported from the EMA portal.

| Column | Type | Description |
|---|---|---|
| `Date` | `YYYY-MM-DD` | Day |
| `Energy(kWh)` | number | Daily production |

### Panel Data CSV

**Path:** `$SOLAR_DATA_DIR/panel_data/panels_YYYY-MM-DD.csv`

One file per day with per-channel (panel) power readings.

| Column | Type | Description |
|---|---|---|
| `time` | `HH:MM` | 5-minute timestamp |
| `<uid>-<channel>` | number | Power (W) for each inverter UID + channel pair |
| `total` | number | Sum across all channels |

Example: columns might be `time`, `800000000001-1`, `800000000001-2`, …, `total`.

### Billing CSV

**Path:** `$SOLAR_DATA_DIR/monthly_billed_usage.csv`
(or pass any path with `--import-billing FILE`)

| Column | Type | Description |
|---|---|---|
| `meter date` | `MM/DD/YYYY` | Billing period end date |
| `energy consumed` | number | Grid consumption (kWh) |
| `energy produced` | number | Solar export (kWh) |
| `actual electric bill` | number | Bill amount ($) |
| `est bill without solar` | number | Estimated bill without solar ($) |

### Finance CSV

**Path:** `$SOLAR_DATA_DIR/finance_data.csv`
(or pass any path with `--import-finance FILE`)

| Column | Type | Description |
|---|---|---|
| `date` | `Month YYYY` | Period (e.g. "March 2026") |
| `heloc balance` | number | Loan balance ($) |
| `interest` | number | Interest paid that month ($) |

## Usage

```bash
# Manual sync
python3 solar_sync.py --sync

# Backfill from local XLS/CSV files
python3 solar_sync.py --backfill

# Import billing/finance CSVs
python3 solar_sync.py --import-billing monthly_billed_usage.csv
python3 solar_sync.py --import-finance finance_data.csv

# Check database status
python3 solar_sync.py --status

# Run API server manually
python3 solar_api.py --port 8080
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard HTML |
| `GET /api/status` | DB summary and sync log |
| `GET /api/system_readings?start=&end=` | 5-min system power (W) |
| `GET /api/daily_energy?start=&end=` | Daily kWh totals |
| `GET /api/panel_readings?start=&end=&uid=` | Per-channel 5-min power |
| `GET /api/panel_daily?start=&end=` | Per-channel daily kWh |
| `GET /api/panel_summary?start=&end=` | Per-channel totals and best hour |
| `GET /api/panel_wide?date=` | Wide-format panel data (one col per panel) |
| `GET /api/inverter_telemetry?start=&end=&uid=` | Full inverter telemetry |
| `GET /api/billing` | Utility billing periods |
| `GET /api/finance` | HELOC balance history |
| `GET /api/inverters` | Inverter registry |
| `GET /api/panels` | Panel metadata/config |
| `PUT /api/panels` | Update panel metadata |
| `GET /api/system_summary` | Live EMA summary (today/month/year/lifetime) |

## Service Management

```bash
# Check status
sudo systemctl status solar-api
sudo systemctl status solar-sync.timer

# View logs
journalctl -u solar-api -f
journalctl -u solar-sync --since today

# Restart
sudo systemctl restart solar-api

# Trigger sync manually
sudo systemctl start solar-sync
```

## Project Structure

```
solar-ema-monitor/
├── ema_api_pull.py        # APsystems EMA OpenAPI client
├── solar_db.py            # SQLite schema and ORM
├── solar_sync.py          # Backfill + incremental API sync
├── solar_api.py           # Flask REST API server
├── solar_dashboard.html   # Live dashboard (served by API)
├── requirements.txt       # Python dependencies
├── .env.template          # Configuration template
├── .gitignore
├── README.md
└── deploy/
    ├── setup.sh           # One-command install script
    ├── solar-api.service  # systemd unit (API server)
    ├── solar-sync.service # systemd unit (sync oneshot)
    └── solar-sync.timer   # systemd timer (daily at 10pm)
```

## EMA API Rate Limits

The APsystems EMA API allows 1,000 calls/month. A daily sync uses ~15 calls,
totaling ~450/month — well within limits.

## License

MIT
