# Plan: My Solar Toolbox — Open-Source Solar Lifecycle Platform

## TL;DR
**My Solar Toolbox** is a collection of open-source tools that grow with you through your entire solar journey: **Plan → Install → Operate**. Each phase is a separate tool/app that shares data via a common API and database. The core data platform (adapters → DB → API) is vendor-agnostic, Docker-first, and runs on a Pi or any cloud. Individual tools are standalone PWAs or web apps that connect to the platform API.

### The Lifecycle

| Phase | Tools | Status |
|---|---|---|
| **Plan** | Shade analysis (SolarScope), production estimator, ROI/financial modeling, system sizing | SolarScope exists; ROI calculator exists; others new |
| **Install** | String sizing calculator, wiring diagram generator, loss waterfall, BOM builder, commissioning validator | All new |
| **Operate** | Live monitoring, per-panel tracking, clipping analysis, degradation detection, billing tracking, goal tracking | Mostly exists (current project) |

### Architecture: Separate Tools, Shared Data
- Each tool is its own app/PWA (can be used standalone or together)
- All tools share data via the My Solar Toolbox API
- Planning tools produce data (array config, panel specs, expected production) that installation tools consume
- Installation tools produce data (string wiring, commissioning baselines) that operation tools consume
- The API + database is the central hub; tools are clients

## Decisions & Assumptions
- **Name**: My Solar Toolbox (lifecycle toolbox that grows with you — "my tools, my data, my server")
- **Target audience**: Broader DIY solar homeowners (→ Docker deploy, setup wizard, clear docs)
- **Tool architecture**: Separate tools that share data via the API (not one monolithic PWA)
- **Adapter scope**: Ship APsystems adapter; define interface with Enphase, SolarEdge, generic CSV, battery/hybrid in mind
- **EZ1 & VUE**: Stay as optional adapter plugins within the adapter framework
- **Dashboards/tools**: PWA-capable, responsive for phone/tablet/TV. API-only data access (no direct SQLite)
- **Deployment**: Docker-first. Local Pi for free/self-hosted, any cloud (AWS/GCP/DO) for run-anywhere. Systemd option preserved.
- **Breaking changes**: Clean v2 with migration script for existing v1 data
- **Battery/hybrid systems**: Schema designed from day one even if adapters come later
- **Related projects**: SolarScope (shade_vis/) stays as separate repo; solar_vis production analysis integrates via API
- **Monetization**: Open core; optional paid hosted tier possible later
- **Repo structure**: `my-solar-toolbox` (core platform + operate tools), companion tool repos for plan/install phase apps

---

## Phase 1: Project Structure & Adapter Framework

### Step 1.1 — New directory layout
```
my-solar-toolbox/
├── adapters/
│   ├── __init__.py           # adapter registry & discovery
│   ├── base.py               # DataSourceAdapter ABC
│   ├── apsystems.py          # wraps existing ema_api_pull.py logic
│   ├── apsystems_ez1.py      # wraps existing ez1_logger.py logic
│   ├── emporia_vue.py        # wraps existing vue_ingest.py logic
│   ├── csv_generic.py        # generic CSV/XLS import adapter
│   └── solcast.py            # weather/irradiance forecast adapter
├── core/
│   ├── __init__.py
│   ├── db.py                 # vendor-agnostic schema (evolved from solar_db.py)
│   ├── api.py                # Flask REST API (evolved from solar_api.py)
│   └── sync.py               # orchestrator (evolved from solar_sync.py)
├── app/                      # PWA frontend
│   ├── index.html            # app shell / navigation
│   ├── manifest.json         # PWA manifest
│   ├── sw.js                 # service worker
│   ├── icons/                # app icons (192, 512, 180 apple-touch)
│   ├── css/
│   │   └── style.css         # unified stylesheet (existing + responsive TV)
│   └── pages/
│       ├── dashboard.html    # primary monitoring dashboard
│       ├── clipping.html     # clipping analysis tool
│       ├── roi.html          # ROI/payback calculator
│       ├── field_log.html    # irradiance field logger
│       ├── ez1.html          # EZ1 micro-inverter
│       └── vue_explorer.html # VUE data explorer
├── config/
│   ├── config.yaml.template  # replaces .env for structured config
│   └── config.py             # config loader (reads YAML, falls back to .env)
├── deploy/
│   ├── Dockerfile            # Docker deployment (primary)
│   ├── docker-compose.yml    # one-command deploy
│   └── systemd/              # preserved for bare-metal Pi users
├── migrate/
│   └── v1_to_v2.py           # data migration script
├── requirements.txt
├── README.md
└── LICENSE
```

### Step 1.2 — Adapter base class (`adapters/base.py`)
Define `DataSourceAdapter` ABC with:
- `name: str` — unique adapter identifier
- `required_config: list[str]` — config keys this adapter needs
- `validate_config(config) -> bool` — check credentials/connectivity
- `capabilities() -> set[str]` — what data types: 'system_power', 'device_readings', 'battery', 'consumption', 'weather', 'daily_energy'
- `fetch_system_power(start, end) -> list[dict]` — optional, returns [{timestamp, power_w}]
- `fetch_device_readings(start, end) -> list[dict]` — optional, returns [{timestamp, device_id, channel, power_w, ...}]
- `fetch_battery(start, end) -> list[dict]` — optional
- `fetch_consumption(start, end) -> list[dict]` — optional
- `fetch_daily_energy(start, end) -> list[dict]` — optional
- `fetch_weather(start, end) -> list[dict]` — optional

Each method raises `NotImplementedError` by default; adapters override only what they support. The `capabilities()` method tells the sync orchestrator which fetch methods to call.

### Step 1.3 — Adapter registry (`adapters/__init__.py`)
- `discover_adapters()` — scan adapters/ for classes inheriting `DataSourceAdapter`
- `load_enabled_adapters(config) -> list[DataSourceAdapter]` — instantiate only adapters enabled in config
- No pkg_resources/entry_points — simple importlib-based directory scan

### Step 1.4 — Config system (`config/config.py`)
- Primary: `config.yaml` (structured, per-adapter config blocks)
- Fallback: `.env` (backward compat for v1 users)
- Schema:
  ```yaml
  server:
    port: 8080
    host: 0.0.0.0
  database:
    path: solar.db
  timezone: auto             # IANA name, or 'auto' to infer from location
  location:                  # optional, enables irradiance models
    lat: 0.00
    lon: 0.00
    alt_m: 0
  adapters:
    apsystems:
      enabled: true
      app_id: "..."
      app_secret: "..."
      system_id: "..."
      ecu_id: "..."
    emporia_vue:
      enabled: true
      data_dir: "/path/to/vue/exports"
    apsystems_ez1:
      enabled: false
      ip: "192.168.1.100"
      port: 8050
    csv_import:
      enabled: true
      import_dir: "/path/to/csv/files"
      column_mapping: auto  # or explicit mapping
  ```

---

## Phase 2: Database Schema Evolution

### Step 2.1 — Generalize existing tables
Current tables that are already vendor-agnostic (keep as-is, minor additions):
- `system_readings` (timestamp, power_w, source) — add `adapter_name` column
- `daily_energy` (date, energy_kwh, source) — add `exported_kwh` for net metering
- `inverters` → rename to `devices` — add `device_type` ('microinverter', 'optimizer', 'string_inverter', 'battery', 'meter'), `adapter_name`, `brand`, `model`
- `inverter_telemetry` → rename to `device_telemetry` — generalize column names (already generic: dc_p, dc_v, ac_p, ac_v, temperature)

### Step 2.2 — New tables for battery, consumption & energy flow
- `energy_flow` (timestamp, grid_import_w, grid_export_w, solar_production_w, battery_charge_w, battery_discharge_w, home_consumption_w, source) — the fundamental energy balance equation at system level. Every adapter contributes what it knows; nulls for unknowns. Derived fields can be computed: `home_consumption = solar + grid_import - grid_export - battery_charge + battery_discharge`
- `battery_readings` (timestamp, device_id, soc_pct, power_w [neg=charging], voltage_v, temperature_c, source)
- `battery_daily` (date, device_id, charge_kwh, discharge_kwh, cycles, min_soc, max_soc)
- `consumption_readings` (timestamp, device_id, circuit_name, power_w, source)
- `consumption_daily` (date, device_id, circuit_name, energy_kwh)

### Step 2.3 — Keep panel/array/slot/string config tables
These are already generic (physical layout metadata). No changes needed.

### Step 2.4 — Unified weather table
Merge `weather_daily` + `solcast_estimates` concepts into:
- `weather_readings` (timestamp, source, type, granularity, temp_c, irradiance_wm2, ghi_wm2, humidity_pct, cloud_cover_pct, precip_mm, pressure_hpa)
- `irradiance_estimates` (timestamp, source, type ['forecast','actual'], pv_estimate_w, pv_estimate10, pv_estimate90)

### Step 2.5 — Migration script (`migrate/v1_to_v2.py`)
- Auto-detect v1 database (check table names)
- Rename tables: `inverters`→`devices`, `inverter_telemetry`→`device_telemetry`, `panel_readings`→`device_readings`
- Add new columns with defaults (`adapter_name`='apsystems', `device_type`='microinverter')
- Create new empty tables (battery, consumption)
- Migrate vue_energy.db into main solar.db consumption tables
- Migrate ez1.db into main solar.db device tables
- Preserve all data; create backup of v1 DB first
- Idempotent — safe to run multiple times

---

## Phase 3: API Layer Cleanup

### Step 3.1 — Versioned API routes
- All new endpoints under `/api/v2/`
- Keep `/api/` endpoints as v1 aliases (backward compat for existing dashboards during transition)

### Step 3.2 — New/modified endpoints
Existing endpoints (keep, route through v2 internally):
- `/api/v2/system_readings` — unchanged
- `/api/v2/daily_energy` — add `exported_kwh` field
- `/api/v2/devices` — formerly `/api/inverters`, now includes all device types
- `/api/v2/device_telemetry` — formerly `/api/inverter_telemetry`
- `/api/v2/device_readings` — formerly `/api/panel_readings`
- `/api/v2/billing`, `/api/v2/finance` — unchanged

New endpoints:
- `/api/v2/battery` — battery SoC and power time series
- `/api/v2/consumption` — whole-home and circuit-level consumption
- `/api/v2/weather` — unified weather data
- `/api/v2/adapters` — list active adapters and their status/capabilities
- `/api/v2/config` — read-only config summary (no secrets)

### Step 3.3 — Dashboard isolation
- Dashboards MUST NOT import `solar_db` or access SQLite directly
- Dashboards fetch all data via `/api/v2/*` endpoints
- API serves dashboard HTML files from `dashboards/` directory

---

## Phase 4: Adapter Implementations

### Step 4.1 — APsystems adapter (`adapters/apsystems.py`)
Refactor `ema_api_pull.py` into adapter class. Implements:
- `fetch_system_power()` — existing power curve logic
- `fetch_device_readings()` — existing per-panel batch logic
- `fetch_daily_energy()` — existing monthly summary logic
- `fetch_device_telemetry()` — existing per-inverter telemetry

### Step 4.2 — APsystems EZ1 adapter (`adapters/apsystems_ez1.py`)
Refactor `ez1_logger.py` into adapter class. Implements:
- `fetch_device_readings()` — poll local REST endpoint

### Step 4.3 — Emporia VUE adapter (`adapters/emporia_vue.py`)
Refactor `vue_ingest.py` into adapter class. Implements:
- `fetch_consumption()` — parse CSV exports into circuit-level readings
- `fetch_system_power()` — derive from solar combiner panel mains

### Step 4.4 — Generic CSV adapter (`adapters/csv_generic.py`)
For users who export data from any vendor portal. Implements:
- All fetch methods based on column mapping configuration
- Auto-detect column semantics where possible (timestamp, power, energy)
- User-configurable column mapping in config.yaml

### Step 4.5 — Solcast adapter (`adapters/solcast.py`)
Refactor existing Solcast fetch logic. Implements:
- `fetch_weather()` — irradiance forecasts and actuals

### Step 4.6 — File backfill support
Each adapter can also implement:
- `backfill(file_path_or_dir) -> list[dict]` — parse historical exports
- APsystems adapter handles XLS backfill (existing logic)
- VUE adapter handles CSV export backfill (existing logic)
- Generic CSV handles arbitrary files

---

## Phase 5: Sync Orchestrator

### Step 5.1 — Refactor `solar_sync.py` → `core/sync.py`
```
for adapter in load_enabled_adapters(config):
    for capability in adapter.capabilities():
        data = adapter.fetch_{capability}(start, end)
        db.upsert(capability_table, data)
    db.update_sync_log(adapter.name, ...)
```

### Step 5.2 — CLI interface
- `solar-sync --sync` — run all enabled adapters
- `solar-sync --sync --adapter apsystems` — run specific adapter
- `solar-sync --backfill --adapter apsystems --dir /path/` — backfill from files
- `solar-sync --status` — show all adapters, last sync, record counts

---

## Phase 6: PWA Dashboard Layer

### Step 6.1 — PWA shell (`app/`)
- `manifest.json` — name: "My Solar Toolbox", display: "standalone", theme_color matching dark theme, icons at 192/512/180px
- `sw.js` — network-first for API calls (cache last response for offline), cache-first for static assets (HTML, CSS, Chart.js CDN)
- `index.html` — app shell with navigation (sidebar or bottom tabs for mobile), loads pages via client-side routing or simple links
- Apple/Android meta tags in all page heads: `apple-mobile-web-app-capable`, `theme-color`, `apple-touch-icon`

### Step 6.2 — Responsive CSS for phone/tablet/TV
- Extend existing custom CSS (already lightweight, ~80 lines)
- Add media queries for large displays:
  - `@media (min-width: 1600px)` — wider grid, larger font
  - `@media (min-width: 2400px)` — Apple TV / signage display sizing
- No CSS framework needed — keep current approach, it's already lighter than Bootstrap
- Consider Pico CSS (7KB) only if starting fresh pages

### Step 6.3 — Update dashboards to use `/api/v2/` endpoints only
- Remove any direct DB access patterns
- Update fetch URLs from `/api/panel_readings` → `/api/v2/device_readings`, etc.

### Step 6.4 — Adapter-aware dashboard features
- Main dashboard auto-detects available data via `/api/v2/adapters` (capabilities list)
- Show/hide sections: battery panel appears only if battery adapter active, consumption breakdown only if consumption adapter active
- New pages (future): battery dashboard, consumption breakdown, energy flow Sankey diagram

---

## Phase 7: Deployment & Docker-First Strategy

### Step 7.1 — Dockerfile (primary deployment method)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
VOLUME ["/data"]  # solar.db + config.yaml live here
CMD ["python", "-m", "core.api", "--port", "8080"]
```

### Step 7.2 — docker-compose.yml
```yaml
services:
  my-solar-toolbox:
    build: .
    ports: ["8080:8080"]
    volumes:
      - mst-data:/data
      - ./config.yaml:/app/config/config.yaml
    restart: unless-stopped
    environment:
      - TZ=UTC
volumes:
  mst-data:
```
- One-command deploy: `docker compose up -d`
- Works on Pi, any Linux, AWS/GCP/DO, Synology NAS
- Data persists in named volume (survives container rebuilds)

### Step 7.3 — Sync as scheduled container job
- `solar-sync` runs as a cron job inside the container, or as a separate one-shot container triggered by host cron / cloud scheduler
- Docker healthcheck endpoint at `/api/v2/health`

### Step 7.4 — Preserve systemd option
- Keep `deploy/systemd/` for bare-metal Pi users who don't want Docker
- Update service files to reference new directory structure
- `setup.sh` generates config.yaml instead of .env

### Step 7.5 — Cloud deployment docs
- README section: "Deploy to AWS" (Lightsail one-click, or EC2 + docker compose)
- README section: "Deploy to any VPS" (DigitalOcean, Hetzner, etc.)
- Key point: same Docker image everywhere, just mount your config + data volume

---

## Phase 8: Companion Tool Integration & Lifecycle Tools

### Planning Phase Tools (separate repos, API-connected)

#### 8.1 — SolarScope (shade_vis/ — exists)
- Insta360-based shade analysis → SAV, TOF, TSRF per panel position
- Can POST results to My Solar Toolbox API → stored as panel/slot metadata
- My Solar Toolbox dashboards display shade metrics alongside production data

#### 8.2 — Production Estimator (new tool, future)
- Input: array config (from My Solar Toolbox API), location, shade data (from SolarScope)
- Uses pvlib or clear-sky models to estimate annual/monthly production
- Compares estimates to actuals once system is operational (closes the loop)
- Could be a page within the main app or standalone

#### 8.3 — Financial Modeler (exists as roi.html, evolve)
- Already calculates ROI/HELOC payback from actual billing data
- Extend for pre-purchase: "given X system size, Y cost, Z financing, what's my payback?"
- Pre-purchase estimates → actual tracking once operational

### Installation Phase Tools (new, separate app or pages)

#### 8.4 — String Sizing Calculator
- Input: panel specs (Voc, Vmp, Isc, Imp, temp coefficients), inverter MPPT specs (Vmin, Vmax, Imax), site location (for temperature extremes)
- Calculates: min/max panels per string based on local temperature range, validates string voltage stays within MPPT window at coldest/hottest conditions
- Output: recommended string configurations, warning if over/under voltage
- Reference data: NEC 690.7 temperature correction factors

#### 8.5 — Wiring Diagram Generator
- Input: array/slot config from My Solar Toolbox API (panels, positions, inverter assignments, string assignments)
- Generates: visual wiring diagram showing panel → string → inverter → combiner connections
- SVG or Canvas-based rendering, exportable as PDF
- Auto-generated from the existing `arrays`, `strings`, `slots` tables — this is the visual representation of data already in the DB

#### 8.6 — Loss Waterfall Calculator
- Models system losses from ideal to expected real output:
  - Irradiance → nameplate DC → soiling loss → shade loss (from SolarScope) → mismatch loss → wiring loss → inverter clipping → inverter efficiency → AC output
- Interactive waterfall chart showing each loss category
- Pre-install: estimated losses for planning
- Post-install: compares modeled losses to actual (clipping analysis data feeds back here)

#### 8.7 — BOM Builder
- Input: array config (panels, inverters, strings from DB)
- Generates: bill of materials with quantities
  - Panels (model × count), inverters (model × count), wire (gauge × length estimate from array dimensions), racking (based on array size/tilt), disconnects, breakers
- Exportable as CSV/PDF
- Not a procurement tool — a planning checklist

#### 8.8 — Commissioning Validator
- Run on first day of operation (or any day)
- Compares actual production to expected (from production estimator + weather that day)
- Per-panel comparison: flags panels significantly underperforming vs. neighbors
- Validates string voltages match expected (if telemetry available)
- Generates commissioning report: "system is performing within X% of expected"
- Reusable for annual health checks

### Cross-Phase Data Flow
```
PLAN                          INSTALL                       OPERATE
─────────────────────────────────────────────────────────────────────
SolarScope ──shade data──→ Loss Waterfall            Clipping Analysis
                          ──expected losses──→       ──actual clipping──→ compare
                                                     
Production Estimator ─────────────────────────→      Goal Tracking
  ──annual estimate──→                               ──actual vs predicted──→ alerts
                                                     
Financial Modeler ────────────────────────────→      ROI/Billing Tracker
  ──payback estimate──→                              ──actual savings──→ compare
                                                     
Array Config ──────→ String Sizing                   Per-Panel Monitoring
              ──→ Wiring Diagrams                    Degradation Detection
              ──→ BOM Builder
              ──→ Commissioning Validator ──baseline──→ Annual Health Checks
```

### 8.9 — Optional API key auth
- For cloud deployments that may be internet-exposed
- Config option: `server.api_key: "..."` — if set, all API calls require `Authorization: Bearer <key>` header
- Dashboards (served by same server) get the key injected automatically
- Disabled by default for LAN-only users

---

## Relevant Files (current → new)

- `solar_db.py` → `core/db.py` — extend schema with energy_flow + battery + consumption tables, rename inverter→device
- `solar_api.py` → `core/api.py` — add v2 routes, serve PWA from app/, optional API key auth
- `solar_sync.py` → `core/sync.py` — adapter-driven sync loop
- `ema_api_pull.py` → `adapters/apsystems.py` — wrap in adapter class
- `ez1_logger.py` → `adapters/apsystems_ez1.py` — wrap in adapter class
- `vue_ingest.py` → `adapters/emporia_vue.py` — wrap in adapter class
- `solar_dashboard.html` → `app/pages/dashboard.html` — update API URLs, add PWA meta tags
- `solar_clipping.html` → `app/pages/clipping.html` — update API URLs, add PWA meta tags
- `solar_roi.html` → `app/pages/roi.html` — update API URLs, add PWA meta tags
- `solar_field_log.html` → `app/pages/field_log.html` — add PWA meta tags (already client-only)
- `ez1_dashboard.html` → `app/pages/ez1.html` — update API URLs, add PWA meta tags
- `vue_data_explorer.html` → `app/pages/vue_explorer.html` — update API URLs, add PWA meta tags
- NEW: `adapters/base.py` — adapter ABC
- NEW: `adapters/csv_generic.py` — generic CSV/XLS import adapter
- NEW: `config/config.py` + `config/config.yaml.template`
- NEW: `migrate/v1_to_v2.py` — data migration script
- NEW: `app/manifest.json`, `app/sw.js`, `app/index.html` — PWA shell
- NEW: `deploy/Dockerfile` + `deploy/docker-compose.yml`

## Verification
1. `migrate/v1_to_v2.py` against existing v1 solar.db → all row counts match, v1 backup created
2. `docker compose up` → container starts, dashboard accessible at :8080
3. Install PWA on iPhone/Android → loads in standalone mode, icon on home screen
4. Open dashboard on large display (or browser at 2560px) → layout scales cleanly
5. `solar-sync --status` → all adapters listed, sync log intact
6. `solar-sync --sync --adapter apsystems` → new data upserted correctly
7. Hit every `/api/v2/` endpoint → JSON responses match v1 output format
8. Open each dashboard page → all charts render with v2 API, no direct DB access
9. Run with only `csv_generic` adapter enabled → system works without vendor-specific code
10. Enable `api_key` in config → unauthenticated requests rejected, dashboards still work
11. Clean install on empty DB (fresh Docker deploy) → setup flow works end-to-end
12. String sizing calculator → feed panel/inverter specs, verify voltage range calculations match manual NEC calc
13. Wiring diagram → create array config via API, verify SVG diagram renders correct topology

## Implementation Priority
The lifecycle tools span a wide scope. Recommended build order:

**Sprint 1 — Core Platform (Phases 1-5)**: Adapter framework, schema evolution, API v2, sync orchestrator. This is the foundation everything else depends on.

**Sprint 2 — Operate Tools (Phase 6)**: PWA shell, migrate existing dashboards to v2 API. This preserves current functionality in the new architecture.

**Sprint 3 — Docker & Migration (Phase 7 + 2.5)**: Docker deployment, v1→v2 migration script. Makes it usable by others.

**Sprint 4 — Install Tools (Phase 8.4-8.8)**: String sizing, wiring diagrams, loss waterfall, BOM, commissioning. These are the most novel features — pure client-side calculators that read/write array config via API.

**Sprint 5 — Plan Tools (Phase 8.1-8.3)**: SolarScope API integration, production estimator, pre-purchase financial modeling. Closes the lifecycle loop.

## Further Considerations
1. **Energy flow Sankey diagram** — real-time visualization of solar→home, solar→grid, battery↔home flows. Killer operate-phase dashboard feature (D3.js).
2. **Setup wizard** — interactive first-run web wizard: adapter selection → credentials → test connectivity → first sync. Critical for broader audience.
3. **Push notifications** — PWA "Your system produced X kWh today" or "Panel #3 underperforming 3 days." Future operate-phase feature.
4. **Community template library** — users share array configs, wiring templates, loss assumptions for specific panel/inverter combos. Future social feature.
5. **Panel spec database** — crowd-sourced or scraped panel/inverter spec sheets (Voc, Vmp, Isc, temp coefficients) so string sizing calculator can look up specs by model. Would be a valuable shared resource.
