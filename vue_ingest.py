#!/usr/bin/env python3
"""
Ingest Emporia VUE 15-minute CSV exports into a unified SQLite database.

Handles:
  - Standalone Solar Combiner Panel exports (17 data cols, device 33555)
  - Standalone Breaker Box exports (19 data cols, device 385843, pre-March 2025)
  - Combined Breaker Box exports (36 data cols, device 385843, post-March 2025)
  - DST spring-forward (missing clock hour) and fall-back (repeated clock hour)
    via monotonic row-order walk with zoneinfo-aware UTC conversion
  - Deduplication across overlapping exports (latest export wins)
"""

import csv
import os
import sqlite3
import sys
import zipfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOCAL_TZ = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vue_energy.db")
ZIP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_vue_data")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS vue_energy_15min (
    timestamp_utc       TEXT NOT NULL PRIMARY KEY,   -- ISO 8601 UTC
    timestamp_local     TEXT NOT NULL,               -- ISO 8601 with offset

    -- Grid / Breaker Box mains (kW avg over 15 min)
    grid_mains_a_kw     REAL,    -- Breaker Box Mains_A  (positive = consuming)
    grid_mains_b_kw     REAL,    -- Breaker Box Mains_B

    -- Individual breaker circuits (kW avg)
    basement_kw         REAL,    -- Breaker 18
    office_bedroom_kw   REAL,    -- Breaker 16
    garage_bath_kw      REAL,    -- Breaker 13
    living_room_kw      REAL,    -- Breaker 14
    fridge_kw           REAL,    -- Breaker 2
    stove_kw            REAL,    -- Breaker 5
    ac_kw               REAL,    -- Breaker 19
    furnace_fan_kw      REAL,    -- Breaker 17a

    -- Solar Combiner Panel mains (kW avg)
    solar_mains_a_kw    REAL,    -- Solar Panel Mains_A  (negative = exporting)
    solar_mains_b_kw    REAL,    -- Solar Panel Mains_B

    -- Solar generation by string (kW avg)
    solar_top_kw        REAL,    -- Top String generation
    solar_bottom_kw     REAL,    -- Bottom String generation

    -- Computed convenience columns
    solar_total_kw      REAL,    -- abs(solar_top) + abs(solar_bottom)
    grid_net_kw         REAL,    -- grid_mains_a + grid_mains_b  (positive = consuming from grid)
    consumption_kw      REAL,    -- grid_net + solar_total  (gross household consumption)

    -- Metadata
    source_file         TEXT     -- which zip/csv provided this row
);

CREATE INDEX IF NOT EXISTS idx_vue15_local ON vue_energy_15min(timestamp_local);
CREATE INDEX IF NOT EXISTS idx_vue15_date  ON vue_energy_15min(substr(timestamp_utc, 1, 10));

-- 1-hour resolution table (same columns as 15min)
CREATE TABLE IF NOT EXISTS vue_energy_1h (
    timestamp_utc       TEXT NOT NULL PRIMARY KEY,   -- ISO 8601 UTC, top of hour
    timestamp_local     TEXT NOT NULL,

    grid_mains_a_kw     REAL,
    grid_mains_b_kw     REAL,
    basement_kw         REAL,
    office_bedroom_kw   REAL,
    garage_bath_kw      REAL,
    living_room_kw      REAL,
    fridge_kw           REAL,
    stove_kw            REAL,
    ac_kw               REAL,
    furnace_fan_kw      REAL,
    solar_mains_a_kw    REAL,
    solar_mains_b_kw    REAL,
    solar_top_kw        REAL,
    solar_bottom_kw     REAL,
    solar_total_kw      REAL,
    grid_net_kw         REAL,
    consumption_kw      REAL,
    source_file         TEXT
);

CREATE INDEX IF NOT EXISTS idx_vue1h_local ON vue_energy_1h(timestamp_local);
CREATE INDEX IF NOT EXISTS idx_vue1h_date  ON vue_energy_1h(substr(timestamp_utc, 1, 10));

-- Unified hourly table (materialized): prefer 15min aggregated, fill gaps with native 1H.
-- Rebuilt by ingest_1h() after each ingest run.
CREATE TABLE IF NOT EXISTS vue_energy_hourly (
    timestamp_utc       TEXT NOT NULL PRIMARY KEY,
    timestamp_local     TEXT NOT NULL,
    grid_mains_a_kw     REAL,
    grid_mains_b_kw     REAL,
    basement_kw         REAL,
    office_bedroom_kw   REAL,
    garage_bath_kw      REAL,
    living_room_kw      REAL,
    fridge_kw           REAL,
    stove_kw            REAL,
    ac_kw               REAL,
    furnace_fan_kw      REAL,
    solar_mains_a_kw    REAL,
    solar_mains_b_kw    REAL,
    solar_top_kw        REAL,
    solar_bottom_kw     REAL,
    solar_total_kw      REAL,
    grid_net_kw         REAL,
    consumption_kw      REAL,
    source_resolution   TEXT     -- '15min_agg' or '1h_native'
);

CREATE INDEX IF NOT EXISTS idx_vueh_date ON vue_energy_hourly(substr(timestamp_utc, 1, 10));

-- Ingest log to track which files have been loaded
CREATE TABLE IF NOT EXISTS vue_ingest_log (
    filename        TEXT NOT NULL PRIMARY KEY,
    ingested_at     TEXT NOT NULL,
    rows_loaded     INTEGER,
    date_range      TEXT
);
"""

REBUILD_HOURLY_SQL = """
-- Rebuild materialized hourly table from 15min + 1h sources
DELETE FROM vue_energy_hourly;

-- Step 1: insert all 15-min data aggregated to hourly
INSERT INTO vue_energy_hourly
SELECT
    substr(timestamp_utc, 1, 13) || ':00:00Z',
    MIN(timestamp_local),
    AVG(grid_mains_a_kw), AVG(grid_mains_b_kw),
    AVG(basement_kw), AVG(office_bedroom_kw), AVG(garage_bath_kw), AVG(living_room_kw),
    AVG(fridge_kw), AVG(stove_kw), AVG(ac_kw), AVG(furnace_fan_kw),
    AVG(solar_mains_a_kw), AVG(solar_mains_b_kw),
    AVG(solar_top_kw), AVG(solar_bottom_kw),
    AVG(solar_total_kw), AVG(grid_net_kw), AVG(consumption_kw),
    '15min_agg'
FROM vue_energy_15min
GROUP BY substr(timestamp_utc, 1, 13);

-- Step 2: fill gaps with native 1H rows (only hours not already covered)
INSERT OR IGNORE INTO vue_energy_hourly
SELECT
    timestamp_utc, timestamp_local,
    grid_mains_a_kw, grid_mains_b_kw,
    basement_kw, office_bedroom_kw, garage_bath_kw, living_room_kw,
    fridge_kw, stove_kw, ac_kw, furnace_fan_kw,
    solar_mains_a_kw, solar_mains_b_kw,
    solar_top_kw, solar_bottom_kw,
    solar_total_kw, grid_net_kw, consumption_kw,
    '1h_native'
FROM vue_energy_1h;
"""

# ---------------------------------------------------------------------------
# Column mapping: CSV header substring -> our column name
# ---------------------------------------------------------------------------
# For Solar Combiner Panel standalone CSVs (17 data cols)
SOLAR_MAP = {
    "Solar Combiner Panel-Mains_A":                     "solar_mains_a_kw",
    "Solar Combiner Panel-Mains_B":                     "solar_mains_b_kw",
    "Solar Combiner Panel-Solar/Generation-Top String":  "solar_top_kw",
    "Solar Combiner Panel-Solar/Generation-Bottom String": "solar_bottom_kw",
}

# For Breaker Box CSVs (19 or 36 data cols)
BREAKER_MAP = {
    "Breaker Box-Mains_A":                              "grid_mains_a_kw",
    "Breaker Box-Mains_B":                              "grid_mains_b_kw",
    "Basement (18)":                                    "basement_kw",
    "Office/Kids Bedroom (16)":                         "office_bedroom_kw",
    "Garage/Main Bath (13)":                            "garage_bath_kw",
    "Living Room & Main Lights (14)":                   "living_room_kw",
    "Fridge (2)":                                       "fridge_kw",
    "Stove (5)":                                        "stove_kw",
    "AC (19)":                                          "ac_kw",
    "Furnace Fan (17a)":                                "furnace_fan_kw",
}

# Combined map (36-col files contain both)
COMBINED_MAP = {**BREAKER_MAP, **SOLAR_MAP}

# Old "100A Panel" format (device 33555 before solar conversion, 2021-2022)
# Same physical circuits, different column names.
OLD_PANEL_MAP = {
    "100A Panel_1":      "grid_mains_a_kw",
    "100A Panel_2":      "grid_mains_b_kw",
    "Laundry Rm (A18)":  "basement_kw",
    "A/C (B19/A20)":     "ac_kw",
    "Up Bedrooms (A16)": "office_bedroom_kw",
    "Stove (B5/A6)":     "stove_kw",
    "Furnace (A17a)":    "furnace_fan_kw",
    "Living Rm + (B14)": "living_room_kw",
    "Garage/Bath (B13)": "garage_bath_kw",
    "Fridge (A2)":       "fridge_kw",
}


def build_col_index(header, mapping):
    """Map CSV column indices to DB column names using substring matching."""
    idx = {}
    for i, col in enumerate(header):
        if i == 0:
            continue  # skip timestamp
        for pattern, db_col in mapping.items():
            if pattern in col:
                idx[i] = db_col
                break
    return idx


def parse_naive_ts(s):
    """Parse VUE timestamp 'MM/DD/YYYY HH:MM:SS' -> naive datetime."""
    return datetime.strptime(s.strip(), "%m/%d/%Y %H:%M:%S")


def monotonic_to_utc(rows_naive):
    """
    Walk rows in file order (monotonic real time), converting naive
    America/Chicago timestamps to UTC, correctly handling:
      - Spring forward: 01:45 -> 03:00 (no 02:xx exists; both are unambiguous)
      - Fall back: 01:xx appears twice; first occurrence is CDT, second is CST
    
    Returns list of (utc_iso, local_iso, naive_dt) tuples.
    """
    results = []
    prev_utc = None
    is_fold = False  # tracks if we're in the repeated fall-back hour

    for naive_dt in rows_naive:
        # Try to localize. During fall-back, the same wall clock appears twice.
        # We use the fold parameter: fold=0 -> first (DST), fold=1 -> second (STD)
        try:
            aware_dt = naive_dt.replace(tzinfo=LOCAL_TZ, fold=0)
            utc_dt = aware_dt.astimezone(UTC)

            # Detect fall-back: if UTC went backwards, we need fold=1
            if prev_utc is not None and utc_dt <= prev_utc:
                is_fold = True

            if is_fold:
                aware_dt = naive_dt.replace(tzinfo=LOCAL_TZ, fold=1)
                utc_dt = aware_dt.astimezone(UTC)
                # Check if we've exited the ambiguous period
                if prev_utc is not None and utc_dt > prev_utc + timedelta(minutes=20):
                    is_fold = False
                    aware_dt = naive_dt.replace(tzinfo=LOCAL_TZ, fold=0)
                    utc_dt = aware_dt.astimezone(UTC)

        except Exception:
            # Fallback: just offset by -6 (CST)
            utc_dt = naive_dt + timedelta(hours=6)
            aware_dt = naive_dt.replace(tzinfo=ZoneInfo("Etc/GMT+6"))

        utc_iso = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        local_iso = aware_dt.isoformat()
        results.append((utc_iso, local_iso, naive_dt))
        prev_utc = utc_dt

    return results


def identify_file_type(header):
    """Determine if CSV is solar-only, breaker-only, combined, or old 100A panel."""
    header_str = ",".join(header)
    has_solar = "Solar Combiner Panel-Solar/Generation" in header_str
    has_breaker = "Breaker Box-Mains_A" in header_str
    has_old_panel = "100A Panel_1" in header_str

    if has_breaker and has_solar:
        return "combined", COMBINED_MAP
    elif has_breaker:
        return "breaker", BREAKER_MAP
    elif has_solar:
        return "solar", SOLAR_MAP
    elif has_old_panel:
        return "old_panel", OLD_PANEL_MAP
    else:
        return "unknown", {}


def parse_val(s):
    """Parse a CSV cell value, returning None for 'No CT' or empty."""
    s = s.strip()
    if s in ("No CT", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ingest_csv(conn, csv_path, source_label):
    """Ingest one 15MIN CSV into the database."""
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)

    file_type, mapping = identify_file_type(header)
    if file_type == "unknown":
        print(f"  SKIP {source_label}: unknown column format")
        return 0

    col_idx = build_col_index(header, mapping)
    if not col_idx:
        print(f"  SKIP {source_label}: no columns matched")
        return 0

    # Read all rows
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        raw_rows = [r for r in reader if r and r[0].strip()]

    if not raw_rows:
        return 0

    # Parse naive timestamps, walk monotonically to UTC
    naive_dts = [parse_naive_ts(r[0]) for r in raw_rows]
    utc_results = monotonic_to_utc(naive_dts)

    # Build insert data
    db_cols = [
        "timestamp_utc", "timestamp_local",
        "grid_mains_a_kw", "grid_mains_b_kw",
        "basement_kw", "office_bedroom_kw", "garage_bath_kw", "living_room_kw",
        "fridge_kw", "stove_kw", "ac_kw", "furnace_fan_kw",
        "solar_mains_a_kw", "solar_mains_b_kw",
        "solar_top_kw", "solar_bottom_kw",
        "solar_total_kw", "grid_net_kw", "consumption_kw",
        "source_file",
    ]
    placeholders = ",".join(["?"] * len(db_cols))
    col_names = ",".join(db_cols)

    # UPSERT: later exports overwrite earlier for the same timestamp
    sql = f"""INSERT INTO vue_energy_15min ({col_names})
              VALUES ({placeholders})
              ON CONFLICT(timestamp_utc) DO UPDATE SET
                {', '.join(f'{c}=COALESCE(excluded.{c},{c})' for c in db_cols if c not in ('timestamp_utc','timestamp_local','source_file'))},
                source_file = excluded.source_file
    """

    batch = []
    for i, (utc_iso, local_iso, _naive) in enumerate(utc_results):
        row = raw_rows[i]
        vals = {}
        for csv_i, db_col in col_idx.items():
            if csv_i < len(row):
                vals[db_col] = parse_val(row[csv_i])

        # Compute convenience columns
        solar_top = vals.get("solar_top_kw")
        solar_bot = vals.get("solar_bottom_kw")
        solar_total = None
        if solar_top is not None and solar_bot is not None:
            solar_total = abs(solar_top) + abs(solar_bot)
        elif solar_top is not None:
            solar_total = abs(solar_top)
        elif solar_bot is not None:
            solar_total = abs(solar_bot)

        grid_a = vals.get("grid_mains_a_kw")
        grid_b = vals.get("grid_mains_b_kw")
        grid_net = None
        if grid_a is not None and grid_b is not None:
            grid_net = grid_a + grid_b

        consumption = None
        if grid_net is not None and solar_total is not None:
            consumption = grid_net + solar_total

        record = (
            utc_iso,
            local_iso,
            vals.get("grid_mains_a_kw"),
            vals.get("grid_mains_b_kw"),
            vals.get("basement_kw"),
            vals.get("office_bedroom_kw"),
            vals.get("garage_bath_kw"),
            vals.get("living_room_kw"),
            vals.get("fridge_kw"),
            vals.get("stove_kw"),
            vals.get("ac_kw"),
            vals.get("furnace_fan_kw"),
            vals.get("solar_mains_a_kw"),
            vals.get("solar_mains_b_kw"),
            solar_top,
            solar_bot,
            solar_total,
            grid_net,
            consumption,
            source_label,
        )
        batch.append(record)

    conn.executemany(sql, batch)
    conn.commit()
    return len(batch)


def ingest_csv_1h(conn, csv_path, source_label):
    """Ingest one 1H CSV into the vue_energy_1h table."""
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)

    file_type, mapping = identify_file_type(header)
    if file_type == "unknown":
        print(f"  SKIP {source_label}: unknown column format")
        return 0

    col_idx = build_col_index(header, mapping)
    if not col_idx:
        print(f"  SKIP {source_label}: no columns matched")
        return 0

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)
        raw_rows = [r for r in reader if r and r[0].strip()]

    if not raw_rows:
        return 0

    naive_dts = [parse_naive_ts(r[0]) for r in raw_rows]
    utc_results = monotonic_to_utc(naive_dts)

    db_cols = [
        "timestamp_utc", "timestamp_local",
        "grid_mains_a_kw", "grid_mains_b_kw",
        "basement_kw", "office_bedroom_kw", "garage_bath_kw", "living_room_kw",
        "fridge_kw", "stove_kw", "ac_kw", "furnace_fan_kw",
        "solar_mains_a_kw", "solar_mains_b_kw",
        "solar_top_kw", "solar_bottom_kw",
        "solar_total_kw", "grid_net_kw", "consumption_kw",
        "source_file",
    ]
    placeholders = ",".join(["?"] * len(db_cols))
    col_names = ",".join(db_cols)

    sql = f"""INSERT INTO vue_energy_1h ({col_names})
              VALUES ({placeholders})
              ON CONFLICT(timestamp_utc) DO UPDATE SET
                {', '.join(f'{c}=COALESCE(excluded.{c},{c})' for c in db_cols if c not in ('timestamp_utc','timestamp_local','source_file'))},
                source_file = excluded.source_file
    """

    batch = []
    for i, (utc_iso, local_iso, _naive) in enumerate(utc_results):
        row = raw_rows[i]
        vals = {}
        for csv_i, db_col in col_idx.items():
            if csv_i < len(row):
                vals[db_col] = parse_val(row[csv_i])

        solar_top = vals.get("solar_top_kw")
        solar_bot = vals.get("solar_bottom_kw")
        solar_total = None
        if solar_top is not None and solar_bot is not None:
            solar_total = abs(solar_top) + abs(solar_bot)
        elif solar_top is not None:
            solar_total = abs(solar_top)
        elif solar_bot is not None:
            solar_total = abs(solar_bot)

        grid_a = vals.get("grid_mains_a_kw")
        grid_b = vals.get("grid_mains_b_kw")
        grid_net = None
        if grid_a is not None and grid_b is not None:
            grid_net = grid_a + grid_b

        consumption = None
        if grid_net is not None and solar_total is not None:
            consumption = grid_net + solar_total

        record = (
            utc_iso, local_iso,
            vals.get("grid_mains_a_kw"), vals.get("grid_mains_b_kw"),
            vals.get("basement_kw"), vals.get("office_bedroom_kw"),
            vals.get("garage_bath_kw"), vals.get("living_room_kw"),
            vals.get("fridge_kw"), vals.get("stove_kw"),
            vals.get("ac_kw"), vals.get("furnace_fan_kw"),
            vals.get("solar_mains_a_kw"), vals.get("solar_mains_b_kw"),
            solar_top, solar_bot, solar_total, grid_net, consumption,
            source_label,
        )
        batch.append(record)

    conn.executemany(sql, batch)
    conn.commit()
    return len(batch)


def _ingest_zips(conn, scale="15MIN"):
    """Generic ingest loop for a given time scale (15MIN or 1H)."""
    table = "vue_energy_15min" if scale == "15MIN" else "vue_energy_1h"
    log_prefix = f"{scale}:"
    ingest_fn = ingest_csv if scale == "15MIN" else ingest_csv_1h

    zips = sorted(
        [f for f in os.listdir(ZIP_DIR) if f.endswith(".zip")],
        key=lambda f: os.path.getmtime(os.path.join(ZIP_DIR, f))
    )

    print(f"Found {len(zips)} zip files (looking for {scale} CSVs)\n")

    total_rows = 0
    import tempfile

    for zf_name in zips:
        zf_path = os.path.join(ZIP_DIR, zf_name)
        log_key = f"{log_prefix}{zf_name}"

        existing = conn.execute(
            "SELECT 1 FROM vue_ingest_log WHERE filename = ?", (log_key,)
        ).fetchone()
        if existing:
            print(f"  SKIP (already ingested): {log_key}")
            continue

        with zipfile.ZipFile(zf_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if scale in n and n.endswith(".csv")]
            if not csv_names:
                continue

            csv_name = csv_names[0]
            print(f"  Processing: {log_key}")
            with tempfile.TemporaryDirectory() as tmpdir:
                zf.extract(csv_name, tmpdir)
                csv_path = os.path.join(tmpdir, csv_name)

                n = ingest_fn(conn, csv_path, log_key)
                total_rows += n

                ts_min = conn.execute(
                    f"SELECT MIN(timestamp_utc) FROM {table} WHERE source_file = ?",
                    (log_key,)
                ).fetchone()[0]
                ts_max = conn.execute(
                    f"SELECT MAX(timestamp_utc) FROM {table} WHERE source_file = ?",
                    (log_key,)
                ).fetchone()[0]
                date_range = f"{ts_min} -> {ts_max}" if ts_min else "empty"

                conn.execute(
                    "INSERT OR REPLACE INTO vue_ingest_log VALUES (?, ?, ?, ?)",
                    (log_key, datetime.now(UTC).isoformat(), n, date_range)
                )
                conn.commit()
                print(f"    {csv_name}: {n:,} rows  ({date_range})")

    return total_rows


def ingest_all():
    """Ingest all 15MIN CSVs from zip files in temp_vue_data/."""
    if not os.path.isdir(ZIP_DIR):
        print(f"ERROR: ZIP directory not found: {ZIP_DIR}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)

    print(f"Database: {DB_PATH}")
    print(f"Source:   {ZIP_DIR}")

    total_rows = _ingest_zips(conn, "15MIN")

    # Summary
    row_count = conn.execute("SELECT COUNT(*) FROM vue_energy_15min").fetchone()[0]
    date_min = conn.execute("SELECT MIN(timestamp_utc) FROM vue_energy_15min").fetchone()[0]
    date_max = conn.execute("SELECT MAX(timestamp_utc) FROM vue_energy_15min").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"Done. {total_rows:,} rows ingested this run (15MIN).")
    print(f"Total rows in vue_energy_15min: {row_count:,}")
    print(f"Date range: {date_min} -> {date_max}")

    # Quick integrity check
    gap_check = conn.execute("""
        WITH ordered AS (
            SELECT timestamp_utc,
                   LAG(timestamp_utc) OVER (ORDER BY timestamp_utc) AS prev_utc
            FROM vue_energy_15min
        )
        SELECT prev_utc, timestamp_utc,
               ROUND((julianday(timestamp_utc) - julianday(prev_utc)) * 24 * 60, 1) AS gap_min
        FROM ordered
        WHERE prev_utc IS NOT NULL
          AND (julianday(timestamp_utc) - julianday(prev_utc)) * 24 * 60 > 20
        ORDER BY gap_min DESC
        LIMIT 10
    """).fetchall()

    if gap_check:
        print(f"\nGaps > 20 min in vue_energy_15min:")
        for prev, curr, gap in gap_check:
            print(f"  {prev} -> {curr}  ({gap:.0f} min / {gap/60:.1f}h / {gap/1440:.1f}d)")
    else:
        print("\nNo gaps > 20 min — 15min dataset is continuous!")

    conn.close()


def ingest_1h():
    """Ingest all 1H CSVs from zip files in temp_vue_data/."""
    if not os.path.isdir(ZIP_DIR):
        print(f"ERROR: ZIP directory not found: {ZIP_DIR}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)

    print(f"Database: {DB_PATH}")
    print(f"Source:   {ZIP_DIR}")

    total_rows = _ingest_zips(conn, "1H")

    row_count = conn.execute("SELECT COUNT(*) FROM vue_energy_1h").fetchone()[0]
    date_min = conn.execute("SELECT MIN(timestamp_utc) FROM vue_energy_1h").fetchone()[0]
    date_max = conn.execute("SELECT MAX(timestamp_utc) FROM vue_energy_1h").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"Done. {total_rows:,} rows ingested this run (1H).")
    print(f"Total rows in vue_energy_1h: {row_count:,}")
    print(f"Date range: {date_min} -> {date_max}")

    # Gap check for 1H
    gap_check = conn.execute("""
        WITH ordered AS (
            SELECT timestamp_utc,
                   LAG(timestamp_utc) OVER (ORDER BY timestamp_utc) AS prev_utc
            FROM vue_energy_1h
        )
        SELECT prev_utc, timestamp_utc,
               ROUND((julianday(timestamp_utc) - julianday(prev_utc)) * 24 * 60, 1) AS gap_min
        FROM ordered
        WHERE prev_utc IS NOT NULL
          AND (julianday(timestamp_utc) - julianday(prev_utc)) * 24 * 60 > 65
        ORDER BY gap_min DESC
        LIMIT 10
    """).fetchall()

    if gap_check:
        print(f"\nGaps > 65 min in vue_energy_1h:")
        for prev, curr, gap in gap_check:
            print(f"  {prev} -> {curr}  ({gap:.0f} min / {gap/60:.1f}h / {gap/1440:.1f}d)")
    else:
        print("\nNo gaps > 65 min — 1h dataset is continuous!")

    # Rebuild materialized hourly table
    print("\nRebuilding unified hourly table...")
    conn.executescript(REBUILD_HOURLY_SQL)
    conn.commit()

    # Unified hourly view summary
    hourly_count = conn.execute("SELECT COUNT(*) FROM vue_energy_hourly").fetchone()[0]
    hourly_min = conn.execute("SELECT MIN(timestamp_utc) FROM vue_energy_hourly").fetchone()[0]
    hourly_max = conn.execute("SELECT MAX(timestamp_utc) FROM vue_energy_hourly").fetchone()[0]
    native_1h = conn.execute("SELECT COUNT(*) FROM vue_energy_hourly WHERE source_resolution='1h_native'").fetchone()[0]
    agg_15m = conn.execute("SELECT COUNT(*) FROM vue_energy_hourly WHERE source_resolution='15min_agg'").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"Unified hourly view (vue_energy_hourly):")
    print(f"  Total hours: {hourly_count:,}")
    print(f"  From 15min aggregated: {agg_15m:,}")
    print(f"  From native 1H fill:  {native_1h:,}")
    print(f"  Date range: {hourly_min} -> {hourly_max}")

    conn.close()


if __name__ == "__main__":
    if "--1h" in sys.argv:
        ingest_1h()
    elif "--all" in sys.argv:
        ingest_all()
        ingest_1h()
    else:
        ingest_all()
