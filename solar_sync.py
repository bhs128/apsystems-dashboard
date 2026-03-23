#!/usr/bin/env python3
"""
solar_sync.py — Sync service for the solar data store.

Imports existing local files (XLS power curves, daily energy reports,
panel CSVs, billing/finance CSVs) and pulls new data from the APsystems
EMA OpenAPI into solar.db.

Usage:
  python solar_sync.py --backfill          # one-time: import all local files
  python solar_sync.py --sync              # incremental: pull new data from API
  python solar_sync.py --backfill --sync   # both
  python solar_sync.py --status            # show what's in the DB
  python solar_sync.py --import-billing monthly_billed_usage.csv
  python solar_sync.py --import-finance finance_data.csv

Designed to be safe to re-run (upsert semantics throughout).
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta
from calendar import month_name

import pandas as pd

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))

from solar_db import SolarDB

from ema_api_pull import (
    load_credentials, pull_power_curve, pull_daily_energy,
    pull_panel_batch, pull_panel_single_inverter, pull_inverter_list,
    pull_system_summary,
    date_range as api_date_range,
)

# DATA_DIR: where XLS/CSV data files live (for backfill).
# Defaults to SERVICE_DIR; override with SOLAR_DATA_DIR env var.
DATA_DIR = os.environ.get('SOLAR_DATA_DIR', SERVICE_DIR)
CURVE_DIR = os.path.join(DATA_DIR, 'daily_prod_curves')
PANEL_DIR = os.path.join(DATA_DIR, 'panel_data')

# Panel data availability start (from probe results)
PANEL_DATA_START = '2025-08-01'


# ======================================================================
# File parsers — local XLS / CSV → DB rows
# ======================================================================

def parse_power_curve_xls(filepath):
    """Parse a single power curve XLS into DB rows.

    Returns list of (timestamp_str, power_w, 'xls') tuples.
    Applies monotonic time reconstruction (DST fix).
    """
    # Extract date from filename
    m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(filepath))
    if not m:
        return []
    date_str = m.group(1)

    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f'    WARN: cannot read {os.path.basename(filepath)}: {e}')
        return []

    # Normalize column names
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl == 'time':
            col_map[c] = 'time'
        elif 'power' in cl:
            col_map[c] = 'Power(W)'
    df.rename(columns=col_map, inplace=True)

    if 'time' not in df.columns or 'Power(W)' not in df.columns:
        return []

    df = df.dropna(subset=['Power(W)'])
    if df.empty:
        return []

    # Strip commas from power values (e.g. "1,225" → "1225")
    df['Power(W)'] = pd.to_numeric(
        df['Power(W)'].astype(str).str.replace(',', '', regex=False),
        errors='coerce').fillna(0.0)

    # Monotonic time reconstruction: first timestamp + 5min increments
    t0 = pd.to_datetime(date_str + ' ' + str(df['time'].iloc[0]))
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        ts = t0 + timedelta(minutes=5 * i)
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
        rows.append((ts_str, float(row['Power(W)']), 'xls'))
    return rows


def parse_daily_energy_xls(filepath):
    """Parse a daily energy report XLS into DB rows.

    Returns list of (date_str, energy_kwh, 'xls') tuples.
    Handles column name variations (Energy(kWh) / energy (kWh) / etc).
    """
    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f'    WARN: cannot read {os.path.basename(filepath)}: {e}')
        return []

    # Normalize column names (case-insensitive, space-insensitive)
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower().replace(' ', '')
        if cl == 'date':
            col_map[c] = 'Date'
        elif cl == 'energy(kwh)':
            col_map[c] = 'Energy(kWh)'
    df.rename(columns=col_map, inplace=True)

    if 'Date' not in df.columns or 'Energy(kWh)' not in df.columns:
        return []

    rows = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
            kwh = float(row['Energy(kWh)'])
            if kwh > 0:
                rows.append((d, kwh, 'xls'))
        except (ValueError, TypeError):
            continue
    return rows


def parse_panel_csv(filepath):
    """Parse a panel data CSV (wide format) into narrow DB rows.

    Returns list of (timestamp_str, inverter_uid, channel, power_w) tuples.
    """
    m = re.search(r'panels_(\d{4}-\d{2}-\d{2})', os.path.basename(filepath))
    if not m:
        return []
    date_str = m.group(1)

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f'    WARN: cannot read {os.path.basename(filepath)}: {e}')
        return []

    if 'time' not in df.columns:
        return []

    # Identify uid-channel columns (pattern: 12-digit UID + dash + channel)
    uid_cols = [c for c in df.columns if re.match(r'^\d+-\d+$', c)]

    # Monotonic time from first timestamp
    t0 = pd.to_datetime(date_str + ' ' + str(df['time'].iloc[0]))

    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        ts = t0 + timedelta(minutes=5 * i)
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
        for col in uid_cols:
            uid, ch = col.rsplit('-', 1)
            power = float(row[col]) if pd.notna(row[col]) else 0.0
            rows.append((ts_str, uid, int(ch), power))
    return rows


def parse_billing_csv(filepath):
    """Parse monthly_billed_usage.csv into DB rows.

    Returns list of (meter_date, consumed, produced, actual_bill, est_bill).
    """
    df = pd.read_csv(filepath)
    # Normalize column names
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if 'meter' in cl and 'date' in cl:
            col_map[c] = 'meter_date'
        elif cl == 'energy consumed':
            col_map[c] = 'consumed'
        elif cl == 'energy produced':
            col_map[c] = 'produced'
        elif 'actual' in cl and 'bill' in cl:
            col_map[c] = 'actual_bill'
        elif 'est' in cl and 'without' in cl:
            col_map[c] = 'est_bill'
    df.rename(columns=col_map, inplace=True)

    rows = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row['meter_date']).strftime('%Y-%m-%d')
            rows.append((
                d,
                float(row.get('consumed', 0)),
                float(row.get('produced', 0)),
                float(row.get('actual_bill', 0)),
                float(row.get('est_bill', 0)),
            ))
        except (ValueError, TypeError):
            continue
    return rows


def parse_finance_csv(filepath):
    """Parse finance_data.csv into DB rows.

    Returns list of (date_str, heloc_balance, interest).
    Date format in file: 'Month YYYY' → normalized to YYYY-MM-01.
    """
    df = pd.read_csv(filepath)
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl == 'date':
            col_map[c] = 'date'
        elif 'heloc' in cl or 'balance' in cl:
            col_map[c] = 'heloc_balance'
        elif 'interest' in cl:
            col_map[c] = 'interest'
    df.rename(columns=col_map, inplace=True)

    # Build month name → number lookup
    month_lookup = {name.lower(): i for i, name in enumerate(month_name) if i}

    rows = []
    for _, row in df.iterrows():
        try:
            raw = str(row['date']).strip()
            parts = raw.split()
            if len(parts) == 2:
                mon = month_lookup.get(parts[0].lower())
                yr = int(parts[1])
                if mon:
                    d = f'{yr}-{mon:02d}-01'
                    rows.append((
                        d,
                        float(row.get('heloc_balance', 0)),
                        float(row.get('interest', 0)),
                    ))
        except (ValueError, TypeError, KeyError):
            continue
    return rows


# ======================================================================
# Backfill — import all local files into DB
# ======================================================================

def backfill_power_curves(db):
    """Import all XLS power curves from daily_prod_curves/."""
    if not os.path.isdir(CURVE_DIR):
        print('  No daily_prod_curves/ directory found.')
        return

    files = sorted([f for f in os.listdir(CURVE_DIR)
                    if f.endswith('.xls') and ':' not in f])
    print(f'  Importing {len(files)} power curve files ...')

    existing = db.get_dates_with_data('system_readings')
    total_rows = 0
    imported = 0

    for f in files:
        m = re.search(r'(\d{4}-\d{2}-\d{2})', f)
        if m and m.group(1) in existing:
            continue
        rows = parse_power_curve_xls(os.path.join(CURVE_DIR, f))
        if rows:
            db.upsert_system_readings(rows)
            total_rows += len(rows)
            imported += 1

    print(f'    {imported} new days, {total_rows} readings inserted'
          f' ({len(files) - imported} already in DB)')
    if files:
        last_date = re.search(r'(\d{4}-\d{2}-\d{2})', files[-1])
        if last_date:
            db.update_sync_log('xls_curves', last_date.group(1), total_rows)


def backfill_daily_energy(db):
    """Import all Daily Energy Report XLS files."""
    files = sorted([f for f in os.listdir(DATA_DIR)
                    if f.startswith('Daily Energy Report') and f.endswith('.xls')
                    and ':' not in f])
    if not files:
        print('  No daily energy report files found.')
        return

    print(f'  Importing {len(files)} daily energy report(s) ...')
    total_rows = 0
    for f in files:
        rows = parse_daily_energy_xls(os.path.join(DATA_DIR, f))
        if rows:
            db.upsert_daily_energy(rows)
            total_rows += len(rows)
            print(f'    {f}: {len(rows)} days')

    print(f'    {total_rows} total daily energy records')
    if total_rows:
        db.update_sync_log('xls_daily', rows[-1][0] if rows else '', total_rows)


def backfill_panel_data(db):
    """Import all panel CSV files from panel_data/."""
    if not os.path.isdir(PANEL_DIR):
        print('  No panel_data/ directory found.')
        return

    files = sorted([f for f in os.listdir(PANEL_DIR)
                    if f.startswith('panels_') and f.endswith('.csv')])
    if not files:
        print('  No panel CSV files found.')
        return

    existing = db.get_dates_with_data('panel_readings')
    print(f'  Importing {len(files)} panel data file(s) ...')
    total_rows = 0
    imported = 0

    for f in files:
        m = re.search(r'panels_(\d{4}-\d{2}-\d{2})', f)
        if m and m.group(1) in existing:
            continue
        rows = parse_panel_csv(os.path.join(PANEL_DIR, f))
        if rows:
            db.upsert_panel_readings(rows)
            total_rows += len(rows)
            imported += 1

    print(f'    {imported} new days, {total_rows} panel readings inserted')
    if files:
        last_date = re.search(r'panels_(\d{4}-\d{2}-\d{2})', files[-1])
        if last_date:
            db.update_sync_log('xls_panels', last_date.group(1), total_rows)


def backfill_billing(db, filepath=None):
    """Import monthly_billed_usage.csv."""
    filepath = filepath or os.path.join(DATA_DIR, 'monthly_billed_usage.csv')
    if not os.path.exists(filepath):
        print(f'  Billing file not found: {filepath}')
        return
    rows = parse_billing_csv(filepath)
    if rows:
        db.upsert_billing(rows)
        print(f'  Imported {len(rows)} billing periods')
        db.update_sync_log('csv_billing', rows[-1][0], len(rows))
    else:
        print('  No billing data parsed.')


def backfill_finance(db, filepath=None):
    """Import finance_data.csv."""
    filepath = filepath or os.path.join(DATA_DIR, 'finance_data.csv')
    if not os.path.exists(filepath):
        print(f'  Finance file not found: {filepath}')
        return
    rows = parse_finance_csv(filepath)
    if rows:
        db.upsert_finance(rows)
        print(f'  Imported {len(rows)} finance records')
        db.update_sync_log('csv_finance', rows[-1][0], len(rows))
    else:
        print('  No finance data parsed.')


# ======================================================================
# API sync — pull new data into DB
# ======================================================================

def sync_power_curves(db, app_id, app_secret, start=None, end=None):
    """Pull power curves from API for dates not yet in the DB."""
    today = datetime.now().strftime('%Y-%m-%d')
    end = end or today

    if not start:
        # Resume from last synced date
        existing = db.get_dates_with_data('system_readings')
        if existing:
            latest = max(existing)
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)
            start = start_dt.strftime('%Y-%m-%d')
        else:
            start = '2024-11-05'

    if start > end:
        print('  Power curves: up to date.')
        return

    dates = [d for d in api_date_range(start, end)
             if d not in db.get_dates_with_data('system_readings')]
    if not dates:
        print('  Power curves: up to date.')
        return

    print(f'  Pulling {len(dates)} power curves from API ({dates[0]} .. {dates[-1]}) ...')
    total = 0
    for i, ds in enumerate(dates):
        records = pull_power_curve(app_id, app_secret, ds)
        if records:
            # Monotonic time reconstruction
            t0 = pd.to_datetime(ds + ' ' + records[0][0])
            rows = []
            for j, (_, power) in enumerate(records):
                ts = t0 + timedelta(minutes=5 * j)
                rows.append((ts.strftime('%Y-%m-%d %H:%M:%S'), power, 'api'))
            db.upsert_system_readings(rows)
            total += len(rows)
            print(f'    [{i+1}/{len(dates)}] {ds}: {len(rows)} points')
        else:
            print(f'    [{i+1}/{len(dates)}] {ds}: no data')
        time.sleep(0.5)

    if total:
        db.update_sync_log('api_curves', dates[-1], total)


def sync_daily_energy(db, app_id, app_secret, start=None, end=None):
    """Pull daily energy from API for missing months."""
    today = datetime.now().strftime('%Y-%m-%d')
    end = end or today

    if not start:
        existing = db.get_dates_with_data('daily_energy')
        if existing:
            latest = max(existing)
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)
            start = start_dt.strftime('%Y-%m-%d')
        else:
            start = '2024-11-05'

    if start > end:
        print('  Daily energy: up to date.')
        return

    # Collect unique months in the range
    months = sorted(set(d[:7] for d in api_date_range(start, end)))
    print(f'  Pulling daily energy for {len(months)} month(s) ...')

    total = 0
    for ym in months:
        records = pull_daily_energy(app_id, app_secret, ym)
        rows = [(ds, kwh, 'api') for ds, kwh in records
                if kwh > 0 and ds >= start and ds <= end]
        if rows:
            db.upsert_daily_energy(rows)
            total += len(rows)
        time.sleep(0.5)

    print(f'    {total} daily energy records')
    if total:
        db.update_sync_log('api_daily', end, total)


def sync_panel_data(db, app_id, app_secret, start=None, end=None, inverters=None):
    """Pull per-panel batch data from API for missing dates."""
    today = datetime.now().strftime('%Y-%m-%d')
    end = end or today

    if not start:
        existing = db.get_dates_with_data('panel_readings')
        if existing:
            latest = max(existing)
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)
            start = start_dt.strftime('%Y-%m-%d')
        else:
            start = PANEL_DATA_START

    # Panel data only available from Aug 2025
    if start < PANEL_DATA_START:
        start = PANEL_DATA_START

    if start > end:
        print('  Panel data: up to date.')
        return

    dates = [d for d in api_date_range(start, end)
             if d not in db.get_dates_with_data('panel_readings')]
    if not dates:
        print('  Panel data: up to date.')
        return

    print(f'  Pulling {len(dates)} days of panel data from API ({dates[0]} .. {dates[-1]}) ...')
    total = 0
    for i, ds in enumerate(dates):
        times, power_map = pull_panel_batch(app_id, app_secret, ds)
        if times and power_map:
            # Monotonic time reconstruction
            t0 = pd.to_datetime(ds + ' ' + times[0])
            rows = []
            for j, _ in enumerate(times):
                ts = t0 + timedelta(minutes=5 * j)
                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                for key, vals in power_map.items():
                    uid, ch = key.rsplit('-', 1)
                    try:
                        power = float(vals[j]) if j < len(vals) and vals[j] else 0.0
                    except (ValueError, TypeError):
                        power = 0.0
                    rows.append((ts_str, uid, int(ch), power))
            db.upsert_panel_readings(rows)
            total += len(rows)
            n_ch = len(power_map)
            print(f'    [{i+1}/{len(dates)}] {ds}: {n_ch} channels, {len(times)} intervals')
        else:
            print(f'    [{i+1}/{len(dates)}] {ds}: no data')
        time.sleep(0.5)

    if total:
        db.update_sync_log('api_panels', dates[-1], total)

    # Register inverter UIDs
    if inverters is None:
        inverters = pull_inverter_list(app_id, app_secret)
    for uid in inverters:
        db.upsert_inverter(uid)


def sync_inverter_telemetry(db, app_id, app_secret, start=None, end=None, inverters=None):
    """Pull detailed per-inverter telemetry (DC/AC power, voltage, current,
    frequency, temperature) from the single-inverter minutely endpoint."""
    today = datetime.now().strftime('%Y-%m-%d')
    end = end or today

    if not start:
        existing = db.get_dates_with_data('inverter_telemetry')
        if existing:
            latest = max(existing)
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)
            start = start_dt.strftime('%Y-%m-%d')
        else:
            start = PANEL_DATA_START

    if start < PANEL_DATA_START:
        start = PANEL_DATA_START

    if start > end:
        print('  Inverter telemetry: up to date.')
        return

    existing_dates = db.get_dates_with_data('inverter_telemetry')
    dates = [d for d in api_date_range(start, end) if d not in existing_dates]
    if not dates:
        print('  Inverter telemetry: up to date.')
        return

    # Get active inverter UIDs (use cached list if provided)
    if inverters is None:
        inverters = pull_inverter_list(app_id, app_secret)
    if not inverters:
        print('  No inverters found — skipping telemetry.')
        return

    print(f'  Pulling inverter telemetry for {len(dates)} day(s) x '
          f'{len(inverters)} inverters ({dates[0]} .. {dates[-1]}) ...')

    total = 0
    for i, ds in enumerate(dates):
        day_rows = 0
        for uid in inverters:
            data = pull_panel_single_inverter(app_id, app_secret, uid, ds)
            if not data or 't' not in data:
                continue
            times = data['t']
            t0 = pd.to_datetime(ds + ' ' + times[0])
            for j in range(len(times)):
                ts = t0 + timedelta(minutes=5 * j)
                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')

                def fval(key, idx):
                    try:
                        v = data.get(key, [])
                        return float(v[idx]) if idx < len(v) and v[idx] else None
                    except (ValueError, TypeError):
                        return None

                row = (
                    ts_str, uid,
                    fval('dc_p1', j), fval('dc_p2', j),
                    fval('dc_v1', j), fval('dc_v2', j),
                    fval('dc_i1', j), fval('dc_i2', j),
                    fval('dc_e1', j), fval('dc_e2', j),
                    fval('ac_p1', j), fval('ac_v1', j),
                    fval('ac_f', j),  fval('ac_t', j),
                )
                db.conn.execute(
                    'INSERT OR REPLACE INTO inverter_telemetry'
                    ' (timestamp, inverter_uid, dc_p1, dc_p2, dc_v1, dc_v2,'
                    '  dc_i1, dc_i2, dc_e1, dc_e2, ac_p, ac_v, ac_f, ac_t)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', row)
                day_rows += 1
            time.sleep(0.3)  # rate limit per inverter
        db.conn.commit()
        total += day_rows
        print(f'    [{i+1}/{len(dates)}] {ds}: {day_rows} rows '
              f'({day_rows // max(len(inverters), 1)} intervals x {len(inverters)} inverters)')

    if total:
        db.update_sync_log('api_telemetry', dates[-1], total)


# ======================================================================
# Status report
# ======================================================================

def show_status(db):
    """Print a summary of what's in the database."""
    print('\n=== Solar Database Status ===')
    print(f'  DB file: {db.db_path}')
    print(f'  Size: {os.path.getsize(db.db_path) / 1024 / 1024:.1f} MB')

    tables = [
        ('system_readings',     'System Readings (5-min)'),
        ('daily_energy',        'Daily Energy'),
        ('panel_readings',      'Panel Readings (per-channel)'),
        ('inverter_telemetry',  'Inverter Telemetry (detailed)'),
        ('billing_periods',     'Billing Periods'),
        ('finance',             'Finance (HELOC)'),
        ('inverters',           'Inverters'),
    ]
    for table, label in tables:
        mn, mx, cnt = db.get_date_range(table)
        if cnt:
            dates_ct = len(db.get_dates_with_data(table)) if table != 'inverters' else cnt
            print(f'\n  {label}:')
            print(f'    Rows: {cnt:,}')
            print(f'    Range: {mn} .. {mx}')
            if table not in ('inverters',):
                print(f'    Unique dates: {dates_ct}')
        else:
            print(f'\n  {label}: (empty)')

    # Sync log
    sync = db.get_sync_status()
    if not sync.empty:
        print(f'\n  Sync Log:')
        for _, row in sync.iterrows():
            print(f'    {row["source"]:15s}  last: {row["last_date"]}  '
                  f'at: {row["synced_at"][:19]}  rows: {row["record_count"]}')

    print()


# ======================================================================
# CLI
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Solar data sync service — backfill from local files '
                    'and/or pull new data from the EMA API into solar.db')
    parser.add_argument('--backfill', action='store_true',
                        help='Import all existing local files (XLS, CSV)')
    parser.add_argument('--sync', action='store_true',
                        help='Pull new data from the EMA API')
    parser.add_argument('--status', action='store_true',
                        help='Show database contents summary')
    parser.add_argument('--import-billing', metavar='FILE',
                        help='Import a billing CSV file')
    parser.add_argument('--import-finance', metavar='FILE',
                        help='Import a finance CSV file')
    parser.add_argument('--start', help='Override start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='Override end date (YYYY-MM-DD)')
    parser.add_argument('--db', help='Database path (default: solar.db)')
    args = parser.parse_args()

    if not any([args.backfill, args.sync, args.status,
                args.import_billing, args.import_finance]):
        parser.print_help()
        return

    db = SolarDB(db_path=args.db)
    print(f'Database: {db.db_path}')

    try:
        # --- Backfill from local files ---
        if args.backfill:
            print('\n--- Backfill from local files ---')
            backfill_power_curves(db)
            backfill_daily_energy(db)
            backfill_panel_data(db)
            backfill_billing(db)
            backfill_finance(db)

        # --- One-off CSV imports ---
        if args.import_billing:
            backfill_billing(db, filepath=args.import_billing)
        if args.import_finance:
            backfill_finance(db, filepath=args.import_finance)

        # --- API sync ---
        if args.sync:
            print('\n--- API sync ---')
            app_id, app_secret = load_credentials()
            print(f'  EMA API ready (App ID: {app_id[:8]}...)')
            # Fetch inverter list once for all sync steps
            inverters = pull_inverter_list(app_id, app_secret)
            print(f'  Cached {len(inverters)} inverter UIDs (1 API call)')
            sync_power_curves(db, app_id, app_secret,
                              start=args.start, end=args.end)
            sync_daily_energy(db, app_id, app_secret,
                              start=args.start, end=args.end)
            sync_panel_data(db, app_id, app_secret,
                            start=args.start, end=args.end,
                            inverters=inverters)
            sync_inverter_telemetry(db, app_id, app_secret,
                                    start=args.start, end=args.end,
                                    inverters=inverters)
            # Show today's summary
            print('\n  System summary:')
            pull_system_summary(app_id, app_secret)

        # --- Status ---
        if args.status or args.backfill or args.sync:
            show_status(db)

    finally:
        db.close()

    print('Done.')


if __name__ == '__main__':
    main()
