#!/usr/bin/env python3
"""
APsystems EMA OpenAPI Data Puller
==================================
Pulls daily energy reports and 5-min power curves from the APsystems EMA API,
saving them in the same formats consumed by solar_analysis.py and build_interactive.py.

Setup:
  1. Email APsystems support to get your App Id and App Secret.
  2. Create a file called .ema_credentials in this directory (or set env vars):
       APP_ID=your_32char_app_id
       APP_SECRET=your_12char_app_secret
  3. Run:  python ema_api_pull.py
     Or with date range:  python ema_api_pull.py --start 2026-03-01 --end 2026-03-21

The script auto-detects which dates are already on disk and only pulls missing ones.
"""

import argparse
import hashlib
import hmac
import base64
import os
import sys
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or .env file
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CURVE_DIR = os.path.join(DATA_DIR, 'daily_prod_curves')
BASE_URL = os.environ.get('EMA_BASE_URL', 'https://api.apsystemsema.com:9282')

SYSTEM_ID = os.environ.get('EMA_SYSTEM_ID', '')
ECU_ID = os.environ.get('EMA_ECU_ID', '')


def load_credentials():
    """Load APP_ID and APP_SECRET from env vars, .env, or .ema_credentials."""
    app_id = os.environ.get('EMA_APP_ID', '')
    app_secret = os.environ.get('EMA_APP_SECRET', '')

    # Try .env file, then legacy .ema_credentials
    for cred_name in ['.env', '.ema_credentials']:
        if app_id and app_secret:
            break
        cred_file = os.path.join(DATA_DIR, cred_name)
        if not os.path.exists(cred_file):
            continue
        with open(cred_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key, val = key.strip(), val.strip()
                if key in ('APP_ID', 'EMA_APP_ID') and not app_id:
                    app_id = val
                elif key in ('APP_SECRET', 'EMA_APP_SECRET') and not app_secret:
                    app_secret = val
                elif key == 'EMA_SYSTEM_ID':
                    global SYSTEM_ID
                    SYSTEM_ID = val
                elif key == 'EMA_ECU_ID':
                    global ECU_ID
                    ECU_ID = val

    if not app_id or not app_secret:
        print('ERROR: Missing API credentials.')
        print('Set EMA_APP_ID / EMA_APP_SECRET in .env or environment.')
        sys.exit(1)

    return app_id, app_secret


def compute_signature(app_id, app_secret, timestamp, nonce, request_path, method='GET',
                      sig_method='HmacSHA256'):
    """Compute the HMAC signature per the APsystems OpenAPI spec."""
    string_to_sign = f'{timestamp}/{nonce}/{app_id}/{request_path}/{method}/{sig_method}'
    secret_bytes = app_secret.encode('utf-8')
    message_bytes = string_to_sign.encode('utf-8')

    if sig_method == 'HmacSHA256':
        mac = hmac.new(secret_bytes, message_bytes, hashlib.sha256)
    else:
        mac = hmac.new(secret_bytes, message_bytes, hashlib.sha1)

    return base64.b64encode(mac.digest()).decode('utf-8')


def api_request(app_id, app_secret, path, params=None, method='GET'):
    """Make an authenticated request to the EMA OpenAPI."""
    # request_path = last segment of the URL path
    request_path = path.rstrip('/').rsplit('/', 1)[-1]
    timestamp = str(int(time.time() * 1000))
    nonce = uuid.uuid4().hex
    sig_method = 'HmacSHA256'

    signature = compute_signature(app_id, app_secret, timestamp, nonce,
                                  request_path, method, sig_method)

    headers = {
        'X-CA-AppId': app_id,
        'X-CA-Timestamp': timestamp,
        'X-CA-Nonce': nonce,
        'X-CA-Signature-Method': sig_method,
        'X-CA-Signature': signature,
    }

    url = BASE_URL + path
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get('code') != 0:
        code = data.get('code', '?')
        print(f'  API error {code} on {path}: {data}')
        return None
    return data.get('data')


# ---------------------------------------------------------------------------
# Pull functions
# ---------------------------------------------------------------------------

def pull_system_summary(app_id, app_secret):
    """Pull and display the system energy summary."""
    path = f'/user/api/v2/systems/summary/{SYSTEM_ID}'
    data = api_request(app_id, app_secret, path)
    if data:
        print(f'\n  System Summary:')
        print(f'    Today:    {data.get("today", "?")} kWh')
        print(f'    Month:    {data.get("month", "?")} kWh')
        print(f'    Year:     {data.get("year", "?")} kWh')
        print(f'    Lifetime: {data.get("lifetime", "?")} kWh')
    return data


def pull_inverter_list(app_id, app_secret):
    """Get the list of inverter UIDs for the system."""
    path = f'/user/api/v2/systems/inverters/{SYSTEM_ID}'
    data = api_request(app_id, app_secret, path)
    if not data:
        return []
    # Response is a list of ECU dicts, each with an 'inverter' list
    if isinstance(data, list) and data and isinstance(data[0], dict):
        inverters = []
        for ecu in data:
            for inv in ecu.get('inverter', []):
                uid = inv.get('uid', '')
                if uid:
                    inverters.append(uid)
        return inverters
    return []


def pull_panel_batch(app_id, app_secret, date_str):
    """Pull per-inverter-channel 5-min power telemetry for all inverters in one call.
    Returns (times, power_dict) where power_dict maps 'uid-channel' -> [watts...]."""
    path = f'/user/api/v2/systems/{SYSTEM_ID}/devices/inverter/batch/energy/{ECU_ID}'
    params = {'energy_level': 'power', 'date_range': date_str}
    data = api_request(app_id, app_secret, path, params)
    if not data or 'time' not in data:
        return None, None
    times = data['time']
    power_map = data.get('power', {})
    return times, power_map


def pull_panel_single_inverter(app_id, app_secret, uid, date_str):
    """Pull detailed per-channel minutely data for a single inverter.
    Returns the full data dict with dc_p1, dc_p2, ac_p, etc."""
    path = f'/user/api/v2/systems/{SYSTEM_ID}/devices/inverter/energy/{uid}'
    params = {'energy_level': 'minutely', 'date_range': date_str}
    data = api_request(app_id, app_secret, path, params)
    return data


def pull_daily_energy(app_id, app_secret, year_month):
    """Pull daily energy for a given month (format: 'YYYY-MM').
    Returns list of (date_str, kwh) tuples."""
    path = f'/user/api/v2/systems/{SYSTEM_ID}/devices/ecu/energy/{ECU_ID}'
    params = {'energy_level': 'daily', 'date_range': year_month}
    data = api_request(app_id, app_secret, path, params)
    if not data:
        return []

    year, month = year_month.split('-')
    results = []
    for day_idx, val in enumerate(data, start=1):
        kwh = float(val) if val else 0.0
        date_str = f'{year}-{month}-{day_idx:02d}'
        results.append((date_str, kwh))
    return results


def pull_power_curve(app_id, app_secret, date_str):
    """Pull 5-min power telemetry for a single day (format: 'YYYY-MM-DD').
    Returns list of (time_str, power_w) tuples."""
    path = f'/user/api/v2/systems/{SYSTEM_ID}/devices/ecu/energy/{ECU_ID}'
    params = {'energy_level': 'minutely', 'date_range': date_str}
    data = api_request(app_id, app_secret, path, params)
    if not data or 'time' not in data:
        return []

    times = data['time']  # list of "HH:mm"
    powers = data.get('power', [])  # list of power values in W
    results = []
    for t, p in zip(times, powers):
        results.append((t, float(p) if p else 0.0))
    return results


# ---------------------------------------------------------------------------
# File I/O — save in formats matching existing data
# ---------------------------------------------------------------------------

def existing_curve_dates():
    """Return set of date strings already on disk in daily_prod_curves/."""
    dates = set()
    if not os.path.isdir(CURVE_DIR):
        return dates
    for f in os.listdir(CURVE_DIR):
        if f.endswith('.xls') and ':' not in f:
            import re
            m = re.search(r'(\d{4}-\d{2}-\d{2})', f)
            if m:
                dates.add(m.group(1))
    return dates


def save_power_curve_xls(date_str, records):
    """Save power curve as .xls matching the format of existing files."""
    if not records:
        return
    os.makedirs(CURVE_DIR, exist_ok=True)
    df = pd.DataFrame(records, columns=['time', 'Power(W)'])
    fname = f'Power Curve for {SYSTEM_ID} in {date_str}.xls'
    fpath = os.path.join(CURVE_DIR, fname)
    df.to_excel(fpath, index=False)
    return fpath


def save_daily_energy_xls(records, start_date, end_date):
    """Save daily energy as .xls matching existing report format."""
    if not records:
        return
    df = pd.DataFrame(records, columns=['Date', 'Energy(kWh)'])
    df = df[df['Energy(kWh)'] > 0]
    if df.empty:
        return
    fname = f'Daily Energy Report {start_date} to {end_date}.xls'
    fpath = os.path.join(DATA_DIR, fname)
    df.to_excel(fpath, index=False)
    print(f'  Saved {fname} ({len(df)} days)')
    return fpath


PANEL_DIR = os.path.join(DATA_DIR, 'panel_data')


def save_panel_data_csv(date_str, times, power_map):
    """Save per-channel power data as a CSV.
    Columns: time, uid-1, uid-2, ..., total."""
    if not times or not power_map:
        return
    os.makedirs(PANEL_DIR, exist_ok=True)
    df = pd.DataFrame({'time': times})
    total = None
    for key in sorted(power_map.keys()):
        vals = [float(v) if v else 0.0 for v in power_map[key]]
        # Pad or trim to match time length
        if len(vals) < len(times):
            vals.extend([0.0] * (len(times) - len(vals)))
        elif len(vals) > len(times):
            vals = vals[:len(times)]
        df[key] = vals
        if total is None:
            total = pd.Series(vals, dtype=float)
        else:
            total = total + pd.Series(vals, dtype=float)
    if total is not None:
        df['total'] = total.values
    fpath = os.path.join(PANEL_DIR, f'panels_{date_str}.csv')
    df.to_csv(fpath, index=False)
    return fpath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def date_range(start, end):
    """Yield date strings from start to end inclusive."""
    current = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    while current <= end_dt:
        yield current.strftime('%Y-%m-%d')
        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description='Pull data from APsystems EMA OpenAPI')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD). Default: day after latest on disk.')
    parser.add_argument('--end', help='End date (YYYY-MM-DD). Default: yesterday.')
    parser.add_argument('--summary', action='store_true', help='Just print system summary and exit.')
    parser.add_argument('--panels', action='store_true', help='Pull per-panel (inverter-channel) minutely data.')
    parser.add_argument('--panels-probe', action='store_true', help='Probe how far back per-panel data is available.')
    parser.add_argument('--skip-curves', action='store_true', help='Skip power curve downloads.')
    parser.add_argument('--skip-daily', action='store_true', help='Skip daily energy report.')
    parser.add_argument('--force', action='store_true', help='Re-download even if files exist.')
    args = parser.parse_args()

    app_id, app_secret = load_credentials()
    print(f'EMA OpenAPI client ready  (App ID: {app_id[:8]}...)')

    if args.summary:
        pull_system_summary(app_id, app_secret)
        return

    # --- Per-panel probe: find how far back per-panel data goes ---
    if args.panels_probe:
        print('Probing per-panel data availability ...')
        inverters = pull_inverter_list(app_id, app_secret)
        if not inverters:
            print('  No inverters found.')
            return
        test_uid = inverters[0]
        print(f'  Test inverter: {test_uid}')
        # Walk backwards from yesterday, first-of-month, until no data
        today = datetime.now()
        probe_dates = []
        # Recent week
        for i in range(1, 8):
            probe_dates.append((today - timedelta(days=i)).strftime('%Y-%m-%d'))
        # First of each month going back 18 months
        for m in range(0, 18):
            dt = today.replace(day=1) - timedelta(days=m * 30)
            probe_dates.append(dt.replace(day=1).strftime('%Y-%m-%d'))
        probe_dates = sorted(set(probe_dates))
        has_data = []
        no_data = []
        for ds in probe_dates:
            result = pull_panel_single_inverter(app_id, app_secret, test_uid, ds)
            tag = 'OK' if result else '--'
            (has_data if result else no_data).append(ds)
            print(f'    {ds}: {tag}')
            time.sleep(0.5)
        if has_data:
            print(f'\n  Data available: {min(has_data)} .. {max(has_data)} ({len(has_data)} of {len(probe_dates)} probed dates)')
        else:
            print('  No data found for any probed date.')
        return

    # --- Per-panel pull ---
    if args.panels:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start = args.start or yesterday
        end = args.end or yesterday
        panel_dates = list(date_range(start, end))
        # Check which dates already have CSVs
        existing_panel = set()
        if os.path.isdir(PANEL_DIR):
            for fn in os.listdir(PANEL_DIR):
                if fn.startswith('panels_') and fn.endswith('.csv'):
                    existing_panel.add(fn[7:17])  # panels_YYYY-MM-DD.csv
        if not args.force:
            panel_dates = [d for d in panel_dates if d not in existing_panel]
        if not panel_dates:
            print('  All panel data already on disk.')
            return
        print(f'  Pulling per-panel data for {len(panel_dates)} day(s): {panel_dates[0]} .. {panel_dates[-1]}')
        for i, ds in enumerate(panel_dates):
            times, power_map = pull_panel_batch(app_id, app_secret, ds)
            if times and power_map:
                fpath = save_panel_data_csv(ds, times, power_map)
                print(f'  [{i+1}/{len(panel_dates)}] {ds}: {len(power_map)} channels -> {os.path.basename(fpath)}')
            else:
                print(f'  [{i+1}/{len(panel_dates)}] {ds}: no data')
            time.sleep(0.5)
        print('Done!')
        return

    # Determine date range
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    if args.start:
        start = args.start
    else:
        existing = existing_curve_dates()
        if existing:
            latest = max(existing)
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)
            start = start_dt.strftime('%Y-%m-%d')
            print(f'  Latest data on disk: {latest}')
        else:
            start = '2024-11-05'  # system online date

    end = args.end or yesterday

    if start > end:
        print(f'  Already up to date (latest on disk >= {end})')
        return

    print(f'  Pulling data: {start} -> {end}')
    dates_to_pull = list(date_range(start, end))
    existing_dates = existing_curve_dates() if not args.force else set()

    # --- Power curves ---
    if not args.skip_curves:
        new_curves = [d for d in dates_to_pull if d not in existing_dates]
        if new_curves:
            print(f'\nPulling {len(new_curves)} power curves ...')
            for i, ds in enumerate(new_curves):
                records = pull_power_curve(app_id, app_secret, ds)
                if records:
                    save_power_curve_xls(ds, records)
                    print(f'  [{i+1}/{len(new_curves)}] {ds}: {len(records)} points')
                else:
                    print(f'  [{i+1}/{len(new_curves)}] {ds}: no data')
                # Rate limiting — be respectful to the API
                time.sleep(0.5)
        else:
            print('  All power curves already on disk.')

    # --- Daily energy ---
    if not args.skip_daily:
        print(f'\nPulling daily energy ...')
        # Collect unique months in the range
        months = sorted(set(d[:7] for d in dates_to_pull))
        all_daily = []
        for ym in months:
            records = pull_daily_energy(app_id, app_secret, ym)
            for ds, kwh in records:
                if ds >= start and ds <= end:
                    all_daily.append((ds, kwh))
            time.sleep(0.5)

        if all_daily:
            save_daily_energy_xls(all_daily, start, end)

    # --- Summary ---
    print('\nSystem summary:')
    pull_system_summary(app_id, app_secret)
    print('\nDone!')


if __name__ == '__main__':
    main()
