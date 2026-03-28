#!/usr/bin/env python3
"""
ez1_logger.py — Poll APsystems EZ1 micro-inverter local API and log to SQLite.

The EZ1 is a standalone side-panel inverter, separate from the main roof array.
It exposes a simple REST API on the local network (port 8050).

Usage:
  python ez1_logger.py                # single poll → insert → exit
  python ez1_logger.py --loop 60      # poll every 60 s until stopped
  python ez1_logger.py --info         # print device info and exit

Environment:
  EZ1_IP   — inverter LAN IP (required, from .env)
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Load .env ────────────────────────────────────────────────
_env_file = os.path.join(SERVICE_DIR, '.env')
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

EZ1_IP = os.environ.get('EZ1_IP', '')
EZ1_PORT = int(os.environ.get('EZ1_PORT', '8050'))
DB_PATH = os.path.join(SERVICE_DIR, 'ez1.db')

# ── DB schema ────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS ez1_readings (
    timestamp   TEXT PRIMARY KEY,   -- YYYY-MM-DD HH:MM:SS local
    p1          REAL,               -- channel 1 power (W)
    p2          REAL,               -- channel 2 power (W)
    e1          REAL,               -- channel 1 energy since startup (kWh)
    e2          REAL,               -- channel 2 energy since startup (kWh)
    te1         REAL,               -- channel 1 lifetime energy (kWh)
    te2         REAL                -- channel 2 lifetime energy (kWh)
);

CREATE TABLE IF NOT EXISTS ez1_device (
    device_id   TEXT PRIMARY KEY,
    dev_ver     TEXT,
    min_power   INTEGER,
    max_power   INTEGER,
    last_seen   TEXT
);

CREATE TABLE IF NOT EXISTS ez1_alarms (
    timestamp   TEXT PRIMARY KEY,
    off_grid    INTEGER,            -- 0=normal 1=alarm
    output_err  INTEGER,
    dc1_short   INTEGER,
    dc2_short   INTEGER
);

CREATE TABLE IF NOT EXISTS ez1_daily (
    date        TEXT PRIMARY KEY,   -- YYYY-MM-DD
    energy_kwh  REAL,               -- total daily production (ch1+ch2)
    peak_w      REAL,               -- max combined power
    hours       REAL                -- hours with production > 0
);
"""


def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    return conn


def _base_url():
    if not EZ1_IP:
        print('ERROR: EZ1_IP not set. Add EZ1_IP=<ip> to .env', file=sys.stderr)
        sys.exit(1)
    return f'http://{EZ1_IP}:{EZ1_PORT}'


def _fetch_json(path, timeout=5):
    url = f'{_base_url()}{path}'
    try:
        req = Request(url)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (URLError, OSError, json.JSONDecodeError) as e:
        print(f'  EZ1 API error ({url}): {e}', file=sys.stderr)
        return None


# ── API wrappers ─────────────────────────────────────────────

def get_device_info():
    return _fetch_json('/getDeviceInfo')


def get_output_data():
    return _fetch_json('/getOutputData')


def get_max_power():
    return _fetch_json('/getMaxPower')


def get_alarms():
    return _fetch_json('/getAlarm')


# ── Logging ──────────────────────────────────────────────────

def poll_and_log():
    """Single poll: read output data + alarms, write to DB."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    today = now[:10]

    output = get_output_data()
    if not output or output.get('message') != 'SUCCESS':
        print(f'  [{now}] No output data from EZ1')
        return False

    d = output['data']
    p1 = float(d.get('p1', 0))
    p2 = float(d.get('p2', 0))
    e1 = float(d.get('e1', 0))
    e2 = float(d.get('e2', 0))
    te1 = float(d.get('te1', 0))
    te2 = float(d.get('te2', 0))

    conn = _get_db()
    conn.execute(
        'INSERT OR REPLACE INTO ez1_readings'
        ' (timestamp, p1, p2, e1, e2, te1, te2)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?)',
        (now, p1, p2, e1, e2, te1, te2))

    # Update daily rollup
    rows = conn.execute(
        'SELECT p1+p2 as ptotal FROM ez1_readings WHERE timestamp LIKE ?',
        (today + '%',)).fetchall()
    total_power_samples = [r[0] for r in rows]
    peak_w = max(total_power_samples) if total_power_samples else 0
    producing_count = sum(1 for p in total_power_samples if p > 0)
    # Approximate hours: count of readings with power > 0, spaced by poll interval
    # For daily energy, use the lifetime counter diff
    first_te = conn.execute(
        'SELECT te1+te2 FROM ez1_readings WHERE timestamp LIKE ? ORDER BY timestamp ASC LIMIT 1',
        (today + '%',)).fetchone()
    last_te = te1 + te2
    first_te_val = first_te[0] if first_te else last_te
    daily_kwh = max(0, last_te - first_te_val)

    # Estimate producing hours from sample count
    # (assumes roughly uniform polling interval)
    total_samples = len(total_power_samples)
    if total_samples > 1:
        first_ts = conn.execute(
            'SELECT timestamp FROM ez1_readings WHERE timestamp LIKE ? ORDER BY timestamp ASC LIMIT 1',
            (today + '%',)).fetchone()[0]
        last_ts = now
        t0 = datetime.strptime(first_ts, '%Y-%m-%d %H:%M:%S')
        t1 = datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S')
        span_hours = (t1 - t0).total_seconds() / 3600
        hours = span_hours * producing_count / total_samples if total_samples else 0
    else:
        hours = 0

    conn.execute(
        'INSERT OR REPLACE INTO ez1_daily (date, energy_kwh, peak_w, hours)'
        ' VALUES (?, ?, ?, ?)',
        (today, round(daily_kwh, 4), round(peak_w, 1), round(hours, 2)))

    # Log alarms
    alarm_resp = get_alarms()
    if alarm_resp and alarm_resp.get('message') == 'SUCCESS':
        a = alarm_resp['data']
        conn.execute(
            'INSERT OR REPLACE INTO ez1_alarms'
            ' (timestamp, off_grid, output_err, dc1_short, dc2_short)'
            ' VALUES (?, ?, ?, ?, ?)',
            (now, int(a.get('og', 0)), int(a.get('oe', 0)),
             int(a.get('isce1', 0)), int(a.get('isce2', 0))))

    # Update device info (infrequent, but keep last_seen fresh)
    info = get_device_info()
    if info and info.get('message') == 'SUCCESS':
        di = info['data']
        conn.execute(
            'INSERT OR REPLACE INTO ez1_device'
            ' (device_id, dev_ver, min_power, max_power, last_seen)'
            ' VALUES (?, ?, ?, ?, ?)',
            (di.get('deviceId', ''), di.get('devVer', ''),
             int(di.get('minPower', 0)), int(di.get('maxPower', 800)), now))

    conn.commit()
    conn.close()
    total_w = p1 + p2
    print(f'  [{now}] p1={p1:.0f}W  p2={p2:.0f}W  total={total_w:.0f}W  day={daily_kwh:.3f}kWh')
    return True


def print_device_info():
    info = get_device_info()
    if not info:
        print('Could not reach EZ1')
        return
    print(json.dumps(info, indent=2))
    output = get_output_data()
    if output:
        print(json.dumps(output, indent=2))
    alarm = get_alarms()
    if alarm:
        print(json.dumps(alarm, indent=2))


# ── Main ─────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='APsystems EZ1 data logger')
    parser.add_argument('--info', action='store_true',
                        help='Print device info and exit')
    parser.add_argument('--loop', type=int, metavar='SECS',
                        help='Poll every N seconds (default: single poll)')
    args = parser.parse_args()

    if args.info:
        print_device_info()
        sys.exit(0)

    if args.loop:
        print(f'EZ1 logger: polling {EZ1_IP}:{EZ1_PORT} every {args.loop}s  (Ctrl+C to stop)')
        while True:
            try:
                poll_and_log()
            except Exception as e:
                print(f'  Poll error: {e}', file=sys.stderr)
            time.sleep(args.loop)
    else:
        poll_and_log()
