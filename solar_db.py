#!/usr/bin/env python3
"""
solar_db.py — SQLite data store for solar production, billing, and financial data.

Schema is designed for:
  - Direct querying by analysis/visualization scripts via pd.read_sql()
  - Narrow time-series format (tag + field) for future InfluxDB migration
  - Idempotent upsert semantics (safe to re-import)

Tables:
  system_readings   — 5-min system-level power (W)
  daily_energy     — daily kWh totals
  panel_readings   — per-inverter-channel 5-min power (narrow)
  inverters        — inverter registry
  billing_periods  — utility meter readings
  finance          — HELOC balance tracking
  sync_log         — incremental sync state
"""

import os
import sqlite3
from datetime import datetime

import pandas as pd

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SERVICE_DIR)
DEFAULT_DB = os.path.join(SERVICE_DIR, 'solar.db')

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS system_readings (
    timestamp TEXT NOT NULL PRIMARY KEY,   -- YYYY-MM-DD HH:MM:SS (local)
    power_w   REAL NOT NULL,
    source    TEXT                          -- 'xls' | 'api'
);
CREATE INDEX IF NOT EXISTS idx_system_date
    ON system_readings (SUBSTR(timestamp, 1, 10));

CREATE TABLE IF NOT EXISTS daily_energy (
    date       TEXT NOT NULL PRIMARY KEY,  -- YYYY-MM-DD
    energy_kwh REAL NOT NULL,
    source     TEXT
);

CREATE TABLE IF NOT EXISTS panel_readings (
    timestamp    TEXT    NOT NULL,          -- YYYY-MM-DD HH:MM:SS (local)
    inverter_uid TEXT    NOT NULL,
    channel      INTEGER NOT NULL,         -- 1 or 2
    power_w      REAL    NOT NULL,
    PRIMARY KEY (timestamp, inverter_uid, channel)
);
CREATE INDEX IF NOT EXISTS idx_panel_date
    ON panel_readings (SUBSTR(timestamp, 1, 10));
CREATE INDEX IF NOT EXISTS idx_panel_uid
    ON panel_readings (inverter_uid);

CREATE TABLE IF NOT EXISTS inverters (
    uid         TEXT PRIMARY KEY,
    is_active   INTEGER NOT NULL DEFAULT 1,
    replaced_by TEXT,
    first_seen  TEXT,
    last_seen   TEXT
);

CREATE TABLE IF NOT EXISTS billing_periods (
    meter_date             TEXT PRIMARY KEY,  -- YYYY-MM-DD
    energy_consumed_kwh    REAL,
    energy_produced_kwh    REAL,
    actual_bill            REAL,
    est_bill_without_solar REAL
);

CREATE TABLE IF NOT EXISTS finance (
    date          TEXT PRIMARY KEY,  -- YYYY-MM-DD (first of month)
    heloc_balance REAL,
    interest      REAL
);

CREATE TABLE IF NOT EXISTS inverter_telemetry (
    timestamp    TEXT    NOT NULL,          -- YYYY-MM-DD HH:MM:SS (local)
    inverter_uid TEXT    NOT NULL,
    dc_p1        REAL,                       -- DC power channel 1 (W)
    dc_p2        REAL,                       -- DC power channel 2 (W)
    dc_v1        REAL,                       -- DC voltage channel 1 (V)
    dc_v2        REAL,                       -- DC voltage channel 2 (V)
    dc_i1        REAL,                       -- DC current channel 1 (A)
    dc_i2        REAL,                       -- DC current channel 2 (A)
    dc_e1        REAL,                       -- DC cumulative energy ch1 (kWh)
    dc_e2        REAL,                       -- DC cumulative energy ch2 (kWh)
    ac_p         REAL,                       -- AC output power (W)
    ac_v         REAL,                       -- AC grid voltage (V)
    ac_f         REAL,                       -- AC grid frequency (Hz)
    ac_t         REAL,                       -- inverter temperature (°C)
    PRIMARY KEY (timestamp, inverter_uid)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_date
    ON inverter_telemetry (SUBSTR(timestamp, 1, 10));
CREATE INDEX IF NOT EXISTS idx_telemetry_uid
    ON inverter_telemetry (inverter_uid);

CREATE TABLE IF NOT EXISTS panels (
    inverter_uid TEXT    NOT NULL,
    channel      INTEGER NOT NULL,          -- 1 or 2
    panel_name   TEXT,                       -- friendly name e.g. "A1", "West-3"
    array_name   TEXT,                       -- group: "West Roof", "South Roof"
    array_row    INTEGER,                    -- row position within array
    array_col    INTEGER,                    -- column position within array
    tilt_deg     REAL,                       -- tilt angle in degrees
    azimuth_deg  REAL,                       -- orientation degrees from north (180=south)
    model        TEXT,                       -- e.g. "Phono Solar PS410M7GFH-18/VNH"
    capacity_w   REAL,                       -- nameplate watts
    width_mm     REAL,                       -- physical width
    height_mm    REAL,                       -- physical height
    install_date TEXT,                       -- YYYY-MM-DD
    notes        TEXT,
    PRIMARY KEY (inverter_uid, channel)
);

CREATE TABLE IF NOT EXISTS sync_log (
    source       TEXT PRIMARY KEY,  -- e.g. 'xls_curves', 'api_panels', ...
    last_date    TEXT NOT NULL,
    synced_at    TEXT NOT NULL,
    record_count INTEGER
);
"""


class SolarDB:
    def __init__(self, db_path=None):
        self.db_path = db_path or DEFAULT_DB
        self._conn = None
        self._ensure_schema()

    @property
    def conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.execute('PRAGMA journal_mode=WAL')
            self._conn.execute('PRAGMA foreign_keys=ON')
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _ensure_schema(self):
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Upsert methods (INSERT OR REPLACE — idempotent)
    # ------------------------------------------------------------------

    def upsert_system_readings(self, rows):
        """rows: [(timestamp_str, power_w, source), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO system_readings (timestamp, power_w, source)'
            ' VALUES (?, ?, ?)', rows)
        self.conn.commit()

    def upsert_daily_energy(self, rows):
        """rows: [(date_str, energy_kwh, source), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO daily_energy (date, energy_kwh, source)'
            ' VALUES (?, ?, ?)', rows)
        self.conn.commit()

    def upsert_panel_readings(self, rows):
        """rows: [(timestamp_str, inverter_uid, channel, power_w), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO panel_readings'
            ' (timestamp, inverter_uid, channel, power_w)'
            ' VALUES (?, ?, ?, ?)', rows)
        self.conn.commit()

    def upsert_inverter_telemetry(self, rows):
        """rows: [(timestamp, uid, dc_p1, dc_p2, dc_v1, dc_v2,
                   dc_i1, dc_i2, dc_e1, dc_e2, ac_p, ac_v, ac_f, ac_t), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO inverter_telemetry'
            ' (timestamp, inverter_uid, dc_p1, dc_p2, dc_v1, dc_v2,'
            '  dc_i1, dc_i2, dc_e1, dc_e2, ac_p, ac_v, ac_f, ac_t)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
        self.conn.commit()

    def upsert_billing(self, rows):
        """rows: [(meter_date, consumed, produced, actual_bill, est_bill), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO billing_periods'
            ' (meter_date, energy_consumed_kwh, energy_produced_kwh,'
            '  actual_bill, est_bill_without_solar)'
            ' VALUES (?, ?, ?, ?, ?)', rows)
        self.conn.commit()

    def upsert_finance(self, rows):
        """rows: [(date_str, heloc_balance, interest), ...]"""
        self.conn.executemany(
            'INSERT OR REPLACE INTO finance (date, heloc_balance, interest)'
            ' VALUES (?, ?, ?)', rows)
        self.conn.commit()

    def upsert_inverter(self, uid, is_active=1, replaced_by=None,
                        first_seen=None, last_seen=None):
        self.conn.execute('''
            INSERT INTO inverters (uid, is_active, replaced_by, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                is_active   = COALESCE(excluded.is_active, is_active),
                replaced_by = COALESCE(excluded.replaced_by, replaced_by),
                first_seen  = MIN(COALESCE(excluded.first_seen, first_seen),
                                  COALESCE(first_seen, excluded.first_seen)),
                last_seen   = MAX(COALESCE(excluded.last_seen, last_seen),
                                  COALESCE(last_seen, excluded.last_seen))
        ''', (uid, is_active, replaced_by, first_seen, last_seen))
        self.conn.commit()

    def upsert_panel_config(self, inverter_uid, channel, **fields):
        """Update panel metadata. Only supplied fields are changed."""
        allowed = {'panel_name', 'array_name', 'array_row', 'array_col',
                   'tilt_deg', 'azimuth_deg', 'model', 'capacity_w',
                   'width_mm', 'height_mm', 'install_date', 'notes'}
        updates = {k: v for k, v in fields.items() if k in allowed}
        # Ensure row exists
        self.conn.execute(
            'INSERT OR IGNORE INTO panels (inverter_uid, channel) VALUES (?, ?)',
            (inverter_uid, channel))
        if updates:
            set_clause = ', '.join(f'{k} = ?' for k in updates)
            vals = list(updates.values()) + [inverter_uid, channel]
            self.conn.execute(
                f'UPDATE panels SET {set_clause}'
                f' WHERE inverter_uid = ? AND channel = ?', vals)
        self.conn.commit()

    def upsert_panel_configs_bulk(self, rows):
        """rows: list of dicts with inverter_uid, channel, + optional fields."""
        for row in rows:
            uid = row['inverter_uid']
            ch = row['channel']
            fields = {k: v for k, v in row.items()
                      if k not in ('inverter_uid', 'channel')}
            self.upsert_panel_config(uid, ch, **fields)

    def get_panels(self):
        return pd.read_sql(
            'SELECT * FROM panels ORDER BY array_name, array_row, array_col,'
            ' inverter_uid, channel', self.conn)

    def update_sync_log(self, source, last_date, record_count=None):
        self.conn.execute(
            'INSERT OR REPLACE INTO sync_log'
            ' (source, last_date, synced_at, record_count)'
            ' VALUES (?, ?, ?, ?)',
            (source, last_date, datetime.now().isoformat(), record_count))
        self.conn.commit()

    # ------------------------------------------------------------------
    # Query methods — return DataFrames for easy downstream use
    # ------------------------------------------------------------------

    def get_system_readings(self, start_date=None, end_date=None):
        q = 'SELECT timestamp, power_w, source FROM system_readings'
        params, clauses = [], []
        if start_date:
            clauses.append('timestamp >= ?')
            params.append(start_date)
        if end_date:
            clauses.append('timestamp <= ?')
            params.append(end_date + ' 23:59:59' if len(end_date) == 10 else end_date)
        if clauses:
            q += ' WHERE ' + ' AND '.join(clauses)
        q += ' ORDER BY timestamp'
        return pd.read_sql(q, self.conn, params=params, parse_dates=['timestamp'])

    def get_daily_energy(self, start_date=None, end_date=None):
        q = 'SELECT date, energy_kwh, source FROM daily_energy'
        params, clauses = [], []
        if start_date:
            clauses.append('date >= ?'); params.append(start_date)
        if end_date:
            clauses.append('date <= ?'); params.append(end_date)
        if clauses:
            q += ' WHERE ' + ' AND '.join(clauses)
        q += ' ORDER BY date'
        return pd.read_sql(q, self.conn, params=params)

    def get_panel_readings(self, start_date=None, end_date=None, uid=None):
        """Narrow format: timestamp, inverter_uid, channel, power_w."""
        q = 'SELECT timestamp, inverter_uid, channel, power_w FROM panel_readings'
        params, clauses = [], []
        if start_date:
            clauses.append('timestamp >= ?'); params.append(start_date)
        if end_date:
            clauses.append('timestamp <= ?')
            params.append(end_date + ' 23:59:59' if len(end_date) == 10 else end_date)
        if uid:
            clauses.append('inverter_uid = ?'); params.append(uid)
        if clauses:
            q += ' WHERE ' + ' AND '.join(clauses)
        q += ' ORDER BY timestamp, inverter_uid, channel'
        return pd.read_sql(q, self.conn, params=params, parse_dates=['timestamp'])

    def get_panel_readings_wide(self, date):
        """Pivot to wide format: timestamp index, uid-channel columns, total."""
        df = self.get_panel_readings(start_date=date, end_date=date)
        if df.empty:
            return df
        df['label'] = df['inverter_uid'] + '-' + df['channel'].astype(str)
        wide = df.pivot_table(index='timestamp', columns='label', values='power_w')
        wide['total'] = wide.sum(axis=1)
        return wide

    def get_inverter_telemetry(self, start_date=None, end_date=None, uid=None):
        q = ('SELECT timestamp, inverter_uid, dc_p1, dc_p2, dc_v1, dc_v2,'
             ' dc_i1, dc_i2, dc_e1, dc_e2, ac_p, ac_v, ac_f, ac_t'
             ' FROM inverter_telemetry')
        params, clauses = [], []
        if start_date:
            clauses.append('timestamp >= ?'); params.append(start_date)
        if end_date:
            clauses.append('timestamp <= ?')
            params.append(end_date + ' 23:59:59' if len(end_date) == 10 else end_date)
        if uid:
            clauses.append('inverter_uid = ?'); params.append(uid)
        if clauses:
            q += ' WHERE ' + ' AND '.join(clauses)
        q += ' ORDER BY timestamp, inverter_uid'
        return pd.read_sql(q, self.conn, params=params, parse_dates=['timestamp'])

    def get_billing(self):
        return pd.read_sql(
            'SELECT * FROM billing_periods ORDER BY meter_date',
            self.conn, parse_dates=['meter_date'])

    def get_finance(self):
        return pd.read_sql('SELECT * FROM finance ORDER BY date', self.conn)

    def get_inverters(self):
        return pd.read_sql('SELECT * FROM inverters ORDER BY uid', self.conn)

    def get_sync_status(self):
        return pd.read_sql('SELECT * FROM sync_log ORDER BY source', self.conn)

    def get_date_range(self, table):
        """Return (min_date, max_date, count) for a table."""
        col = {'system_readings': 'timestamp', 'panel_readings': 'timestamp',
               'inverter_telemetry': 'timestamp',
               'billing_periods': 'meter_date', 'inverters': 'uid'}.get(table, 'date')
        row = self.conn.execute(
            f'SELECT MIN({col}), MAX({col}), COUNT(*) FROM {table}'
        ).fetchone()
        return row

    def get_dates_with_data(self, table):
        """Return set of YYYY-MM-DD dates that have data."""
        col = {'system_readings': 'timestamp', 'panel_readings': 'timestamp',
               'inverter_telemetry': 'timestamp',
               'billing_periods': 'meter_date', 'inverters': 'uid'}.get(table, 'date')
        rows = self.conn.execute(
            f'SELECT DISTINCT SUBSTR({col}, 1, 10) FROM {table}'
        ).fetchall()
        return {r[0] for r in rows}
