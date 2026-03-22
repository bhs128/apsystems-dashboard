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
    install_date  TEXT,                       -- YYYY-MM-DD
    removed_date TEXT,                       -- YYYY-MM-DD (NULL = still active)
    notes        TEXT,
    PRIMARY KEY (inverter_uid, channel)
);

CREATE TABLE IF NOT EXISTS arrays (
    array_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    tilt_deg    REAL,
    azimuth_deg REAL,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS strings (
    string_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS slots (
    array_id              INTEGER NOT NULL,
    row                   INTEGER NOT NULL,
    col                   INTEGER NOT NULL,
    string_id             INTEGER,
    panel_name            TEXT,
    panel_model           TEXT,
    panel_capacity_w      REAL,
    panel_width_mm        REAL,
    panel_height_mm       REAL,
    panel_serial          TEXT,
    panel_install_date    TEXT,
    inverter_uid          TEXT,
    inverter_channel      INTEGER,
    inverter_install_date TEXT,
    removed_date          TEXT,
    notes                 TEXT,
    PRIMARY KEY (array_id, row, col),
    FOREIGN KEY (array_id) REFERENCES arrays(array_id),
    FOREIGN KEY (string_id) REFERENCES strings(string_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_slots_inverter_unique
    ON slots (inverter_uid, inverter_channel)
    WHERE inverter_uid IS NOT NULL;

CREATE TABLE IF NOT EXISTS slot_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    array_id      INTEGER NOT NULL,
    row           INTEGER NOT NULL,
    col           INTEGER NOT NULL,
    event_type    TEXT NOT NULL CHECK(event_type IN ('panel', 'inverter')),
    old_value     TEXT,
    new_value     TEXT,
    changed_date  TEXT NOT NULL,
    notes         TEXT,
    FOREIGN KEY (array_id, row, col) REFERENCES slots(array_id, row, col)
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
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        """Add columns / tables that may not exist in older databases."""
        cols = {row[1] for row in
                self.conn.execute('PRAGMA table_info(panels)').fetchall()}
        if 'removed_date' not in cols:
            self.conn.execute(
                'ALTER TABLE panels ADD COLUMN removed_date TEXT')

        # Migrate flat panels → normalized arrays + slots (one-time)
        panels_count = self.conn.execute(
            'SELECT COUNT(*) FROM panels').fetchone()[0]
        slots_count = self.conn.execute(
            'SELECT COUNT(*) FROM slots').fetchone()[0]
        if panels_count > 0 and slots_count == 0:
            self._migrate_panels_to_slots()

    def _migrate_panels_to_slots(self):
        """One-time migration: populate arrays + slots from legacy panels table."""
        cursor = self.conn.execute('SELECT * FROM panels')
        col_names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()

        # Build array lookup: name → (tilt, azimuth)
        array_map = {}
        for row in rows:
            p = dict(zip(col_names, row))
            name = p.get('array_name')
            if not name:
                continue
            if name not in array_map:
                array_map[name] = (p.get('tilt_deg'), p.get('azimuth_deg'))

        # Insert arrays
        name_to_id = {}
        for name, (tilt, azimuth) in array_map.items():
            self.conn.execute(
                'INSERT OR IGNORE INTO arrays (name, tilt_deg, azimuth_deg)'
                ' VALUES (?, ?, ?)', (name, tilt, azimuth))
            aid = self.conn.execute(
                'SELECT array_id FROM arrays WHERE name = ?',
                (name,)).fetchone()[0]
            name_to_id[name] = aid

        # Insert slots
        for row in rows:
            p = dict(zip(col_names, row))
            name = p.get('array_name')
            r, c = p.get('array_row'), p.get('array_col')
            if not name or r is None or c is None:
                continue
            self.conn.execute('''
                INSERT OR IGNORE INTO slots
                    (array_id, row, col, panel_name, panel_model,
                     panel_capacity_w, panel_width_mm, panel_height_mm,
                     panel_install_date, inverter_uid, inverter_channel,
                     removed_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name_to_id[name], r, c, p.get('panel_name'), p.get('model'),
                  p.get('capacity_w'), p.get('width_mm'), p.get('height_mm'),
                  p.get('install_date'), p.get('inverter_uid'), p.get('channel'),
                  p.get('removed_date'), p.get('notes')))

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
                   'width_mm', 'height_mm', 'install_date', 'removed_date',
                   'notes'}
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

    # ------------------------------------------------------------------
    # Arrays / Strings / Slots CRUD
    # ------------------------------------------------------------------

    def upsert_array(self, name, **fields):
        """Create or update an array. Returns array_id."""
        allowed = {'tilt_deg', 'azimuth_deg', 'notes'}
        updates = {k: v for k, v in fields.items() if k in allowed}
        self.conn.execute(
            'INSERT OR IGNORE INTO arrays (name) VALUES (?)', (name,))
        if updates:
            set_clause = ', '.join(f'{k} = ?' for k in updates)
            vals = list(updates.values()) + [name]
            self.conn.execute(
                f'UPDATE arrays SET {set_clause} WHERE name = ?', vals)
        self.conn.commit()
        return self.conn.execute(
            'SELECT array_id FROM arrays WHERE name = ?',
            (name,)).fetchone()[0]

    def upsert_string(self, name, **fields):
        """Create or update a string. Returns string_id."""
        allowed = {'notes'}
        updates = {k: v for k, v in fields.items() if k in allowed}
        self.conn.execute(
            'INSERT OR IGNORE INTO strings (name) VALUES (?)', (name,))
        if updates:
            set_clause = ', '.join(f'{k} = ?' for k in updates)
            vals = list(updates.values()) + [name]
            self.conn.execute(
                f'UPDATE strings SET {set_clause} WHERE name = ?', vals)
        self.conn.commit()
        return self.conn.execute(
            'SELECT string_id FROM strings WHERE name = ?',
            (name,)).fetchone()[0]

    def upsert_slot(self, array_id, row, col, **fields):
        """Create or update a physical slot."""
        allowed = {'string_id', 'panel_name', 'panel_model', 'panel_capacity_w',
                   'panel_width_mm', 'panel_height_mm', 'panel_serial',
                   'panel_install_date', 'inverter_uid', 'inverter_channel',
                   'inverter_install_date', 'removed_date', 'notes'}
        updates = {k: v for k, v in fields.items() if k in allowed}
        self.conn.execute(
            'INSERT OR IGNORE INTO slots (array_id, row, col)'
            ' VALUES (?, ?, ?)', (array_id, row, col))
        if updates:
            set_clause = ', '.join(f'{k} = ?' for k in updates)
            vals = list(updates.values()) + [array_id, row, col]
            self.conn.execute(
                f'UPDATE slots SET {set_clause}'
                f' WHERE array_id = ? AND row = ? AND col = ?', vals)
        self.conn.commit()

    def upsert_slots_bulk(self, rows):
        """rows: list of dicts with array_id, row, col, + optional fields."""
        for row in rows:
            aid, r, c = row['array_id'], row['row'], row['col']
            fields = {k: v for k, v in row.items()
                      if k not in ('array_id', 'row', 'col')}
            self.upsert_slot(aid, r, c, **fields)

    def log_slot_change(self, array_id, row, col, event_type,
                        old_value, new_value, changed_date, notes=None):
        """Record a panel or inverter swap at a slot position."""
        self.conn.execute(
            'INSERT INTO slot_history'
            ' (array_id, row, col, event_type, old_value, new_value,'
            '  changed_date, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (array_id, row, col, event_type, old_value, new_value,
             changed_date, notes))
        self.conn.commit()

    def get_arrays(self):
        return pd.read_sql(
            'SELECT * FROM arrays ORDER BY name', self.conn)

    def get_strings(self):
        return pd.read_sql(
            'SELECT * FROM strings ORDER BY name', self.conn)

    def get_slots(self):
        """Return all slots joined with array and string names."""
        return pd.read_sql('''
            SELECT s.array_id, a.name AS array_name, s.row, s.col,
                   s.string_id, st.name AS string_name,
                   s.panel_name, s.panel_model, s.panel_capacity_w,
                   s.panel_width_mm, s.panel_height_mm, s.panel_serial,
                   s.panel_install_date, s.inverter_uid, s.inverter_channel,
                   s.inverter_install_date, s.removed_date, s.notes
            FROM slots s
            JOIN arrays a ON a.array_id = s.array_id
            LEFT JOIN strings st ON st.string_id = s.string_id
            ORDER BY a.name, s.row, s.col
        ''', self.conn)

    def get_slot_history(self, array_id=None, row=None, col=None):
        q = ('SELECT h.*, a.name AS array_name FROM slot_history h'
             ' JOIN arrays a ON a.array_id = h.array_id')
        params, clauses = [], []
        if array_id is not None:
            clauses.append('h.array_id = ?'); params.append(array_id)
        if row is not None:
            clauses.append('h.row = ?'); params.append(row)
        if col is not None:
            clauses.append('h.col = ?'); params.append(col)
        if clauses:
            q += ' WHERE ' + ' AND '.join(clauses)
        q += ' ORDER BY h.changed_date DESC, h.id DESC'
        return pd.read_sql(q, self.conn, params=params)

    def get_panels(self):
        """Backward-compatible panel view from normalized tables."""
        return pd.read_sql('''
            SELECT s.inverter_uid, s.inverter_channel AS channel,
                   s.panel_name, a.name AS array_name,
                   s.row AS array_row, s.col AS array_col,
                   a.tilt_deg, a.azimuth_deg,
                   s.panel_model AS model, s.panel_capacity_w AS capacity_w,
                   s.panel_width_mm AS width_mm, s.panel_height_mm AS height_mm,
                   s.panel_install_date AS install_date,
                   s.removed_date, s.notes
            FROM slots s
            JOIN arrays a ON a.array_id = s.array_id
            ORDER BY a.name, s.row, s.col,
                     s.inverter_uid, s.inverter_channel
        ''', self.conn)

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
