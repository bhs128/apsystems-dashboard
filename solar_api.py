#!/usr/bin/env python3
"""
solar_api.py — Lightweight REST API serving solar.db over HTTP.

Endpoints:
  GET /api/status              — DB summary + sync log
  GET /api/system_readings     — 5-min system power (?start=&end=)
  GET /api/daily_energy        — daily kWh (?start=&end=)
  GET /api/panel_readings      — per-channel 5-min (?start=&end=&uid=)
  GET /api/panel_daily         — per-channel daily kWh (?start=&end=)
  GET /api/panel_wide          — wide-format panel data (?date=)
  GET /api/billing             — utility billing periods
  GET /api/finance             — HELOC balance history
  GET /api/inverters           — inverter registry
  GET /                        — serves solar_dashboard.html

Designed to run on a Raspberry Pi as a systemd service.
Bind to 0.0.0.0 so any device on the LAN can reach it.
"""

import os

from flask import Flask, jsonify, request, send_from_directory

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))

from solar_db import SolarDB

app = Flask(__name__, static_folder=SERVICE_DIR)
db = SolarDB()


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def date_params():
    """Extract common start/end query params."""
    return request.args.get('start'), request.args.get('end')


def df_to_json(df):
    """Convert a DataFrame to a JSON-serializable list of dicts."""
    # Convert timestamps to strings for JSON
    for col in df.columns:
        if hasattr(df[col], 'dt'):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df.to_dict(orient='records')


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.route('/')
def index():
    return send_from_directory(SERVICE_DIR, 'solar_dashboard.html')


@app.route('/api/status')
def api_status():
    tables = ['system_readings', 'daily_energy', 'panel_readings',
              'inverter_telemetry', 'billing_periods', 'finance', 'inverters']
    summary = {}
    for t in tables:
        mn, mx, cnt = db.get_date_range(t)
        summary[t] = {'min': mn, 'max': mx, 'count': cnt}
        if t not in ('inverters',):
            summary[t]['unique_dates'] = len(db.get_dates_with_data(t))

    sync = db.get_sync_status()
    sync_list = df_to_json(sync) if not sync.empty else []

    return jsonify({
        'db_size_mb': round(os.path.getsize(db.db_path) / 1024 / 1024, 1),
        'tables': summary,
        'sync_log': sync_list,
    })


@app.route('/api/system_readings')
def api_system_readings():
    start, end = date_params()
    df = db.get_system_readings(start_date=start, end_date=end)
    return jsonify(df_to_json(df))


@app.route('/api/daily_energy')
def api_daily_energy():
    start, end = date_params()
    df = db.get_daily_energy(start_date=start, end_date=end)
    return jsonify(df.to_dict(orient='records'))


@app.route('/api/panel_readings')
def api_panel_readings():
    start, end = date_params()
    uid = request.args.get('uid')
    df = db.get_panel_readings(start_date=start, end_date=end, uid=uid)
    return jsonify(df_to_json(df))


@app.route('/api/panel_daily')
def api_panel_daily():
    """Per-channel daily kWh totals — useful for heatmaps."""
    start, end = date_params()
    df = db.get_panel_readings(start_date=start, end_date=end)
    if df.empty:
        return jsonify([])
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df['label'] = df['inverter_uid'] + '-' + df['channel'].astype(str)
    agg = df.groupby(['date', 'label'])['power_w'].sum().reset_index()
    agg['kwh'] = round(agg['power_w'] * 5 / 60 / 1000, 3)
    return jsonify(agg[['date', 'label', 'kwh']].to_dict(orient='records'))


@app.route('/api/panel_summary')
def api_panel_summary():
    """Per-channel summary: total kWh, best hour (W avg), data point count."""
    start, end = date_params()
    df = db.get_panel_readings(start_date=start, end_date=end)
    if df.empty:
        return jsonify([])
    df['label'] = df['inverter_uid'] + '-' + df['channel'].astype(str)
    # Total kWh per channel (5-min intervals → hours)
    totals = df.groupby('label')['power_w'].agg(['sum', 'count']).reset_index()
    totals['total_kwh'] = round(totals['sum'] * 5 / 60 / 1000, 3)
    # Best hour: resample to 1-hour means, find max per channel
    df['hour'] = df['timestamp'].dt.floor('h')
    hourly = df.groupby(['label', 'hour'])['power_w'].mean().reset_index()
    best = hourly.groupby('label')['power_w'].max().reset_index()
    best.columns = ['label', 'best_hour_w']
    best['best_hour_w'] = round(best['best_hour_w'], 1)
    merged = totals.merge(best, on='label', how='left')
    result = merged[['label', 'total_kwh', 'best_hour_w', 'count']].rename(
        columns={'count': 'data_points'})
    return jsonify(result.to_dict(orient='records'))


@app.route('/api/panel_wide')
def api_panel_wide():
    date = request.args.get('date')
    if not date:
        return jsonify({'error': 'date parameter required'}), 400
    df = db.get_panel_readings_wide(date)
    if df.empty:
        return jsonify([])
    df = df.reset_index()
    return jsonify(df_to_json(df))


@app.route('/api/billing')
def api_billing():
    df = db.get_billing()
    return jsonify(df_to_json(df))


@app.route('/api/finance')
def api_finance():
    df = db.get_finance()
    return jsonify(df.to_dict(orient='records'))


@app.route('/api/inverters')
def api_inverters():
    df = db.get_inverters()
    return jsonify(df.to_dict(orient='records'))


@app.route('/api/inverter_telemetry')
def api_inverter_telemetry():
    """Detailed per-inverter telemetry: DC/AC power, voltage, current, freq, temp."""
    start, end = date_params()
    uid = request.args.get('uid')
    df = db.get_inverter_telemetry(start_date=start, end_date=end, uid=uid)
    return jsonify(df_to_json(df))


@app.route('/api/panels', methods=['GET'])
def api_panels_get():
    """Return panel metadata (user-assigned config + specs)."""
    df = db.get_panels()
    return jsonify(df.to_dict(orient='records'))


@app.route('/api/panels', methods=['PUT', 'POST'])
def api_panels_put():
    """Update panel metadata. Body: single dict or list of dicts.
    Each must have inverter_uid and channel."""
    data = request.get_json(force=True)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return jsonify({'error': 'expected object or array'}), 400
    for row in data:
        if 'inverter_uid' not in row or 'channel' not in row:
            return jsonify({'error': 'each item needs inverter_uid and channel'}), 400
        row['channel'] = int(row['channel'])
    db.upsert_panel_configs_bulk(data)
    return jsonify({'updated': len(data)})


@app.route('/api/system_summary')
def api_system_summary():
    """Live summary from EMA API: today/month/year/lifetime kWh."""
    try:
        from ema_api_pull import load_credentials, pull_system_summary
        app_id, app_secret = load_credentials()
        data = pull_system_summary(app_id, app_secret)
        if data:
            return jsonify(data)
        return jsonify({'error': 'no data from EMA API'}), 502
    except Exception as e:
        return jsonify({'error': str(e)}), 502


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Solar data REST API server')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    print(f'Solar API starting on http://{args.host}:{args.port}/')
    print(f'  DB: {db.db_path} ({os.path.getsize(db.db_path)/1024/1024:.1f} MB)')
    print(f'  Dashboard: {SERVICE_DIR}/solar_dashboard.html')
    app.run(host=args.host, port=args.port, debug=args.debug)
