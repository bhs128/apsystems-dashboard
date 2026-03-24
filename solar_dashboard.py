#!/usr/bin/env python3
"""
solar_dashboard.py — Simple HTML dashboard generated from solar.db.

Usage:
  python solar_dashboard.py              # generates solar_dashboard.html
  python solar_dashboard.py --serve      # generates + starts local HTTP server
"""

import argparse
import json
import os
import sys

import pandas as pd

from solar_db import SolarDB

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(DATA_DIR, 'solar_dashboard.html')


def build_dashboard():
    db = SolarDB()

    # --- Summary stats ---
    sys_min, sys_max, sys_count = db.get_date_range('system_readings')
    de_min, de_max, de_count = db.get_date_range('daily_energy')
    pr_min, pr_max, pr_count = db.get_date_range('panel_readings')
    bl_min, bl_max, bl_count = db.get_date_range('billing_periods')
    fi_min, fi_max, fi_count = db.get_date_range('finance')
    inv_min, inv_max, inv_count = db.get_date_range('inverters')

    panel_dates = len(db.get_dates_with_data('panel_readings'))
    sys_dates = len(db.get_dates_with_data('system_readings'))

    db_size_mb = os.path.getsize(db.db_path) / 1024 / 1024

    # --- Daily energy chart data ---
    daily = db.get_daily_energy()
    daily_dates = daily['date'].tolist()
    daily_kwh = daily['energy_kwh'].tolist()

    # --- Recent 7-day panel heatmap data ---
    panel_df = db.get_panel_readings()
    panel_heat = {}
    if not panel_df.empty:
        panel_df['date'] = panel_df['timestamp'].dt.strftime('%Y-%m-%d')
        panel_df['label'] = panel_df['inverter_uid'] + '-' + panel_df['channel'].astype(str)
        # Daily totals per channel (kWh = sum of 5-min W readings * 5/60 / 1000)
        agg = panel_df.groupby(['date', 'label'])['power_w'].sum().reset_index()
        agg['kwh'] = agg['power_w'] * 5 / 60 / 1000
        # Last 7 days
        recent_dates = sorted(agg['date'].unique())[-7:]
        agg = agg[agg['date'].isin(recent_dates)]
        labels = sorted(agg['label'].unique())
        panel_heat = {
            'dates': recent_dates,
            'labels': labels,
            'values': []
        }
        for lbl in labels:
            row_vals = []
            for d in recent_dates:
                match = agg[(agg['label'] == lbl) & (agg['date'] == d)]
                row_vals.append(round(match['kwh'].iloc[0], 3) if len(match) else 0)
            panel_heat['values'].append(row_vals)

    # --- Billing chart data ---
    billing = db.get_billing()
    bill_dates = billing['meter_date'].dt.strftime('%Y-%m-%d').tolist() if not billing.empty else []
    bill_actual = billing['actual_bill'].tolist() if not billing.empty else []
    bill_est = billing['est_bill_without_solar'].tolist() if not billing.empty else []

    # --- Finance chart data ---
    finance = db.get_finance()
    fin_dates = finance['date'].tolist() if not finance.empty else []
    fin_balance = finance['heloc_balance'].tolist() if not finance.empty else []

    # --- Sync log ---
    sync = db.get_sync_status()
    sync_rows = []
    if not sync.empty:
        for _, r in sync.iterrows():
            sync_rows.append({
                'source': r['source'],
                'last_date': r['last_date'],
                'synced_at': r['synced_at'][:19],
                'records': int(r['record_count']) if pd.notna(r['record_count']) else 0
            })

    db.close()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Solar DB Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 1.5rem; }}
  h1 {{ font-size: 1.6rem; margin-bottom: 0.3rem; color: #f59e0b; }}
  .subtitle {{ color: #94a3b8; font-size: 0.85rem; margin-bottom: 1.5rem; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
           gap: 1rem; margin-bottom: 1.5rem; }}
  .card {{ background: #1e293b; border-radius: 0.75rem; padding: 1rem; }}
  .card .label {{ font-size: 0.75rem; color: #94a3b8; text-transform: uppercase;
                  letter-spacing: 0.05em; }}
  .card .value {{ font-size: 1.5rem; font-weight: 700; color: #f8fafc; margin-top: 0.25rem; }}
  .card .detail {{ font-size: 0.7rem; color: #64748b; margin-top: 0.2rem; }}
  .chart-box {{ background: #1e293b; border-radius: 0.75rem; padding: 1rem;
                margin-bottom: 1.5rem; }}
  .chart-box h2 {{ font-size: 1rem; color: #cbd5e1; margin-bottom: 0.75rem; }}
  .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }}
  @media (max-width: 800px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
  canvas {{ max-height: 280px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th, td {{ padding: 0.4rem 0.6rem; text-align: left; border-bottom: 1px solid #334155; }}
  th {{ color: #94a3b8; font-weight: 600; }}
  .heatmap {{ overflow-x: auto; }}
  .heatmap table {{ font-size: 0.7rem; }}
  .heatmap td {{ text-align: center; padding: 0.25rem 0.4rem; border-radius: 0.25rem; }}
</style>
</head>
<body>
<h1>&#9728; Solar DB Dashboard</h1>
<p class="subtitle">solar.db &mdash; {db_size_mb:.1f} MB &mdash; generated {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>

<div class="grid">
  <div class="card">
    <div class="label">System Readings</div>
    <div class="value">{sys_count:,}</div>
    <div class="detail">{sys_dates} days &middot; {sys_min[:10] if sys_min else '—'} → {sys_max[:10] if sys_max else '—'}</div>
  </div>
  <div class="card">
    <div class="label">Daily Energy</div>
    <div class="value">{de_count:,}</div>
    <div class="detail">{de_min or '—'} → {de_max or '—'}</div>
  </div>
  <div class="card">
    <div class="label">Panel Readings</div>
    <div class="value">{pr_count:,}</div>
    <div class="detail">{panel_dates} days &middot; {pr_min[:10] if pr_min else '—'} → {pr_max[:10] if pr_max else '—'}</div>
  </div>
  <div class="card">
    <div class="label">Inverters</div>
    <div class="value">{inv_count}</div>
    <div class="detail">registered UIDs</div>
  </div>
  <div class="card">
    <div class="label">Billing Periods</div>
    <div class="value">{bl_count}</div>
    <div class="detail">{bl_min or '—'} → {bl_max or '—'}</div>
  </div>
  <div class="card">
    <div class="label">Finance Records</div>
    <div class="value">{fi_count}</div>
    <div class="detail">{fi_min or '—'} → {fi_max or '—'}</div>
  </div>
</div>

<div class="chart-box">
  <h2>Daily Production (kWh)</h2>
  <canvas id="dailyChart"></canvas>
</div>

<div class="chart-row">
  <div class="chart-box">
    <h2>Monthly Electric Bill ($)</h2>
    <canvas id="billChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>HELOC Balance ($)</h2>
    <canvas id="financeChart"></canvas>
  </div>
</div>

<div class="chart-box">
  <h2>Per-Panel Daily kWh — Last 7 Days</h2>
  <div class="heatmap" id="heatmapBox"></div>
</div>

<div class="chart-box">
  <h2>Sync Log</h2>
  <table>
    <tr><th>Source</th><th>Last Date</th><th>Synced At</th><th>Records</th></tr>
    {''.join(f'<tr><td>{r["source"]}</td><td>{r["last_date"]}</td><td>{r["synced_at"]}</td><td>{r["records"]:,}</td></tr>' for r in sync_rows)}
  </table>
</div>

<script>
const chartOpts = {{
  responsive: true,
  plugins: {{ legend: {{ display: false }} }},
  scales: {{
    x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 12 }}, grid: {{ color: '#1e293b' }} }},
    y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#334155' }} }}
  }}
}};

// Daily production
new Chart(document.getElementById('dailyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(daily_dates)},
    datasets: [{{ data: {json.dumps(daily_kwh)}, backgroundColor: '#f59e0b88', borderColor: '#f59e0b', borderWidth: 1 }}]
  }},
  options: chartOpts
}});

// Billing
new Chart(document.getElementById('billChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(bill_dates)},
    datasets: [
      {{ label: 'Actual', data: {json.dumps(bill_actual)}, backgroundColor: '#22c55e88' }},
      {{ label: 'Est. w/o Solar', data: {json.dumps(bill_est)}, backgroundColor: '#ef444488' }}
    ]
  }},
  options: {{ ...chartOpts,
    plugins: {{ legend: {{ display: true, labels: {{ color: '#94a3b8' }} }} }},
    scales: {{ ...chartOpts.scales,
      x: {{ ...chartOpts.scales.x,
        ticks: {{ ...chartOpts.scales.x.ticks, maxTicksLimit: 30, maxRotation: 45, minRotation: 25 }}
      }}
    }}
  }}
}});

// Finance
new Chart(document.getElementById('financeChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(fin_dates)},
    datasets: [{{ data: {json.dumps(fin_balance)}, borderColor: '#3b82f6', borderWidth: 2,
                  fill: true, backgroundColor: '#3b82f622', pointRadius: 3, tension: 0.3 }}]
  }},
  options: chartOpts
}});

// Panel heatmap
(function() {{
  const heat = {json.dumps(panel_heat)};
  if (!heat.dates) return;
  const box = document.getElementById('heatmapBox');
  let html = '<table><tr><th></th>';
  heat.dates.forEach(d => html += '<th>' + d.slice(5) + '</th>');
  html += '</tr>';
  const allVals = heat.values.flat();
  const maxV = Math.max(...allVals);
  heat.labels.forEach((lbl, i) => {{
    html += '<tr><td style="color:#94a3b8;white-space:nowrap">' + lbl + '</td>';
    heat.values[i].forEach(v => {{
      const pct = maxV > 0 ? v / maxV : 0;
      const r = Math.round(15 + 240 * (1 - pct));
      const g = Math.round(15 + 150 * pct);
      const b = 15;
      const bg = 'rgb(' + r + ',' + g + ',' + b + ')';
      html += '<td style="background:' + bg + ';color:#f8fafc">' + v.toFixed(2) + '</td>';
    }});
    html += '</tr>';
  }});
  html += '</table>';
  box.innerHTML = html;
}})();
</script>
</body>
</html>"""

    with open(OUT_FILE, 'w') as f:
        f.write(html)
    print(f'Dashboard written to {OUT_FILE}')
    return OUT_FILE


def main():
    parser = argparse.ArgumentParser(description='Generate solar DB dashboard')
    parser.add_argument('--serve', action='store_true',
                        help='Start a local HTTP server after generating')
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()

    path = build_dashboard()

    if args.serve:
        import http.server
        import socketserver
        os.chdir(DATA_DIR)
        handler = http.server.SimpleHTTPRequestHandler
        with socketserver.TCPServer(('', args.port), handler) as httpd:
            print(f'Serving at http://localhost:{args.port}/solar_dashboard.html')
            httpd.serve_forever()


if __name__ == '__main__':
    main()
