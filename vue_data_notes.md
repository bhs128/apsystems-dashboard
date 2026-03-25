# Emporia VUE Data Export — Format & Merge Notes

> Reference for integrating VUE exports into solar_sync ingest pipeline.  
> Generated from analysis of 11 manual exports, 2026-03-24.

## Devices

| Device ID | Name | MAC | Monitor Location |
|-----------|------|-----|-----------------|
| 33555 | Solar Combiner Panel | B880A4 | Sub-panel where solar feeds in |
| 385843 | Breaker Box | 130C98 | Main electrical panel |

## Export File Format

### File Naming
```
{MAC}-{Device_Name}-{TIMESCALE}.csv
```
Examples:
- `B880A4-Solar_Combiner_Panel-15MIN.csv`
- `130C98-Breaker_Box-1H.csv`

### ZIP Naming
```
data-export-{DEVICE_ID}-{UNIQUE_ID}.zip
```
Each ZIP contains exactly 5 CSV files (one per timescale).

### CSV Structure
- **Delimiter:** Comma
- **Encoding:** UTF-8 (may have BOM)
- **Timestamp column:** `Time Bucket (America/Chicago)` — format `MM/DD/YYYY HH:MM:SS`
- **Timezone:** America/Chicago (Central)
- **Values:** Decimal, may be negative (solar production shows as negative on mains)
- **Missing CTs:** Value is literal string `No CT`
- **Units vary by timescale:**
  - 1SEC, 1MIN, 15MIN → kWatts (instantaneous power)
  - 1H, 1DAY → kWhs (energy over the period)

### Column Schemas

**Solar Combiner Panel (17 data columns, all exports):**
| # | Column | Active? | Notes |
|---|--------|---------|-------|
| 0 | Mains_A | **Yes** | Phase A net power/energy |
| 1 | Mains_B | **Yes** | Phase B net power/energy |
| 2 | Mains_C | No | Always "No CT" |
| 3-14 | Circuit_5 through Circuit_16 | No | All "No CT" |
| 15 | Solar/Generation-Top String | **Yes** | Top string solar generation |
| 16 | Solar/Generation-Bottom String | **Yes** | Bottom string solar generation |

**Key columns:** Mains_A + Mains_B = total net solar feed; Top String + Bottom String = total solar generation.

**Breaker Box — 19 columns (pre-March 2025):**
| # | Column | Active? | Notes |
|---|--------|---------|-------|
| 0 | Mains_A | **Yes** | Phase A grid power |
| 1 | Mains_B | **Yes** | Phase B grid power |
| 2 | Mains_C | No | "No CT" |
| 3 | Basement (18) | **Yes** | |
| 4 | Office/Kids Bedroom (16) | **Yes** | |
| 5 | Garage/Main Bath (13) | **Yes** | |
| 6 | Living Room & Main Lights (14) | **Yes** | |
| 7 | Fridge (2) | **Yes** | |
| 8 | Stove (5) | **Yes** | |
| 9 | AC (19) | **Yes** | |
| 10 | Furnace Fan (17a) | **Yes** | |
| 11-18 | Circuit_9 through Circuit_16 | No | "No CT" |

**Breaker Box — 36 columns (post-March 2025, combined):**
Columns 0-18 = same as above, then columns 19-35 = Solar Combiner Panel cols 0-16.

## Timescale Characteristics

| Scale | Row Cap per Export | Typical Span | Practical Use |
|-------|-------------------|--------------|---------------|
| 1SEC | ~10,800 | ~3 hours | Snapshots only; not useful for historical |
| 1MIN | ~10,100 | ~7 days | Sparse; large gaps between exports |
| 15MIN | Uncapped* | Full range | **Best resolution with complete coverage** |
| 1H | Uncapped* | Full range | Complete; good for daily patterns |
| 1DAY | Uncapped* | Full range | Complete; good for seasonal trends |

*VUE appears to cap 1SEC/1MIN exports to ~10K rows but provides full history for 15MIN and coarser.

## Coverage Summary (deduplicated across exports)

### Solar Combiner Panel
- **1DAY:** 2024-11-09 → 2025-05-05 (177 days, 100%)
- **1H:** 2024-11-09 → 2025-05-05 (177 days, 100%)
- **15MIN:** 2024-11-09 → 2025-05-05 (177 days, 100% — 1 DST gap only)
- **1MIN:** 2024-11-11 → 2025-05-05 (21.5% coverage, 5 gaps)
- **1SEC:** 2024-11-18 → 2025-05-05 (0.6% coverage, 7 gaps)

### Breaker Box
- **1DAY:** 2024-11-10 → 2026-03-24 (499 days, 100%)
- **1H:** 2024-11-10 → 2026-03-24 (499 days, 100%)
- **15MIN:** 2024-11-10 → 2026-03-24 (499 days, 100% — 2 DST gaps only)
- **1MIN:** 2025-04-16 → 2026-03-24 (6.2% coverage, 2 gaps)
- **1SEC:** 2025-04-23 → 2026-03-24 (0.1% coverage, 2 gaps)

## Merge Strategy for Ingest Pipeline

### Recommended Approach: 15MIN Resolution

1. **Parse all exports** → collect (device, timestamp, column_values) tuples
2. **Deduplicate by timestamp** → when overlapping, prefer later export (more recent data may have corrections)
3. **Handle column transition:**
   - For dates **before 2025-03-25**: Join Solar Combiner Panel and Breaker Box data by timestamp
   - For dates **after 2025-03-25**: Use the 36-column Breaker Box export which already contains both devices' data
   - When both standalone solar + combined export exist for the same timestamp, prefer the combined export
4. **Normalize column names** to a unified schema (e.g., strip device prefix, use snake_case)
5. **Handle "No CT" values** → treat as NULL, not zero

### Key Merge Considerations

- **Sign convention:** Solar generation columns are positive (kW produced). On the Solar Combiner Panel's Mains_A/B, negative values indicate power flowing TO the grid (export). On Breaker Box Mains_A/B, positive = consuming from grid, negative = exporting to grid.
- **DST gaps:** Spring-forward creates a 1.25h gap in 15MIN data (01:45 → 03:00). Not a data loss, just a missing clock hour. Fall-back may create duplicate timestamps — deduplicate by keeping first.
- **Overlapping exports:** Multiple exports cover the same date ranges. Values should be identical for the same timestamp; prefer later-dated exports if discrepancies exist.
- **Solar total:** `total_solar_kw = Top_String + Bottom_String` (always both columns)
- **Net consumption:** `net_grid_kw = Breaker_Box_Mains_A + Breaker_Box_Mains_B` (positive = consuming, negative = exporting)
- **Gross consumption:** `gross_consumption = net_grid_kw + total_solar_kw`

### Ingest File Drop Format

For solar_sync CSV ingest folder, transform VUE data into:
```csv
timestamp_utc,source,solar_kw,grid_kw,consumption_kw,solar_top_kw,solar_bottom_kw,...circuit_columns...
```

Convert timestamps from America/Chicago to UTC before writing.

### Future API Integration Note

VUE exports are manual and capped. For ongoing data collection, consider:
- Emporia cloud API (unofficial, requires auth token from app)
- Local API via Home Assistant integration (if HA is running)
- Schedule periodic manual exports for 15MIN data to fill gaps
