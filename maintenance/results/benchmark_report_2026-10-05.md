# Trino benchmark: before vs after table maintenance

* Before: 2026-10-05T07:25:51+00:00, after: 2026-10-05T07:27:52+00:00 (15 measured runs per query after 3 warm-up runs; medians shown)
* Maintenance between the runs: bin-packing compaction, snapshot (log) expiry, vacuum

## Table layout

| Table | Rows before | Rows after | Data files before | after | Size before | after |
|---|---:|---:|---:|---:|---:|---:|
| user_events | 6,541 | 6,541 | 130 | 2 | 498 KB | 120 KB |
| user_events_rejected | 745 | 745 | 303 | 2 | 684 KB | 18 KB |

## Query response time (client, median)

| Query | Before ms | After ms | Speed-up | Trino CPU before ms | after ms | Splits before | after |
|---|---:|---:|---:|---:|---:|---:|---:|
| total_events | 707 | 458 | 1.5x | 20 | 1 | 139 | 11 |
| kpi_tiles | 761 | 507 | 1.5x | 286 | 11 | 163 | 35 |
| events_by_type | 642 | 466 | 1.4x | 171 | 6 | 156 | 28 |
| events_per_minute | 838 | 475 | 1.8x | 241 | 7 | 156 | 28 |
| user_activity_matrix | 828 | 475 | 1.7x | 238 | 6 | 156 | 28 |
| ingest_latency_p95 | 638 | 459 | 1.4x | 200 | 5 | 139 | 11 |
| data_quality_pct | 2112 | 904 | 2.3x | 76 | 2 | 459 | 30 |
| rejected_by_reason | 747 | 469 | 1.6x | 265 | 4 | 329 | 28 |
| one_day_partition | 624 | 454 | 1.4x | 157 | 2 | 139 | 17 |
| point_lookup | 455 | 455 | 1.0x | 3 | 2 | 1 | 1 |
| **all queries (sum)** | **8352** | **5121** | **1.6x** | | | | |
