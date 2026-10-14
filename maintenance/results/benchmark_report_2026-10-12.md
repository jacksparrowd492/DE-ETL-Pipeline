# Trino benchmark: before vs after table maintenance

* Before: 2026-10-12T15:48:11+00:00, after: 2026-10-12T15:54:14+00:00 (15 measured runs per query after 3 warm-up runs; medians shown)
* Maintenance between the runs: compaction, snapshot expiry, vacuum
* Before read older table versions via time travel: user_events v456, user_events_rejected v451

## Table layout

| Table | Rows before | Rows after | Data files before | after | Size before | after |
|---|---:|---:|---:|---:|---:|---:|
| user_events | 22,079 | 22,079 | 327 | 3 | 1,346 KB | 400 KB |
| user_events_rejected | 2,435 | 2,435 | 798 | 3 | 1,811 KB | 50 KB |

## Query response time (client, median)

| Query | Before ms | After ms | Speed-up | Trino CPU before ms | after ms | Splits before | after |
|---|---:|---:|---:|---:|---:|---:|---:|
| total_events | 2495 | 1776 | 1.4x | 43 | 2 | 336 | 12 |
| kpi_tiles | 1853 | 1860 | 1.0x | 421 | 22 | 360 | 36 |
| events_by_type | 2147 | 1789 | 1.2x | 505 | 11 | 353 | 29 |
| events_per_minute | 1983 | 1800 | 1.1x | 325 | 18 | 353 | 29 |
| user_activity_matrix | 1941 | 1726 | 1.1x | 329 | 9 | 353 | 29 |
| ingest_latency_p95 | 3115 | 1727 | 1.8x | 2296 | 12 | 336 | 12 |
| data_quality_pct | 4849 | 3949 | 1.2x | 122 | 4 | 1151 | 32 |
| rejected_by_reason | 1361 | 2116 | 0.6x | 3811 | 8 | 824 | 29 |
| one_day_partition | 2377 | 1566 | 1.5x | 1596 | 4 | 339 | 17 |
| point_lookup | 1728 | 1391 | 1.2x | 4 | 3 | 1 | 1 |
| **all queries (sum)** | **23849** | **19700** | **1.2x** | | | | |

## Maintenance between the runs

| Step | Table | Result |
|---|---|---|
| compaction | user_events | 327 -> 3 data files (326 removed, 2 added) |
| compaction | user_events_rejected | 798 -> 3 data files (796 removed, 1 added) |
| snapshot_expiry | user_events | 0 commits removed, 338 kept (oldest version 120 -> 120) |
| snapshot_expiry | user_events_rejected | 0 commits removed, 333 kept (oldest version 120 -> 120) |
| vacuum | user_events | 130 files deleted, 498 KB freed (retention 168 h) |
| vacuum | user_events_rejected | 303 files deleted, 684 KB freed (retention 168 h) |
