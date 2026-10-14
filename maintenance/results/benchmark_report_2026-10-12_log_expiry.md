# Trino benchmark: before vs after table maintenance

* Before: 2026-10-12T16:10:56+00:00, after: 2026-10-12T16:11:51+00:00 (15 measured runs per query after 3 warm-up runs; medians shown)
* Maintenance between the runs: snapshot expiry
* Measured on a `cp -a` copy of both (already compacted) tables, registered as `delta.bench.*`
  (`TRINO_SCHEMA=bench`). Only the log retention changed: 7 days -> 1 day, plus removal of
  orphaned Hadoop `.crc` sidecars. The real tables keep the 7-day retention. Copy deleted afterwards.

## Table layout

| Table | Rows before | Rows after | Data files before | after | Size before | after |
|---|---:|---:|---:|---:|---:|---:|
| user_events | 22,079 | 22,079 | 3 | 3 | 400 KB | 400 KB |
| user_events_rejected | 2,435 | 2,435 | 3 | 3 | 50 KB | 50 KB |

## Query response time (client, median)

| Query | Before ms | After ms | Speed-up | Trino CPU before ms | after ms | Splits before | after |
|---|---:|---:|---:|---:|---:|---:|---:|
| total_events | 1414 | 173 | 8.2x | 2 | 1 | 12 | 12 |
| kpi_tiles | 1339 | 238 | 5.6x | 15 | 22 | 36 | 36 |
| events_by_type | 1345 | 190 | 7.1x | 8 | 10 | 29 | 29 |
| events_per_minute | 1380 | 205 | 6.7x | 14 | 17 | 29 | 29 |
| user_activity_matrix | 1433 | 192 | 7.5x | 12 | 12 | 29 | 29 |
| ingest_latency_p95 | 1385 | 177 | 7.8x | 11 | 13 | 12 | 12 |
| data_quality_pct | 2604 | 327 | 8.0x | 3 | 4 | 32 | 32 |
| rejected_by_reason | 1333 | 178 | 7.5x | 6 | 8 | 29 | 29 |
| one_day_partition | 1341 | 175 | 7.7x | 4 | 4 | 17 | 17 |
| point_lookup | 1539 | 173 | 8.9x | 3 | 3 | 1 | 1 |
| **all queries (sum)** | **15113** | **2028** | **7.5x** | | | | |

## Maintenance between the runs

| Step | Table | Result |
|---|---|---|
| snapshot_expiry | user_events | 330 commits removed, 11 kept (oldest version 120 -> 450), 484 orphaned .crc files removed, 24 _delta_log entries left |
| snapshot_expiry | user_events_rejected | 330 commits removed, 8 kept (oldest version 120 -> 450), 484 orphaned .crc files removed, 15 _delta_log entries left |
