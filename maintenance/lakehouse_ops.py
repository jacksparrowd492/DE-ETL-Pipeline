"""
lakehouse_ops.py
----------------
Week 4: table maintenance for the Delta Lake tables written by streaming/spark_delta.py.
The Airflow DAGs in maintenance/dags call these functions; they also run by hand:

    python maintenance/lakehouse_ops.py stats
    python maintenance/lakehouse_ops.py compact  [--table user_events] [--hot-days 1]
    python maintenance/lakehouse_ops.py expire   [--retention-days 7]
    python maintenance/lakehouse_ops.py vacuum   [--retention-hours 168] [--dry-run]

Engine: delta-rs (the `deltalake` package), not Trino. Trino's Delta connector refuses to
write to a local file system ("delta.enable-non-concurrent-writes"), because it cannot
commit atomically there while Spark is also writing. delta-rs commits with an atomic
"create if not exists", so it can run next to the streaming job.

Delta has no separate snapshot objects (unlike Iceberg). A snapshot = a table version =
a _delta_log/N.json commit, so:
    compaction       = OPTIMIZE (bin-packing): rewrite many small files into few large ones
    snapshot expiry  = delete commit files older than delta.logRetentionDuration
                       (only versions before the latest checkpoint; time travel to them ends)
    vacuum           = delete data files no snapshot references any more, once they have
                       been unreferenced longer than delta.deletedFileRetentionDuration
"""

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from deltalake import DeltaTable

HERE = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_ROOT = os.environ.get(
    "LAKEHOUSE_ROOT", os.path.join(HERE, "..", "streaming", "_lakehouse")
)

# table -> partition column. Both are partitioned by a date (one folder per day).
TABLES = {
    "user_events": "event_date",
    "user_events_rejected": "ingest_date",
}

# A file below this size counts as "small" (Trino opens one split per file)
SMALL_FILE_BYTES = 16 * 1024 * 1024
TARGET_FILE_BYTES = 128 * 1024 * 1024

# Never vacuum below this without force=True: Spark may have written data files
# that it has not committed yet, and vacuum would see them as unreferenced.
MIN_SAFE_VACUUM_HOURS = 1


def table_path(table: str) -> str:
    if table not in TABLES:
        raise ValueError(f"unknown table {table!r}, expected one of {list(TABLES)}")
    return os.path.abspath(os.path.join(LAKEHOUSE_ROOT, "warehouse", "lakehouse.db", table))


def _interval_hours(value: str) -> float:
    """'interval 7 days' -> 168.0 (the format Delta uses for retention properties)."""
    m = re.fullmatch(r"\s*interval\s+(\d+)\s+(\w+?)s?\s*", value or "", re.IGNORECASE)
    if not m:
        raise ValueError(f"cannot parse Delta interval {value!r}")
    per_unit = {"week": 168, "day": 24, "hour": 1, "minute": 1 / 60, "second": 1 / 3600}
    return int(m.group(1)) * per_unit[m.group(2).lower()]


def _log_versions(path: str) -> list[int]:
    log = os.path.join(path, "_delta_log")
    return sorted(int(f[:20]) for f in os.listdir(log) if re.fullmatch(r"\d{20}\.json", f))


def _remove_orphan_checksums(path: str) -> int:
    """
    Spark writes through Hadoop's local file system, which puts a hidden checksum
    sidecar (.N.json.crc, .N.checkpoint.parquet.crc) next to every log file. delta-rs's
    log cleanup deletes N.json and old checkpoints but not these sidecars, so they pile
    up, and Trino lists (and stats) every entry of _delta_log on each query. Only
    sidecars whose log file is already gone are removed; temp files never match.
    """
    log = os.path.join(path, "_delta_log")
    names = set(os.listdir(log))
    orphans = [
        n
        for n in names
        if re.fullmatch(r"\.\d{20}\.(json|checkpoint\.parquet)\.crc", n) and n[1:-4] not in names
    ]
    for n in orphans:
        os.remove(os.path.join(log, n))
    return len(orphans)


def _parquet_on_disk(path: str) -> tuple[int, int]:
    """Data files physically present (referenced or not), skipping _delta_log."""
    count = size = 0
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if not d.startswith("_")]
        for f in files:
            if f.endswith(".parquet"):
                count += 1
                size += os.path.getsize(os.path.join(root, f))
    return count, size


def table_stats(table: str) -> dict:
    """File layout of the current snapshot plus what is on disk."""
    path = table_path(table)
    dt = DeltaTable(path)
    sizes = pa.table(dt.get_add_actions(flatten=True)).column("size_bytes").to_pylist()
    versions = _log_versions(path)
    disk_files, disk_bytes = _parquet_on_disk(path)
    config = dt.metadata().configuration
    return {
        "table": table,
        "version": dt.version(),
        "data_files": len(sizes),
        "data_bytes": sum(sizes),
        "avg_file_kb": round(sum(sizes) / len(sizes) / 1024, 1) if sizes else 0,
        "small_files": sum(1 for s in sizes if s < SMALL_FILE_BYTES),
        "partitions": len({tuple(sorted(p.items())) for p in dt.partitions()}),
        "files_on_disk": disk_files,
        "bytes_on_disk": disk_bytes,
        "log_commits": len(versions),
        "oldest_version": versions[0] if versions else None,
        "log_retention": config.get("delta.logRetentionDuration", "interval 30 days"),
        "deleted_file_retention": config.get(
            "delta.deletedFileRetentionDuration", "interval 7 days"
        ),
    }


def _cold_cutoff(hot_days: int) -> str:
    """Partitions before this date are 'cold' (no longer written by the stream)."""
    return (datetime.now(timezone.utc).date() - timedelta(days=hot_days - 1)).isoformat()


def cold_small_files(table: str, hot_days: int = 1) -> int:
    """Small files that compact(table, hot_days) would be allowed to rewrite."""
    adds = pa.table(DeltaTable(table_path(table)).get_add_actions(flatten=True)).to_pylist()
    cutoff, part_col = _cold_cutoff(hot_days), f"partition.{TABLES[table]}"
    return sum(1 for a in adds if a["size_bytes"] < SMALL_FILE_BYTES and str(a[part_col]) < cutoff)


def compact(table: str, hot_days: int = 1, target_bytes: int = TARGET_FILE_BYTES) -> dict:
    """
    Bin-packing compaction (Delta OPTIMIZE) of the "cold" partitions.

    The newest `hot_days` daily partitions are skipped: spark_delta.py runs a MERGE on
    the partitions of each micro-batch, and a compaction that removes files it has just
    read would fail that MERGE with ConcurrentDeleteReadException. Older days are
    no longer written, so rewriting them never conflicts with the stream.
    hot_days=0 compacts everything (only when the stream is stopped).
    """
    path = table_path(table)
    part_col = TABLES[table]
    before = table_stats(table)
    cutoff = _cold_cutoff(hot_days)
    metrics = DeltaTable(path).optimize.compact(
        partition_filters=[(part_col, "<", cutoff)], target_size=target_bytes
    )
    after = table_stats(table)
    return {
        "table": table,
        "partitions_before": cutoff,
        "files_removed": metrics["numFilesRemoved"],
        "files_added": metrics["numFilesAdded"],
        "partitions_optimized": metrics["partitionsOptimized"],
        "data_files": f"{before['data_files']} -> {after['data_files']}",
        "avg_file_kb": f"{before['avg_file_kb']} -> {after['avg_file_kb']}",
        "version": after["version"],
    }


def expire_snapshots(table: str, retention_days: int = 7) -> dict:
    """
    Snapshot expiry: delete _delta_log commits (and old checkpoints) older than the
    retention, the Delta equivalent of Iceberg's expire_snapshots, plus the checksum
    sidecars those commits leave behind (see _remove_orphan_checksums). The retention is
    stored on the table (delta.logRetentionDuration) so Spark's own log cleanup uses
    the same policy. It is only written when it changes: that commit is a metadata
    change, and a MERGE running at the same moment would have to retry its batch.
    Delta only removes commits up to a checkpoint that is itself older than the
    retention (the oldest kept version must be rebuildable from a checkpoint), so the
    current state is never affected; time travel to the removed versions ends.
    """
    path = table_path(table)
    dt = DeltaTable(path)
    wanted = f"interval {retention_days} days"
    current = dt.metadata().configuration.get("delta.logRetentionDuration", "interval 30 days")
    before = _log_versions(path)
    if _interval_hours(current) != _interval_hours(wanted):
        # delta-rs already runs the log cleanup as a post-commit hook of this commit
        dt.alter.set_table_properties({"delta.logRetentionDuration": wanted})
        dt = DeltaTable(path)
    dt.cleanup_metadata()
    checksums_removed = _remove_orphan_checksums(path)
    after = _log_versions(path)
    return {
        "table": table,
        "log_retention": wanted,
        "retention_changed_from": None
        if _interval_hours(current) == _interval_hours(wanted)
        else current,
        "commits_removed": len(set(before) - set(after)),
        "commits_kept": len(after),
        "checksums_removed": checksums_removed,
        "log_entries": len(os.listdir(os.path.join(path, "_delta_log"))),
        "oldest_version": f"{before[0]} -> {after[0]}",
        "current_version": dt.version(),
    }


def vacuum(
    table: str, retention_hours: float | None = None, dry_run: bool = False, force: bool = False
) -> dict:
    """
    VACUUM: delete data files that are no longer in the current snapshot (e.g. the small
    files a compaction replaced) once they have been unreferenced for `retention_hours`.
    Default = the table's delta.deletedFileRetentionDuration (7 days), so readers and
    time travel inside that window keep working. Below MIN_SAFE_VACUUM_HOURS force=True
    is required, and only safe while spark_delta.py is stopped.
    """
    path = table_path(table)
    dt = DeltaTable(path)
    table_default = _interval_hours(
        dt.metadata().configuration.get("delta.deletedFileRetentionDuration", "interval 7 days")
    )
    hours = table_default if retention_hours is None else float(retention_hours)
    if hours < MIN_SAFE_VACUUM_HOURS and not force:
        raise ValueError(
            f"retention {hours}h is below {MIN_SAFE_VACUUM_HOURS}h: it could delete files the "
            "streaming job has written but not committed yet. Stop spark_delta.py and pass force=True."
        )
    kwargs = {"retention_hours": int(hours), "enforce_retention_duration": hours >= table_default}
    # delta-rs lists candidates from the log's tombstones, which can still name files an
    # earlier vacuum already deleted, so only count the ones that are really on disk.
    candidates = [
        f for f in dt.vacuum(dry_run=True, **kwargs) if os.path.exists(os.path.join(path, f))
    ]
    freed = sum(os.path.getsize(os.path.join(path, f)) for f in candidates)
    if candidates and not dry_run:
        dt.vacuum(dry_run=False, **kwargs)
    disk_files, disk_bytes = _parquet_on_disk(path)
    return {
        "table": table,
        "retention_hours": hours,
        "dry_run": dry_run,
        "files_deleted": 0 if dry_run else len(candidates),
        "files_eligible": len(candidates),
        "bytes_freed": 0 if dry_run else freed,
        "files_on_disk": disk_files,
        "bytes_on_disk": disk_bytes,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("action", choices=["stats", "compact", "expire", "vacuum"])
    ap.add_argument("--table", choices=list(TABLES), help="default: all tables")
    ap.add_argument("--hot-days", type=int, default=1, help="compact: newest days to skip")
    ap.add_argument("--retention-days", type=int, default=7, help="expire: log retention")
    ap.add_argument("--retention-hours", type=float, help="vacuum: default = table property")
    ap.add_argument("--dry-run", action="store_true", help="vacuum: only list what would go")
    ap.add_argument("--force", action="store_true", help="vacuum: allow < 1h (stream stopped)")
    args = ap.parse_args()

    for table in [args.table] if args.table else list(TABLES):
        if args.action == "stats":
            result = table_stats(table)
        elif args.action == "compact":
            result = compact(table, hot_days=args.hot_days)
        elif args.action == "expire":
            result = expire_snapshots(table, retention_days=args.retention_days)
        else:
            result = vacuum(table, args.retention_hours, dry_run=args.dry_run, force=args.force)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
