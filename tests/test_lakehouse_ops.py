"""
test_lakehouse_ops.py
---------------------
Tests for maintenance/lakehouse_ops.py against real (tiny) Delta tables that
delta-rs writes into a temp directory, so compaction, snapshot expiry and
vacuum run exactly as they do on the lakehouse, just without Spark.
"""

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "maintenance"))
import lakehouse_ops as ops  # noqa: E402

TODAY = datetime.now(timezone.utc).date()
COLD_DAY = TODAY - timedelta(days=3)


def _append(path: str, part_col: str, day: date, n: int = 3) -> None:
    """One micro-batch = one commit with one small file, like spark_delta.py."""
    batch = pa.table(
        {
            "event_id": [f"{day}-{time.perf_counter_ns()}-{i}" for i in range(n)],
            "event": ["click"] * n,
            part_col: pa.array([day] * n, pa.date32()),
        }
    )
    write_deltalake(path, batch, mode="append", partition_by=[part_col])


@pytest.fixture
def lakehouse(tmp_path, monkeypatch):
    """Both tables: 5 small files in a cold partition, 2 in today's (hot) one."""
    monkeypatch.setattr(ops, "LAKEHOUSE_ROOT", str(tmp_path))
    for table, part_col in ops.TABLES.items():
        path = ops.table_path(table)
        for _ in range(5):
            _append(path, part_col, COLD_DAY)
        for _ in range(2):
            _append(path, part_col, TODAY)
    return tmp_path


def _rows(table: str) -> int:
    return DeltaTable(ops.table_path(table)).to_pyarrow_table().num_rows


# --------------------------------------------------------
# helpers
# --------------------------------------------------------


@pytest.mark.parametrize(
    "value, hours",
    [
        ("interval 7 days", 168),
        ("interval 1 day", 24),
        ("INTERVAL 2 weeks", 336),
        ("interval 90 minutes", 1.5),
    ],
)
def test_interval_hours_parses_delta_intervals(value, hours):
    assert ops._interval_hours(value) == hours


def test_interval_hours_rejects_garbage():
    with pytest.raises(ValueError):
        ops._interval_hours("7 days")


def test_table_path_rejects_unknown_table():
    with pytest.raises(ValueError, match="unknown table"):
        ops.table_path("orders")


# --------------------------------------------------------
# stats
# --------------------------------------------------------


def test_table_stats_counts_files_partitions_and_commits(lakehouse):
    stats = ops.table_stats("user_events")
    assert stats["data_files"] == 7
    assert stats["small_files"] == 7
    assert stats["partitions"] == 2
    assert stats["files_on_disk"] == 7
    assert stats["log_commits"] == 7
    assert stats["oldest_version"] == 0
    assert stats["log_retention"] == "interval 30 days"  # Delta default, not set yet


# --------------------------------------------------------
# compaction
# --------------------------------------------------------


def test_cold_small_files_ignores_hot_partition(lakehouse):
    assert ops.cold_small_files("user_events", hot_days=1) == 5
    assert ops.cold_small_files("user_events", hot_days=0) == 7


def test_compact_bin_packs_only_cold_partitions(lakehouse):
    rows = _rows("user_events")
    result = ops.compact("user_events", hot_days=1)

    assert result["files_removed"] == 5
    assert result["files_added"] == 1
    assert result["data_files"] == "7 -> 3"  # 1 compacted cold file + 2 untouched hot files
    assert _rows("user_events") == rows
    assert ops.cold_small_files("user_events", hot_days=1) == 1


def test_compact_hot_days_zero_compacts_everything(lakehouse):
    result = ops.compact("user_events_rejected", hot_days=0)
    assert result["data_files"] == "7 -> 2"  # one file per partition


def test_compact_leaves_replaced_files_on_disk_for_vacuum(lakehouse):
    ops.compact("user_events", hot_days=1)
    stats = ops.table_stats("user_events")
    assert stats["data_files"] == 3
    assert stats["files_on_disk"] == 8  # the 5 replaced files wait for vacuum


# --------------------------------------------------------
# snapshot expiry
# --------------------------------------------------------


def test_expire_sets_log_retention_once(lakehouse):
    first = ops.expire_snapshots("user_events", retention_days=7)
    version = DeltaTable(ops.table_path("user_events")).version()
    second = ops.expire_snapshots("user_events", retention_days=7)

    assert first["retention_changed_from"] == "interval 30 days"
    assert second["retention_changed_from"] is None
    # the unchanged property is not rewritten, so no extra commit for the stream to race
    assert DeltaTable(ops.table_path("user_events")).version() == version
    assert ops.table_stats("user_events")["log_retention"] == "interval 7 days"


def test_expire_removes_commits_older_than_retention(lakehouse):
    path = ops.table_path("user_events")
    DeltaTable(path).create_checkpoint()
    old = (datetime.now() - timedelta(days=10)).timestamp()
    log = os.path.join(path, "_delta_log")
    for f in os.listdir(log):
        os.utime(os.path.join(log, f), (old, old))
    rows = _rows("user_events")

    result = ops.expire_snapshots("user_events", retention_days=7)

    assert result["commits_removed"] > 0
    assert result["oldest_version"].startswith("0 -> ")
    assert _rows("user_events") == rows  # current snapshot is untouched


def test_expire_removes_only_orphaned_checksum_sidecars(lakehouse):
    log = os.path.join(ops.table_path("user_events"), "_delta_log")
    kept = f".{0:020d}.json.crc"  # its commit still exists
    orphan_commit = f".{9:020d}.json.crc"  # Spark's sidecar of an expired commit
    orphan_checkpoint = f".{5:020d}.checkpoint.parquet.crc"
    temp = f"..{10:020d}.json.0b1c.tmp.crc"  # an in-flight write, never touched
    for name in (kept, orphan_commit, orphan_checkpoint, temp):
        Path(log, name).write_text("crc")

    result = ops.expire_snapshots("user_events", retention_days=7)

    assert result["checksums_removed"] == 2
    left = set(os.listdir(log))
    assert {kept, temp} <= left
    assert not {orphan_commit, orphan_checkpoint} & left
    assert result["log_entries"] == len(left)


# --------------------------------------------------------
# vacuum
# --------------------------------------------------------


def test_vacuum_refuses_short_retention_without_force(lakehouse):
    with pytest.raises(ValueError, match="force=True"):
        ops.vacuum("user_events", retention_hours=0)


def test_vacuum_default_retention_keeps_recently_replaced_files(lakehouse):
    ops.compact("user_events", hot_days=1)
    result = ops.vacuum("user_events")
    assert result["retention_hours"] == 168
    assert result["files_eligible"] == 0
    assert result["files_on_disk"] == 8


def test_vacuum_dry_run_deletes_nothing(lakehouse):
    ops.compact("user_events", hot_days=1)
    result = ops.vacuum("user_events", retention_hours=0, dry_run=True, force=True)
    assert result["files_eligible"] == 5
    assert result["files_deleted"] == 0
    assert result["files_on_disk"] == 8


def test_vacuum_deletes_replaced_files_and_keeps_data(lakehouse):
    rows = _rows("user_events")
    ops.compact("user_events", hot_days=1)

    result = ops.vacuum("user_events", retention_hours=0, force=True)

    assert result["files_deleted"] == 5
    assert result["bytes_freed"] > 0
    assert result["files_on_disk"] == 3
    assert _rows("user_events") == rows


def test_vacuum_second_run_does_not_recount_deleted_files(lakehouse):
    ops.compact("user_events", hot_days=1)
    ops.vacuum("user_events", retention_hours=0, force=True)

    again = ops.vacuum("user_events", retention_hours=0, force=True)

    assert again["files_eligible"] == 0
    assert again["files_deleted"] == 0
    assert again["files_on_disk"] == 3


def test_vacuum_ignores_tombstones_of_files_already_gone(lakehouse, mocker):
    # delta-rs can list tombstoned files that an earlier vacuum already removed
    # (seen on the real tables: 303 "deleted", 0 bytes freed)
    ops.compact("user_events", hot_days=1)
    ops.vacuum("user_events", retention_hours=0, force=True)
    mocker.patch.object(DeltaTable, "vacuum", return_value=["event_date=2026-01-01/gone.parquet"])

    again = ops.vacuum("user_events", retention_hours=0, force=True)

    assert again["files_deleted"] == 0
    DeltaTable.vacuum.assert_called_once()  # dry run only, no real vacuum commit


# --------------------------------------------------------
# CLI
# --------------------------------------------------------


def test_cli_stats_runs_for_all_tables(lakehouse, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["lakehouse_ops.py", "stats"])
    ops.main()
    out = capsys.readouterr().out
    assert '"table": "user_events"' in out
    assert '"table": "user_events_rejected"' in out
