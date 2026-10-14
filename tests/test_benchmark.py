"""
test_benchmark.py
-----------------
Mock tests for maintenance/benchmark.py. trino.dbapi.connect is patched with a
fake cursor, so no test needs a running Trino; results go to a temp directory.
"""

import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "maintenance"))
import benchmark  # noqa: E402


class FakeCursor:
    """Answers the benchmark's setup queries and records every SQL it is sent."""

    def __init__(self, splits: int = 40):
        self.executed = []
        self.splits = splits
        self.stats = {}

    def execute(self, sql):
        self.executed.append(sql)
        self.stats = {
            "elapsedTimeMillis": 100,
            "cpuTimeMillis": 7,
            "totalSplits": self.splits,
            "processedRows": 1000,
            "processedBytes": 4096,
        }

    def fetchall(self):
        sql = self.executed[-1]
        if "max(event_date)" in sql:
            return [["2026-10-06"]]
        if "min(event_id)" in sql:
            return [["evt-1"]]
        if "$path" in sql:
            return [[self.splits, 123_456]]
        if sql.startswith("SELECT count(*) FROM user_events"):
            return [[1000]]
        return [[1], [2]]


@pytest.fixture
def results_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark, "RESULTS_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def cursor(mocker):
    cur = FakeCursor()
    mocker.patch("benchmark.trino.dbapi.connect").return_value.cursor.return_value = cur
    return cur


# --------------------------------------------------------
# time travel
# --------------------------------------------------------


def test_time_travel_rewrites_each_table_separately():
    sql = "SELECT * FROM user_events CROSS JOIN (SELECT count(*) FROM user_events_rejected)"
    out = benchmark.time_travel(sql, {"user_events": 456, "user_events_rejected": 451})
    assert "user_events FOR VERSION AS OF 456 CROSS" in out
    assert "user_events_rejected FOR VERSION AS OF 451)" in out


def test_time_travel_without_versions_is_a_no_op():
    assert benchmark.time_travel("SELECT 1 FROM user_events", None) == "SELECT 1 FROM user_events"


# --------------------------------------------------------
# run
# --------------------------------------------------------


def test_run_measures_every_query_and_writes_json(results_dir, cursor):
    out = benchmark.run("before", runs=3, warmup=1)

    data = json.loads(Path(out).read_text())
    assert Path(out).name == "benchmark_before.json"
    assert set(data["queries"]) == set(benchmark.QUERIES)
    assert data["layout"]["user_events"] == {"data_files": 40, "data_bytes": 123_456, "rows": 1000}
    assert data["params"] == {"latest_date": "2026-10-06", "sample_id": "evt-1"}
    q = data["queries"]["total_events"]
    assert len(q["runs_ms"]) == 3
    assert q["splits"] == 40
    assert q["trino_cpu_median_ms"] == 7
    # templates are filled from the data
    assert "DATE '2026-10-06'" in data["queries"]["one_day_partition"]["sql"]
    assert "'evt-1'" in data["queries"]["point_lookup"]["sql"]
    # 6 setup queries + (warmup + runs) per benchmark query
    assert len(cursor.executed) == 6 + 4 * len(benchmark.QUERIES)


def test_run_as_of_sends_only_time_travel_queries(results_dir, cursor):
    out = benchmark.run("old", runs=1, warmup=0, as_of={"user_events": 456})

    reads = [s for s in cursor.executed if re.search(r"\buser_events\b", s)]
    assert reads
    assert all("user_events FOR VERSION AS OF 456" in s for s in reads)
    # the other table is not pinned to a version
    assert not any("user_events_rejected FOR VERSION" in s for s in cursor.executed)
    assert json.loads(Path(out).read_text())["as_of"] == {"user_events": 456}


# --------------------------------------------------------
# compare
# --------------------------------------------------------


def _write(results_dir, label, median_ms, splits, rows=1000, as_of=None):
    query = {"median_ms": median_ms, "trino_cpu_median_ms": 5, "splits": splits}
    layout = {"data_files": splits, "data_bytes": 2048 * splits, "rows": rows}
    data = {
        "measured_at": "2026-10-12T00:00:00+00:00",
        "runs": 15,
        "warmup": 3,
        "as_of": as_of,
        "layout": {"user_events": layout},
        "queries": {"total_events": query, "kpi_tiles": dict(query)},
    }
    (results_dir / f"benchmark_{label}.json").write_text(json.dumps(data))


def test_compare_writes_speedup_report(results_dir):
    _write(results_dir, "before_x", median_ms=800, splits=300)
    _write(results_dir, "after_x", median_ms=400, splits=10)

    out = benchmark.compare("before_x", "after_x", report="benchmark_report_x.md")

    text = Path(out).read_text()
    assert Path(out).name == "benchmark_report_x.md"
    assert "| total_events | 800 | 400 | 2.0x | 5 | 5 | 300 | 10 |" in text
    assert "| **all queries (sum)** | **1600** | **800** | **2.0x** |" in text
    assert "row counts differ" not in text
    assert "time travel" not in text
    assert "Maintenance between the runs\n" not in text


def test_compare_flags_rows_written_between_runs(results_dir):
    _write(results_dir, "before", median_ms=800, splits=300, rows=1000)
    _write(results_dir, "after", median_ms=400, splits=10, rows=1200)
    text = Path(benchmark.compare()).read_text()
    assert "row counts differ" in text


def test_compare_notes_time_travel_and_maintenance(results_dir):
    _write(results_dir, "before", median_ms=800, splits=300, as_of={"user_events": 456})
    _write(results_dir, "after", median_ms=400, splits=10)
    maintenance = {
        "compaction": [
            {
                "table": "user_events",
                "data_files": "300 -> 3",
                "files_removed": 300,
                "files_added": 3,
            }
        ],
        "snapshot_expiry": [
            {
                "table": "user_events",
                "commits_removed": 60,
                "commits_kept": 397,
                "oldest_version": "0 -> 60",
            }
        ],
        "vacuum": [
            {
                "table": "user_events",
                "files_deleted": 130,
                "bytes_freed": 512_000,
                "retention_hours": 168.0,
            }
        ],
    }

    text = Path(benchmark.compare(maintenance=maintenance)).read_text()

    assert "* Before read older table versions via time travel: user_events v456" in text
    assert "| compaction | user_events | 300 -> 3 data files (300 removed, 3 added) |" in text
    assert (
        "| snapshot_expiry | user_events | 60 commits removed, 397 kept (oldest version 0 -> 60) |"
        in text
    )
    assert "| vacuum | user_events | 130 files deleted, 500 KB freed (retention 168 h) |" in text


def test_cli_compare_uses_report_name(results_dir, monkeypatch):
    _write(results_dir, "b", median_ms=800, splits=300)
    _write(results_dir, "a", median_ms=400, splits=10)
    monkeypatch.setattr(sys, "argv", ["benchmark.py", "compare", "b", "a", "--report", "r.md"])
    benchmark.main()
    assert (results_dir / "r.md").exists()


def test_cli_run_parses_as_of(results_dir, monkeypatch, mocker):
    run = mocker.patch("benchmark.run")
    argv = ["benchmark.py", "run", "--label", "old", "--as-of", "user_events=456"]
    monkeypatch.setattr(sys, "argv", argv)
    benchmark.main()
    run.assert_called_once_with("old", 15, 3, {"user_events": 456})
