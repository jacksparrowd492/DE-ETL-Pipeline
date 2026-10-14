"""
benchmark.py
------------
Week 4: Trino query response times before and after table maintenance.

    python maintenance/benchmark.py run --label before   [--runs 15 --warmup 3]
                                       [--as-of user_events=456 --as-of user_events_rejected=451]
    ... run maintenance (Airflow DAG or lakehouse_ops.py) ...
    python maintenance/benchmark.py run --label after
    python maintenance/benchmark.py compare before after [--report benchmark_report.md]

Each query is run `warmup` times (JIT, metadata and OS file caches), then `runs`
times; the report uses the median (and p95) of the measured runs. Besides the
client-side response time it keeps Trino's own numbers for the last run: splits
(one per data file), processed rows/bytes and CPU time. Results go to
maintenance/results/benchmark_<label>.json, the comparison to benchmark_report.md (the
Airflow DAG uses dated labels, e.g. before_2026-10-12 -> benchmark_report_2026-10-12.md).
"""

import argparse
import json
import os
import re
import statistics
import time
from datetime import datetime, timezone

import trino

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(HERE, "results")
TRINO = {
    "host": os.environ.get("TRINO_HOST", "localhost"),
    "port": int(os.environ.get("TRINO_PORT", "8080")),
    "user": "benchmark",
    "catalog": "delta",
    "schema": os.environ.get("TRINO_SCHEMA", "lakehouse"),
}

# The dashboard's queries (serving/metabase_setup.py) plus two selective ones.
# {latest_date} and {sample_id} are filled from the data once per run.
QUERIES = {
    "total_events": "SELECT count(*) FROM user_events",
    "kpi_tiles": (
        "SELECT count(DISTINCT user_id), count(DISTINCT event), "
        "round(100.0 * count_if(event = 'purchase') / nullif(count_if(event = 'add_to_cart'), 0), 1) "
        "FROM user_events"
    ),
    "events_by_type": "SELECT event, count(*) AS events FROM user_events GROUP BY event ORDER BY events DESC",
    "events_per_minute": (
        "SELECT date_trunc('minute', event_time) AS minute, event, count(*) AS events "
        "FROM user_events GROUP BY 1, 2 ORDER BY 1"
    ),
    "user_activity_matrix": (
        "SELECT user_id, count_if(event = 'page_view') AS page_view, count_if(event = 'click') AS click, "
        "count_if(event = 'add_to_cart') AS add_to_cart, count_if(event = 'purchase') AS purchase, "
        "count(*) AS total FROM user_events GROUP BY user_id ORDER BY total DESC"
    ),
    "ingest_latency_p95": (
        "SELECT approx_percentile(date_diff('millisecond', event_time, ingested_at) / 1000.0, 0.95) "
        "FROM user_events"
    ),
    "data_quality_pct": (
        "SELECT round(100.0 * v / nullif(v + r, 0), 1) FROM (SELECT count(*) AS v FROM user_events) "
        "CROSS JOIN (SELECT count(*) AS r FROM user_events_rejected)"
    ),
    "rejected_by_reason": "SELECT reason, count(*) FROM user_events_rejected GROUP BY reason ORDER BY 2 DESC",
    "one_day_partition": (
        "SELECT event, count(*) FROM user_events WHERE event_date = DATE '{latest_date}' GROUP BY event"
    ),
    "point_lookup": "SELECT * FROM user_events WHERE event_id = '{sample_id}'",
}

LAYOUT_SQL = 'SELECT count(*), coalesce(sum(size), 0) FROM (SELECT DISTINCT "$path", "$file_size" AS size FROM {table})'


def _query(cur, sql: str) -> tuple[list, dict, float]:
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return rows, cur.stats, (time.perf_counter() - t0) * 1000


def time_travel(sql: str, as_of: dict | None) -> str:
    """Read the given table versions instead of the current ones (Trino FOR VERSION AS OF)."""
    for table, version in (as_of or {}).items():
        sql = re.sub(rf"\b{table}\b", f"{table} FOR VERSION AS OF {int(version)}", sql)
    return sql


def run(label: str, runs: int = 15, warmup: int = 3, as_of: dict | None = None) -> str:
    """
    as_of={"user_events": 456, ...} benchmarks older table versions, e.g. the layout
    from before a compaction that has already run (until vacuum deletes its files).
    """
    cur = trino.dbapi.connect(**TRINO).cursor()

    def q(sql: str) -> tuple[list, dict, float]:
        return _query(cur, time_travel(sql, as_of))

    params = {
        "latest_date": q("SELECT CAST(max(event_date) AS varchar) FROM user_events")[0][0][0],
        "sample_id": q("SELECT min(event_id) FROM user_events")[0][0][0],
    }
    layout = {}
    for table in ("user_events", "user_events_rejected"):
        files, size = q(LAYOUT_SQL.format(table=table))[0][0]
        rows = q(f"SELECT count(*) FROM {table}")[0][0][0]
        layout[table] = {"data_files": files, "data_bytes": size, "rows": rows}

    results = {}
    for name, template in QUERIES.items():
        sql = time_travel(template.format(**params), as_of)
        for _ in range(warmup):
            _query(cur, sql)
        times, trino_ms, cpu_ms = [], [], []
        for _ in range(runs):
            rows, stats, ms = _query(cur, sql)
            times.append(ms)
            trino_ms.append(stats["elapsedTimeMillis"])
            cpu_ms.append(stats["cpuTimeMillis"])
        times.sort()
        results[name] = {
            "sql": sql,
            "median_ms": round(statistics.median(times), 1),
            "p95_ms": round(times[max(0, int(len(times) * 0.95) - 1)], 1),
            "min_ms": round(times[0], 1),
            "trino_elapsed_median_ms": statistics.median(trino_ms),
            "trino_cpu_median_ms": statistics.median(cpu_ms),
            "splits": stats["totalSplits"],
            "processed_rows": stats["processedRows"],
            "processed_bytes": stats["processedBytes"],
            "result_rows": len(rows),
            "runs_ms": [round(t, 1) for t in times],
        }
        print(
            f"{name:22s} median {results[name]['median_ms']:8.1f} ms   splits {stats['totalSplits']:4d}",
            flush=True,
        )

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out = os.path.join(RESULTS_DIR, f"benchmark_{label}.json")
    with open(out, "w") as f:
        json.dump(
            {
                "label": label,
                "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "trino": f"{TRINO['host']}:{TRINO['port']}",
                "runs": runs,
                "warmup": warmup,
                "as_of": as_of,
                "params": params,
                "layout": layout,
                "queries": results,
            },
            f,
            indent=2,
        )
    print(f">> wrote {out}")
    return out


def _load(label: str) -> dict:
    with open(os.path.join(RESULTS_DIR, f"benchmark_{label}.json")) as f:
        return json.load(f)


def _maintenance_lines(maintenance: dict) -> list[str]:
    """Rows for what lakehouse_ops did between the runs ({step: [result per table]})."""
    describe = {
        "compaction": lambda r: (
            f"{r['data_files']} data files ({r['files_removed']} removed, {r['files_added']} added)"
        ),
        "snapshot_expiry": lambda r: (
            f"{r['commits_removed']} commits removed, {r['commits_kept']} kept "
            f"(oldest version {r['oldest_version']})"
            + (
                f", {r['checksums_removed']} orphaned .crc files removed, "
                f"{r['log_entries']} _delta_log entries left"
                if "checksums_removed" in r
                else ""
            )
        ),
        "vacuum": lambda r: (
            f"{r['files_deleted']} files deleted, {r['bytes_freed'] / 1024:,.0f} KB freed "
            f"(retention {r['retention_hours']:g} h)"
        ),
    }
    lines = [
        "",
        "## Maintenance between the runs",
        "",
        "| Step | Table | Result |",
        "|---|---|---|",
    ]
    for step, results in maintenance.items():
        for r in results:
            lines.append(f"| {step} | {r['table']} | {describe[step](r)} |")
    return lines


def compare(
    before_label: str = "before",
    after_label: str = "after",
    report: str = "benchmark_report.md",
    maintenance: dict | None = None,
) -> str:
    b, a = _load(before_label), _load(after_label)
    lines = [
        "# Trino benchmark: before vs after table maintenance",
        "",
        f"* Before: {b['measured_at']}, after: {a['measured_at']} "
        f"({a['runs']} measured runs per query after {a['warmup']} warm-up runs; medians shown)",
        "* Maintenance between the runs: "
        + (
            ", ".join(s.replace("_", " ") for s in maintenance)
            if maintenance
            else "bin-packing compaction, snapshot (log) expiry, vacuum"
        ),
    ]
    for side, res in (("Before", b), ("After", a)):
        if res.get("as_of"):
            versions = ", ".join(f"{t} v{v}" for t, v in res["as_of"].items())
            lines.append(f"* {side} read older table versions via time travel: {versions}")
    lines += [
        "",
        "## Table layout",
        "",
        "| Table | Rows before | Rows after | Data files before | after | Size before | after |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for t in b["layout"]:
        lb, la = b["layout"][t], a["layout"][t]
        lines.append(
            f"| {t} | {lb['rows']:,} | {la['rows']:,} | {lb['data_files']} | {la['data_files']} "
            f"| {lb['data_bytes'] / 1024:,.0f} KB | {la['data_bytes'] / 1024:,.0f} KB |"
        )
    if any(b["layout"][t]["rows"] != a["layout"][t]["rows"] for t in b["layout"]):
        lines += ["", "> **Note:** row counts differ, the stream wrote data between the two runs."]

    lines += [
        "",
        "## Query response time (client, median)",
        "",
        "| Query | Before ms | After ms | Speed-up | Trino CPU before ms | after ms | Splits before | after |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    tot_b = tot_a = 0.0
    for name, qb in b["queries"].items():
        qa = a["queries"].get(name)
        if not qa:
            continue
        tot_b += qb["median_ms"]
        tot_a += qa["median_ms"]
        lines.append(
            f"| {name} | {qb['median_ms']:.0f} | {qa['median_ms']:.0f} | {qb['median_ms'] / qa['median_ms']:.1f}x "
            f"| {qb['trino_cpu_median_ms']:.0f} | {qa['trino_cpu_median_ms']:.0f} "
            f"| {qb['splits']} | {qa['splits']} |"
        )
    lines.append(
        f"| **all queries (sum)** | **{tot_b:.0f}** | **{tot_a:.0f}** | **{tot_b / tot_a:.1f}x** | | | | |"
    )
    if maintenance:
        lines += _maintenance_lines(maintenance)
    text = "\n".join(lines) + "\n"

    out = os.path.join(RESULTS_DIR, report)
    with open(out, "w") as f:
        f.write(text)
    print(text)
    print(f">> wrote {out}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--label", required=True)
    r.add_argument("--runs", type=int, default=15)
    r.add_argument("--warmup", type=int, default=3)
    r.add_argument(
        "--as-of",
        action="append",
        default=[],
        metavar="TABLE=VERSION",
        help="benchmark an older table version (repeatable)",
    )
    c = sub.add_parser("compare")
    c.add_argument("before", nargs="?", default="before")
    c.add_argument("after", nargs="?", default="after")
    c.add_argument("--report", default="benchmark_report.md", help="file name in results/")
    args = ap.parse_args()
    if args.cmd == "run":
        as_of = dict(pair.split("=", 1) for pair in args.as_of)
        run(args.label, args.runs, args.warmup, {t: int(v) for t, v in as_of.items()} or None)
    else:
        compare(args.before, args.after, args.report)


if __name__ == "__main__":
    main()
