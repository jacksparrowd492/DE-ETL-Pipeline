"""
run_demo.py
-----------
One-command demo: runs the Spark streaming job AND the producer together,
printing both outputs in this single terminal.

    [SPARK]    lines come from spark_streaming.py (RAW / PARSED / COUNTS tables)
    [PRODUCER] lines come from producer.py (each JSON message before it is sent)

Kafka must already be running on localhost:9092.

Usage
-----
    python streaming/run_demo.py                 # runs until Ctrl+C
    python streaming/run_demo.py --count 50      # send 50 messages, then stop
    python streaming/run_demo.py --rate 200      # high velocity
    python streaming/run_demo.py --lakehouse     # Week 2: sink into Delta Lake instead of the console
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import threading
import time

# PySpark 3.5.1 / Hadoop 3.3.4 requires Java 8-17 (Java 23+ removes Subject.getSubject)
for _jdk in [
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot",
    r"C:\Program Files\Eclipse Adoptium\jdk-17",
    r"C:\Program Files\Java\jdk-17",
]:
    if os.path.isdir(_jdk):
        os.environ["JAVA_HOME"] = _jdk
        os.environ["PATH"] = os.path.join(_jdk, "bin") + os.pathsep + os.environ.get("PATH", "")
        break

HERE = os.path.dirname(os.path.abspath(__file__))

CYAN = "\033[96m"
MAGENTA = "\033[95m"
RESET = "\033[0m"

# Spark prints this line once all three streaming queries have started
SPARK_READY_MARKER = "Queries running"

# Dependency-resolution chatter printed by spark.jars.packages; hidden for readability
SPARK_NOISE = (":: ", "Ivy Default Cache", "The jars for the packages", "\tfound ", "\tconfs:")

print_lock = threading.Lock()


def start(script: str, extra_args: list[str]) -> subprocess.Popen:
    """Start a sibling script with unbuffered, piped output."""

    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")

    return subprocess.Popen(
        [sys.executable, os.path.join(HERE, script), *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Spark's WARN lines go to stderr; keep them in order
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )


def forward(proc: subprocess.Popen, label: str, colour: str, ready: threading.Event | None = None):
    """Copy a child's output to this terminal, prefixing every line with its source."""

    for line in proc.stdout:
        line = line.rstrip("\n")

        if label == "SPARK" and line.lstrip().startswith(SPARK_NOISE):
            continue

        with print_lock:
            print(f"{colour}[{label:<8}]{RESET} {line}", flush=True)

        if ready is not None and SPARK_READY_MARKER in line:
            ready.set()


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    if os.name == "nt":
        # Kill the whole tree: the Spark script spawns a java.exe child that
        # would otherwise keep running after its Python parent is gone
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> None:

    parser = argparse.ArgumentParser(description="Run Spark job + producer in one terminal")
    parser.add_argument("--rate", default="5", help="Producer messages per second")
    parser.add_argument("--count", default=None, help="Stop after N messages (default: forever)")
    parser.add_argument("--bad-ratio", default="0.1", help="Share of invalid messages")
    parser.add_argument(
        "--counts-mode",
        choices=["update", "append"],
        default="update",
        help="COUNTS output mode passed to the Spark job",
    )
    parser.add_argument(
        "--lakehouse",
        action="store_true",
        help="Run spark_delta.py (Kafka -> Delta Lake) instead of the console job",
    )
    parser.add_argument(
        "--linger",
        type=int,
        default=None,
        help="Seconds to keep Spark running after a --count run, so the last batches print "
        "(default: 25, or 60 with --lakehouse because the first MERGE in a new JVM is slow)",
    )
    args = parser.parse_args()
    if args.linger is None:
        args.linger = 60 if args.lakehouse else 25

    # 1) Start Spark and wait until its queries are running
    spark_ready = threading.Event()
    if args.lakehouse:
        spark = start("spark_delta.py", [])
    else:
        spark = start("spark_streaming.py", ["--counts-mode", args.counts_mode])
    threading.Thread(target=forward, args=(spark, "SPARK", CYAN, spark_ready), daemon=True).start()

    print("Starting Spark streaming job (first start takes ~20-40 s)...", flush=True)

    while not spark_ready.wait(timeout=1):
        if spark.poll() is not None:
            print("Spark job exited before it was ready - see [SPARK] lines above.")
            sys.exit(1)

    # 2) Start the producer once Spark is listening (Spark reads only new messages)
    producer_args = ["--rate", args.rate, "--bad-ratio", args.bad_ratio]
    if args.count:
        producer_args += ["--count", args.count]

    producer = start("producer.py", producer_args)
    threading.Thread(target=forward, args=(producer, "PRODUCER", MAGENTA), daemon=True).start()

    # 3) Run until Ctrl+C, or until the producer finishes a --count run
    try:
        while producer.poll() is None:
            time.sleep(0.5)

        print(f"\nProducer finished; letting Spark print its last batches for {args.linger}s...")
        time.sleep(args.linger)

    except KeyboardInterrupt:
        print("\nCtrl+C received, shutting down...")

    finally:
        stop(producer)
        stop(spark)

        # A force-killed Spark job can't clean up after itself, so remove its
        # checkpoint folder here (spark_streaming.py names it run-<time>-<pid>)
        for folder in glob.glob(os.path.join(HERE, "_checkpoints", f"run-*-{spark.pid}")):
            shutil.rmtree(folder, ignore_errors=True)
        print("Demo stopped.")


if __name__ == "__main__":
    main()
