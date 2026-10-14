"""
package.py
----------
Builds the deployable artifacts into dist/, with plain Python - no Docker:

    dist/emr/spark_delta.py            streaming job entry point
    dist/emr/emr_maintenance_job.py    maintenance job entry point
    dist/emr/pyfiles.zip               spark.submit.pyFiles: observability/, governance/,
                                       streaming/lakehouse.py + producer.py at the zip root,
                                       and the pure-Python deps (requests, kafka-python)
                                       downloaded as Linux / Python 3.9 wheels for EMR 7.x
    dist/mwaa/dags/                    fakestore_pipeline.py + every package it imports,
                                       lakehouse_maintenance_emr.py + emr_settings.py
    dist/consumer/app.zip              the Kafka -> Databricks consumer + requirements.txt
    dist/manifest.json                 git commit, build time, file hashes (what is deployed)

    python deploy/package.py              # full build (pip downloads the EMR deps)
    python deploy/package.py --no-deps    # code only (CI smoke check, offline)

deploy/aws/deploy.sh uploads dist/ to S3.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"

PIPELINE_PACKAGES = [
    "extract",
    "validation",
    "transform",
    "load",
    "staging",
    "kafka1",
    "replay",
    "observability",
    "governance",
]
PIPELINE_MODULES = ["config.py", "Data_Connection.py", "pipeline_tasks.py"]

EMR_DEPS = ["requests", "kafka-python"]  # pyspark + Delta come with EMR; boto3 too
CONSUMER_REQUIREMENTS = [
    "python-dotenv",
    "requests",
    "kafka-python",
    "databricks-sql-connector",
    "boto3",  # Secrets Manager + botocore for kafka-python's AWS_MSK_IAM
]


def _files(path: Path):
    if path.is_file():
        yield path
        return
    for p in sorted(path.rglob("*")):
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
            yield p


def _copy(src: Path, dest: Path) -> None:
    for f in _files(src):
        target = dest / f.relative_to(src.parent if src.is_dir() else src.parent)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, target)


def _zip(zip_path: Path, entries: list) -> None:
    """entries: (source path, name inside the zip)"""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for src, arcname in entries:
            for f in _files(src):
                rel = f.relative_to(src) if src.is_dir() else Path()
                z.write(f, str(Path(arcname) / rel) if src.is_dir() else arcname)


def _download_emr_deps(target: Path) -> None:
    """Linux x86_64 / CPython 3.9 wheels (EMR 7.x), whatever OS this runs on."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--target",
            str(target),
            "--platform",
            "manylinux2014_x86_64",
            "--python-version",
            "3.9",
            "--implementation",
            "cp",
            "--only-binary=:all:",
            *EMR_DEPS,
        ],
        check=True,
    )


def build_emr(with_deps: bool) -> None:
    out = DIST / "emr"
    out.mkdir(parents=True)
    shutil.copy2(ROOT / "streaming" / "spark_delta.py", out / "spark_delta.py")
    shutil.copy2(ROOT / "deploy" / "aws" / "emr_maintenance_job.py", out / "emr_maintenance_job.py")

    entries = [
        (ROOT / "observability", "observability"),
        (ROOT / "governance", "governance"),
        (ROOT / "streaming" / "lakehouse.py", "lakehouse.py"),
        (ROOT / "streaming" / "producer.py", "producer.py"),
    ]
    if with_deps:
        deps = DIST / "_emr_deps"
        _download_emr_deps(deps)
        entries += [(p, p.name) for p in sorted(deps.iterdir()) if p.name != "bin"]
    _zip(out / "pyfiles.zip", entries)
    shutil.rmtree(DIST / "_emr_deps", ignore_errors=True)


def build_mwaa() -> None:
    dags = DIST / "mwaa" / "dags"
    dags.mkdir(parents=True)
    for pkg in PIPELINE_PACKAGES:
        _copy(ROOT / pkg, dags)
    for module in PIPELINE_MODULES + ["fakestore_pipeline.py"]:
        shutil.copy2(ROOT / module, dags / module)
    shutil.copy2(ROOT / "deploy/aws/dags/lakehouse_maintenance_emr.py", dags)
    shutil.copy2(ROOT / "deploy/aws/emr_settings.py", dags)
    # the scheduler only needs to parse the two DAG files
    (dags / ".airflowignore").write_text(
        "\n".join(PIPELINE_PACKAGES + [m[:-3] for m in PIPELINE_MODULES] + ["emr_settings"]) + "\n"
    )


def build_consumer() -> None:
    staging = DIST / "_consumer"
    for pkg in PIPELINE_PACKAGES:
        _copy(ROOT / pkg, staging)
    for module in PIPELINE_MODULES:
        shutil.copy2(ROOT / module, staging / module)
    (staging / "failed_records").mkdir()
    (staging / "failed_records" / ".gitkeep").touch()  # the replay queue folder must exist
    (staging / "requirements.txt").write_text("\n".join(CONSUMER_REQUIREMENTS) + "\n")
    _zip(DIST / "consumer" / "app.zip", [(staging, "")])
    shutil.rmtree(staging)


def manifest() -> dict:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        commit, dirty = "unknown", True
    files = {
        str(p.relative_to(DIST)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(DIST.rglob("*"))
        if p.is_file()
    }
    return {
        "commit": commit,
        "dirty": dirty,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build deploy artifacts into dist/")
    parser.add_argument("--no-deps", action="store_true", help="skip downloading EMR deps")
    args = parser.parse_args(argv)

    shutil.rmtree(DIST, ignore_errors=True)
    build_emr(with_deps=not args.no_deps)
    build_mwaa()
    build_consumer()
    info = manifest()
    (DIST / "manifest.json").write_text(json.dumps(info, indent=2))

    print(
        f"Built {len(info['files'])} files into {DIST} (commit {info['commit'][:10]}"
        f"{', dirty tree' if info['dirty'] else ''})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
