"""
validate_config.py
-------------------
Lightweight config sanity check for CI's "Config Validation" job. Doesn't
need any real credentials — just checks the *shape* of the project's
configuration is consistent:

  1. Every os.getenv("KEY") / os.environ.get("KEY") referenced in config.py
     and in the Phase II code (observability/, governance/, pipeline_tasks.py,
     kafka1/consumer.py) has a matching KEY= line in .env.example, so a fresh
     clone always has a complete template to copy to .env and no setting is
     undocumented.
  2. requirements-dev.txt actually pulls in requirements.txt.
  3. pytest.ini points testpaths at a directory that exists.
  4. The GitHub Actions workflow files themselves are valid YAML.

Run: python scripts/validate_config.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

errors = []


def check(condition: bool, message: str):
    if not condition:
        errors.append(message)


# --------------------------------------------------------
# 1. env keys read by the code <-> .env.example
# --------------------------------------------------------

ENV_SOURCES = [
    ROOT / "config.py",
    ROOT / "pipeline_tasks.py",
    ROOT / "kafka1" / "consumer.py",
    *sorted((ROOT / "observability").glob("*.py")),
    *sorted((ROOT / "governance").glob("*.py")),
]
# os.getenv / os.environ.get, plus the small env helpers those modules use
ENV_PATTERN = re.compile(
    r'(?:os\.getenv|os\.environ\.get|\benv|env_flag|_env_float)\(\s*["\']([A-Z][A-Z0-9_]+)["\']'
)
env_keys_in_config = set()
for source in ENV_SOURCES:
    env_keys_in_config |= set(ENV_PATTERN.findall(source.read_text(encoding="utf-8")))

env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
env_keys_in_example = set(re.findall(r"^([A-Z0-9_]+)=", env_example, re.MULTILINE))

missing_from_example = env_keys_in_config - env_keys_in_example
check(
    not missing_from_example,
    f".env.example is missing keys referenced in the code: {sorted(missing_from_example)}",
)

# --------------------------------------------------------
# 2. requirements-dev.txt pulls in requirements.txt
# --------------------------------------------------------

req_dev = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
check(
    "-r requirements.txt" in req_dev,
    "requirements-dev.txt should start with '-r requirements.txt' so dev installs "
    "stay in sync with production dependencies.",
)

# --------------------------------------------------------
# 3. pytest.ini testpaths exists
# --------------------------------------------------------

pytest_ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
m = re.search(r"testpaths\s*=\s*(\S+)", pytest_ini)
check(m is not None, "pytest.ini has no testpaths set.")
if m:
    check((ROOT / m.group(1)).is_dir(), f"pytest.ini testpaths={m.group(1)!r} does not exist.")

# --------------------------------------------------------
# 4. the workflow files are valid YAML
# --------------------------------------------------------

try:
    import yaml

    for workflow in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        try:
            yaml.safe_load(workflow.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            errors.append(f"{workflow.relative_to(ROOT)} is not valid YAML: {e}")
except ImportError:
    print("(skipping YAML syntax check — pyyaml not installed)")

# --------------------------------------------------------
# Report
# --------------------------------------------------------

if errors:
    print("Config validation FAILED:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)

print("Config validation passed.")
