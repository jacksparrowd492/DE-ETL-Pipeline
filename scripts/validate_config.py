"""
validate_config.py
-------------------
Lightweight config sanity check for CI's "Config Validation" job. Doesn't
need any real credentials — just checks the *shape* of the project's
configuration is consistent:

  1. Every os.getenv("KEY") referenced in config.py has a matching KEY=
     line in .env.example, so a fresh clone always has a complete template
     to copy to .env.
  2. requirements-dev.txt actually pulls in requirements.txt.
  3. pytest.ini points testpaths at a directory that exists.
  4. The GitHub Actions workflow file itself is valid YAML.

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
# 1. config.py's os.getenv(...) keys <-> .env.example
# --------------------------------------------------------

config_src = (ROOT / "config.py").read_text(encoding="utf-8")
env_keys_in_config = set(re.findall(r'os\.getenv\(\s*["\']([A-Z0-9_]+)["\']', config_src))

env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
env_keys_in_example = set(re.findall(r"^([A-Z0-9_]+)=", env_example, re.MULTILINE))

missing_from_example = env_keys_in_config - env_keys_in_example
check(
    not missing_from_example,
    f".env.example is missing keys referenced in config.py: {sorted(missing_from_example)}",
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
# 4. ci.yml is valid YAML
# --------------------------------------------------------

try:
    import yaml

    yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
except ImportError:
    print("(skipping YAML syntax check — pyyaml not installed)")
except Exception as e:  # noqa: BLE001
    errors.append(f".github/workflows/ci.yml is not valid YAML: {e}")

# --------------------------------------------------------
# Report
# --------------------------------------------------------

if errors:
    print("Config validation FAILED:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)

print("Config validation passed.")
