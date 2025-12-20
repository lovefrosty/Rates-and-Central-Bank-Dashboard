import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import importlib
import json

REQUIRED_FILES = [
    ROOT / "update.py",
    ROOT / "Data" / "__init__.py",
    ROOT / "Signals" / "__init__.py",
    ROOT / "UI" / "__init__.py",
    ROOT / "Signals" / "validate.py",
    ROOT / "Signals" / "raw_state_schema.py",
    ROOT / "Data" / "fetch_policy.py",
    ROOT / "Data" / "fetch_yields.py",
    ROOT / "Data" / "fetch_vol.py",
    ROOT / "Data" / "fetch_liquidity.py",
    ROOT / "Signals" / "resolve_yield_curve.py",
    ROOT / "Signals" / "resolve_policy.py",
    ROOT / "Signals" / "resolve_liquidity.py",
    ROOT / "Signals" / "resolve_vol.py",
    ROOT / "signals" / "raw_state.json",
]

OPTIONAL_FILES = [
    ROOT / "Signals" / "daily_state.json",
]

REQUIRED_MODULES = [
    "Data.fetch_policy",
    "Data.fetch_yields",
    "Data.fetch_vol",
    "Data.fetch_liquidity",
    "Signals.resolve_yield_curve",
    "Signals.resolve_policy",
    "Signals.resolve_liquidity",
    "Signals.resolve_vol",
]

REQUIRED_RAW_STATE_KEYS = [
    "meta",
    "policy",
    "duration",
    "volatility",
    "liquidity",
]

REQUIRED_DAILY_STATE_KEYS = [
    "meta",
]


def audit_project():
    errors = []
    warnings = []

    # 1. File existence
    for path in REQUIRED_FILES:
        if not path.exists():
            errors.append(f"Missing file: {path}")

    for path in OPTIONAL_FILES:
        if not path.exists():
            warnings.append(f"Missing optional file: {path}")

    # 2. Import checks (no symbol expectations)
    for module_path in REQUIRED_MODULES:
        try:
            importlib.import_module(module_path)
        except Exception as e:
            errors.append(f"Failed to import {module_path}: {e}")

    # 3. raw_state.json structure
    raw_path = ROOT / "signals" / "raw_state.json"
    if raw_path.exists():
        with open(raw_path) as f:
            raw = json.load(f)
        for k in REQUIRED_RAW_STATE_KEYS:
            if k not in raw:
                errors.append(f"raw_state.json missing key: {k}")

    # 4. daily_state.json structure (optional)
    daily_path = ROOT / "Signals" / "daily_state.json"
    if daily_path.exists():
        with open(daily_path) as f:
            daily = json.load(f)
        for k in REQUIRED_DAILY_STATE_KEYS:
            if k not in daily:
                errors.append(f"daily_state.json missing key: {k}")

    if errors:
        message = "❌ PROJECT AUDIT FAILED:\n" + "\n".join(f"- {e}" for e in errors)
        if warnings:
            message += "\nWarnings:\n" + "\n".join(f"- {w}" for w in warnings)
        raise RuntimeError(message)

    print("✅ PROJECT AUDIT PASSED")
    for warning in warnings:
        print(f"- {warning}")


if __name__ == "__main__":
    try:
        audit_project()
    except Exception as e:
        print(e)
        sys.exit(1)
