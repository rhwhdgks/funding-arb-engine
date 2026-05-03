#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ACTION="${1:-status}"
REASON="${2:-manual_pause}"
PYTHON_BIN="${STRATEGY_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

export PROJECT_ROOT
export ACTION
export REASON
export STRATEGY_ENV_FILE="${STRATEGY_ENV_FILE:-$PROJECT_ROOT/.env}"

"$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


project_root = Path(os.environ["PROJECT_ROOT"])
action = os.environ["ACTION"].strip().lower()
reason = os.environ["REASON"].strip() or "manual_pause"
env_file = Path(os.environ.get("STRATEGY_ENV_FILE") or project_root / ".env")
if not env_file.is_absolute():
    env_file = project_root / env_file


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        os.environ.setdefault(key, value)


def normalize_reason(value: str) -> str:
    text = re.sub(r"\s+", "_", str(value or "").strip().lower())
    text = re.sub(r"[^a-z0-9_.:-]+", "_", text)
    return text.strip("_") or "manual_pause"


def control_path() -> Path:
    raw = os.environ.get("STRATEGY_ENTRY_CONTROL_FILE")
    path = Path(raw) if raw else project_root / "runtime" / "entry_control.json"
    if not path.is_absolute():
        path = project_root / path
    return path


def current_payload(path: Path) -> dict:
    if not path.exists():
        return {"entry_paused": False, "reason": "", "source_path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"entry_paused": True, "reason": "entry_control_read_error", "error": str(exc), "source_path": str(path)}
    if not isinstance(payload, dict):
        return {"entry_paused": True, "reason": "entry_control_invalid_payload", "source_path": str(path)}
    payload["source_path"] = str(path)
    return payload


def write_payload(path: Path, paused: bool, pause_reason: str) -> dict:
    now_ms = int(time.time() * 1000)
    payload = {
        "entry_paused": bool(paused),
        "reason": normalize_reason(pause_reason) if paused else normalize_reason(pause_reason or "manual_resume"),
        "updated_at_ms": now_ms,
        "updated_at_utc": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "updated_by": os.environ.get("USER") or "operator",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)
    payload["source_path"] = str(path)
    return payload


load_env_file(env_file)
path = control_path()

if action in {"on", "pause", "paused", "1", "true"}:
    payload = write_payload(path, True, reason)
elif action in {"off", "resume", "unpause", "0", "false"}:
    payload = write_payload(path, False, reason if reason != "manual_pause" else "manual_resume")
elif action in {"status", "show", "cat"}:
    payload = current_payload(path)
else:
    print("usage: scripts/runtime/entry_pause.sh [on|off|status] [reason]", file=sys.stderr)
    sys.exit(2)

print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
