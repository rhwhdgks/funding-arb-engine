#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import tarfile
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN_BASENAMES = {".env", ".env.notion", ".env.collector"}
PLACEHOLDERS = {
    "",
    "changeme",
    "change_me",
    "example",
    "placeholder",
    "redacted",
    "your_key_here",
    "your_secret_here",
}
SECRET_ASSIGNMENT = re.compile(
    rb"(?im)^[ \t]*([A-Z0-9_]*(?:SECRET|PASSWORD|PASSPHRASE|TOKEN|ACCESS_KEY|API_KEY)[A-Z0-9_]*)[ \t]*=[ \t]*([^#\r\n]*)"
)
# Keep the marker split so scanning this scanner does not flag its own source.
PRIVATE_KEY_MARKER = b"-----BEGIN " + b"PRIVATE KEY-----"


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=PROJECT_ROOT)


def tracked_files() -> list[str]:
    return sorted(item.decode() for item in git("ls-files", "-z").split(b"\0") if item)


def secret_findings(path: str, data: bytes) -> list[str]:
    findings: list[str] = []
    base = Path(path).name
    is_private_env = base == ".env" or (
        base.startswith(".env.") and not base.endswith(".example")
    )
    if base in FORBIDDEN_BASENAMES or is_private_env:
        findings.append(f"forbidden runtime secret file: {path}")
    if PRIVATE_KEY_MARKER in data:
        findings.append(f"private key material: {path}")
    for match in SECRET_ASSIGNMENT.finditer(data):
        key = match.group(1).decode(errors="replace")
        if key.endswith(("_REQUIRED", "_ENABLED", "_FILE", "_SEC", "_MS")):
            continue
        raw_value = match.group(2).decode(errors="replace").strip()
        if "<" in raw_value and ">" in raw_value:
            continue
        if any(
            marker in raw_value
            for marker in (
                "$",
                "os.getenv(",
                "os.environ",
                ".get(",
                "getattr(",
                "request.",
                "payload",
                "None",
                "str(",
                "bool(",
            )
        ):
            continue
        suffix = Path(path).suffix.lower()
        if suffix in {".py", ".js", ".ts"} and not (
            len(raw_value) >= 2 and raw_value[0] in {"\"", "'"} and raw_value[-1] == raw_value[0]
        ):
            continue
        value = raw_value.strip("\"'")
        normalized = value.lower().replace("<", "").replace(">", "")
        if normalized in PLACEHOLDERS or normalized.startswith("your_"):
            continue
        findings.append(f"non-placeholder secret assignment: {path}:{key}")
    return findings


def build(output: Path) -> None:
    dirty = git("status", "--porcelain=v1").decode().strip()
    if dirty:
        raise SystemExit("release refused: working tree is dirty; commit intentionally before packaging")

    files = tracked_files()
    rows: list[dict[str, str | int]] = []
    findings: list[str] = []
    payloads: dict[str, bytes] = {}
    for relative in files:
        data = (PROJECT_ROOT / relative).read_bytes()
        payloads[relative] = data
        findings.extend(secret_findings(relative, data))
        rows.append(
            {
                "path": relative,
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
            }
        )
    if findings:
        raise SystemExit("release refused:\n- " + "\n- ".join(findings))

    manifest = {
        "format": 1,
        "commit": git("rev-parse", "HEAD").decode().strip(),
        "created_at_ms": int(time.time() * 1000),
        "source": "clean tracked git files",
        "file_count": len(rows),
        "files": rows,
    }
    manifest_data = (json.dumps(manifest, ensure_ascii=True, indent=2) + "\n").encode()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w:xz") as archive:
        for row in rows:
            relative = str(row["path"])
            info = tarfile.TarInfo(f"funding-arb/{relative}")
            info.size = int(row["size"])
            info.mode = (PROJECT_ROOT / relative).stat().st_mode & 0o777
            archive.addfile(info, io.BytesIO(payloads[relative]))
        info = tarfile.TarInfo("funding-arb/RELEASE_MANIFEST.json")
        info.size = len(manifest_data)
        info.mode = 0o644
        archive.addfile(info, io.BytesIO(manifest_data))
    print(f"release_artifact={output} commit={manifest['commit']} files={len(rows)} secret_findings=0")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a clean, secret-scanned handoff artifact")
    parser.add_argument("--output", default="dist/funding-arb-handoff.tar.xz")
    args = parser.parse_args()
    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = PROJECT_ROOT / output
    build(output)


if __name__ == "__main__":
    main()
