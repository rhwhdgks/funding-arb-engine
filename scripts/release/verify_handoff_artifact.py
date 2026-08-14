#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

from build_handoff_artifact import secret_findings


def verify(path: Path) -> None:
    with tarfile.open(path, "r:xz") as archive:
        members = {member.name: member for member in archive.getmembers() if member.isfile()}
        manifest_member = members.get("funding-arb/RELEASE_MANIFEST.json")
        if manifest_member is None:
            raise SystemExit("artifact verification failed: RELEASE_MANIFEST.json missing")
        handle = archive.extractfile(manifest_member)
        manifest = json.load(handle) if handle is not None else {}
        expected = {f"funding-arb/{row['path']}": row for row in manifest.get("files") or []}
        actual = set(members) - {"funding-arb/RELEASE_MANIFEST.json"}
        missing = sorted(set(expected) - actual)
        extra = sorted(actual - set(expected))
        findings: list[str] = []
        corrupt: list[str] = []
        for name, row in expected.items():
            member = members.get(name)
            if member is None:
                continue
            file_handle = archive.extractfile(member)
            data = file_handle.read() if file_handle is not None else b""
            relative = name.removeprefix("funding-arb/")
            if hashlib.sha256(data).hexdigest() != row.get("sha256"):
                corrupt.append(relative)
            findings.extend(secret_findings(relative, data))
    if missing or extra or corrupt or findings:
        raise SystemExit(
            "artifact verification failed: "
            f"missing={missing} extra={extra} corrupt={corrupt} secret_findings={findings}"
        )
    print(
        f"artifact_verdict=PASS commit={manifest.get('commit')} "
        f"files={len(expected)} secret_findings=0"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify handoff inventory, hashes, and secrets")
    parser.add_argument("artifact")
    args = parser.parse_args()
    verify(Path(args.artifact).expanduser().resolve())


if __name__ == "__main__":
    main()
