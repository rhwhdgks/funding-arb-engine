#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess

from build_handoff_artifact import PROJECT_ROOT, secret_findings, tracked_files


def _git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=PROJECT_ROOT)


def current_tree_paths(*, include_untracked: bool) -> list[str]:
    paths = set(tracked_files())
    if include_untracked:
        raw = _git("ls-files", "--others", "--exclude-standard", "-z")
        paths.update(item.decode() for item in raw.split(b"\0") if item)
    return sorted(paths)


def scan_current_tree(*, include_untracked: bool) -> list[str]:
    findings: set[str] = set()
    for relative in current_tree_paths(include_untracked=include_untracked):
        path = PROJECT_ROOT / relative
        if path.is_file():
            findings.update(secret_findings(relative, path.read_bytes()))
    return sorted(findings)


def _history_objects() -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for raw_line in _git("rev-list", "--objects", "--all").decode(errors="replace").splitlines():
        object_id, separator, relative = raw_line.partition(" ")
        if object_id:
            rows.append((object_id, relative if separator else ""))
    return rows


def scan_history() -> list[str]:
    findings: set[str] = set()
    seen_objects: set[str] = set()
    process = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        cwd=PROJECT_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    try:
        for object_id, relative in _history_objects():
            if object_id in seen_objects:
                continue
            seen_objects.add(object_id)
            process.stdin.write(f"{object_id}\n".encode())
            process.stdin.flush()
            header = process.stdout.readline().decode(errors="replace").strip().split()
            if len(header) < 3 or header[1] == "missing":
                continue
            object_type = header[1]
            size = int(header[2])
            data = process.stdout.read(size)
            process.stdout.read(1)
            if object_type == "blob" and relative:
                findings.update(secret_findings(relative, data))
    finally:
        process.stdin.close()
        process.wait(timeout=10)
    return sorted(findings)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit publishable files and optional Git history without printing secret values"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="also scan every reachable Git blob, including deleted files",
    )
    parser.add_argument(
        "--include-untracked",
        action="store_true",
        help="include non-ignored untracked files in the current-tree scan",
    )
    args = parser.parse_args()

    current = scan_current_tree(include_untracked=args.include_untracked)
    history = scan_history() if args.history else []
    findings = sorted(set(current + history))
    if findings:
        print(
            f"repository_audit=FAIL current_findings={len(current)} "
            f"history_findings={len(history)}"
        )
        for finding in findings:
            print(f"- {finding}")
        raise SystemExit(1)
    print(
        f"repository_audit=PASS files={len(current_tree_paths(include_untracked=args.include_untracked))} "
        f"history_scanned={'yes' if args.history else 'no'} findings=0"
    )


if __name__ == "__main__":
    main()
