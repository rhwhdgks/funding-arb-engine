#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int | None) -> str:
    if not ms:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def load_env_file(path: Path | None) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path or not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key:
            values[key] = value
    return values


def load_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def resolve_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def default_artifact_path(meta_path: Path, prefix: str, suffix: str, label: str) -> Path:
    safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (label or "latest"))
    return meta_path.parent / f"{prefix}_{safe_label}{suffix}"


def candidate_key(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    return str(candidate.get("candidate_id") or "")


def candidate_market_data_ready(candidate: dict[str, Any]) -> bool:
    reasons = [str(reason) for reason in (candidate.get("normalized_block_reasons") or candidate.get("block_reasons") or [])]
    bad_tokens = ("ask_missing", "bid_missing", "ws_not_ready", "stale_orderbook")
    return not any(any(token in reason for token in bad_tokens) for reason in reasons)


def update_candidate_stats(stats: dict[str, Any], candidate: dict[str, Any], *, enterable: bool) -> None:
    key = candidate_key(candidate)
    if not key:
        return
    row = stats.setdefault(
        key,
        {
            "candidate_id": key,
            "symbol": candidate.get("symbol"),
            "pair": candidate.get("pair"),
            "observations": 0,
            "enterable_observations": 0,
            "max_expected_net_edge_usd": None,
            "max_expected_net_edge_bps": None,
            "market_ready_observations": 0,
            "max_market_ready_edge_usd": None,
            "max_market_ready_edge_bps": None,
            "last_expected_net_edge_usd": None,
            "last_expected_net_edge_bps": None,
            "last_reasons": [],
        },
    )
    edge_usd = safe_float(candidate.get("expected_net_edge_usd"), 0.0)
    edge_bps = safe_float(candidate.get("expected_net_edge_bps"), 0.0)
    market_ready = candidate_market_data_ready(candidate)
    row["observations"] += 1
    if enterable:
        row["enterable_observations"] += 1
    if row["max_expected_net_edge_usd"] is None or edge_usd > float(row["max_expected_net_edge_usd"]):
        row["max_expected_net_edge_usd"] = edge_usd
    if row["max_expected_net_edge_bps"] is None or edge_bps > float(row["max_expected_net_edge_bps"]):
        row["max_expected_net_edge_bps"] = edge_bps
    if market_ready:
        row["market_ready_observations"] += 1
        if row["max_market_ready_edge_usd"] is None or edge_usd > float(row["max_market_ready_edge_usd"]):
            row["max_market_ready_edge_usd"] = edge_usd
        if row["max_market_ready_edge_bps"] is None or edge_bps > float(row["max_market_ready_edge_bps"]):
            row["max_market_ready_edge_bps"] = edge_bps
    row["last_expected_net_edge_usd"] = edge_usd
    row["last_expected_net_edge_bps"] = edge_bps
    row["last_reasons"] = list(candidate.get("normalized_block_reasons") or candidate.get("block_reasons") or [])[:5]


def summarize(rows: list[dict[str, Any]], *, since_ms: int, run_meta: dict[str, Any]) -> dict[str, Any]:
    run_rows = [row for row in rows if safe_int(row.get("ts_ms"), 0) >= since_ms]
    event_counts = Counter(str(row.get("event_type") or row.get("event") or "unknown") for row in run_rows)
    summaries = [row for row in run_rows if (row.get("event_type") or row.get("event")) == "candidate_scan_summary"]
    reason_totals: Counter[str] = Counter()
    top_stats: dict[str, Any] = {}
    enterable_stats: dict[str, Any] = {}
    enterable_scan_count = 0
    max_enterable = 0
    enterable_sum = 0

    for summary in summaries:
        enterable_count = safe_int(summary.get("enterable_candidates"), 0)
        enterable_sum += enterable_count
        max_enterable = max(max_enterable, enterable_count)
        if enterable_count > 0:
            enterable_scan_count += 1
        for reason, count in (summary.get("reason_counts") or {}).items():
            reason_totals[str(reason)] += safe_int(count, 0)
        top = summary.get("top_candidate")
        if isinstance(top, dict):
            update_candidate_stats(top_stats, top, enterable=bool(top.get("can_enter")))
        top_enterable = summary.get("top_enterable_candidate")
        if isinstance(top_enterable, dict):
            update_candidate_stats(enterable_stats, top_enterable, enterable=True)

    first_ts = safe_int(run_rows[0].get("ts_ms"), 0) if run_rows else since_ms
    last_ts = safe_int(run_rows[-1].get("ts_ms"), 0) if run_rows else since_ms
    first_scan_ts = safe_int(summaries[0].get("ts_ms"), 0) if summaries else 0
    last_scan_ts = safe_int(summaries[-1].get("ts_ms"), 0) if summaries else 0
    latest_summary = summaries[-1] if summaries else {}

    market_ready_top_candidates = [
        row
        for row in top_stats.values()
        if safe_int(row.get("market_ready_observations"), 0) > 0
    ]
    top_candidates = sorted(
        market_ready_top_candidates,
        key=lambda item: (
            safe_float(item.get("max_market_ready_edge_bps"), -999999.0),
            safe_float(item.get("max_market_ready_edge_usd"), -999999.0),
            safe_int(item.get("market_ready_observations"), 0),
        ),
        reverse=True,
    )
    top_enterable_candidates = sorted(
        enterable_stats.values(),
        key=lambda item: (
            safe_int(item.get("enterable_observations"), 0),
            safe_float(item.get("max_expected_net_edge_bps"), -999999.0),
        ),
        reverse=True,
    )

    submit_count = event_counts.get("entry_limit_order_submit_result", 0)
    pair_opened_count = event_counts.get("pair_opened", 0)
    first_leg_fill_count = event_counts.get("entry_first_leg_fill_progress", 0)
    hedge_fill_count = event_counts.get("entry_hedge_fill_progress", 0)

    diagnosis: list[str] = []
    if not summaries:
        diagnosis.append("candidate_scan_summary 없음: 후보 판단 로그가 충분하지 않음")
    elif enterable_scan_count == 0:
        top_reasons = ", ".join(reason for reason, _ in reason_totals.most_common(3))
        diagnosis.append(f"진입 가능 후보가 관측되지 않음. 주요 차단 이유: {top_reasons or '-'}")
    if submit_count > pair_opened_count:
        diagnosis.append(
            f"주문 제출 {submit_count}건 대비 페어 오픈 {pair_opened_count}건: first-leg 지정가 미체결/취소 영향이 큼"
        )
    if pair_opened_count > 0 and event_counts.get("pair_closed", 0) >= pair_opened_count:
        diagnosis.append("열린 페어는 종료까지 이어짐")
    if not diagnosis:
        diagnosis.append("특이사항 없음")

    return {
        "generated_at_ms": now_ms(),
        "generated_at_utc": iso_utc(now_ms()),
        "since_ms": since_ms,
        "since_utc": iso_utc(since_ms),
        "run_started_at_utc": run_meta.get("started_at_iso"),
        "jsonl_log": run_meta.get("jsonl_log"),
        "duration_hours_observed": round(max(last_ts - first_ts, 0) / 3_600_000, 4),
        "scan_count": len(summaries),
        "first_scan_utc": iso_utc(first_scan_ts),
        "last_scan_utc": iso_utc(last_scan_ts),
        "enterable_scan_count": enterable_scan_count,
        "enterable_scan_ratio": (enterable_scan_count / len(summaries)) if summaries else 0.0,
        "avg_enterable_candidates": (enterable_sum / len(summaries)) if summaries else 0.0,
        "max_enterable_candidates": max_enterable,
        "event_counts": dict(event_counts.most_common()),
        "order_flow": {
            "entry_limit_order_submit_result": submit_count,
            "entry_first_leg_fill_progress": first_leg_fill_count,
            "entry_hedge_fill_progress": hedge_fill_count,
            "pair_opened": pair_opened_count,
            "pair_closed": event_counts.get("pair_closed", 0),
        },
        "reason_totals": dict(reason_totals.most_common(20)),
        "latest_summary": latest_summary,
        "top_candidates_by_edge": top_candidates[:10],
        "top_enterable_candidates": top_enterable_candidates[:10],
        "diagnosis": diagnosis,
    }


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    if not rows:
        return ["- none"]
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return out


def render_markdown(report: dict[str, Any]) -> str:
    latest = report.get("latest_summary") or {}
    latest_top = latest.get("top_candidate") or {}
    reason_rows = [
        {"reason": reason, "count": count}
        for reason, count in (report.get("reason_totals") or {}).items()
    ]
    candidate_rows = []
    for row in report.get("top_candidates_by_edge") or []:
        candidate_rows.append(
            {
                "candidate": row.get("candidate_id"),
                "symbol": row.get("symbol"),
                "pair": row.get("pair"),
                "max_bps": f"{safe_float(row.get('max_market_ready_edge_bps')):.4f}",
                "max_usd": f"{safe_float(row.get('max_market_ready_edge_usd')):.6f}",
                "observations": row.get("market_ready_observations"),
                "last_reasons": ",".join(row.get("last_reasons") or []),
            }
        )

    lines = [
        "# Candidate Opportunity Report",
        "",
        "## Summary",
        f"- generated_at_utc: {report.get('generated_at_utc')}",
        f"- since_utc: {report.get('since_utc')}",
        f"- run_started_at_utc: {report.get('run_started_at_utc') or '-'}",
        f"- scan_count: {report.get('scan_count')}",
        f"- enterable_scan_count: {report.get('enterable_scan_count')}",
        f"- enterable_scan_ratio: {float(report.get('enterable_scan_ratio') or 0):.4f}",
        f"- avg_enterable_candidates: {float(report.get('avg_enterable_candidates') or 0):.4f}",
        f"- max_enterable_candidates: {report.get('max_enterable_candidates')}",
        "",
        "## Order Flow",
    ]
    for key, value in (report.get("order_flow") or {}).items():
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## Diagnosis"])
    for item in report.get("diagnosis") or []:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Latest Top Candidate",
            f"- ts_utc: {latest.get('ts_utc') or '-'}",
            f"- candidate: {latest_top.get('candidate_id') or '-'}",
            f"- expected_net_edge_bps: {safe_float(latest_top.get('expected_net_edge_bps')):.4f}",
            f"- expected_net_edge_usd: {safe_float(latest_top.get('expected_net_edge_usd')):.6f}",
            f"- reasons: {','.join(latest_top.get('normalized_block_reasons') or latest_top.get('block_reasons') or []) or '-'}",
            "",
            "## Block Reason Totals",
            *markdown_table(reason_rows[:20], ["reason", "count"]),
            "",
            "## Top Market-Ready Candidates By Observed Edge",
            *markdown_table(candidate_rows, ["candidate", "symbol", "pair", "max_bps", "max_usd", "observations", "last_reasons"]),
        ]
    )
    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize candidate opportunity and no-entry reasons for a strategy run.")
    parser.add_argument("--env-file", default=os.getenv("STRATEGY_ENV_FILE"))
    parser.add_argument("--meta", default=os.getenv("STRATEGY_LIVE_RUN_META_FILE"))
    parser.add_argument("--jsonl", default=os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH"))
    parser.add_argument("--since-ms", type=int)
    parser.add_argument("--hours", type=float)
    parser.add_argument("--label", default="latest")
    parser.add_argument("--output")
    parser.add_argument("--json-output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_values = load_env_file(resolve_path(args.env_file, PROJECT_ROOT / ".env") if args.env_file else None)
    meta_path = resolve_path(args.meta, PROJECT_ROOT / "logs" / "strategy_live.run.json")
    run_meta = load_json(meta_path)

    jsonl_path = resolve_path(
        args.jsonl or run_meta.get("jsonl_log") or env_values.get("STRATEGY_LIVE_EVENT_LOG_PATH"),
        PROJECT_ROOT / "logs" / "strategy_live.jsonl",
    )
    since_ms = int(
        args.since_ms
        or run_meta.get("started_at_ms")
        or (now_ms() - int(float(args.hours or 24.0) * 3_600_000))
    )

    report = summarize(iter_jsonl(jsonl_path), since_ms=since_ms, run_meta=run_meta)
    report["jsonl_log"] = str(jsonl_path)
    output_path = resolve_path(args.output, default_artifact_path(meta_path, "candidate_opportunity_report", ".md", args.label))
    json_output_path = resolve_path(args.json_output, default_artifact_path(meta_path, "candidate_opportunity_report", ".json", args.label))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(report), encoding="utf-8")
    write_json(json_output_path, report)
    print(
        f"candidate_opportunity_report scans={report['scan_count']} "
        f"enterable_scans={report['enterable_scan_count']} "
        f"pair_opened={report['order_flow']['pair_opened']} output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
