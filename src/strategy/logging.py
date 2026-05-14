from __future__ import annotations

from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def fmt_qty(value: Any) -> str:
    return f"{_safe_float(value):.6f}"


def fmt_usd(value: Any) -> str:
    return f"{_safe_float(value):.4f}"


def fmt_bps(value: Any) -> str:
    return f"{_safe_float(value):.2f}"


def korean_side(side: Any) -> str:
    side_text = str(side or "").lower()
    if side_text == "buy":
        return "매수"
    if side_text == "sell":
        return "매도"
    return str(side or "")


def korean_reason(reason: Any) -> str:
    reason_text = str(reason or "")
    mapping = {
        "after_cancel": "취소후정산",
        "cooldown": "재주문쿨다운",
        "final_settle": "최종정산",
        "loop": "계정반영",
        "order_disappeared": "주문정산",
        "before_final_cancel": "최종취소전확인",
        "market": "시장가체결확인",
        "probe_before_cancel": "취소전확인",
        "after_cancel_probe": "취소후확인",
        "min_executable_qty": "최소주문수량미만",
        "micro_residual": "소량잔량",
    }
    if reason_text in mapping:
        return mapping[reason_text]
    if reason_text.endswith("_market"):
        return "시장가체결확인"
    if reason_text.endswith("_probe_before_cancel"):
        return "취소전확인"
    if reason_text.endswith("_after_cancel"):
        return "취소후확인"
    return reason_text or "확인"


def korean_close_reason(reason: Any) -> str:
    reason_text = str(reason or "")
    mapping = {
        "stop_loss": "손절",
        "take_profit": "익절",
        "max_hold": "최대보유",
        "close_candidate": "후보전환",
        "manual": "수동",
        "shutdown": "종료정리",
    }
    return mapping.get(reason_text, reason_text or "확인")


def log_entry_attempt(
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    expected_net_edge_usd: float,
    expected_net_edge_bps: float,
) -> None:
    print(
        f"[진입][시도] 심볼={symbol} 롱={long_exchange} 숏={short_exchange} "
        f"예상손익USD={fmt_usd(expected_net_edge_usd)} 예상bps={fmt_bps(expected_net_edge_bps)}"
    )


def log_entry_blocked(*, symbol: str, reasons: Any) -> None:
    print(f"[진입][차단] 심볼={symbol} 사유={reasons}")


def log_entry_partial_fill(
    *,
    symbol: str,
    exchange: str,
    side: str,
    filled_delta: float,
    filled_total: float,
    target_qty: float,
    remaining_qty: float,
    reason: str,
) -> None:
    fill_label = "부분체결" if abs(float(remaining_qty or 0.0)) > 1e-12 else "완료"
    print(
        f"[진입][{fill_label}] 심볼={symbol} 거래소={exchange} 방향={korean_side(side)} "
        f"이번체결={fmt_qty(filled_delta)} 누적체결={fmt_qty(filled_total)} "
        f"목표={fmt_qty(target_qty)} 잔량={fmt_qty(remaining_qty)} 사유={korean_reason(reason)}"
    )


def log_hedge_start(
    *,
    symbol: str,
    exchange: str,
    side: str,
    target_qty: float,
    stage_label: str,
    is_close: bool,
) -> None:
    label = "청산헷지" if is_close else "헷지"
    stage = "" if stage_label in ("", "entry") else f" 단계={stage_label}"
    print(
        f"[{label}][시작] 심볼={symbol} 거래소={exchange} 방향={korean_side(side)} "
        f"목표수량={fmt_qty(target_qty)}{stage}"
    )


def log_hedge_partial_fill(
    *,
    symbol: str,
    exchange: str,
    side: str,
    filled_delta: float,
    filled_total: float,
    target_qty: float,
    remaining_qty: float,
    stage_label: str,
    is_close: bool,
    reason: str,
) -> None:
    label = "청산헷지" if is_close else "헷지"
    fill_label = "부분체결" if abs(float(remaining_qty or 0.0)) > 1e-12 else "체결"
    stage = "" if stage_label in ("", "entry") else f" 단계={stage_label}"
    print(
        f"[{label}][{fill_label}] 심볼={symbol} 거래소={exchange} 방향={korean_side(side)} "
        f"이번헷지={fmt_qty(filled_delta)} 누적헷지={fmt_qty(filled_total)} "
        f"목표={fmt_qty(target_qty)} 잔량={fmt_qty(remaining_qty)} 사유={korean_reason(reason)}{stage}"
    )


def log_hedge_done(
    *,
    symbol: str,
    exchange: str,
    filled_qty: float,
    remaining_qty: float,
    stage_label: str,
    is_close: bool,
) -> None:
    label = "청산헷지" if is_close else "헷지"
    stage = "" if stage_label in ("", "entry") else f" 단계={stage_label}"
    print(
        f"[{label}][완료] 심볼={symbol} 거래소={exchange} "
        f"체결={fmt_qty(filled_qty)} 잔량={fmt_qty(remaining_qty)}{stage}"
    )


def log_residual_ignored(
    *,
    symbol: str,
    first_filled_qty: float,
    hedged_qty: float,
    residual_qty: float,
    reason: str = "min_executable_qty",
) -> None:
    print(
        f"[헷지][잔량무시] 심볼={symbol} 첫체결={fmt_qty(first_filled_qty)} "
        f"헷지완료={fmt_qty(hedged_qty)} 잔량={fmt_qty(residual_qty)} 사유={korean_reason(reason)}"
    )


def log_hedge_incomplete(
    *,
    symbol: str,
    first_filled_qty: float,
    hedged_qty: float,
    remaining_qty: float,
) -> None:
    print(
        f"[위험][헷지미완료] 심볼={symbol} 첫체결={fmt_qty(first_filled_qty)} "
        f"헷지완료={fmt_qty(hedged_qty)} 잔량={fmt_qty(remaining_qty)}"
    )


def log_pair_opened(
    *,
    pair_id: str,
    symbol: str,
    qty: float,
    long_exchange: str,
    short_exchange: str,
) -> None:
    print(
        f"[페어][오픈] id={pair_id} 심볼={symbol} 수량={fmt_qty(qty)} "
        f"롱={long_exchange} 숏={short_exchange}"
    )


def log_pair_close_trigger(*, pair_id: str, symbol: str, reason: str) -> None:
    print(f"[페어][청산트리거] id={pair_id} 심볼={symbol} 사유={korean_close_reason(reason)}")


def log_pair_close_attempt(
    *,
    attempt: int,
    exchange: str,
    symbol: str,
    side: str,
    qty: float,
    price_preview: float,
    min_executable_qty: float,
) -> None:
    print(
        f"[페어종료][주문시도] 시도={attempt} 거래소={exchange} 심볼={symbol} "
        f"방향={korean_side(side)} 수량={fmt_qty(qty)} 예상가={price_preview:.8f} "
        f"최소수량={min_executable_qty:.12f}"
    )


def log_pair_closed(*, pair_id: str) -> None:
    print(f"[페어][종료완료] id={pair_id}")


def log_pair_closed_detected(*, pair_id: str) -> None:
    print(f"[페어][종료감지] id={pair_id}")


def log_micro_partial(*, symbol: str, requested_qty: float, pair_qty: float, ratio: float) -> None:
    print(
        f"[진입][소량부분체결] 심볼={symbol} 요청={fmt_qty(requested_qty)} "
        f"페어수량={fmt_qty(pair_qty)} 비율={ratio:.6f}"
    )


def log_overfill(
    *,
    symbol: str,
    requested_qty: float,
    pair_qty: float,
    overfill_qty: float,
    long_executable_qty: float,
    short_executable_qty: float,
    actionable: bool,
) -> None:
    label = "[위험][초과체결]" if actionable else "[진입][초과체결무시]"
    print(
        f"{label} 심볼={symbol} 요청={fmt_qty(requested_qty)} 페어수량={fmt_qty(pair_qty)} "
        f"초과={fmt_qty(overfill_qty)} 롱정리가능={fmt_qty(long_executable_qty)} "
        f"숏정리가능={fmt_qty(short_executable_qty)}"
    )


def log_danger(*, title: str, **payload: Any) -> None:
    label_map = {
        "symbol": "심볼",
        "result": "결과",
        "long_exchange": "롱거래소",
        "short_exchange": "숏거래소",
        "long_qty": "롱수량",
        "short_qty": "숏수량",
        "long_before": "롱이전",
        "long_after": "롱이후",
        "long_delta": "롱증가",
        "short_before": "숏이전",
        "short_after": "숏이후",
        "short_delta": "숏증가",
        "long_entry": "롱평단",
        "short_entry": "숏평단",
        "failed_exchange": "실패거래소",
        "failed_side": "실패방향",
        "failed_qty": "실패수량",
        "reason": "사유",
        "other_leg_action": "반대레그",
        "net_qty": "순수량",
        "residual_gap": "잔량차이",
        "state": "상태",
        "flattened": "종료성공",
        "cause": "원인",
    }
    details = " ".join(f"{label_map.get(key, key)}={value}" for key, value in payload.items())
    suffix = f" {details}" if details else ""
    print(f"[위험][{title}]{suffix}")
