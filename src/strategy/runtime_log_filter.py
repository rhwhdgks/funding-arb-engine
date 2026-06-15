from __future__ import annotations

import builtins
import re
import sys
import threading
import time
from collections.abc import Callable
from typing import Any


_ORIGINAL_PRINT = builtins.print
_INSTALLED = False

_ACCOUNT_REFRESH_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\]\[account_refresh\]\s+event=(?P<event>\S+).*?"
    r"\breason=(?P<reason>.*?)\s+priority=.*?"
    r"\bsuccess=(?P<success>\S+)\s+error=(?P<error>.*)$"
)
_OKX_RETRY_RE = re.compile(
    r"^\[okx\]\[_request\]\[(?P<kind>retry|retry_exhausted)\]\s+"
    r"path=(?P<path>\S+)\s+reason=(?P<reason>\S+)"
)
_PRIVATE_READ_STARTED_RE = re.compile(
    r"^\[okx\]\[private_read_pause\]\[started\]\s+path=(?P<path>\S+)\s+reason=(?P<reason>\S+)"
)
_RUN_LOOP_ERROR_RE = re.compile(r"^\[[^\]]+\]\[(?P<loop>run_[^\]]+)\]\[error\]")
_WS_RECONNECT_RE = re.compile(r"^\[(?P<exchange>OKXFuture|BinanceFuture) WS\] reconnect after error: (?P<reason>.*)$")
_WS_CALLBACK_RE = re.compile(
    r"^\[(?P<exchange>OKXFuture|BinanceFuture) WS\]\[callback\]\[(?P<channel>[^\]]+)\]\s+(?P<reason>.*)$"
)
_WS_ERROR_RE = re.compile(r"^\[(?P<exchange>OKXFuture|BinanceFuture) WS\]\[error\]\s+(?P<reason>.*)$")

_ACCOUNT_REFRESH_DEFERRED_ERRORS = {"", "already_refreshing", "cooldown", "okx_private_read_paused"}


def _korean_exchange(value: str) -> str:
    text = str(value or "")
    mapping = {
        "OKXFuture": "okx",
        "BinanceFuture": "binance",
        "okxFuture": "okx",
        "binanceFuture": "binance",
    }
    return mapping.get(text, text)


def _korean_reason(value: str) -> str:
    text = str(value or "")
    lowered = text.lower()
    if "no close frame" in lowered:
        return "웹소켓 연결 끊김"
    if lowered in {"http_503", "50001", "service_temporarily_unavailable"}:
        return "거래소 일시 장애"
    if lowered in {"rate_limit", "http_429", "-1003"}:
        return "호출 제한"
    return text or "-"


class _RuntimePrintFilter:
    """Central human-stdout noise filter.

    This wrapper only changes console/stdout rendering. Structured JSONL event
    writes use file IO directly and remain available for reports.
    """

    def __init__(self, printer: Callable[..., None]) -> None:
        self._printer = printer
        self._lock = threading.Lock()
        self._last_emit_monotonic: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        text = self._render_text(args, kwargs)
        if text is None:
            self._printer(*args, **kwargs)
            return

        action, key, cooldown_sec = _classify_runtime_line(text)
        if action == "allow":
            replacement = _human_runtime_line(text)
            if replacement is not None and len(args) == 1 and isinstance(args[0], str):
                args = (replacement,)
            self._printer(*args, **kwargs)
            return
        if action == "suppress":
            return

        if key is None or cooldown_sec <= 0:
            self._printer(*args, **kwargs)
            return

        allowed, suppressed_count = self._rate_limit(key, cooldown_sec)
        if not allowed:
            return

        replacement = _human_runtime_line(text)
        if len(args) == 1 and isinstance(args[0], str):
            output = replacement or args[0]
            if suppressed_count > 0:
                output = f"{output} 숨긴반복={suppressed_count}"
            args = (output,)
        self._printer(*args, **kwargs)

    def _rate_limit(self, key: str, cooldown_sec: float) -> tuple[bool, int]:
        now = time.monotonic()
        with self._lock:
            last_emit = self._last_emit_monotonic.get(key)
            if last_emit is not None and (now - last_emit) < cooldown_sec:
                self._suppressed[key] = self._suppressed.get(key, 0) + 1
                return False, 0
            suppressed_count = self._suppressed.pop(key, 0)
            self._last_emit_monotonic[key] = now
            return True, suppressed_count

    def _render_text(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
        target = kwargs.get("file")
        if target is not None and target is not sys.stdout:
            return None
        if kwargs.get("end", "\n") != "\n":
            return None
        sep = kwargs.get("sep", " ")
        if sep is None:
            sep = " "
        return str(sep).join(str(arg) for arg in args)


def _classify_runtime_line(text: str) -> tuple[str, str | None, float]:
    """Return (action, key, cooldown_sec) for a rendered stdout line."""

    account_match = _ACCOUNT_REFRESH_RE.match(text)
    if account_match is not None:
        event_type = account_match.group("event")
        success = account_match.group("success")
        error = account_match.group("error").strip()
        if event_type == "account_refresh_requested":
            return "suppress", None, 0.0
        if event_type == "account_refresh_executed" and success == "True":
            return "suppress", None, 0.0
        if event_type == "account_refresh_skipped_cooldown" and error in _ACCOUNT_REFRESH_DEFERRED_ERRORS:
            return "suppress", None, 0.0
        return "allow", None, 0.0

    if text.startswith("[okx][private_read_pause] skip "):
        return "suppress", None, 0.0

    private_pause_match = _PRIVATE_READ_STARTED_RE.match(text)
    if private_pause_match is not None:
        key = "okx_private_read_pause_started:{path}:{reason}".format(
            path=private_pause_match.group("path"),
            reason=private_pause_match.group("reason"),
        )
        return "rate_limit", key, 120.0

    ws_match = _WS_RECONNECT_RE.match(text)
    if ws_match is not None:
        key = f"ws_reconnect:{ws_match.group('exchange')}:{ws_match.group('reason')}"
        return "rate_limit", key, 120.0

    ws_callback_match = _WS_CALLBACK_RE.match(text)
    if ws_callback_match is not None:
        key = "ws_callback:{exchange}:{channel}:{reason}".format(
            exchange=ws_callback_match.group("exchange"),
            channel=ws_callback_match.group("channel"),
            reason=ws_callback_match.group("reason"),
        )
        return "rate_limit", key, 120.0

    ws_error_match = _WS_ERROR_RE.match(text)
    if ws_error_match is not None:
        key = f"ws_error:{ws_error_match.group('exchange')}:{ws_error_match.group('reason')}"
        return "rate_limit", key, 120.0

    okx_retry_match = _OKX_RETRY_RE.match(text)
    if okx_retry_match is not None:
        kind = okx_retry_match.group("kind")
        path = okx_retry_match.group("path")
        reason = okx_retry_match.group("reason")
        cooldown_sec = 60.0 if kind == "retry_exhausted" else 30.0
        return "rate_limit", f"okx_request:{kind}:{path}:{reason}", cooldown_sec

    run_error_match = _RUN_LOOP_ERROR_RE.match(text)
    if run_error_match is not None:
        return "rate_limit", f"run_loop_error:{run_error_match.group('loop')}", 60.0

    return "allow", None, 0.0


def _human_runtime_line(text: str) -> str | None:
    """Convert noisy technical lines into concise Korean operator messages."""

    account_match = _ACCOUNT_REFRESH_RE.match(text)
    if account_match is not None:
        error = account_match.group("error").strip() or "-"
        reason = account_match.group("reason").strip() or "-"
        return (
            "[계정갱신][경고] "
            f"거래소={_korean_exchange(account_match.group('exchange'))} "
            f"사유={_korean_reason(reason)} 오류={_korean_reason(error)}"
        )

    private_pause_match = _PRIVATE_READ_STARTED_RE.match(text)
    if private_pause_match is not None:
        return (
            "[거래소][일시중단] "
            f"거래소=okx 종류=private조회 경로={private_pause_match.group('path')} "
            f"사유={_korean_reason(private_pause_match.group('reason'))}"
        )

    ws_match = _WS_RECONNECT_RE.match(text)
    if ws_match is not None:
        return (
            "[시세연결][재연결] "
            f"거래소={_korean_exchange(ws_match.group('exchange'))} "
            f"사유={_korean_reason(ws_match.group('reason'))}"
        )

    ws_callback_match = _WS_CALLBACK_RE.match(text)
    if ws_callback_match is not None:
        return (
            "[시세연결][경고] "
            f"거래소={_korean_exchange(ws_callback_match.group('exchange'))} "
            f"채널={ws_callback_match.group('channel')} "
            f"사유={_korean_reason(ws_callback_match.group('reason'))}"
        )

    ws_error_match = _WS_ERROR_RE.match(text)
    if ws_error_match is not None:
        return (
            "[시세연결][오류] "
            f"거래소={_korean_exchange(ws_error_match.group('exchange'))} "
            f"사유={_korean_reason(ws_error_match.group('reason'))}"
        )

    okx_retry_match = _OKX_RETRY_RE.match(text)
    if okx_retry_match is not None:
        kind = okx_retry_match.group("kind")
        label = "요청실패" if kind == "retry_exhausted" else "요청재시도"
        return (
            f"[거래소][{label}] "
            f"거래소=okx 경로={okx_retry_match.group('path')} "
            f"사유={_korean_reason(okx_retry_match.group('reason'))}"
        )

    run_error_match = _RUN_LOOP_ERROR_RE.match(text)
    if run_error_match is not None:
        return f"[런타임][경고] 반복루프오류={run_error_match.group('loop')}"

    return None


def install_runtime_log_filter() -> None:
    """Install the stdout filter once for the strategy process."""

    global _INSTALLED
    if _INSTALLED:
        return
    builtins.print = _RuntimePrintFilter(_ORIGINAL_PRINT)
    _INSTALLED = True
