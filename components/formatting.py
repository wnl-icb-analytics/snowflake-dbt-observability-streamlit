"""Display formatters and status labels shared across pages."""

from datetime import datetime

import pandas as pd


def is_missing(value) -> bool:
    """True for None, NaN and NaT."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def format_timestamp(ts) -> str:
    """Format a timestamp handling both datetime and string types."""
    if is_missing(ts):
        return "N/A"
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(ts)[:16] if ts else "N/A"


def format_relative_time(ts) -> str:
    """Format a timestamp as relative time (e.g. '2h ago')."""
    if is_missing(ts):
        return "N/A"
    try:
        if isinstance(ts, str):
            ts = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        seconds = (datetime.now() - ts).total_seconds()
        if seconds < 60:
            return "Just now"
        if seconds < 3600:
            return f"{int(seconds // 60)}m ago"
        if seconds < 86400:
            return f"{int(seconds // 3600)}h ago"
        return f"{int(seconds // 86400)}d ago"
    except Exception:
        return format_timestamp(ts)


def truncate(text, max_len: int = 50) -> str:
    """Truncate text with an ellipsis."""
    if is_missing(text) or not text:
        return ""
    text = str(text)
    return text[:max_len] + "..." if len(text) > max_len else text


def format_duration(seconds) -> str:
    """Format a duration in seconds as e.g. '45s', '3m 20s', '1h 5m'."""
    if is_missing(seconds) or seconds <= 0:
        return ""
    seconds = int(seconds)
    if seconds >= 3600:
        hours, mins = seconds // 3600, (seconds % 3600) // 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    if seconds >= 60:
        mins, secs = seconds // 60, seconds % 60
        return f"{mins}m {secs}s" if secs and mins < 10 else f"{mins}m"
    return f"{seconds}s"


def format_hours(hours) -> str:
    """Format a number of hours compactly (e.g. '40m', '5.2h', '1.5d')."""
    if is_missing(hours):
        return "N/A"
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def format_row_count(count, with_sign: bool = False) -> str:
    """Format a row count with K/M/B suffixes."""
    if is_missing(count):
        return "N/A"
    count = int(count)
    abs_count = abs(count)
    if abs_count >= 1_000_000_000:
        formatted = f"{count / 1_000_000_000:.1f}B"
    elif abs_count >= 1_000_000:
        formatted = f"{count / 1_000_000:.1f}M"
    elif abs_count >= 1_000:
        formatted = f"{count / 1_000:.1f}K"
    else:
        formatted = str(count)
    return f"+{formatted}" if with_sign and count > 0 else formatted


def issue_status(status) -> str:
    """Human label for the state of an open issue."""
    status = (status or "").lower()
    if status in ("fail", "error"):
        return "Failing"
    if status == "skipped":
        return "Skipped"
    if status == "warn":
        return "Warn"
    return status.title() if status else "Unknown"


# Raw dbt / elementary statuses -> readable label with a colour cue.
_STATUS_LABELS = {
    "success": "🟢 Success",
    "pass": "🟢 Pass",
    "warn": "🟡 Warn",
    "fail": "🔴 Fail",
    "error": "🔴 Error",
    "runtime error": "🔴 Error",
    "skipped": "⚪ Skipped",
    "no_runs": "⚪ No runs",
}


def status_label(status, dot: bool = True) -> str:
    """Readable status for a table cell (e.g. 'fail' -> '🔴 Fail').
    dot=False drops the colour dot (for metrics)."""
    key = "no_runs" if is_missing(status) or not status else str(status).lower()
    label = _STATUS_LABELS.get(key)
    if label is None:
        return key.replace("_", " ").title()
    return label if dot else label.split(" ", 1)[1]


def run_status_label(row) -> str:
    """Overall status of an invocation: failed if any model or test failed,
    warnings if only test warnings, skipped if nothing succeeded, else passed."""
    def n(col):
        value = row.get(col)
        return 0 if is_missing(value) else int(value)

    if n("FAIL_COUNT") or n("TESTS_FAILED"):
        return "🔴 Failed"
    if n("TESTS_WARNED"):
        return "🟡 Warnings"
    if n("SKIPPED_COUNT") and not n("SUCCESS_COUNT"):
        return "⚪ Skipped"
    return "🟢 Passed"


def to_datetime(df: pd.DataFrame, *columns) -> pd.DataFrame:
    """Parse text timestamp columns (elementary stores some as strings) to
    tz-naive datetimes so tables can format and sort them."""
    for col in columns:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = parsed.dt.tz_localize(None)
    return df
