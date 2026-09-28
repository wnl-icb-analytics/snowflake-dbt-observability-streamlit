"""Display formatters and status labels shared across pages.

Timestamps: dbt and elementary write run times in UTC
(dbt_invocations.run_started_at/run_completed_at/generated_at,
dbt_run_results.generated_at and execute/compile times,
elementary_test_results.detected_at, ROW_COUNT_LOG.run_started_at). The
created_at columns and ROW_COUNT_LOG.recorded_at are in the account timezone,
Europe/London. The time helpers read naive values in the source zone passed as
tz (UTC by default) and show them in Europe/London.
"""

import json

import pandas as pd

UTC = "UTC"
LOCAL_TZ = "Europe/London"  # account timezone and display timezone


def is_missing(value) -> bool:
    """True for None, NaN and NaT."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def to_local(ts, tz: str = UTC):
    """ts as a tz-aware Europe/London Timestamp, or None. Naive values and
    strings without an offset are read in tz."""
    if is_missing(ts):
        return None
    try:
        value = pd.Timestamp(ts)
    except (TypeError, ValueError):
        return None
    if pd.isna(value):
        return None
    if value.tzinfo is None:
        value = value.tz_localize(tz, ambiguous=False, nonexistent="shift_forward")
    return value.tz_convert(LOCAL_TZ)


def format_timestamp(ts, tz: str = UTC) -> str:
    """'YYYY-MM-DD HH:MM' in Europe/London. Naive values are read in tz."""
    local = to_local(ts, tz)
    return local.strftime("%Y-%m-%d %H:%M") if local is not None else "N/A"


def format_relative_time(ts, tz: str = UTC) -> str:
    """Time since ts, e.g. '2h ago'. Naive values are read in tz."""
    local = to_local(ts, tz)
    if local is None:
        return "N/A"
    seconds = (pd.Timestamp.now(tz=UTC) - local).total_seconds()
    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def format_when(ts, tz: str = UTC) -> str:
    """Local time with relative time, e.g. '2026-09-24 15:52 (4d ago)'."""
    if to_local(ts, tz) is None:
        return "N/A"
    return f"{format_timestamp(ts, tz)} ({format_relative_time(ts, tz)})"


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


def json_list(value) -> list[str]:
    """Items of a JSON array string such as '["a", "b"]'; [] when empty or
    not an array."""
    if is_missing(value) or not str(value).strip():
        return []
    try:
        items = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return [str(value)]
    if isinstance(items, (list, tuple)):
        return [str(i) for i in items if not is_missing(i) and str(i).strip()]
    return [str(items)] if str(items).strip() else []


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


def to_datetime(df: pd.DataFrame, *columns, tz: str = UTC) -> pd.DataFrame:
    """Parse timestamp columns (elementary stores some as text) to tz-aware
    Europe/London datetimes so tables and charts show local time and sort.
    Naive values are read in tz."""
    for col in columns:
        if col in df.columns:
            df[col] = _to_local_series(df[col], tz)
    return df


def _to_local_series(values: pd.Series, tz: str) -> pd.Series:
    parsed = None
    if values.dtype == object:
        try:
            # Text timestamps vary in precision; ISO8601 parses them all.
            parsed = pd.to_datetime(values, errors="coerce", format="ISO8601")
        except (TypeError, ValueError):
            parsed = None
    if parsed is None:
        parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    return parsed.dt.tz_convert(LOCAL_TZ)
