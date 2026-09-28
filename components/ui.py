"""Layout helpers so every page shares the same header, metrics, tables and
empty states."""

import numbers
import re

import pandas as pd
import streamlit as st

from components.formatting import is_missing

DATETIME_FORMAT = "YYYY-MM-DD HH:mm"


def page_header(title: str, caption: str | None = None):
    """Page title with a one-line caption."""
    st.title(title)
    if caption:
        st.caption(caption)


def metric_row(items):
    """Bordered metrics in one row. items: (label, value) or (label, value, kwargs)."""
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        label, value, *extra = item
        col.metric(label, value, border=True, **(extra[0] if extra else {}))


def empty_state(message: str, ok: bool = False):
    """Consistent empty state: green when 'nothing found' is good news."""
    (st.success if ok else st.info)(message)


def contains(df: pd.DataFrame, columns, term: str) -> pd.DataFrame:
    """Rows where any of the columns contains term (case-insensitive, literal)."""
    term = (term or "").strip().lower().replace("\\", "/")
    if not term or df.empty:
        return df
    mask = pd.Series(False, index=df.index)
    for col in columns:
        values = df[col].fillna("").astype(str).str.lower().str.replace("\\", "/", regex=False)
        mask |= values.str.contains(term, regex=False)
    return df[mask]


def table(df: pd.DataFrame, *, key: str, columns: dict, height="auto", noun: str | None = None):
    """Single-row-selectable table. columns maps source column -> column_config
    (or a label string); only these columns are shown, in this order. Missing
    values show as blank cells. Returns the selected row (Series) or None."""
    if noun:
        st.caption(f"{len(df):,} {noun} · select a row to open it")
    shown, config = display_frame(df, columns)
    event = st.dataframe(
        shown,
        key=key,
        column_order=list(columns),
        column_config=config,
        hide_index=True,
        width="stretch",
        height=height,
        on_select="rerun",
        selection_mode="single-row",
    )
    rows = event.selection.rows
    return df.iloc[rows[0]] if rows else None


def display_frame(df: pd.DataFrame, columns: dict):
    """(frame, column_config) for st.dataframe. columns maps source column ->
    column_config or a label string (shown as text). Missing values show as
    blank cells; the grid renders them as "None"."""
    config = {
        col: (st.column_config.TextColumn(cfg) if isinstance(cfg, str) else cfg)
        for col, cfg in columns.items()
    }
    shown = df.reset_index(drop=True)
    for col in columns:
        config[col] = _blank_missing(shown, col, config[col])
    return shown, config


def _blank_missing(shown: pd.DataFrame, col: str, cfg: dict) -> dict:
    """Show missing values as blank cells; the grid renders them as "None".
    Number, datetime, date and time columns with gaps are formatted to text in
    place (they then sort as text); returns the column config to use."""
    values = shown[col]
    if not values.isna().any():
        return cfg
    type_config = cfg.get("type_config") or {}
    kind, fmt = type_config.get("type"), type_config.get("format")
    if kind == "text":
        shown[col] = values.map(lambda v: "" if is_missing(v) else _format_number(v, None) if _is_number(v) else str(v))
        return cfg
    if kind == "number":
        shown[col] = values.map(lambda v: "" if is_missing(v) else _format_number(v, fmt))
    elif kind in _DEFAULT_PATTERNS:
        pattern = _strftime_pattern(fmt, _DEFAULT_PATTERNS[kind])
        shown[col] = values.map(lambda v: "" if is_missing(v) else pd.Timestamp(v).strftime(pattern))
    else:
        return cfg
    return st.column_config.TextColumn(cfg.get("label"), help=cfg.get("help"), width=cfg.get("width"), pinned=cfg.get("pinned"))


def _is_number(value) -> bool:
    return isinstance(value, numbers.Number) and not isinstance(value, bool)


def _format_number(value, fmt) -> str:
    """Text for a number as NumberColumn would show it: localized, percent or
    a printf-style format such as '%.1f s'."""
    v = float(value)
    if fmt == "localized":
        return f"{int(v):,}" if v.is_integer() else f"{v:,.2f}"
    if fmt == "percent":
        return f"{v * 100:.1f}%"
    if fmt and "%" in fmt:
        try:
            return fmt % v
        except (TypeError, ValueError):
            pass
    return f"{int(v)}" if v.is_integer() else f"{v:g}"


# moment.js tokens used by date/time column formats -> strftime codes.
_MOMENT_TOKENS = (("YYYY", "%Y"), ("MMM", "%b"), ("MM", "%m"), ("DD", "%d"), ("HH", "%H"), ("mm", "%M"), ("ss", "%S"))
# Column type -> strftime pattern when the column has no simple format.
_DEFAULT_PATTERNS = {"datetime": "%Y-%m-%d %H:%M", "date": "%Y-%m-%d", "time": "%H:%M"}


def _strftime_pattern(moment_format, default: str) -> str:
    """strftime pattern for a simple moment.js format, else default."""
    if not moment_format:
        return default
    pattern = moment_format
    for token, code in _MOMENT_TOKENS:
        pattern = pattern.replace(token, code)
    if re.search(r"[A-Za-z]", re.sub(r"%[A-Za-z]", "", pattern)):
        return default
    return pattern


def datetime_column(label: str, **kwargs):
    return st.column_config.DatetimeColumn(label, format=DATETIME_FORMAT, **kwargs)


def seconds_column(label: str, **kwargs):
    return st.column_config.NumberColumn(label, format="%.1f s", **kwargs)
