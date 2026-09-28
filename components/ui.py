"""Layout helpers so every page shares the same header, metrics, tables and
empty states."""

import pandas as pd
import streamlit as st

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
    text shows as a blank cell. Returns the selected row (Series) or None."""
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
    column_config or a label string (shown as text). Missing text shows as a
    blank cell (the grid renders it as "None"); number and date columns keep
    their type so they sort correctly."""
    config = {
        col: (st.column_config.TextColumn(cfg) if isinstance(cfg, str) else cfg)
        for col, cfg in columns.items()
    }
    shown = df.reset_index(drop=True)
    for col in columns:
        values = shown[col]
        if values.dtype == object and values.dropna().map(lambda v: isinstance(v, str)).all():
            shown[col] = values.fillna("")
    return shown, config


def datetime_column(label: str, **kwargs):
    return st.column_config.DatetimeColumn(label, format=DATETIME_FORMAT, **kwargs)


def seconds_column(label: str, **kwargs):
    return st.column_config.NumberColumn(label, format="%.1f s", **kwargs)
