"""Model detail view - full view of a single model, snapshot or seed."""

import json

import pandas as pd
import streamlit as st

from components import nav, ui
from components.charts import execution_time_chart, row_count_change_chart, row_count_trend_chart
from components.formatting import format_row_count, format_timestamp, json_list, status_label, to_datetime
from services.models_service import (
    get_model_compiled_code,
    get_model_details,
    get_model_execution_trend,
    get_model_latest_row_count,
    get_model_row_count_history,
    get_model_run_history,
)
from services.tests_service import get_tests_for_model


def _row_count_metric(details, latest_row_count_df):
    """(label, value, kwargs) for the latest row count with change from the previous run."""
    if latest_row_count_df.empty:
        materialization = (details.get("MATERIALIZATION") or "").lower()
        if materialization in ("view", "ephemeral"):
            return ("Row count", "N/A", {"help": f"Not tracked for {materialization} models"})
        return ("Row count", "N/A")

    row_data = latest_row_count_df.iloc[0]
    row_change = row_data.get("ROW_CHANGE")
    change_pct = row_data.get("CHANGE_PCT")
    if pd.isna(row_change) or pd.isna(change_pct):
        return ("Row count", format_row_count(row_data["ROW_COUNT"]))
    delta = f"{format_row_count(int(row_change), with_sign=True)} ({change_pct:+.1f}%)"
    return ("Row count", format_row_count(row_data["ROW_COUNT"]), {"delta": delta})


def _owners(details) -> list[str]:
    """Owner names from meta.owner (a name, a list, or {"name": ...}), else the
    owner column. Elementary stores a {"name": ...} owner as ["name"]."""
    try:
        meta = json.loads(details.get("META") or "{}")
    except (TypeError, ValueError):
        meta = {}
    owner = meta.get("owner") if isinstance(meta, dict) else None
    if isinstance(owner, dict):
        owner = owner.get("name") or [str(v) for v in owner.values() if v]
    if isinstance(owner, str) and owner.strip():
        return [owner.strip()]
    if isinstance(owner, list) and owner:
        return [str(o) for o in owner if o]
    return json_list(details.get("OWNER"))


def render(unique_id: str):
    details_df = get_model_details(unique_id)
    if details_df.empty:
        st.error(f"Model not found: {unique_id}")
        return

    details = details_df.iloc[0]
    days = nav.days()
    ui.page_header(details["NAME"], f"`{unique_id}`")

    owners = _owners(details)
    tags = json_list(details.get("TAGS"))
    meta = [
        ".".join(p for p in [details.get("DATABASE_NAME"), details.get("SCHEMA_NAME")] if p),
        details.get("MATERIALIZATION") or "",
        f"owner {', '.join(owners)}" if owners else "",
        f"tags {', '.join(tags)}" if tags else "",
    ]
    st.caption(" · ".join(p for p in meta if p))
    path = details.get("ORIGINAL_PATH") or details.get("PATH")
    if path:
        st.code(path, language=None)
    if details.get("DESCRIPTION"):
        st.markdown(details["DESCRIPTION"])

    history_df = get_model_run_history(unique_id, days)
    latest_row_count_df = get_model_latest_row_count(details["NAME"])

    if history_df.empty:
        ui.empty_state(f"No runs in the last {days} days")
    else:
        latest = history_df.iloc[0]
        total_runs = len(history_df)
        success_runs = int((history_df["STATUS"] == "success").sum())
        avg_time = history_df["EXECUTION_TIME"].mean()
        ui.metric_row([
            ("Last status", status_label(latest["STATUS"], dot=False)),
            ("Avg time", f"{avg_time:.1f}s" if pd.notna(avg_time) else "N/A"),
            (f"Runs ({days}d)", total_runs),
            ("Success rate", f"{success_runs / total_runs * 100:.0f}%"),
            _row_count_metric(details, latest_row_count_df),
        ])

        if latest["STATUS"] in ("fail", "error") and latest.get("MESSAGE"):
            st.error(f"Latest run failed ({format_timestamp(latest['GENERATED_AT'])})")
            st.code(str(latest["MESSAGE"]), language="text")

        st.subheader("Run history")
        runs = to_datetime(history_df.copy(), "GENERATED_AT")
        runs["STATUS_LABEL"] = runs["STATUS"].map(status_label)
        selected = ui.table(
            runs,
            key="model_runs_table",
            noun="runs",
            columns={
                "STATUS_LABEL": "Status",
                "GENERATED_AT": ui.datetime_column("Run at"),
                "EXECUTION_TIME": ui.seconds_column("Duration"),
                "ROW_COUNT": st.column_config.NumberColumn("Rows", format="localized"),
                "MESSAGE": st.column_config.TextColumn("Message", width="large"),
            },
        )
        if selected is not None:
            nav.open_run(selected["INVOCATION_ID"])

        trend_df = get_model_execution_trend(unique_id, days)
        if len(trend_df) > 1:
            st.subheader("Execution time")
            st.altair_chart(execution_time_chart(trend_df, height=240))

    if not latest_row_count_df.empty:
        row_count_df = to_datetime(get_model_row_count_history(details["NAME"], days).copy(), "RUN_STARTED_AT")
        st.subheader("Row count")
        if len(row_count_df) > 1:
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.caption("Total rows")
                st.altair_chart(row_count_trend_chart(row_count_df, height=200))
            with chart_col2:
                st.caption("Daily change")
                st.altair_chart(row_count_change_chart(row_count_df, height=200))
        else:
            ui.empty_state("Not enough row count history to chart (needs at least 2 data points)")

    _render_tests(unique_id, days)

    compiled = get_model_compiled_code(unique_id)
    if not compiled.empty and compiled.iloc[0]["COMPILED_CODE"]:
        with st.expander("Compiled SQL (latest run)"):
            st.code(compiled.iloc[0]["COMPILED_CODE"], language="sql")

    # Imported here so this section stays self-contained.
    from page_modules.sources import render_upstream_sources

    render_upstream_sources(unique_id)


def _render_tests(unique_id: str, days: int):
    st.subheader("Tests")
    tests_df = get_tests_for_model(unique_id, days)
    if tests_df.empty:
        ui.empty_state("No tests defined on this model")
        return

    tests_df = to_datetime(tests_df.copy(), "LAST_RUN")
    tests_df["STATUS_LABEL"] = tests_df["LATEST_STATUS"].map(status_label)
    selected = ui.table(
        tests_df,
        key="model_tests_table",
        noun="tests",
        columns={
            "STATUS_LABEL": "Latest status",
            "TEST_NAME": st.column_config.TextColumn("Test", width="large"),
            "TEST_NAMESPACE": "Type",
            "TEST_COLUMN_NAME": "Column",
            "SEVERITY": "Severity",
            "LAST_RUN": ui.datetime_column("Last run"),
        },
    )
    if selected is not None:
        nav.open_test(selected["TEST_UNIQUE_ID"])
