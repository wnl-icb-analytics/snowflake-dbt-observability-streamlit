"""Runs page - dbt invocations in the selected range."""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import run_status_label, to_datetime
from services.runs_service import get_invocations


def runs_table(df: pd.DataFrame, *, key: str, height="auto", noun: str | None = None):
    """Invocations as a selectable table; returns the selected row or None."""
    df = to_datetime(df.copy(), "CREATED_AT")
    df["STATUS_LABEL"] = df.apply(run_status_label, axis=1)
    df["DURATION_MIN"] = pd.to_numeric(df["DURATION_SECONDS"], errors="coerce") / 60
    return ui.table(
        df,
        key=key,
        height=height,
        noun=noun,
        columns={
            "STATUS_LABEL": "Status",
            "CREATED_AT": ui.datetime_column("Started"),
            "COMMAND": "Command",
            "SELECTED": st.column_config.TextColumn("Selection", width="medium"),
            "MODELS_RUN": st.column_config.NumberColumn("Models"),
            "FAIL_COUNT": st.column_config.NumberColumn("Failed"),
            "SKIPPED_COUNT": st.column_config.NumberColumn("Skipped"),
            "TESTS_RUN": st.column_config.NumberColumn("Tests"),
            "TESTS_FAILED": st.column_config.NumberColumn("Tests failed"),
            "TESTS_WARNED": st.column_config.NumberColumn("Warnings"),
            "DURATION_MIN": st.column_config.NumberColumn("Duration", format="%.1f min"),
            "TARGET_NAME": "Target",
        },
    )


def render():
    days = nav.days()
    ui.page_header("Runs", f"dbt invocations in the last {days} days. Select a run to see its models, tests and timeline.")

    df = get_invocations(days=days)
    if df.empty:
        ui.empty_state("No runs found in this time range")
        return

    selected = runs_table(df, key="runs_table", height=600, noun="runs")
    if selected is not None:
        nav.open_run(selected["INVOCATION_ID"])
