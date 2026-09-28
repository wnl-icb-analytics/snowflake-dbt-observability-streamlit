"""Test detail view - full view of a single test."""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.charts import test_status_history_chart
from components.formatting import json_list, status_label, to_datetime
from services.models_service import get_model_by_name
from services.tests_service import get_test_details, get_test_run_history


def render(test_unique_id: str):
    details_df = get_test_details(test_unique_id)
    if details_df.empty:
        st.error(f"Test not found: {test_unique_id}")
        return

    details = details_df.iloc[0]
    days = nav.days()
    test_ns = details.get("TEST_NAMESPACE") or details.get("TEST_TYPE") or ""
    ui.page_header(details.get("SHORT_NAME") or details["TEST_NAME"], f"`{test_unique_id}`")

    column = details.get("TEST_COLUMN_NAME") or details.get("COLUMN_NAME")
    tags = json_list(details.get("TAGS"))
    meta = [
        test_ns,
        f"model {details['TABLE_NAME']}" if details.get("TABLE_NAME") else "",
        f"column {column}" if column else "",
        f"schema {details['SCHEMA_NAME']}" if details.get("SCHEMA_NAME") else "",
        f"severity {details['SEVERITY']}" if details.get("SEVERITY") else "",
        f"tags {', '.join(tags)}" if tags else "",
    ]
    st.caption(" · ".join(p for p in meta if p))
    if details.get("ORIGINAL_PATH"):
        st.code(details["ORIGINAL_PATH"], language=None)
    if details.get("DESCRIPTION"):
        st.markdown(details["DESCRIPTION"])

    _render_model_link(details)

    if details.get("TEST_PARAMS"):
        with st.expander("Test parameters"):
            st.json(details["TEST_PARAMS"])

    history_df = get_test_run_history(test_unique_id, days)
    if history_df.empty:
        ui.empty_state(f"No runs in the last {days} days")
        return

    total_runs = len(history_df)
    pass_runs = int((history_df["STATUS"] == "pass").sum())
    latest = history_df.iloc[0]
    ui.metric_row([
        ("Last status", status_label(latest["STATUS"], dot=False)),
        ("Pass rate", f"{pass_runs / total_runs * 100:.0f}%"),
        (f"Runs ({days}d)", total_runs),
        ("Passed", pass_runs),
    ])

    st.subheader("Run history")
    runs = to_datetime(history_df.copy(), "DETECTED_AT")
    if total_runs > 1:
        st.altair_chart(test_status_history_chart(runs))
    runs["STATUS_LABEL"] = runs["STATUS"].map(status_label)
    selected = ui.table(
        runs,
        key="test_runs_table",
        noun="runs",
        columns={
            "STATUS_LABEL": "Status",
            "DETECTED_AT": ui.datetime_column("Run at"),
            "FAILURES": st.column_config.NumberColumn("Failing rows", format="localized"),
            "TEST_RESULTS_DESCRIPTION": st.column_config.TextColumn("Result", width="large"),
        },
    )
    if selected is not None and pd.notna(selected["INVOCATION_ID"]):
        nav.open_run(selected["INVOCATION_ID"])

    if latest.get("TEST_RESULTS_QUERY"):
        with st.expander("Test SQL (latest run)"):
            st.code(latest["TEST_RESULTS_QUERY"], language="sql")


def _render_model_link(details):
    """Button to the tested model, found by parent_model_unique_id (or by name
    for tests no longer in the manifest)."""
    model_uid = details.get("PARENT_MODEL_UNIQUE_ID")
    model_name = details.get("TABLE_NAME")
    if not model_uid and model_name:
        model_df = get_model_by_name(model_name)
        if not model_df.empty:
            model_uid = model_df.iloc[0]["UNIQUE_ID"]
    if model_uid:
        label = f"Open model {model_name}" if model_name else "Open model"
        if st.button(label, key="test_open_model", icon=":material/open_in_new:"):
            nav.open_model(model_uid)
