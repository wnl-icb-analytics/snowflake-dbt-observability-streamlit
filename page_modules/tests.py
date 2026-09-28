"""Tests page - pass rates, flaky tests and models without tests."""

import streamlit as st

from components import nav, ui
from components.formatting import status_label, to_datetime
from config import FLAKY_TEST_THRESHOLD
from services.tests_service import get_flaky_tests, get_models_without_tests, get_tests_summary


def render():
    days = nav.days()
    ui.page_header("Tests", f"Test pass rates, flaky tests and models without tests over the last {days} days.")

    search = st.text_input("Search", placeholder="Test, model or column", key="tests_search")

    tab_all, tab_flaky, tab_coverage = st.tabs(["All tests", "Flaky tests", "Coverage gaps"])
    with tab_all:
        _render_all_tests(days, search)
    with tab_flaky:
        _render_flaky_tests(days, search)
    with tab_coverage:
        _render_coverage_gaps(search)


def _render_all_tests(days: int, search: str):
    df = get_tests_summary(days=days)
    if df.empty:
        ui.empty_state(f"No test runs in the last {days} days")
        return

    df = ui.contains(df, ["SHORT_NAME", "TABLE_NAME", "TEST_UNIQUE_ID"], search)
    if df.empty:
        ui.empty_state("No tests match the search")
        return

    df = to_datetime(df.copy(), "LAST_RUN")
    df["STATUS_LABEL"] = df["LATEST_STATUS"].map(status_label)
    st.caption("Sorted by pass rate, lowest first.")
    selected = ui.table(
        df,
        key="tests_table",
        height=600,
        noun="tests",
        columns={
            "STATUS_LABEL": "Latest status",
            "SHORT_NAME": st.column_config.TextColumn("Test", width="large"),
            "TABLE_NAME": "Model",
            "TEST_NAMESPACE": "Type",
            "PASS_RATE": st.column_config.ProgressColumn("Pass rate", format="percent", min_value=0, max_value=1),
            "TOTAL_RUNS": st.column_config.NumberColumn("Runs"),
            "LAST_RUN": ui.datetime_column("Last run"),
            "IS_FLAKY": st.column_config.CheckboxColumn("Flaky"),
        },
    )
    if selected is not None:
        nav.open_test(selected["TEST_UNIQUE_ID"])


def _render_flaky_tests(days: int, search: str):
    st.caption(f"Tests failing at least {FLAKY_TEST_THRESHOLD * 100:.0f}% of runs, with 3 or more runs.")
    df = get_flaky_tests(days)
    if df.empty:
        ui.empty_state("No flaky tests detected", ok=True)
        return
    df = ui.contains(df, ["SHORT_NAME", "TABLE_NAME", "TEST_UNIQUE_ID"], search)
    if df.empty:
        ui.empty_state("No flaky tests match the search")
        return

    selected = ui.table(
        df,
        key="flaky_tests_table",
        noun="flaky tests",
        columns={
            "SHORT_NAME": st.column_config.TextColumn("Test", width="large"),
            "TABLE_NAME": "Model",
            "TEST_NAMESPACE": "Type",
            "FAILURE_RATE": st.column_config.ProgressColumn("Failure rate", format="percent", min_value=0, max_value=1),
            "FAIL_COUNT": st.column_config.NumberColumn("Failures"),
            "TOTAL_RUNS": st.column_config.NumberColumn("Runs"),
        },
    )
    if selected is not None:
        nav.open_test(selected["TEST_UNIQUE_ID"])


def _render_coverage_gaps(search: str):
    st.caption("Models with no tests defined on them.")
    df = get_models_without_tests()
    if df.empty:
        ui.empty_state("Every model has at least one test", ok=True)
        return
    df = ui.contains(df, ["NAME", "MODEL_PATH"], search)
    if df.empty:
        ui.empty_state("No untested models match the search")
        return

    selected = ui.table(
        df,
        key="coverage_gaps_table",
        height=600,
        noun="models without tests",
        columns={
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "DATABASE_NAME": "Database",
            "MODEL_PATH": st.column_config.TextColumn("Path", width="large"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
