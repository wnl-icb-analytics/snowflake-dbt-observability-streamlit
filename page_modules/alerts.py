"""Alerts page - current and historical test and model failures."""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.charts import project_test_failures_chart
from components.formatting import format_hours, status_label, to_datetime, truncate
from services.alerts_service import (
    get_alert_counts,
    get_current_model_failures,
    get_current_test_failures,
    get_historical_alert_counts,
    get_historical_model_failures,
    get_historical_test_failures,
    get_project_test_status_history,
)


def _calculate_test_resolution_metrics(history_df: pd.DataFrame):
    """Build daily trend and fail-to-pass episode metrics from test history."""
    if history_df.empty:
        empty_daily = pd.DataFrame(columns=["DATE", "FAILED_TEST_RUNS", "DISTINCT_TESTS_FAILING", "RESOLVED_TESTS"])
        empty_episodes = pd.DataFrame(columns=["TEST_UNIQUE_ID", "FAIL_STARTED_AT", "RESOLVED_AT", "RESOLUTION_HOURS", "FAILURE_RUNS"])
        return empty_daily, empty_episodes

    df = history_df.copy()
    df["DETECTED_AT"] = pd.to_datetime(df["DETECTED_AT"])
    df["DATE"] = df["DETECTED_AT"].dt.floor("D")
    df["IS_FAIL"] = df["STATUS"].isin(["fail", "error"])
    df["IS_PASS"] = df["STATUS"] == "pass"

    failed_runs = (
        df[df["IS_FAIL"]]
        .groupby("DATE")
        .size()
        .rename("FAILED_TEST_RUNS")
    )
    distinct_failing = (
        df[df["IS_FAIL"]]
        .groupby("DATE")["TEST_UNIQUE_ID"]
        .nunique()
        .rename("DISTINCT_TESTS_FAILING")
    )

    episodes = []
    for test_unique_id, group in df.sort_values("DETECTED_AT").groupby("TEST_UNIQUE_ID"):
        active_failure = None
        failure_runs = 0

        for row in group.itertuples():
            status = str(row.STATUS).lower()
            if status in ("fail", "error"):
                if active_failure is None:
                    active_failure = row.DETECTED_AT
                    failure_runs = 1
                else:
                    failure_runs += 1
            elif status == "pass" and active_failure is not None:
                resolution_hours = (row.DETECTED_AT - active_failure).total_seconds() / 3600
                episodes.append(
                    {
                        "TEST_UNIQUE_ID": test_unique_id,
                        "FAIL_STARTED_AT": active_failure,
                        "RESOLVED_AT": row.DETECTED_AT,
                        "RESOLUTION_HOURS": resolution_hours,
                        "FAILURE_RUNS": failure_runs,
                    }
                )
                active_failure = None
                failure_runs = 0

    episodes_df = pd.DataFrame(episodes)

    if episodes_df.empty:
        resolved_daily = pd.Series(dtype="int64", name="RESOLVED_TESTS")
    else:
        resolved_daily = (
            episodes_df.assign(DATE=episodes_df["RESOLVED_AT"].dt.floor("D"))
            .groupby("DATE")
            .size()
            .rename("RESOLVED_TESTS")
        )

    all_dates = pd.date_range(df["DATE"].min(), df["DATE"].max(), freq="D")
    daily_df = (
        pd.DataFrame({"DATE": all_dates})
        .merge(failed_runs.reset_index(), on="DATE", how="left")
        .merge(distinct_failing.reset_index(), on="DATE", how="left")
        .merge(resolved_daily.reset_index(), on="DATE", how="left")
        .fillna(0)
    )

    for col in ["FAILED_TEST_RUNS", "DISTINCT_TESTS_FAILING", "RESOLVED_TESTS"]:
        daily_df[col] = daily_df[col].astype(int)

    return daily_df, episodes_df


def render():
    days = nav.days()
    ui.page_header("Alerts", f"What is failing now, and every failure in the last {days} days.")

    search = st.text_input("Search", placeholder="Model or test name", key="alerts_search")

    tab_active, tab_history = st.tabs(["Active", "History"])
    with tab_active:
        _render_active(days, search)
    with tab_history:
        _render_history(days, search)


def _render_active(days: int, search: str):
    """Tests and models whose most recent run in the range failed."""
    st.caption("Tests and models where the most recent run failed.")

    counts = get_alert_counts(days=days)
    if counts.empty:
        ui.empty_state("No data available")
        return

    row = counts.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    if failed_tests + failed_models == 0:
        ui.empty_state("No current failures: all tests and models are passing", ok=True)
        return

    ui.metric_row([
        ("Total failures", failed_tests + failed_models),
        ("Test failures", failed_tests),
        ("Model failures", failed_models),
    ])

    st.subheader("Failing tests")
    _test_table(get_current_test_failures(days, search), key="active_tests_table", search=search)

    st.subheader("Failing models")
    _model_table(get_current_model_failures(days, search), key="active_models_table", search=search)


def _render_history(days: int, search: str):
    """Every failure in the range, with the project-wide trend."""
    counts = get_historical_alert_counts(days)
    if counts.empty:
        ui.empty_state("No data available")
        return

    row = counts.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    if failed_tests == 0 and failed_models == 0:
        ui.empty_state("No failures in this time range", ok=True)
        return

    metrics = [("Test failures", failed_tests), ("Model failures", failed_models)]
    test_history_df = get_project_test_status_history(days)
    trend_df, episodes_df = _calculate_test_resolution_metrics(test_history_df)
    if not test_history_df.empty:
        current_history_df = test_history_df[test_history_df["IS_CURRENT"]]
        latest_status_df = current_history_df.sort_values("DETECTED_AT").groupby("TEST_UNIQUE_ID").tail(1)
        open_failures = int(latest_status_df["STATUS"].isin(["fail", "error"]).sum())
        median_resolution = episodes_df["RESOLUTION_HOURS"].median() if not episodes_df.empty else None
        p75_resolution = episodes_df["RESOLUTION_HOURS"].quantile(0.75) if not episodes_df.empty else None
        metrics += [
            ("Open test failures", open_failures),
            ("Median resolution", format_hours(median_resolution)),
            ("P75 resolution", format_hours(p75_resolution)),
        ]
    ui.metric_row(metrics)

    if not test_history_df.empty:
        st.subheader("Test failure trend")
        st.caption("Daily failed test runs, distinct failing tests, and fail-to-pass resolutions.")
        st.altair_chart(project_test_failures_chart(trend_df))

    st.subheader("Test failures")
    _test_table(get_historical_test_failures(days, search), key="history_tests_table", search=search, limit_note=True)

    st.subheader("Model failures")
    _model_table(get_historical_model_failures(days, search), key="history_models_table", search=search, limit_note=True)


def _test_table(df: pd.DataFrame, *, key: str, search: str, limit_note: bool = False):
    if df.empty:
        ui.empty_state("No test failures match the search" if search else "No test failures", ok=not search)
        return
    if limit_note and len(df) >= 200:
        st.caption("Showing the 200 most recent.")
    df = to_datetime(df.copy(), "DETECTED_AT")
    df["STATUS_LABEL"] = df["STATUS"].map(status_label)
    selected = ui.table(
        df,
        key=key,
        noun="test failures",
        columns={
            "STATUS_LABEL": "Status",
            "TABLE_NAME": "Model",
            "SHORT_NAME": st.column_config.TextColumn("Test", width="large"),
            "TEST_NAMESPACE": "Type",
            "SCHEMA_NAME": "Schema",
            "DETECTED_AT": ui.datetime_column("Detected"),
        },
    )
    if selected is not None:
        nav.open_test(selected["TEST_UNIQUE_ID"])


def _model_table(df: pd.DataFrame, *, key: str, search: str, limit_note: bool = False):
    if df.empty:
        ui.empty_state("No model failures match the search" if search else "No model failures", ok=not search)
        return
    if limit_note and len(df) >= 200:
        st.caption("Showing the 200 most recent.")
    df = to_datetime(df.copy(), "GENERATED_AT")
    df["STATUS_LABEL"] = df["STATUS"].map(status_label)
    df["MESSAGE_SHORT"] = df["MESSAGE"].map(lambda m: truncate(" ".join(str(m).split()), 200) if pd.notna(m) else "")
    selected = ui.table(
        df,
        key=key,
        noun="model failures",
        columns={
            "STATUS_LABEL": "Status",
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "EXECUTION_TIME": ui.seconds_column("Duration"),
            "GENERATED_AT": ui.datetime_column("Run at"),
            "MESSAGE_SHORT": st.column_config.TextColumn("Error", width="large"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
