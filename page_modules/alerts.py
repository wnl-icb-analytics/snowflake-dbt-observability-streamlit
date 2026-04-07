"""Alerts page - Current and historical test and model failures."""

import pandas as pd
import streamlit as st
from services.alerts_service import (
    get_current_test_failures,
    get_current_model_failures,
    get_alert_counts,
    get_historical_test_failures,
    get_historical_model_failures,
    get_historical_alert_counts,
    get_project_test_status_history,
)
from components.charts import project_test_failures_chart


def _truncate(text: str, max_len: int = 50) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    return text[:max_len] + "..." if len(text) > max_len else text


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


def _format_resolution_duration(hours: float) -> str:
    """Format hours as compact duration."""
    if hours is None or pd.isna(hours):
        return "N/A"
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def render(search_filter: str = ""):
    st.title("Alerts")

    # Mode tabs
    tab_active, tab_history = st.tabs(["Active", "History"])

    with tab_active:
        _render_active_alerts(search_filter)

    with tab_history:
        _render_historical_alerts(search_filter)


def _render_active_alerts(search_filter: str):
    """Render currently active failures (latest run is failing)."""
    st.caption("Tests and models where the most recent run failed")

    # Alert counts summary
    counts = get_alert_counts(days=7)
    if counts.empty:
        st.info("No data available")
        return

    row = counts.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    total_failures = failed_tests + failed_models

    if total_failures == 0:
        st.success("No current failures - all tests and models are passing")
        return

    # Summary metrics
    metric_cols = st.columns(3)
    with metric_cols[0]:
        st.metric("Total Failures", total_failures)
    with metric_cols[1]:
        st.metric("Test Failures", failed_tests)
    with metric_cols[2]:
        st.metric("Model Failures", failed_models)

    st.divider()

    # Side-by-side layout
    test_col, model_col = st.columns(2)

    with test_col:
        st.subheader(f"Test Failures ({failed_tests})")
        _render_test_failures(days=7, search_filter=search_filter)

    with model_col:
        st.subheader(f"Model Failures ({failed_models})")
        _render_model_failures(days=7, search_filter=search_filter)


def _render_historical_alerts(search_filter: str):
    """Render all failures in time period."""
    # Filters
    col1, _ = st.columns([1, 4])
    with col1:
        days = st.selectbox("Time range", [7, 14, 30], index=0, format_func=lambda x: f"{x}d", key="history_days")

    st.caption(f"All failures in the last {days} days")

    # Alert counts summary
    counts = get_historical_alert_counts(days)
    if counts.empty:
        st.info("No data available")
        return

    row = counts.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)

    test_history_df = get_project_test_status_history(days)
    trend_df, episodes_df = _calculate_test_resolution_metrics(test_history_df)

    if failed_tests == 0 and failed_models == 0:
        st.success("No failures in this time period")
        return

    # Summary metrics
    metric_cols = st.columns(2)
    with metric_cols[0]:
        st.metric("Test Failures", failed_tests)
    with metric_cols[1]:
        st.metric("Model Failures", failed_models)

    if not test_history_df.empty:
        latest_status_df = (
            test_history_df.sort_values("DETECTED_AT")
            .groupby("TEST_UNIQUE_ID")
            .tail(1)
        )
        open_failures = int(latest_status_df["STATUS"].isin(["fail", "error"]).sum())
        median_resolution = episodes_df["RESOLUTION_HOURS"].median() if not episodes_df.empty else None
        p75_resolution = episodes_df["RESOLUTION_HOURS"].quantile(0.75) if not episodes_df.empty else None

        st.divider()
        trend_cols = st.columns(3)
        with trend_cols[0]:
            st.metric("Open Test Failures", open_failures)
        with trend_cols[1]:
            st.metric("Median Resolution", _format_resolution_duration(median_resolution))
        with trend_cols[2]:
            st.metric("P75 Resolution", _format_resolution_duration(p75_resolution))

        st.subheader("Project Test Failure Trend")
        st.caption("Daily failed test runs, distinct failing tests, and fail-to-pass resolutions.")
        st.altair_chart(project_test_failures_chart(trend_df), use_container_width=True)

    st.divider()

    # Side-by-side layout
    test_col, model_col = st.columns(2)

    with test_col:
        st.subheader("Test Failures")
        _render_historical_test_failures(days, search_filter)

    with model_col:
        st.subheader("Model Failures")
        _render_historical_model_failures(days, search_filter)


def _render_test_failures(days: int, search_filter: str):
    """Render current test failures with click navigation."""
    df = get_current_test_failures(days, search_filter)

    if df.empty:
        st.info("No test failures")
        return

    for _, row in df.iterrows():
        short_name = row.get("SHORT_NAME") or row["TEST_NAME"]
        name = _truncate(short_name)
        model = row["TABLE_NAME"] or "N/A"
        test_ns = row.get("TEST_NAMESPACE") or row["TEST_TYPE"] or ""

        with st.container(border=True):
            cols = st.columns([4, 1])
            with cols[0]:
                st.markdown(f"🔴 **{model}**")
                st.caption(f"{name} | {test_ns}" if test_ns else name)
                st.caption(f"{row['SCHEMA_NAME']} | {str(row['DETECTED_AT'])[:16]}")
            with cols[1]:
                if st.button("View", key=f"alert_test_{row['TEST_UNIQUE_ID']}"):
                    st.session_state["selected_test"] = row["TEST_UNIQUE_ID"]
                    st.session_state["selected_model"] = None
                    st.rerun()


def _render_model_failures(days: int, search_filter: str):
    """Render current model failures with click navigation."""
    df = get_current_model_failures(days, search_filter)

    if df.empty:
        st.info("No model failures")
        return

    for _, row in df.iterrows():
        name = _truncate(row["NAME"])
        schema = row["SCHEMA_NAME"] or "unknown"

        with st.container(border=True):
            cols = st.columns([4, 1])
            with cols[0]:
                st.markdown(f"🔴 **{name}**")
                st.caption(f"{schema} | {row['STATUS']}")
                if row["EXECUTION_TIME"]:
                    st.caption(f"{row['EXECUTION_TIME']:.1f}s | {str(row['GENERATED_AT'])[:16]}")
                else:
                    st.caption(str(row["GENERATED_AT"])[:16])
            with cols[1]:
                if st.button("View", key=f"alert_model_{row['UNIQUE_ID']}"):
                    st.session_state["selected_model"] = row["UNIQUE_ID"]
                    st.session_state["selected_test"] = None
                    st.rerun()


def _render_historical_test_failures(days: int, search_filter: str):
    """Render historical test failures."""
    df = get_historical_test_failures(days, search_filter)

    if df.empty:
        st.info("No test failures")
        return

    with st.container(height=400):
        for _, row in df.iterrows():
            short_name = row.get("SHORT_NAME") or row["TEST_NAME"]
            name = _truncate(short_name)
            model = row["TABLE_NAME"] or "N/A"
            status = row["STATUS"].lower()

            # Yellow for warn, red for fail/error
            if status == "warn":
                icon = "🟡"
            else:
                icon = "🔴"

            with st.container(border=True):
                cols = st.columns([4, 1])
                with cols[0]:
                    st.markdown(f"{icon} **{model}**")
                    st.caption(f"{name}")
                    st.caption(f"{status.upper()} | {str(row['DETECTED_AT'])[:16]}")
                with cols[1]:
                    if st.button("View", key=f"hist_test_{row['TEST_UNIQUE_ID']}_{row['DETECTED_AT']}"):
                        st.session_state["selected_test"] = row["TEST_UNIQUE_ID"]
                        st.session_state["selected_model"] = None
                        st.rerun()


def _render_historical_model_failures(days: int, search_filter: str):
    """Render historical model failures."""
    df = get_historical_model_failures(days, search_filter)

    if df.empty:
        st.info("No model failures")
        return

    with st.container(height=400):
        for _, row in df.iterrows():
            name = _truncate(row["NAME"])
            schema = row["SCHEMA_NAME"] or "unknown"

            with st.container(border=True):
                cols = st.columns([4, 1])
                with cols[0]:
                    st.markdown(f"🔴 **{name}**")
                    st.caption(f"{schema} | {row['STATUS']}")
                    st.caption(str(row["GENERATED_AT"])[:16])
                with cols[1]:
                    if st.button("View", key=f"hist_model_{row['UNIQUE_ID']}_{row['GENERATED_AT']}"):
                        st.session_state["selected_model"] = row["UNIQUE_ID"]
                        st.session_state["selected_test"] = None
                        st.rerun()
