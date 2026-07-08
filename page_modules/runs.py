"""Runs page - View dbt invocations and model execution waterfall."""

import streamlit as st
import pandas as pd
import altair as alt
from services.runs_service import (
    get_invocations,
    get_invocations_count,
    get_invocation_details,
    get_invocation_models,
    get_invocation_tests,
)
from services.alerts_service import get_downstream_skips
from components.issue_cards import render_model_error_card, render_test_issue_card


def _format_duration(seconds) -> str:
    """Format duration in human-readable form."""
    if not seconds or seconds <= 0:
        return ""
    seconds = int(seconds)
    if seconds >= 3600:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        if mins > 0:
            return f"{hours}h {mins}m"
        return f"{hours}h"
    elif seconds >= 60:
        mins = seconds // 60
        secs = seconds % 60
        if secs > 0 and mins < 10:
            return f"{mins}m {secs}s"
        return f"{mins}m"
    else:
        return f"{seconds}s"


def _format_timestamp(ts):
    """Format timestamp for display."""
    if ts is None:
        return "N/A"
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(ts)[:16] if ts else "N/A"


def _truncate(text: str, max_len: int = 50) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    return text[:max_len] + "..." if len(text) > max_len else text


def render(search_filter: str = ""):
    # Check if viewing invocation detail
    if st.session_state.get("selected_invocation"):
        _render_invocation_detail(st.session_state["selected_invocation"])
        return

    st.title("Runs")

    # Filters
    col1, col2 = st.columns([1, 4])
    with col1:
        days = st.selectbox("Time range", [7, 14, 30], index=0, format_func=lambda x: f"{x}d", key="runs_days")

    # Get count and paginate
    count_df = get_invocations_count(days)
    total_runs = int(count_df.iloc[0]["TOTAL"]) if not count_df.empty else 0

    page_size = 20
    total_pages = max(1, (total_runs + page_size - 1) // page_size)

    if "runs_page" not in st.session_state:
        st.session_state["runs_page"] = 0

    current_page = st.session_state["runs_page"]
    if current_page >= total_pages:
        current_page = 0
        st.session_state["runs_page"] = 0

    offset = current_page * page_size
    df = get_invocations(days=days, limit=page_size, offset=offset)

    if df.empty:
        st.info("No runs found in this time range")
        return

    # Header with pagination
    header_cols = st.columns([3, 2])
    with header_cols[0]:
        st.write(f"**{total_runs} invocations**")
    with header_cols[1]:
        if total_pages > 1:
            nav_cols = st.columns([1, 2, 1])
            with nav_cols[0]:
                if st.button("← Prev", disabled=current_page == 0, key="runs_prev"):
                    st.session_state["runs_page"] = current_page - 1
                    st.rerun()
            with nav_cols[1]:
                st.caption(f"Page {current_page + 1} of {total_pages}")
            with nav_cols[2]:
                if st.button("Next →", disabled=current_page >= total_pages - 1, key="runs_next"):
                    st.session_state["runs_page"] = current_page + 1
                    st.rerun()

    # Render invocations list
    for _, row in df.iterrows():
        success = int(row["SUCCESS_COUNT"] or 0)
        fail = int(row["FAIL_COUNT"] or 0)
        skipped = int(row["SKIPPED_COUNT"] or 0)
        models_run = int(row["MODELS_RUN"] or 0)
        duration = row["DURATION_SECONDS"] or 0
        tests_run = int(row.get("TESTS_RUN") or 0)
        tests_passed = int(row.get("TESTS_PASSED") or 0)
        tests_failed = int(row.get("TESTS_FAILED") or 0)
        tests_warned = int(row.get("TESTS_WARNED") or 0)

        # Red if anything failed, yellow if only warnings, white if only skips, else green.
        if fail > 0 or tests_failed > 0:
            status_icon = "🔴"
        elif tests_warned > 0:
            status_icon = "🟡"
        elif skipped > 0 and success == 0:
            status_icon = "⚪"
        else:
            status_icon = "🟢"

        with st.container(border=True):
            cols = st.columns([3, 1, 1, 1])
            with cols[0]:
                st.markdown(f"{status_icon} **{_format_timestamp(row['CREATED_AT'])}**")
                cmd = row["COMMAND"] or "dbt"
                target = row["TARGET_NAME"] or ""
                warehouse = row.get("WAREHOUSE") or ""
                info_parts = [cmd, target]
                if warehouse:
                    info_parts.append(warehouse)
                st.caption(" | ".join(p for p in info_parts if p))
                if row.get("SELECTED"):
                    st.caption(_truncate(row["SELECTED"], 60))
            with cols[1]:
                st.caption("Models")
                model_parts = [f"🟢 {success}"]
                if fail > 0:
                    model_parts.append(f"🔴 {fail}")
                if skipped > 0:
                    model_parts.append(f"⚪ {skipped}")
                st.write(" ".join(model_parts))
                # Test stats
                if tests_run > 0:
                    st.caption("Tests")
                    test_parts = [f"🟢 {tests_passed}"]
                    if tests_failed > 0:
                        test_parts.append(f"🔴 {tests_failed}")
                    if tests_warned > 0:
                        test_parts.append(f"🟡 {tests_warned}")
                    st.write(" ".join(test_parts))
            with cols[2]:
                st.caption("Duration")
                st.write(_format_duration(duration))
            with cols[3]:
                if st.button("View", key=f"run_{row['INVOCATION_ID']}"):
                    st.session_state["selected_invocation"] = row["INVOCATION_ID"]
                    st.rerun()


def _render_invocation_detail(invocation_id: str):
    """Render detail view for a specific invocation."""
    # Back button - navigate to Runs page
    if st.button("← Back to Runs"):
        st.session_state["selected_invocation"] = None
        st.session_state["nav_page"] = "Runs"
        st.rerun()

    # Get invocation details
    details_df = get_invocation_details(invocation_id)
    if details_df.empty:
        st.error(f"Invocation not found: {invocation_id}")
        return

    details = details_df.iloc[0]

    # Header
    st.title(f"Run: {_format_timestamp(details['CREATED_AT'])}")
    st.caption(f"`{invocation_id}`")

    st.divider()

    # Metadata
    meta_cols = st.columns(4)
    with meta_cols[0]:
        st.markdown("**Command**")
        st.write(details.get("COMMAND") or "N/A")
    with meta_cols[1]:
        st.markdown("**Target**")
        st.write(details.get("TARGET_NAME") or "N/A")
    with meta_cols[2]:
        st.markdown("**Warehouse**")
        st.write(details.get("WAREHOUSE") or "N/A")
    with meta_cols[3]:
        st.markdown("**Duration**")
        st.write(_format_duration(details.get("DURATION_SECONDS") or 0))

    if details.get("SELECTED"):
        st.markdown("**Selection:**")
        st.code(details["SELECTED"], language=None)

    st.divider()

    # Tabs for models and tests
    tab_models, tab_tests, tab_waterfall = st.tabs(["Models", "Tests", "Timeline"])

    with tab_models:
        _render_invocation_models(invocation_id)

    with tab_tests:
        _render_invocation_tests(invocation_id)

    with tab_waterfall:
        with st.spinner("Loading timeline..."):
            _render_waterfall_chart(invocation_id, details)


def _render_invocation_models(invocation_id: str):
    """Render models for an invocation - failures as rich cards."""
    df = get_invocation_models(invocation_id)

    if df.empty:
        st.info("No model runs in this invocation")
        return

    success = len(df[df["STATUS"] == "success"])
    fail_df = df[df["STATUS"].isin(["fail", "error"])]
    skipped_df = df[df["STATUS"] == "skipped"]

    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Total Models", len(df))
    with summary_cols[1]:
        st.metric("Success", success)
    with summary_cols[2]:
        st.metric("Failed", len(fail_df))
    with summary_cols[3]:
        st.metric("Skipped", len(skipped_df))

    st.divider()

    # Blast radius per failure, when this run skipped models downstream.
    skips_map = {}
    if not skipped_df.empty and not fail_df.empty:
        ds = get_downstream_skips(invocation_id)
        skips_map = {r["UNIQUE_ID"]: int(r["DOWNSTREAM_SKIPPED"]) for _, r in ds.iterrows()}

    if not fail_df.empty:
        st.markdown("**Failures**")
        for _, row in fail_df.iterrows():
            uid = row.get("UNIQUE_ID")
            uid = str(uid) if pd.notna(uid) else None
            time_str = f"{row['EXECUTION_TIME']:.1f}s" if row.get("EXECUTION_TIME") else ""
            meta = " · ".join(p for p in [row["STATUS"].upper(), time_str, row.get("MODEL_PATH") or ""] if p)
            render_model_error_card(
                object_name=row["NAME"],
                message=row.get("MESSAGE"),
                unique_id=uid,
                meta_line=meta,
                key_prefix="inv_model",
                downstream_skipped=skips_map.get(uid),
            )
    else:
        st.success("No model failures in this run")

    if not skipped_df.empty:
        with st.expander(f"Skipped models ({len(skipped_df)})", expanded=False):
            st.dataframe(
                skipped_df[["NAME", "SCHEMA_NAME"]].rename(columns={"NAME": "Model", "SCHEMA_NAME": "Schema"}),
                use_container_width=True,
                hide_index=True,
            )


def _render_invocation_tests(invocation_id: str):
    """Render tests for an invocation - failures/warnings as rich cards."""
    df = get_invocation_tests(invocation_id)

    if df.empty:
        st.info("No test runs in this invocation")
        return

    passed = len(df[df["STATUS"] == "pass"])
    issue_df = df[df["STATUS"].isin(["fail", "error", "warn"])]
    failed = len(df[df["STATUS"].isin(["fail", "error"])])
    warned = len(df[df["STATUS"] == "warn"])

    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Total Tests", len(df))
    with summary_cols[1]:
        st.metric("Passed", passed)
    with summary_cols[2]:
        st.metric("Failed", failed)
    with summary_cols[3]:
        st.metric("Warned", warned)

    st.divider()

    if issue_df.empty:
        st.success("All tests passed in this run")
        return

    for _, row in issue_df.iterrows():
        render_test_issue_card(row, key_prefix="inv_test")


def _render_waterfall_chart(invocation_id: str, details):
    """Render a waterfall/Gantt chart of model execution."""
    df = get_invocation_models(invocation_id)

    if df.empty:
        st.info("No model timing data available")
        return

    # Filter to models with timing data
    timing_df = df[df["EXECUTE_STARTED_AT"].notna() & df["EXECUTE_COMPLETED_AT"].notna()].copy()

    if timing_df.empty:
        st.info("No execution timing data available for waterfall chart")
        return

    # Convert to datetime (remove timezone to avoid tz-naive/tz-aware mismatch)
    timing_df["START"] = pd.to_datetime(timing_df["EXECUTE_STARTED_AT"]).dt.tz_localize(None)
    timing_df["END"] = pd.to_datetime(timing_df["EXECUTE_COMPLETED_AT"]).dt.tz_localize(None)

    # Get run start time as reference
    run_start = pd.to_datetime(details.get("RUN_STARTED_AT"))
    if hasattr(run_start, 'tz_localize'):
        run_start = run_start.tz_localize(None) if run_start.tzinfo else run_start
    elif hasattr(run_start, 'tzinfo') and run_start.tzinfo is not None:
        run_start = run_start.replace(tzinfo=None)
    if pd.isna(run_start):
        run_start = timing_df["START"].min()

    # Calculate relative times in seconds from run start
    timing_df["START_SEC"] = (timing_df["START"] - run_start).dt.total_seconds()
    timing_df["END_SEC"] = (timing_df["END"] - run_start).dt.total_seconds()

    # Color by status
    status_colors = {
        "success": "#28a745",
        "fail": "#dc3545",
        "error": "#dc3545",
        "skipped": "#6c757d",
    }
    timing_df["COLOR"] = timing_df["STATUS"].map(lambda x: status_colors.get(x, "#6c757d"))

    st.subheader("Execution Timeline")
    st.caption(f"{len(timing_df)} models with timing data")

    # Create Gantt-style chart
    chart = alt.Chart(timing_df).mark_bar().encode(
        x=alt.X("START_SEC:Q", title="Seconds from run start"),
        x2=alt.X2("END_SEC:Q"),
        y=alt.Y("NAME:N", title="Model", sort=alt.EncodingSortField(field="START_SEC", order="ascending")),
        color=alt.Color(
            "STATUS:N",
            scale=alt.Scale(
                domain=["success", "fail", "error", "skipped"],
                range=["#28a745", "#dc3545", "#dc3545", "#6c757d"]
            ),
            legend=alt.Legend(title="Status")
        ),
        tooltip=[
            alt.Tooltip("NAME:N", title="Model"),
            alt.Tooltip("STATUS:N", title="Status"),
            alt.Tooltip("EXECUTION_TIME:Q", title="Duration (s)", format=".1f"),
            alt.Tooltip("START_SEC:Q", title="Start (s)", format=".1f"),
            alt.Tooltip("END_SEC:Q", title="End (s)", format=".1f"),
        ]
    ).properties(
        height=max(200, len(timing_df) * 20)
    )

    st.altair_chart(chart, use_container_width=True)

    # Show summary stats
    total_time = timing_df["EXECUTION_TIME"].sum()
    max_end = timing_df["END_SEC"].max()
    parallelism = total_time / max_end if max_end > 0 else 1

    stat_cols = st.columns(3)
    with stat_cols[0]:
        st.metric("Total Model Time", _format_duration(total_time))
    with stat_cols[1]:
        st.metric("Wall Clock Time", _format_duration(max_end))
    with stat_cols[2]:
        st.metric("Avg Parallelism", f"{parallelism:.1f}x")
