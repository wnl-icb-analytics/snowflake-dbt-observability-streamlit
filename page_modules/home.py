"""Home page - Overview dashboard with KPIs."""

import os
import pandas as pd
import streamlit as st
from services.metrics_service import get_dashboard_kpis, get_recent_runs, get_top_failures, get_project_totals, get_total_execution_time
from services.alerts_service import get_current_issue_summary, get_latest_run_issues

DBT_LOGO_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "dbt-logo.svg")


def _format_timestamp(ts):
    """Format timestamp handling both datetime and string types."""
    if ts is None:
        return "N/A"
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(ts)[:16] if ts else "N/A"


def _format_relative_time(ts):
    """Format timestamp as relative time (e.g., '2 hours ago')."""
    if ts is None:
        return "N/A"
    from datetime import datetime
    try:
        # Convert string to datetime if needed
        if isinstance(ts, str):
            # Handle "2026-01-16 13:12:51" format (space instead of T)
            ts = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")

        now = datetime.now()
        diff = now - ts
        seconds = diff.total_seconds()

        if seconds < 0:
            return "Just now"
        elif seconds < 60:
            return "Just now"
        elif seconds < 3600:
            mins = int(seconds // 60)
            return f"{mins}m ago"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            return f"{hours}h ago"
        else:
            days = int(seconds // 86400)
            return f"{days}d ago"
    except Exception:
        return _format_timestamp(ts)


def _truncate(text: str, max_len: int = 50) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    return text[:max_len] + "..." if len(text) > max_len else text


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


def _format_issue_status(status: str) -> str:
    """Format issue status for display."""
    status = (status or "").lower()
    if status in ("fail", "error"):
        return "Failing"
    if status == "skipped":
        return "Skipped"
    if status == "warn":
        return "Warn"
    return status.title() if status else "Unknown"


def _summarize_issue(row) -> str:
    """Build a short human-readable summary for the current issue table."""
    issue_type = row["ISSUE_TYPE"]
    failure_count = int(row["FAILURE_COUNT"] or 0)
    affected_checks = row.get("AFFECTED_CHECKS")
    sample_message = row.get("SAMPLE_MESSAGE") or ""

    if issue_type == "Model":
        message = str(sample_message).replace("\n", " ")
        if "invalid identifier" in message.lower():
            return "Compilation error from an invalid identifier."
        if "does not exist or not authorized" in message.lower():
            return "Source object is missing or not accessible."
        if "out of sync" in message.lower() or "on_schema_change" in message.lower():
            return "Incremental schema drift between source and target."
        if message:
            return _truncate(message, 70)
        return f"{failure_count} model failures in range."

    checks_text = f"{int(affected_checks)} checks affected" if pd.notna(affected_checks) else "Test failures present"
    return f"{checks_text}; {failure_count} failures in range."


def _render_current_issues(days: int):
    """Render compact open/recurring issues summary table."""
    issues_df = get_current_issue_summary(days)

    st.subheader("Open Or Recurring Issues")
    st.caption("Active model failures and unresolved test areas across the selected time range.")

    if issues_df.empty:
        st.success("No open or recurring issues")
        return

    display_df = issues_df.copy()
    display_df["STATUS_LABEL"] = display_df["CURRENT_STATUS"].map(_format_issue_status)
    display_df["SUMMARY"] = display_df.apply(_summarize_issue, axis=1)
    display_df["FIRST_SEEN"] = display_df["FIRST_ISSUE_AT"].map(_format_timestamp)
    display_df["LAST_SEEN"] = display_df["LAST_ISSUE_AT"].map(_format_timestamp)

    display_df = display_df.rename(
        columns={
            "OBJECT_NAME": "Object",
            "ISSUE_TYPE": "Type",
            "STATUS_LABEL": "Status",
            "FAILURE_COUNT": f"Failures ({days}d)",
            "FIRST_SEEN": "First Seen",
            "LAST_SEEN": "Last Seen",
            "SUMMARY": "Summary",
        }
    )

    st.dataframe(
        display_df[["Object", "Type", "Status", f"Failures ({days}d)", "First Seen", "Last Seen", "Summary"]],
        use_container_width=True,
        hide_index=True,
    )


def _render_latest_run_issues():
    """Render issues from the most recent build invocation."""
    latest_df = get_latest_run_issues()

    st.subheader("Latest Build Issues")
    st.caption("Failures and warnings from the most recent dbt build invocation.")

    if latest_df.empty:
        st.success("Latest build completed without failures or warnings")
        return

    display_df = latest_df.copy()
    display_df["STATUS_LABEL"] = display_df["CURRENT_STATUS"].map(_format_issue_status)
    display_df["EVENT_AT"] = display_df["EVENT_AT"].map(_format_timestamp)
    display_df["SUMMARY"] = display_df["SUMMARY"].fillna("").map(lambda x: _truncate(str(x).replace("\n", " "), 90))

    display_df = display_df.rename(
        columns={
            "OBJECT_NAME": "Object",
            "ISSUE_TYPE": "Type",
            "STATUS_LABEL": "Status",
            "ISSUE_COUNT": "Count",
            "EVENT_AT": "Run Time",
            "SUMMARY": "Summary",
        }
    )

    st.dataframe(
        display_df[["Object", "Type", "Status", "Count", "Run Time", "Summary"]],
        use_container_width=True,
        hide_index=True,
    )



def render(search_filter: str = ""):
    # Title with dbt logo and time range selector
    title_col, range_col = st.columns([4, 1])
    with title_col:
        logo_col, text_col = st.columns([0.15, 3])
        with logo_col:
            st.image(DBT_LOGO_PATH, width=120)
        with text_col:
            st.title("dbt Project Health")
    with range_col:
        time_range = st.selectbox(
            "Time Range",
            options=[7, 30],
            format_func=lambda x: f"{x}d",
            label_visibility="collapsed"
        )

    kpis = get_dashboard_kpis(days=time_range)
    if kpis.empty:
        st.warning("No data available")
        return

    # Get total project counts (all models/tests, not just recent runs)
    totals = get_project_totals()
    if not totals.empty:
        total_models = int(totals.iloc[0]["TOTAL_MODELS"] or 0)
        total_tests = int(totals.iloc[0]["TOTAL_TESTS"] or 0)
    else:
        total_models = 0
        total_tests = 0

    row = kpis.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    total_failures = failed_tests + failed_models
    models_run = int(row.get("TOTAL_MODELS_RUN") or 0)
    tests_run = int(row.get("TOTAL_TESTS_RUN") or 0)

    # Health status banner
    if total_failures == 0:
        st.success("All systems healthy")
    else:
        st.error(f"{total_failures} active failures need attention")

    st.divider()

    # Get total execution time
    exec_time_df = get_total_execution_time(days=time_range)
    total_exec_time = exec_time_df.iloc[0]["TOTAL_TIME"] if not exec_time_df.empty else 0

    # KPI row - 6 metrics
    cols = st.columns(6)
    with cols[0]:
        st.metric("Failed Tests", failed_tests)
    with cols[1]:
        st.metric("Failed Models", failed_models)
    with cols[2]:
        st.metric("Total Models", total_models)
    with cols[3]:
        st.metric("Total Tests", total_tests)
    with cols[4]:
        if total_exec_time:
            st.metric(f"Runtime ({time_range}d)", _format_duration(total_exec_time))
        else:
            st.metric(f"Runtime ({time_range}d)", "N/A")
    with cols[5]:
        st.metric("Last Run", _format_relative_time(row["LAST_RUN_TIME"]))

    st.divider()

    _render_latest_run_issues()

    st.divider()

    _render_current_issues(time_range)

    st.divider()

    # Two column layout: Needs Attention + Recent Runs
    col_failures, col_runs = st.columns(2)

    with col_failures:
        st.subheader("Needs Attention")
        # Get all current failures (no limit)
        failures = get_top_failures(limit=100, days=time_range)
        if failures.empty:
            st.info("No current failures")
        else:
            # Show count from KPIs for consistency with banner/alerts
            st.caption(f"{total_failures} active failures")
            for _, f_row in failures.iterrows():
                icon = ":test_tube:" if f_row["TYPE"] == "test" else ":package:"
                unique_id = f_row["UNIQUE_ID"]
                model_path = f_row.get("MODEL_PATH") or ""

                with st.container(border=True):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        if f_row["TYPE"] == "test":
                            model_name = f_row.get("MODEL_NAME") or "unknown"
                            test_name = _truncate(f_row["NAME"])
                            st.markdown(f"{icon} **{model_name}**")
                            st.caption(f"{test_name}")
                        else:
                            name = _truncate(f_row["NAME"])
                            st.markdown(f"{icon} **{name}**")
                        if model_path:
                            st.caption(_truncate(model_path, 50))
                        st.caption(_format_timestamp(f_row['FAILED_AT']))
                    with col2:
                        if f_row["TYPE"] == "test":
                            if st.button("View", key=f"home_test_{unique_id}"):
                                st.session_state["selected_test"] = unique_id
                                st.session_state["selected_model"] = None
                                st.rerun()
                        else:
                            if st.button("View", key=f"home_model_{unique_id}"):
                                st.session_state["selected_model"] = unique_id
                                st.session_state["selected_test"] = None
                                st.rerun()

    with col_runs:
        st.subheader("Recent Runs")
        runs = get_recent_runs(limit=8)
        if runs.empty:
            st.info("No recent runs")
        else:
            for _, r_row in runs.iterrows():
                invocation_id = r_row["INVOCATION_ID"]
                with st.container(border=True):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"**{_format_timestamp(r_row['CREATED_AT'])}**")
                        cmd = r_row["COMMAND"] or "dbt"
                        target = r_row["TARGET_NAME"] or ""
                        warehouse = r_row.get("WAREHOUSE") or ""
                        selected = r_row.get("SELECTED") or ""
                        models_run = int(r_row.get("MODELS_RUN") or 0)
                        success = int(r_row.get("SUCCESS_COUNT") or 0)
                        fail = int(r_row.get("FAIL_COUNT") or 0)
                        duration = r_row.get("DURATION_SECONDS") or 0
                        tests_run = int(r_row.get("TESTS_RUN") or 0)
                        tests_passed = int(r_row.get("TESTS_PASSED") or 0)
                        tests_failed = int(r_row.get("TESTS_FAILED") or 0)
                        tests_warned = int(r_row.get("TESTS_WARNED") or 0)

                        info_parts = [cmd, target]
                        if warehouse:
                            info_parts.append(warehouse)
                        st.caption(" | ".join(p for p in info_parts if p))

                        if selected:
                            st.caption(_truncate(selected, 50))

                        # Model stats
                        if models_run > 0:
                            time_str = _format_duration(duration)
                            if fail > 0:
                                st.caption(f"Models: 🟢 {success} 🔴 {fail} | {time_str}")
                            else:
                                st.caption(f"Models: 🟢 {success} | {time_str}")

                        # Test stats
                        if tests_run > 0:
                            test_parts = [f"Tests: 🟢 {tests_passed}"]
                            if tests_failed > 0:
                                test_parts.append(f"🔴 {tests_failed}")
                            if tests_warned > 0:
                                test_parts.append(f"🟡 {tests_warned}")
                            st.caption(" ".join(test_parts))
                    with col2:
                        if st.button("View", key=f"home_run_{invocation_id}"):
                            st.session_state["selected_invocation"] = invocation_id
                            st.rerun()
