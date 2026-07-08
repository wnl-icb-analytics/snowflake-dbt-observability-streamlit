"""Home page - Overview dashboard with KPIs."""

import os
import pandas as pd
import streamlit as st
from services.metrics_service import get_dashboard_kpis, get_recent_runs, get_project_totals, get_total_execution_time
from services.alerts_service import (
    get_current_issue_summary,
    get_latest_run_issues,
    get_latest_build_summary,
    get_downstream_skips,
    get_latest_build_test_results,
)
from components.issue_cards import (
    format_timestamp as _format_timestamp,
    format_relative_time as _format_relative_time,
    truncate as _truncate,
    format_duration as _format_duration,
    format_issue_status as _format_issue_status,
    render_model_error_card as _render_model_error_card,
    render_test_issue_card as _render_test_issue_card,
)

DBT_LOGO_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "dbt-logo.svg")


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
    """Current failures across all runs - the true health picture."""
    issues_df = get_current_issue_summary(days)

    st.subheader("Open or recurring issues")
    st.caption(
        f"Everything currently failing across all runs in the last {days}d — the true health "
        "picture, regardless of what the latest run happened to cover."
    )

    if issues_df.empty:
        st.success("No open or recurring issues")
        return

    model_df = issues_df[issues_df["ISSUE_TYPE"] == "Model"]
    test_df = issues_df[issues_df["ISSUE_TYPE"] != "Model"]

    if not model_df.empty:
        st.markdown("**Model failures**")
        for _, row in model_df.iterrows():
            uid = row.get("UNIQUE_ID")
            fails = int(row["FAILURE_COUNT"] or 0)
            meta = (
                f"{_format_issue_status(row['CURRENT_STATUS'])} · "
                f"{fails} failures in {days}d · "
                f"streak since {_format_relative_time(row['FIRST_ISSUE_AT'])} · "
                f"last {_format_relative_time(row['LAST_ISSUE_AT'])}"
            )
            _render_model_error_card(
                object_name=row["OBJECT_NAME"],
                message=row.get("SAMPLE_MESSAGE"),
                unique_id=str(uid) if pd.notna(uid) else None,
                meta_line=meta,
                key_prefix="current",
            )

    if not test_df.empty:
        st.markdown("**Test areas**")
        display_df = test_df.copy()
        display_df["STATUS_LABEL"] = display_df["CURRENT_STATUS"].map(_format_issue_status)
        display_df["SUMMARY"] = display_df.apply(_summarize_issue, axis=1)
        display_df["FIRST_SEEN"] = display_df["FIRST_ISSUE_AT"].map(_format_relative_time)
        display_df["LAST_SEEN"] = display_df["LAST_ISSUE_AT"].map(_format_relative_time)

        display_df = display_df.rename(
            columns={
                "OBJECT_NAME": "Object",
                "ISSUE_TYPE": "Type",
                "STATUS_LABEL": "Status",
                "FAILURE_COUNT": f"Failures ({days}d)",
                "FIRST_SEEN": "Streak Started",
                "LAST_SEEN": "Last Seen",
                "SUMMARY": "Summary",
            }
        )

        st.dataframe(
            display_df[["Object", "Type", "Status", f"Failures ({days}d)", "Streak Started", "Last Seen", "Summary"]],
            use_container_width=True,
            hide_index=True,
        )


def _render_latest_run_issues():
    """Issues from the most recent build invocation (may be a partial run)."""
    summary = get_latest_build_summary()
    latest_df = get_latest_run_issues()

    st.subheader("Latest build")

    skipped_count = 0
    invocation_id = None
    if not summary.empty:
        s = summary.iloc[0]
        skipped_count = int(s["SKIPPED_COUNT"] or 0)
        invocation_id = s["INVOCATION_ID"]
        test_failed = int(s.get("TEST_FAILED_COUNT") or 0)
        test_warned = int(s.get("TEST_WARNED_COUNT") or 0)
        caption = (
            f"Most recent build · Models 🟢 {int(s['SUCCESS_COUNT'] or 0)} "
            f"🔴 {int(s['FAILED_COUNT'] or 0)} ⚪ {skipped_count}"
        )
        if test_failed or test_warned:
            test_bits = []
            if test_failed:
                test_bits.append(f"🔴 {test_failed}")
            if test_warned:
                test_bits.append(f"🟡 {test_warned}")
            caption += " · Tests " + " ".join(test_bits)
        caption += f" · {_format_relative_time(s['CREATED_AT'])}"
        st.caption(caption)
        sel = s.get("SELECTED")
        if sel is not None and str(sel).strip() and str(sel).lower() != "none":
            st.caption(f"Partial run — selection: `{_truncate(str(sel), 80)}`. Project-wide health is above.")
    else:
        st.caption("The most recent dbt build invocation.")

    if latest_df.empty:
        st.success("Latest build completed without failures or warnings")
        return

    # Blast radius: only walk the DAG when the build actually skipped models.
    skips_map = {}
    if skipped_count > 0 and invocation_id:
        ds = get_downstream_skips(invocation_id)
        skips_map = {r["UNIQUE_ID"]: int(r["DOWNSTREAM_SKIPPED"]) for _, r in ds.iterrows()}

    model_df = latest_df[latest_df["ISSUE_TYPE"] == "Model"]

    if not model_df.empty:
        st.markdown("**Model failures**")
        for _, row in model_df.iterrows():
            uid = row.get("UNIQUE_ID")
            uid = str(uid) if pd.notna(uid) else None
            meta = f"{_format_issue_status(row['CURRENT_STATUS'])} · {_format_timestamp(row['EVENT_AT'])}"
            _render_model_error_card(
                object_name=row["OBJECT_NAME"],
                message=row.get("SUMMARY"),
                unique_id=uid,
                meta_line=meta,
                key_prefix="latest",
                downstream_skipped=skips_map.get(uid),
            )

    test_results = get_latest_build_test_results()
    if not test_results.empty:
        st.markdown("**Test issues**")
        for _, row in test_results.iterrows():
            _render_test_issue_card(row, key_prefix="latest_test")


def _render_recent_runs():
    """Recent invocations with an at-a-glance status light and model/test/skip counts."""
    st.subheader("Recent runs")
    runs = get_recent_runs(limit=8)
    if runs.empty:
        st.info("No recent runs")
        return

    for _, r_row in runs.iterrows():
        invocation_id = r_row["INVOCATION_ID"]
        success = int(r_row.get("SUCCESS_COUNT") or 0)
        fail = int(r_row.get("FAIL_COUNT") or 0)
        skipped = int(r_row.get("SKIPPED_COUNT") or 0)
        models_run = int(r_row.get("MODELS_RUN") or 0)
        duration = r_row.get("DURATION_SECONDS") or 0
        tests_run = int(r_row.get("TESTS_RUN") or 0)
        tests_passed = int(r_row.get("TESTS_PASSED") or 0)
        tests_failed = int(r_row.get("TESTS_FAILED") or 0)
        tests_warned = int(r_row.get("TESTS_WARNED") or 0)

        # Red if anything failed, yellow if only warnings, white if only skips, else green.
        if fail > 0 or tests_failed > 0:
            icon = "🔴"
        elif tests_warned > 0:
            icon = "🟡"
        elif skipped > 0 and success == 0:
            icon = "⚪"
        else:
            icon = "🟢"

        with st.container(border=True):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(
                    f"{icon} **{_format_timestamp(r_row['CREATED_AT'])}** · "
                    f"{_format_relative_time(r_row['CREATED_AT'])}"
                )
                cmd = r_row["COMMAND"] or "dbt"
                target = r_row["TARGET_NAME"] or ""
                warehouse = r_row.get("WAREHOUSE") or ""
                st.caption(" | ".join(p for p in [cmd, target, warehouse] if p))

                selected = r_row.get("SELECTED") or ""
                if selected:
                    st.caption(_truncate(str(selected), 50))

                if models_run > 0:
                    parts = [f"Models: 🟢 {success}"]
                    if fail > 0:
                        parts.append(f"🔴 {fail}")
                    if skipped > 0:
                        parts.append(f"⚪ {skipped}")
                    line = " ".join(parts)
                    time_str = _format_duration(duration)
                    if time_str:
                        line += f" | {time_str}"
                    st.caption(line)

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

    totals = get_project_totals()
    total_models = int(totals.iloc[0]["TOTAL_MODELS"] or 0) if not totals.empty else 0
    total_tests = int(totals.iloc[0]["TOTAL_TESTS"] or 0) if not totals.empty else 0

    row = kpis.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    total_failures = failed_tests + failed_models

    # Static project inventory - a caption, not a KPI.
    st.caption(f"{total_models} models · {total_tests} tests in project")

    # Health banner reflects current open state across ALL runs (latest status
    # per model/test), not a single possibly-partial build.
    if total_failures == 0:
        st.success("All systems healthy — nothing currently failing")
    else:
        st.error(
            f"{total_failures} currently failing "
            f"({failed_models} models, {failed_tests} test areas) — see open issues below"
        )

    st.divider()

    exec_time_df = get_total_execution_time(days=time_range)
    total_exec_time = exec_time_df.iloc[0]["TOTAL_TIME"] if not exec_time_df.empty else 0

    # KPI row - 4 actionable metrics (project totals live in the caption above).
    cols = st.columns(4)
    with cols[0]:
        st.metric("Failing models", failed_models)
    with cols[1]:
        st.metric("Failing test areas", failed_tests)
    with cols[2]:
        st.metric(f"Runtime ({time_range}d)", _format_duration(total_exec_time) or "N/A")
    with cols[3]:
        st.metric("Last run", _format_relative_time(row["LAST_RUN_TIME"]))

    st.divider()

    # Current health first (what's broken now), then the latest run, then history.
    _render_current_issues(time_range)

    st.divider()

    _render_latest_run_issues()

    st.divider()

    _render_recent_runs()
