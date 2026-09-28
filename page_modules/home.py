"""Home page - Overview dashboard with KPIs."""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import (
    format_duration,
    format_relative_time,
    format_timestamp,
    issue_status,
    truncate,
)
from components.issue_cards import render_model_error_card, render_test_issue_card
from page_modules.runs import compile_show_control, runs_table
from services.alerts_service import (
    get_current_issue_summary,
    get_downstream_model_counts,
    get_downstream_skips,
    get_latest_build_summary,
    get_latest_build_test_results,
    get_latest_run_issues,
)
from services.metrics_service import (
    get_dashboard_kpis,
    get_project_totals,
    get_recent_runs,
    get_total_execution_time,
)


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
            return truncate(message, 70)
        return f"{failure_count} model failures in range."

    checks_text = f"{int(affected_checks)} checks affected" if pd.notna(affected_checks) else "Test failures present"
    return f"{checks_text}; {failure_count} failures in range."


def _render_current_issues(days: int):
    """Current failures across all runs - the true health picture."""
    issues_df = get_current_issue_summary(days)

    st.subheader("Open issues")
    st.caption(
        f"Everything currently failing across all runs in the last {days} days, "
        "regardless of what the latest run covered."
    )

    if issues_df.empty:
        ui.empty_state("No open or recurring issues", ok=True)
        return

    model_df = issues_df[issues_df["ISSUE_TYPE"] == "Model"]
    test_df = issues_df[issues_df["ISSUE_TYPE"] != "Model"]

    if not model_df.empty:
        st.markdown("**Model failures**")
        # Static DAG impact: how many models depend on each failing model.
        model_uids = [str(u) for u in model_df["UNIQUE_ID"].tolist() if pd.notna(u)]
        impact_map = {}
        if model_uids:
            dc = get_downstream_model_counts(model_uids)
            impact_map = {r["UNIQUE_ID"]: int(r["DOWNSTREAM_COUNT"]) for _, r in dc.iterrows()}
        for _, row in model_df.iterrows():
            uid = row.get("UNIQUE_ID")
            uid = str(uid) if pd.notna(uid) else None
            fails = int(row["FAILURE_COUNT"] or 0)
            meta = (
                f"{issue_status(row['CURRENT_STATUS'])} · "
                f"{fails} failures in {days}d · "
                f"streak since {format_relative_time(row['FIRST_ISSUE_AT'])} · "
                f"last {format_relative_time(row['LAST_ISSUE_AT'])}"
            )
            render_model_error_card(
                object_name=row["OBJECT_NAME"],
                message=row.get("SAMPLE_MESSAGE"),
                unique_id=uid,
                meta_line=meta,
                key_prefix="current",
                downstream_total=impact_map.get(uid),
            )

    if not test_df.empty:
        st.markdown("**Test areas**")
        display_df = test_df.copy()
        display_df["STATUS_LABEL"] = display_df["CURRENT_STATUS"].map(issue_status)
        display_df["SUMMARY"] = display_df.apply(_summarize_issue, axis=1)
        display_df["FIRST_SEEN"] = display_df["FIRST_ISSUE_AT"].map(format_relative_time)
        display_df["LAST_SEEN"] = display_df["LAST_ISSUE_AT"].map(format_relative_time)
        st.dataframe(
            display_df,
            column_order=["OBJECT_NAME", "STATUS_LABEL", "FAILURE_COUNT", "FIRST_SEEN", "LAST_SEEN", "SUMMARY"],
            column_config={
                "OBJECT_NAME": "Object",
                "STATUS_LABEL": "Status",
                "FAILURE_COUNT": st.column_config.NumberColumn(f"Failures ({days}d)"),
                "FIRST_SEEN": "Streak started",
                "LAST_SEEN": "Last seen",
                "SUMMARY": st.column_config.TextColumn("Summary", width="large"),
            },
            hide_index=True,
            width="stretch",
        )


def _render_latest_build():
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
        parts = [
            f"{format_timestamp(s['CREATED_AT'])} ({format_relative_time(s['CREATED_AT'])})",
            f"models: {int(s['SUCCESS_COUNT'] or 0)} ok, {int(s['FAILED_COUNT'] or 0)} failed, {skipped_count} skipped",
        ]
        if test_failed or test_warned:
            parts.append(f"tests: {test_failed} failed, {test_warned} warned")
        st.caption(" · ".join(parts))
        sel = s.get("SELECTED")
        if sel is not None and str(sel).strip() and str(sel).lower() != "none":
            st.caption(f"Partial run, selection: `{truncate(str(sel), 80)}`. Project-wide health is above.")
        if st.button("Open this run", key="latest_build_open", icon=":material/open_in_new:"):
            nav.open_run(invocation_id)
    else:
        st.caption("The most recent dbt build invocation.")

    if latest_df.empty:
        ui.empty_state("Latest build completed without failures or warnings", ok=True)
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
            meta = f"{issue_status(row['CURRENT_STATUS'])} · {format_timestamp(row['EVENT_AT'])}"
            render_model_error_card(
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
            render_test_issue_card(row, key_prefix="latest_test")


def _render_recent_runs():
    st.subheader("Recent runs")
    include = compile_show_control(key="home_runs_compile_show")
    runs = get_recent_runs(limit=8, include_compile_show=include)
    if runs.empty:
        ui.empty_state("No recent runs")
        return
    selected = runs_table(runs, key="home_runs")
    if selected is not None:
        nav.open_run(selected["INVOCATION_ID"])


def render():
    days = nav.days()

    totals = get_project_totals()
    total_models = int(totals.iloc[0]["TOTAL_MODELS"] or 0) if not totals.empty else 0
    total_tests = int(totals.iloc[0]["TOTAL_TESTS"] or 0) if not totals.empty else 0
    ui.page_header(
        "Project health",
        f"{total_models:,} models and {total_tests:,} tests in the project · last {days} days",
    )

    kpis = get_dashboard_kpis(days=days)
    if kpis.empty:
        ui.empty_state("No data available")
        return

    row = kpis.iloc[0]
    failed_tests = int(row["FAILED_TESTS"] or 0)
    failed_models = int(row["FAILED_MODELS"] or 0)
    total_failures = failed_tests + failed_models

    # Health banner reflects current open state across ALL runs (latest status
    # per model/test), not a single possibly-partial build.
    if total_failures == 0:
        st.success("All systems healthy: nothing currently failing")
    else:
        st.error(
            f"{total_failures} currently failing "
            f"({failed_models} models, {failed_tests} test areas). See open issues below."
        )

    exec_time_df = get_total_execution_time(days=days)
    total_exec_time = exec_time_df.iloc[0]["TOTAL_TIME"] if not exec_time_df.empty else 0

    ui.metric_row([
        ("Failing models", failed_models),
        ("Failing test areas", failed_tests),
        (f"Runtime ({days}d)", format_duration(total_exec_time) or "N/A"),
        ("Last run", format_relative_time(row["LAST_RUN_TIME"])),
    ])

    # Current health first (what's broken now), then the latest run, then history.
    _render_current_issues(days)
    _render_latest_build()
    _render_recent_runs()
