"""Home page - Overview dashboard with KPIs."""

import os
import re
import json
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
from database import run_query

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


# Patterns for pulling structured fields out of a dbt/Snowflake error message.
_ERROR_CLASS_RE = re.compile(r"([A-Za-z][A-Za-z ]*?Error)\s+in\s+(?:model|test|seed|snapshot)", re.I)
_SNOWFLAKE_CODE_RE = re.compile(r"\[Snowflake\]\s*(\d+)\s*\(([0-9A-Za-z]+)\)")
_MISSING_OBJECT_RE = re.compile(r"Object '([^']+)' does not exist or not authorized", re.I)
_INVALID_IDENTIFIER_RE = re.compile(r"invalid identifier '([^']+)'", re.I)
_COMPILED_LOC_RE = re.compile(r"\(in (compiled/[^)]+)\)")


def _categorize_error(message: str) -> str:
    """Map an error message to a short category label."""
    m = (message or "").lower()
    if "does not exist or not authorized" in m:
        return "Missing or unauthorized object"
    if "invalid identifier" in m:
        return "Invalid identifier"
    if "out of sync" in m or "on_schema_change" in m:
        return "Incremental schema drift"
    if "syntax error" in m or "compilation error" in m:
        return "SQL compilation error"
    return "Model failure"


def _parse_model_error(message: str) -> dict:
    """Extract structured fields (error class, Snowflake code, offending object,
    compiled file location) from a raw dbt run message."""
    msg = message or ""
    out = {"category": _categorize_error(msg)}
    for key, pattern in (
        ("error_class", _ERROR_CLASS_RE),
        ("missing_object", _MISSING_OBJECT_RE),
        ("invalid_identifier", _INVALID_IDENTIFIER_RE),
        ("compiled_location", _COMPILED_LOC_RE),
    ):
        m = pattern.search(msg)
        if m:
            out[key] = m.group(1)
    code = _SNOWFLAKE_CODE_RE.search(msg)
    if code:
        out["snowflake_code"] = f"{code.group(1)} ({code.group(2)})"
    return out


def _render_model_error_card(object_name, message, unique_id, meta_line, key_prefix, downstream_skipped=None):
    """Expandable card for one failing model: parsed fields + full error + drill-in.
    downstream_skipped, when set, is the blast radius (models skipped downstream in the run)."""
    parsed = _parse_model_error(message)
    title = f"🔴 {object_name} — {parsed['category']}"
    if downstream_skipped:
        title += f" · {downstream_skipped} skipped downstream"
    with st.expander(title, expanded=False):
        if meta_line:
            st.caption(meta_line)
        if downstream_skipped:
            st.markdown(f"**Blast radius:** {downstream_skipped} downstream model(s) skipped in this run")
        if parsed.get("error_class"):
            st.markdown(f"**Error type:** {parsed['error_class']}")
        if parsed.get("snowflake_code"):
            st.markdown(f"**Snowflake code:** `{parsed['snowflake_code']}`")
        if parsed.get("missing_object"):
            st.markdown(f"**Missing / unauthorized object:** `{parsed['missing_object']}`")
        if parsed.get("invalid_identifier"):
            st.markdown(f"**Invalid identifier:** `{parsed['invalid_identifier']}`")
        if parsed.get("compiled_location"):
            st.markdown(f"**Compiled SQL:** `{parsed['compiled_location']}`")
        if message:
            st.markdown("**Full error:**")
            st.code(str(message), language="text")
        else:
            st.caption("No error message captured for this run.")
        if unique_id and st.button("View model", key=f"{key_prefix}_view_{unique_id}"):
            st.session_state["selected_model"] = unique_id
            st.session_state["selected_test"] = None
            st.rerun()


def _test_kind(test_name, namespace) -> str:
    """Short label for the kind of test (accepted_values, not_null, ...)."""
    name = (test_name or "").lower()
    for kind in ("accepted_values", "not_null", "relationships", "accepted_range", "unique"):
        if kind in name:
            return kind
    return test_name or namespace or "test"


def _accepted_values(test_params):
    """Pull the accepted `values` list out of a test_params JSON string, if any."""
    if not test_params:
        return None
    try:
        data = json.loads(test_params) if isinstance(test_params, str) else test_params
    except (ValueError, TypeError):
        return None
    vals = data.get("values") if isinstance(data, dict) else None
    if isinstance(vals, list) and vals:
        return [str(v) for v in vals]
    return None


def _present(value) -> bool:
    """True when a dataframe value holds real content (not None/NaN/empty)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip() not in ("", "{}", "[]")


def _model_fqn(row):
    """Fully-qualified model relation from the joined dbt_models columns."""
    parts = [row.get("MODEL_DATABASE"), row.get("MODEL_SCHEMA"), row.get("MODEL_RELATION")]
    if all(_present(p) for p in parts):
        return ".".join(str(p) for p in parts)
    return None


def _inspect_query(kind, model_fqn, column, accepted):
    """Reconstruct a query to see the offending rows for the common test kinds.
    Elementary only stores this when store_failures is on, so we derive it."""
    if not model_fqn or not _present(column):
        return None
    if kind == "accepted_values" and accepted:
        vals = ", ".join("'" + v.replace("'", "''") + "'" for v in accepted)
        return (
            f"SELECT {column}, COUNT(*) AS failing_rows\n"
            f"FROM {model_fqn}\n"
            f"WHERE {column} IS NOT NULL AND {column} NOT IN ({vals})\n"
            f"GROUP BY {column}\nORDER BY failing_rows DESC;"
        )
    if kind == "not_null":
        return f"SELECT *\nFROM {model_fqn}\nWHERE {column} IS NULL\nLIMIT 100;"
    return None


def _render_test_issue_card(row, key_prefix):
    """Expandable card for one failing/warning test: what it checks, how many
    rows failed, accepted values, source, and a button to pull the failing rows."""
    status = (row.get("STATUS") or "").lower()
    icon = "🟡" if status == "warn" else "🔴"
    kind = _test_kind(row.get("TEST_NAME"), row.get("TEST_NAMESPACE"))
    loc = ".".join(p for p in [row.get("TABLE_NAME") or "", row.get("COLUMN_NAME") or ""] if p)
    title = f"{icon} {kind}" + (f" — {loc}" if loc else "")

    tuid = row.get("TEST_UNIQUE_ID")
    tuid = str(tuid) if (tuid is not None and pd.notna(tuid)) else None
    row_key = f"{key_prefix}_rows_{tuid or loc}"
    show_rows = st.session_state.get(row_key, False)

    accepted = _accepted_values(row.get("TEST_PARAMS"))
    # A runnable query: elementary's stored one, else reconstruct it.
    stored_query = row.get("TEST_RESULTS_QUERY")
    if _present(stored_query):
        query_sql, reconstructed = str(stored_query), False
    else:
        query_sql = _inspect_query(kind, _model_fqn(row), row.get("COLUMN_NAME"), accepted)
        reconstructed = bool(query_sql)

    # Keep the card open across the rerun that a button click triggers.
    with st.expander(title, expanded=show_rows):
        meta = " · ".join(p for p in [
            _format_issue_status(row.get("STATUS")),
            row.get("TEST_NAMESPACE") or "",
            (row.get("SEVERITY") or "").lower(),
            _format_timestamp(row.get("DETECTED_AT")),
        ] if p)
        st.caption(meta)

        failures = row.get("FAILURES")
        if not _present(failures):
            failures = row.get("FAILED_ROW_COUNT")
        if _present(failures):
            try:
                st.markdown(f"**Failing rows:** {int(failures)}")
            except (TypeError, ValueError):
                pass

        if accepted:
            st.markdown("**Accepted values:** " + ", ".join(f"`{v}`" for v in accepted))

        desc = row.get("TEST_RESULTS_DESCRIPTION")
        if desc and str(desc).strip().lower() not in ("", "warn", "fail", "error", "pass"):
            (st.warning if status == "warn" else st.error)(str(desc))

        result_rows = row.get("RESULT_ROWS")
        if _present(result_rows):
            st.markdown("**Sample failing rows:**")
            st.code(str(result_rows), language="json")

        if _present(row.get("ORIGINAL_PATH")):
            st.markdown(f"**Source:** `{row['ORIGINAL_PATH']}`")

        # Run the query on demand and show the offending rows inline.
        if query_sql:
            btn_cols = st.columns([1, 1, 3])
            with btn_cols[0]:
                if st.button("Show failing rows", key=f"{row_key}_show"):
                    st.session_state[row_key] = True
                    st.rerun()
            if show_rows:
                with btn_cols[1]:
                    if st.button("Hide", key=f"{row_key}_hide"):
                        st.session_state[row_key] = False
                        st.rerun()
                try:
                    with st.spinner("Querying failing rows..."):
                        res = run_query(query_sql)
                    if res.empty:
                        st.info("Query returned no rows.")
                    else:
                        note = f"{len(res)} row(s)" + (" (showing first 500)" if len(res) > 500 else "")
                        st.caption(note)
                        st.dataframe(res.head(500), use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Could not run query: {e}")
                st.caption("Reconstructed query" if reconstructed else "Query captured by elementary")
                st.code(query_sql, language="sql")

        if tuid and st.button("View test", key=f"{key_prefix}_view_{tuid}"):
            st.session_state["selected_test"] = tuid
            st.session_state["selected_model"] = None
            st.rerun()


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
    """Render issues from the most recent build invocation."""
    summary = get_latest_build_summary()
    latest_df = get_latest_run_issues()

    st.subheader("Latest Build Issues")

    skipped_count = 0
    invocation_id = None
    if not summary.empty:
        s = summary.iloc[0]
        skipped_count = int(s["SKIPPED_COUNT"] or 0)
        invocation_id = s["INVOCATION_ID"]
        st.caption(
            f"Most recent build · 🟢 {int(s['SUCCESS_COUNT'] or 0)} success · "
            f"🔴 {int(s['FAILED_COUNT'] or 0)} failed · ⚪ {skipped_count} skipped · "
            f"{_format_relative_time(s['CREATED_AT'])}"
        )
    else:
        st.caption("Failures and warnings from the most recent dbt build invocation.")

    if latest_df.empty:
        st.success("Latest build completed without failures or warnings")
        return

    # Blast radius: only run the DAG walk when the build actually skipped models.
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
