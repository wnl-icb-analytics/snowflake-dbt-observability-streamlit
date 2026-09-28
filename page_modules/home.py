"""Home page - what is failing, warning, stale, losing rows or skipped now,
then the latest build and recent runs.

Open issues come from the latest result of every node and test in the current
manifest, across all runs (services/health_service.py), so this page owns the
"what is failing now" counts.
"""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import (
    format_duration,
    format_hours,
    format_relative_time,
    format_timestamp,
    format_when,
    is_missing,
    issue_status,
    to_datetime,
    trigger_label,
    truncate,
)
from components.issue_cards import render_model_error_card, render_test_issue_card, where_it_broke
from config import (
    ROW_DROP_PCT,
    STALE_GAP_MULTIPLIER,
    STALE_LOOKBACK_DAYS,
    STALE_MIN_AGE_HOURS,
    STALE_MIN_BUILDS,
)
from page_modules.runs import compile_show_control, runs_table
from services.alerts_service import (
    get_downstream_model_counts,
    get_downstream_skips,
    get_latest_build_summary,
    get_latest_build_test_results,
    get_latest_run_issues,
)
from services.health_service import (
    get_node_issues,
    get_row_count_drops,
    get_stale_outputs,
    get_test_issues,
)
from services.metrics_service import (
    get_last_run_time,
    get_project_totals,
    get_recent_runs,
    get_total_execution_time,
)

FAILING = ("fail", "error")


def _plural(n: int, noun: str) -> str:
    return f"{n:,} {noun}" + ("" if n == 1 else "s")


def _node_phrase(nodes: pd.DataFrame) -> str:
    """'3 models, 1 seed' from a node issues frame."""
    counts = nodes["RESOURCE_TYPE"].value_counts()
    return ", ".join(_plural(int(counts[t]), t) for t in ("model", "seed", "snapshot") if t in counts)


def _downstream_counts(unique_ids) -> dict:
    """unique_id -> number of models that depend on it (transitively)."""
    ids = [str(u) for u in unique_ids if not is_missing(u)]
    if not ids:
        return {}
    df = get_downstream_model_counts(ids)
    return {r["UNIQUE_ID"]: int(r["DOWNSTREAM_COUNT"]) for _, r in df.iterrows()}


def _skipped_since(row) -> str:
    """Note for an issue whose latest result is a skip."""
    if row.get("LATEST_STATUS") == "skipped":
        return f"Skipped in later runs, latest {format_when(row.get('LATEST_AT'))}"
    return ""


# --- banner and metrics -------------------------------------------------------

def _render_banner(failing_nodes, failing_tests, warnings, stale, drops):
    """Red when anything fails; amber for warnings, stale outputs or row-count
    drops only; green otherwise."""
    failing = [p for p in (_node_phrase(failing_nodes) if len(failing_nodes) else "",
                           _plural(len(failing_tests), "test") if len(failing_tests) else "") if p]
    amber = [p for p in (
        _plural(warnings, "warning") if warnings else "",
        _plural(stale, "stale output") if stale else "",
        _plural(drops, "row-count drop") if drops else "",
    ) if p]
    if failing:
        text = f"**Failing: {' and '.join(failing)}**" + (f" · {' · '.join(amber)}" if amber else "")
        st.error(text, icon=":material/error:")
    elif amber:
        st.warning(f"**Nothing failing** · {' · '.join(amber)}", icon=":material/warning:")
    else:
        st.success("**Healthy** · nothing failing, warning, stale or dropping rows", icon=":material/check_circle:")


# --- open issue sections ------------------------------------------------------

def _render_failing(failing_nodes: pd.DataFrame, failing_tests: pd.DataFrame, days: int):
    st.subheader(f"Failing ({len(failing_nodes) + len(failing_tests):,})")
    st.caption(
        "Models, seeds, snapshots and tests whose latest non-skipped result is fail or error, "
        "across all runs. A later skip does not clear a failure. "
        "Broke = the first failing run of the current streak."
    )
    if failing_nodes.empty and failing_tests.empty:
        ui.empty_state("Nothing failing", ok=True)
        return

    if not failing_nodes.empty:
        impact = _downstream_counts(failing_nodes["UNIQUE_ID"])
        for _, row in failing_nodes.iterrows():
            uid = str(row["UNIQUE_ID"])
            meta = " · ".join(p for p in [
                row["RESOURCE_TYPE"],
                issue_status(row["STATUS"]),
                f"{int(row['FAILURES_IN_RANGE'] or 0)} failures in {days}d",
                f"last failed {format_relative_time(row['LAST_REAL_AT'])}",
                _skipped_since(row),
            ] if p)
            render_model_error_card(
                object_name=row["NAME"],
                message=row.get("MESSAGE"),
                unique_id=uid,
                meta_line=meta,
                key_prefix="open",
                downstream_total=impact.get(uid),
                broke=where_it_broke(row),
                broke_invocation_id=row.get("STREAK_INVOCATION_ID"),
                resource_type=row["RESOURCE_TYPE"],
            )

    for _, row in failing_tests.iterrows():
        note = " · ".join(p for p in [
            f"{int(row['FAILURES_IN_RANGE'] or 0)} failures in {days}d",
            _skipped_since(row),
        ] if p)
        render_test_issue_card(
            row,
            key_prefix="open_test",
            note=note,
            broke=where_it_broke(row),
            broke_invocation_id=row.get("STREAK_INVOCATION_ID"),
        )


def _issue_table(nodes: pd.DataFrame, tests: pd.DataFrame) -> pd.DataFrame:
    """Nodes and tests in one frame for a selectable table."""
    frames = []
    if not tests.empty:
        frames.append(pd.DataFrame({
            "KIND": "test",
            "NAME": tests["TEST_NAME"],
            "MODEL": tests["TABLE_NAME"],
            "ID": tests["TEST_UNIQUE_ID"],
            "FAILING_ROWS": tests["FAILURES"],
            "SINCE": tests["STREAK_STARTED_AT"],
            "LATEST_AT": tests["LATEST_AT"],
            "LAST_REAL_AT": tests["LAST_REAL_AT"],
        }))
    if not nodes.empty:
        frames.append(pd.DataFrame({
            "KIND": nodes["RESOURCE_TYPE"],
            "NAME": nodes["NAME"],
            "MODEL": "",
            "ID": nodes["UNIQUE_ID"],
            "FAILING_ROWS": None,
            "SINCE": nodes["STREAK_STARTED_AT"],
            "LATEST_AT": nodes["LATEST_AT"],
            "LAST_REAL_AT": nodes["LAST_REAL_AT"],
        }))
    df = pd.concat(frames, ignore_index=True)
    return to_datetime(df, "SINCE", "LATEST_AT", "LAST_REAL_AT")


def _open_selected(selected):
    if selected is None:
        return
    if selected["KIND"] == "test":
        nav.open_test(selected["ID"])
    else:
        nav.open_model(selected["ID"])


def _render_warnings(warn_nodes: pd.DataFrame, warn_tests: pd.DataFrame):
    total = len(warn_nodes) + len(warn_tests)
    st.subheader(f"Warnings ({total:,})")
    st.caption(
        "Tests whose latest non-skipped result is warn: the check found rows, but its severity "
        "is warn so the build carried on. Models appear here when they built with warnings. "
        "Since = first warn after the last pass."
    )
    if total == 0:
        ui.empty_state("No warnings", ok=True)
        return
    df = _issue_table(warn_nodes, warn_tests)
    _open_selected(ui.table(
        df,
        key="home_warnings_table",
        noun="warnings",
        columns={
            "NAME": st.column_config.TextColumn("Name", width="large"),
            "KIND": "Type",
            "MODEL": "Model",
            "FAILING_ROWS": st.column_config.NumberColumn("Rows found", format="localized"),
            "SINCE": ui.datetime_column("Warning since"),
            "LAST_REAL_AT": ui.datetime_column("Last warned"),
        },
    ))


def _render_stale(stale: pd.DataFrame):
    st.subheader(f"Stale outputs ({len(stale):,})")
    st.caption(
        f"Tables, incremental models, snapshots and seeds whose last successful build is older than "
        f"{STALE_GAP_MULTIPLIER}x their typical gap between scheduled builds, and at least "
        f"{STALE_MIN_AGE_HOURS}h old. Typical gap = median gap between days with a scheduled build in the "
        f"{STALE_LOOKBACK_DAYS} days before the last success; needs {STALE_MIN_BUILDS} such days. "
        "Views are not checked."
    )
    if stale.empty:
        ui.empty_state("No stale outputs", ok=True)
        return
    df = to_datetime(stale.copy(), "LAST_SUCCESS_AT")
    impact = _downstream_counts(df["UNIQUE_ID"])
    df["DOWNSTREAM"] = df["UNIQUE_ID"].map(lambda u: impact.get(u, 0))
    df["TYPICAL_GAP"] = df["TYPICAL_GAP_HOURS"].map(format_hours)
    df["AGE"] = df["HOURS_SINCE_SUCCESS"].map(format_hours)
    selected = ui.table(
        df,
        key="home_stale_table",
        noun="stale outputs",
        columns={
            "NAME": st.column_config.TextColumn("Name", width="large"),
            "MATERIALIZATION": "Type",
            "LAST_SUCCESS_AT": ui.datetime_column("Last success"),
            "AGE": "Age",
            "TYPICAL_GAP": "Typical gap",
            "DOWNSTREAM": st.column_config.NumberColumn("Downstream models", help="Models that depend on this output"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])


def _render_drops(drops: pd.DataFrame):
    st.subheader(f"Row-count drops ({len(drops):,})")
    st.caption(
        f"Models whose latest logged run has 0 rows after a non-zero run, or more than {ROW_DROP_PCT}% "
        "fewer rows than the previous run. Row counts come from ROW_COUNT_LOG (table models)."
    )
    if drops.empty:
        ui.empty_state("No row-count drops", ok=True)
        return
    df = to_datetime(drops.copy(), "RUN_STARTED_AT")
    df["TRIGGER"] = df["CAUSE_CATEGORY"].map(trigger_label)
    selected = ui.table(
        df,
        key="home_drops_table",
        noun="row-count drops",
        columns={
            "NAME": st.column_config.TextColumn("Model", width="large"),
            "PREVIOUS_ROW_COUNT": st.column_config.NumberColumn("Previous rows", format="localized"),
            "ROW_COUNT": st.column_config.NumberColumn("Rows", format="localized"),
            "CHANGE_PCT": st.column_config.NumberColumn("Change", format="%+.1f%%"),
            "RUN_STARTED_AT": ui.datetime_column("Run started"),
            "TRIGGER": "Trigger",
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])


def _render_skipped(skipped_nodes: pd.DataFrame, skipped_tests: pd.DataFrame):
    total = len(skipped_nodes) + len(skipped_tests)
    label = f"Skipped ({_plural(len(skipped_tests), 'test')}, {_plural(len(skipped_nodes), 'model')})"
    with st.expander(label, expanded=False):
        st.caption(
            "Latest result is skipped, usually because something upstream failed, and the last real "
            "result passed. These have not checked or built anything since. Failures followed by a "
            "skip are listed under Failing."
        )
        if total == 0:
            ui.empty_state("Nothing skipped", ok=True)
            return
        df = _issue_table(skipped_nodes, skipped_tests)
        _open_selected(ui.table(
            df,
            key="home_skipped_table",
            noun="skipped",
            columns={
                "NAME": st.column_config.TextColumn("Name", width="large"),
                "KIND": "Type",
                "MODEL": "Model",
                "LATEST_AT": ui.datetime_column("Skipped"),
                "LAST_REAL_AT": ui.datetime_column("Last ran"),
            },
        ))


# --- latest build ---------------------------------------------------------------

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
            f"Started {format_when(s['RUN_STARTED_AT'])}",
            f"models, seeds and snapshots: {int(s['SUCCESS_COUNT'] or 0)} ok, "
            f"{int(s['FAILED_COUNT'] or 0)} failed, {skipped_count} skipped",
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
        st.markdown("**Build failures**")
        for _, row in model_df.iterrows():
            uid = row.get("UNIQUE_ID")
            uid = str(uid) if pd.notna(uid) else None
            meta = f"{row['RESOURCE_TYPE']} · {issue_status(row['CURRENT_STATUS'])} · {format_timestamp(row['EVENT_AT'])}"
            render_model_error_card(
                object_name=row["OBJECT_NAME"],
                message=row.get("SUMMARY"),
                unique_id=uid,
                meta_line=meta,
                key_prefix="latest",
                downstream_skipped=skips_map.get(uid),
                resource_type=row["RESOURCE_TYPE"],
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
        f"{total_models:,} models and {total_tests:,} tests in the project · runtime and failure counts over the last {days} days",
    )

    nodes = get_node_issues(days)
    tests = get_test_issues(days)
    stale = get_stale_outputs()
    drops = get_row_count_drops()

    failing_nodes = nodes[nodes["STATUS"].isin(FAILING)]
    failing_tests = tests[tests["STATUS"].isin(FAILING)]
    warn_nodes = nodes[nodes["STATUS"] == "warn"]
    warn_tests = tests[tests["STATUS"] == "warn"]
    skipped_nodes = nodes[nodes["STATUS"] == "skipped"]
    skipped_tests = tests[tests["STATUS"] == "skipped"]
    warnings = len(warn_nodes) + len(warn_tests)

    _render_banner(failing_nodes, failing_tests, warnings, len(stale), len(drops))

    exec_time_df = get_total_execution_time(days=days)
    total_exec_time = exec_time_df.iloc[0]["TOTAL_TIME"] if not exec_time_df.empty else 0
    last_run = get_last_run_time()
    last_run_time = last_run.iloc[0]["LAST_RUN_TIME"] if not last_run.empty else None

    ui.metric_row([
        ("Failing models", len(failing_nodes), {"help": "Models, seeds and snapshots whose latest non-skipped result is an error"}),
        ("Failing tests", len(failing_tests), {"help": "Tests whose latest non-skipped result is fail or error"}),
        ("Warnings", warnings, {"help": "Tests (and models) whose latest non-skipped result is warn"}),
        ("Skipped", len(skipped_nodes) + len(skipped_tests), {"help": "Latest result skipped; the last real result passed"}),
        (f"Runtime ({days}d)", format_duration(total_exec_time) or "N/A"),
        ("Last run", format_relative_time(last_run_time), {"help": f"Latest result: {format_timestamp(last_run_time)}"}),
    ])

    # What is wrong now, most urgent first; then the latest build and history.
    _render_failing(failing_nodes, failing_tests, days)
    _render_warnings(warn_nodes, warn_tests)
    _render_stale(stale)
    _render_drops(drops)
    _render_skipped(skipped_nodes, skipped_tests)
    _render_latest_build()
    _render_recent_runs()
