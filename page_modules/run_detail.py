"""Run detail view - models, tests and timeline of one invocation."""

import altair as alt
import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import format_duration, format_timestamp, status_label
from components.issue_cards import render_model_error_card, render_test_issue_card
from services.alerts_service import get_downstream_skips
from services.runs_service import (
    get_invocation_details,
    get_invocation_models,
    get_invocation_tests,
)


def render(invocation_id: str):
    details_df = get_invocation_details(invocation_id)
    if details_df.empty:
        st.error(f"Run not found: {invocation_id}")
        return

    details = details_df.iloc[0]
    ui.page_header(f"Run {format_timestamp(details['CREATED_AT'])}", f"`{invocation_id}`")

    meta = [
        details.get("COMMAND") or "dbt",
        f"target {details['TARGET_NAME']}" if details.get("TARGET_NAME") else "",
        f"warehouse {details['WAREHOUSE']}" if details.get("WAREHOUSE") else "",
        format_duration(details.get("DURATION_SECONDS") or 0),
        f"dbt {details['DBT_VERSION']}" if details.get("DBT_VERSION") else "",
    ]
    st.caption(" · ".join(p for p in meta if p))
    if details.get("SELECTED"):
        st.code(details["SELECTED"], language=None)

    tab_models, tab_tests, tab_timeline = st.tabs(["Models", "Tests", "Timeline"])
    with tab_models:
        _render_models(invocation_id)
    with tab_tests:
        _render_tests(invocation_id)
    with tab_timeline:
        _render_timeline(invocation_id, details)


def _render_models(invocation_id: str):
    """Failures as rich cards, then every model in the run."""
    df = get_invocation_models(invocation_id)
    if df.empty:
        ui.empty_state("No model runs in this invocation")
        return

    fail_df = df[df["STATUS"].isin(["fail", "error"])]
    skipped_df = df[df["STATUS"] == "skipped"]
    ui.metric_row([
        ("Models", len(df)),
        ("Success", int((df["STATUS"] == "success").sum())),
        ("Failed", len(fail_df)),
        ("Skipped", len(skipped_df)),
    ])

    # Blast radius per failure, when this run skipped models downstream.
    skips_map = {}
    if not skipped_df.empty and not fail_df.empty:
        ds = get_downstream_skips(invocation_id)
        skips_map = {r["UNIQUE_ID"]: int(r["DOWNSTREAM_SKIPPED"]) for _, r in ds.iterrows()}

    if fail_df.empty:
        ui.empty_state("No model failures in this run", ok=True)
    else:
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

    st.markdown("**All models**")
    table_df = df.copy()
    table_df["STATUS_LABEL"] = table_df["STATUS"].map(status_label)
    selected = ui.table(
        table_df,
        key="run_models_table",
        noun="models",
        columns={
            "STATUS_LABEL": "Status",
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "EXECUTION_TIME": ui.seconds_column("Duration"),
            "MODEL_PATH": st.column_config.TextColumn("Path", width="large"),
        },
    )
    if selected is not None and pd.notna(selected["UNIQUE_ID"]):
        nav.open_model(selected["UNIQUE_ID"])


def _render_tests(invocation_id: str):
    """Failures/warnings as rich cards, then every test in the run."""
    df = get_invocation_tests(invocation_id)
    if df.empty:
        ui.empty_state("No test runs in this invocation")
        return

    issue_df = df[df["STATUS"].isin(["fail", "error", "warn"])]
    ui.metric_row([
        ("Tests", len(df)),
        ("Passed", int((df["STATUS"] == "pass").sum())),
        ("Failed", int(df["STATUS"].isin(["fail", "error"]).sum())),
        ("Warned", int((df["STATUS"] == "warn").sum())),
    ])

    if issue_df.empty:
        ui.empty_state("All tests passed in this run", ok=True)
    else:
        st.markdown("**Failures and warnings**")
        for _, row in issue_df.iterrows():
            render_test_issue_card(row, key_prefix="inv_test")

    st.markdown("**All tests**")
    table_df = df.copy()
    table_df["STATUS_LABEL"] = table_df["STATUS"].map(status_label)
    selected = ui.table(
        table_df,
        key="run_tests_table",
        noun="tests",
        columns={
            "STATUS_LABEL": "Status",
            "TEST_NAME": st.column_config.TextColumn("Test", width="large"),
            "TABLE_NAME": "Model",
            "COLUMN_NAME": "Column",
            "TEST_NAMESPACE": "Type",
        },
    )
    if selected is not None and pd.notna(selected["TEST_UNIQUE_ID"]):
        nav.open_test(selected["TEST_UNIQUE_ID"])


def _render_timeline(invocation_id: str, details):
    """Gantt chart of model execution."""
    df = get_invocation_models(invocation_id)
    timing_df = df[df["EXECUTE_STARTED_AT"].notna() & df["EXECUTE_COMPLETED_AT"].notna()].copy()
    if timing_df.empty:
        ui.empty_state("No execution timing data available for this run")
        return

    # Drop timezones to avoid tz-naive/tz-aware mismatches.
    timing_df["START"] = pd.to_datetime(timing_df["EXECUTE_STARTED_AT"]).dt.tz_localize(None)
    timing_df["END"] = pd.to_datetime(timing_df["EXECUTE_COMPLETED_AT"]).dt.tz_localize(None)

    run_start = pd.to_datetime(details.get("RUN_STARTED_AT"))
    if not pd.isna(run_start) and run_start.tzinfo is not None:
        run_start = run_start.tz_localize(None)
    if pd.isna(run_start):
        run_start = timing_df["START"].min()

    timing_df["START_SEC"] = (timing_df["START"] - run_start).dt.total_seconds()
    timing_df["END_SEC"] = (timing_df["END"] - run_start).dt.total_seconds()

    st.caption(f"{len(timing_df)} models with timing data")
    chart = alt.Chart(timing_df).mark_bar().encode(
        x=alt.X("START_SEC:Q", title="Seconds from run start"),
        x2=alt.X2("END_SEC:Q"),
        y=alt.Y("NAME:N", title="Model", sort=alt.EncodingSortField(field="START_SEC", order="ascending")),
        color=alt.Color(
            "STATUS:N",
            scale=alt.Scale(
                domain=["success", "fail", "error", "skipped"],
                range=["#28a745", "#dc3545", "#dc3545", "#6c757d"],
            ),
            legend=alt.Legend(title="Status"),
        ),
        tooltip=[
            alt.Tooltip("NAME:N", title="Model"),
            alt.Tooltip("STATUS:N", title="Status"),
            alt.Tooltip("EXECUTION_TIME:Q", title="Duration (s)", format=".1f"),
            alt.Tooltip("START_SEC:Q", title="Start (s)", format=".1f"),
            alt.Tooltip("END_SEC:Q", title="End (s)", format=".1f"),
        ],
    ).properties(height=max(200, len(timing_df) * 20))
    st.altair_chart(chart)

    total_time = timing_df["EXECUTION_TIME"].sum()
    max_end = timing_df["END_SEC"].max()
    parallelism = total_time / max_end if max_end > 0 else 1
    ui.metric_row([
        ("Total model time", format_duration(total_time)),
        ("Wall clock time", format_duration(max_end)),
        ("Avg parallelism", f"{parallelism:.1f}x"),
    ])
