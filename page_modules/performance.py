"""Performance page - slowdowns, model runtime and share of total model time."""

import json

import streamlit as st

from components import nav, ui
from components.charts import top_models_bar_chart
from components.formatting import format_duration, format_timestamp, is_missing, to_datetime
from config import (
    SLOWDOWN_BASELINE_DAYS,
    SLOWDOWN_MIN_EXTRA_SECONDS,
    SLOWDOWN_MIN_PRIOR_RUNS,
    SLOWDOWN_RATIO,
    SLOWDOWN_RUN_MIN_FLAGGED,
)
from services.performance_service import get_model_runtimes, get_runtime_summary, get_slowdowns


def render():
    days = nav.days()
    ui.page_header("Performance", f"Model execution time from successful runs in the last {days} days.")

    summary = get_runtime_summary(days)
    if not summary.empty:
        row = summary.iloc[0]
        # SUM and AVG are NULL (NaN) when the range has no successful runs.
        avg = row["AVG_EXECUTION_TIME"]
        ui.metric_row([
            ("Total model time", format_duration(row["TOTAL_EXECUTION_TIME"]) or "N/A"),
            ("Model runs", f"{int(row['TOTAL_RUNS']):,}"),
            ("Models run", f"{int(row['MODELS_RUN']):,}"),
            ("Avg time per run", "N/A" if is_missing(avg) else f"{avg:.1f}s"),
        ])

    _render_slowdowns(days)

    df = get_model_runtimes(days)
    if df.empty:
        ui.empty_state("No model runs found")
        return

    st.subheader("Top 15 by total time")
    st.altair_chart(top_models_bar_chart(df.head(15)))

    st.subheader("All models")
    st.caption("Share of time: the model's total time as a percentage of all model time in the range.")
    df = df.copy()
    df["SHARE"] = df["TOTAL_TIME"] / df["TOTAL_TIME"].sum() * 100
    search = st.text_input("Search", placeholder="Model name", key="perf_search")
    table_df = ui.contains(df, ["NAME", "UNIQUE_ID"], search)
    if table_df.empty:
        ui.empty_state("No models match the search")
        return

    table_df = table_df.copy()
    table_df["TREND"] = table_df["TREND"].map(lambda s: json.loads(s) if isinstance(s, str) else s)
    selected = ui.table(
        table_df,
        key="perf_table",
        height=600,
        noun="models",
        columns={
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "TOTAL_TIME": ui.seconds_column("Total"),
            "SHARE": st.column_config.ProgressColumn(
                "Share of time", format="%.1f%%", min_value=0, max_value=float(df["SHARE"].max()),
            ),
            "AVG_TIME": ui.seconds_column("Avg"),
            "MAX_TIME": ui.seconds_column("Max"),
            "RUN_COUNT": st.column_config.NumberColumn("Runs"),
            "TREND": st.column_config.LineChartColumn(f"Daily avg time ({days}d)", width="medium"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])


def _job_label(command: str, selected: str, limit: int = 40) -> str:
    """Short 'command selector' label, e.g. 'build source:sdl_wnl+'."""
    text = f"{command} {selected}".strip() if selected else f"{command} (all)"
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _render_slowdowns(days: int):
    """Models whose latest run took much longer than usual for their job. Runs
    with many flagged models get one callout instead of a row per model."""
    st.subheader("Slowdowns")
    st.caption(
        f"Models whose latest successful run (last {days} days) took at least {SLOWDOWN_RATIO:g}x the median "
        f"of their prior runs from the same job (command and selector) in the {SLOWDOWN_BASELINE_DAYS} days "
        f"before it, and at least {SLOWDOWN_MIN_EXTRA_SECONDS}s longer. Needs {SLOWDOWN_MIN_PRIOR_RUNS} "
        f"prior runs from that job."
    )
    df = get_slowdowns(days)
    if df.empty:
        ui.empty_state("No slowdowns: every model's latest run is in line with its job's median", ok=True)
        return

    df = to_datetime(df.copy(), "GENERATED_AT")
    df["JOB"] = [_job_label(c, s) for c, s in zip(df["COMMAND"], df["SELECTED"])]
    runs = df.groupby("INVOCATION_ID").agg(
        FLAGGED=("UNIQUE_ID", "size"),
        RUN_MODELS=("RUN_MODELS", "first"),
        RATIO=("RATIO", "median"),
        RAN_AT=("GENERATED_AT", "min"),
        JOB=("JOB", "first"),
    )
    run_level = runs[runs["FLAGGED"] >= SLOWDOWN_RUN_MIN_FLAGGED].sort_values("RAN_AT", ascending=False)
    for invocation_id, run in run_level.iterrows():
        with st.container(border=True):
            text, action = st.columns([5, 1], vertical_alignment="center")
            text.markdown(
                f"**Run at {format_timestamp(run['RAN_AT'])}** ({run['JOB']}): {int(run['FLAGGED'])} of its "
                f"{int(run['RUN_MODELS']):,} models took at least {SLOWDOWN_RATIO:g}x their usual time, "
                f"a median {run['RATIO']:.1f}x."
            )
            if action.button("Open run", key=f"slowdown_run_{invocation_id}", icon=":material/history:"):
                nav.open_run(invocation_id)

    table_df = df
    if not run_level.empty:
        show = st.toggle(
            f"Include models from {'this run' if len(run_level) == 1 else 'these runs'}",
            key="slowdowns_include_runs",
            help=f"Runs with {SLOWDOWN_RUN_MIN_FLAGGED} or more flagged models are summarised above",
        )
        if not show:
            table_df = df[~df["INVOCATION_ID"].isin(run_level.index)]
    if table_df.empty:
        ui.empty_state("No other slowdowns", ok=True)
        return

    selected = ui.table(
        table_df,
        key="slowdowns_table",
        height=min(400, 38 + 35 * len(table_df)),
        noun="slowdowns",
        columns={
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "JOB": st.column_config.TextColumn(
                "Job", help="Command and selector of the latest run; the median uses prior runs of this job",
            ),
            "LATEST_TIME": ui.seconds_column("Latest"),
            "MEDIAN_TIME": ui.seconds_column("Median"),
            "RATIO": st.column_config.NumberColumn("Ratio", format="%.1fx"),
            "EXTRA_TIME": st.column_config.NumberColumn("Extra", format="+%.0f s"),
            "PRIOR_RUNS": st.column_config.NumberColumn("Prior runs"),
            "GENERATED_AT": ui.datetime_column("Ran at"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
