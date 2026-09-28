"""Performance page - slowdowns, model runtime and share of total model time."""

import json

import streamlit as st

from components import nav, ui
from components.charts import top_models_bar_chart
from components.formatting import format_duration, format_timestamp, to_datetime
from config import (
    SLOWDOWN_BASELINE_DAYS,
    SLOWDOWN_MIN_EXTRA_SECONDS,
    SLOWDOWN_MIN_PRIOR_RUNS,
    SLOWDOWN_RATIO,
)
from services.performance_service import get_model_runtimes, get_runtime_summary, get_slowdowns


def render():
    days = nav.days()
    ui.page_header("Performance", f"Model execution time from successful runs in the last {days} days.")

    summary = get_runtime_summary(days)
    if not summary.empty:
        row = summary.iloc[0]
        ui.metric_row([
            ("Total model time", format_duration(row["TOTAL_EXECUTION_TIME"] or 0) or "N/A"),
            ("Model runs", f"{int(row['TOTAL_RUNS'] or 0):,}"),
            ("Models run", f"{int(row['MODELS_RUN'] or 0):,}"),
            ("Avg time per run", f"{row['AVG_EXECUTION_TIME'] or 0:.1f}s"),
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


def _render_slowdowns(days: int):
    """Models whose latest run took much longer than usual."""
    st.subheader("Slowdowns")
    st.caption(
        f"Models whose latest successful run (last {days} days) took at least {SLOWDOWN_RATIO:g}x the median "
        f"of their successful runs in the {SLOWDOWN_BASELINE_DAYS} days before it and at least "
        f"{SLOWDOWN_MIN_EXTRA_SECONDS}s longer. Needs {SLOWDOWN_MIN_PRIOR_RUNS} prior runs."
    )
    df = get_slowdowns(days)
    if df.empty:
        ui.empty_state("No slowdowns: every model's latest run is in line with its median", ok=True)
        return

    df = to_datetime(df.copy(), "GENERATED_AT")
    per_run = (
        df.groupby("INVOCATION_ID")
        .agg(N=("UNIQUE_ID", "size"), RAN_AT=("GENERATED_AT", "min"))
        .sort_values("N", ascending=False)
    )
    if len(per_run) > 1:
        st.caption("By run: " + " · ".join(
            f"{int(r.N)} in the run at {format_timestamp(r.RAN_AT)}" for r in per_run.head(5).itertuples()
        ))
    selected = ui.table(
        df,
        key="slowdowns_table",
        height=min(400, 38 + 35 * len(df)),
        noun="slowdowns",
        columns={
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
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
