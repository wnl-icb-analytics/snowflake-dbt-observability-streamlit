"""Performance page - model runtime and the slowest models."""

import json

import streamlit as st

from components import nav, ui
from components.charts import top_models_bar_chart
from components.formatting import format_duration
from services.performance_service import get_model_runtimes, get_runtime_summary


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

    df = get_model_runtimes(days)
    if df.empty:
        ui.empty_state("No model runs found")
        return

    st.subheader("Top 15 by total time")
    st.altair_chart(top_models_bar_chart(df.head(15)))

    st.subheader("All models")
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
            "AVG_TIME": ui.seconds_column("Avg"),
            "MAX_TIME": ui.seconds_column("Max"),
            "RUN_COUNT": st.column_config.NumberColumn("Runs"),
            "TREND": st.column_config.LineChartColumn(f"Daily avg time ({days}d)", width="medium"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
