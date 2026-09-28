"""Models page - all models with status and runtime, plus slow models."""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import status_label, to_datetime
from config import SLOW_MODEL_MIN_SECONDS
from services.models_service import get_models_summary


def _folders(paths) -> list[str]:
    """Every folder (and parent folder) in the model paths, sorted."""
    folders = set()
    for path in paths:
        if not path:
            continue
        parts = str(path).replace("\\", "/").split("/")[:-1]
        for i in range(1, len(parts) + 1):
            folders.add("/".join(parts[:i]))
    return sorted(folders)


def render():
    days = nav.days()
    ui.page_header("Models", f"Every model in the project with its latest status and runtime over the last {days} days.")

    df = get_models_summary(days=days)
    if df.empty:
        ui.empty_state("No models found")
        return

    df = to_datetime(df.copy(), "LAST_RUN")
    df["MODEL_PATH"] = df["MODEL_PATH"].fillna("").str.replace("\\", "/", regex=False)
    df["STATUS_LABEL"] = df["LATEST_STATUS"].map(status_label)

    col_search, col_folder = st.columns([3, 2])
    with col_search:
        search = st.text_input("Search", placeholder="Model name or path", key="models_search")
    with col_folder:
        folder = st.selectbox("Folder", ["All folders"] + _folders(df["MODEL_PATH"]), key="models_folder")

    filtered = ui.contains(df, ["NAME", "MODEL_PATH", "UNIQUE_ID"], search)
    if folder != "All folders":
        filtered = filtered[filtered["MODEL_PATH"].str.startswith(folder + "/")]

    tab_all, tab_slow = st.tabs(["All models", "Slow models"])
    with tab_all:
        _render_all(filtered, bool(search) or folder != "All folders")
    with tab_slow:
        _render_slow(filtered)


def _render_all(df: pd.DataFrame, filtered: bool):
    if df.empty:
        ui.empty_state("No models match the filters" if filtered else "No models found")
        return
    selected = ui.table(
        df,
        key="models_table",
        height=600,
        noun="models",
        columns={
            "STATUS_LABEL": "Status",
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "MATERIALIZATION": "Materialization",
            "AVG_EXECUTION_TIME": ui.seconds_column("Avg time"),
            "RUN_COUNT": st.column_config.NumberColumn("Runs"),
            "LAST_RUN": ui.datetime_column("Last run"),
            "IS_SLOW": st.column_config.CheckboxColumn("Slow"),
            "MODEL_PATH": st.column_config.TextColumn("Path", width="large"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])


def _render_slow(df: pd.DataFrame):
    st.caption(f"Slow = top 10% by average execution time and at least {SLOW_MODEL_MIN_SECONDS}s.")
    slow_df = df[df["IS_SLOW"] == True].sort_values("AVG_EXECUTION_TIME", ascending=False)  # noqa: E712
    if slow_df.empty:
        ui.empty_state("No slow models", ok=True)
        return
    selected = ui.table(
        slow_df,
        key="slow_models_table",
        height=600,
        noun="slow models",
        columns={
            "STATUS_LABEL": "Status",
            "NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "AVG_EXECUTION_TIME": st.column_config.ProgressColumn(
                "Avg time",
                format="%.0f s",
                min_value=0,
                max_value=float(slow_df["AVG_EXECUTION_TIME"].max()),
            ),
            "RUN_COUNT": st.column_config.NumberColumn("Runs"),
            "LAST_RUN": ui.datetime_column("Last run"),
            "MODEL_PATH": st.column_config.TextColumn("Path", width="large"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
