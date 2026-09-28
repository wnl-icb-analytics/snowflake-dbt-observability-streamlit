"""Growth page - row count trends for table models."""

import json

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import to_datetime
from config import GROWTH_HIGH_PCT, GROWTH_SHRINK_PCT
from services.models_service import get_growth_summary


def _flag(change_pct) -> str:
    if pd.isna(change_pct):
        return ""
    if change_pct > GROWTH_HIGH_PCT:
        return "High growth"
    if change_pct < GROWTH_SHRINK_PCT:
        return "Shrinking"
    return ""


def render():
    days = nav.days()
    ui.page_header(
        "Growth",
        f"Row counts of table models: latest count against the first count in the last {days} days.",
    )

    col_search, col_trend = st.columns([3, 2])
    with col_search:
        search = st.text_input("Search", placeholder="Model name", key="growth_search")
    with col_trend:
        trend = st.selectbox("Trend", ["All", "Growing", "Shrinking"], key="growth_trend")

    df = get_growth_summary(days)
    if df.empty:
        ui.empty_state("No row count data available. Row counts are logged for table models only.")
        return

    df = ui.contains(df, ["MODEL_NAME"], search)
    if trend == "Growing":
        df = df[df["CHANGE_PCT"] > 0].sort_values("CHANGE_PCT", ascending=False)
    elif trend == "Shrinking":
        df = df[df["CHANGE_PCT"] < 0].sort_values("CHANGE_PCT")
    if df.empty:
        ui.empty_state("No models match the filters")
        return

    df = to_datetime(df.copy(), "LAST_RECORDED")
    df["TREND"] = df["TREND"].map(lambda s: json.loads(s) if isinstance(s, str) else s)
    df["FLAG"] = df["CHANGE_PCT"].map(_flag)
    st.caption(f"Flags: high growth = up more than {GROWTH_HIGH_PCT}%, shrinking = down more than {-GROWTH_SHRINK_PCT}%.")
    selected = ui.table(
        df,
        key="growth_table",
        height=600,
        noun="models with row counts",
        columns={
            "MODEL_NAME": "Model",
            "SCHEMA_NAME": "Schema",
            "LATEST_ROW_COUNT": st.column_config.NumberColumn("Rows", format="localized"),
            "ROW_CHANGE": st.column_config.NumberColumn("Change", format="localized"),
            "CHANGE_PCT": st.column_config.NumberColumn("Change %", format="%+.1f%%"),
            "TREND": st.column_config.LineChartColumn(f"Daily rows ({days}d)", width="medium"),
            "FLAG": "Flag",
            "LAST_RECORDED": ui.datetime_column("Last recorded"),
        },
    )
    if selected is not None:
        if pd.notna(selected["UNIQUE_ID"]):
            nav.open_model(selected["UNIQUE_ID"])
        else:
            st.warning(f"{selected['MODEL_NAME']} is no longer in the project.")
