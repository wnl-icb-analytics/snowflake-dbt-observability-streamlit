"""Run detail view - models, tests and timeline of one invocation."""

import altair as alt
import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import format_duration, format_timestamp, is_missing, status_label
from components.issue_cards import render_model_error_card, render_test_issue_card
from config import RUN_SHARE_TOP_N
from page_modules.jobs import job_caption
from services.alerts_service import get_downstream_skips
from services.performance_service import (
    get_run_model_edges,
    longest_chain,
    pack_lanes,
    replay_schedule,
)
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
    st.caption(job_caption(details))
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


_STATUS_SCALE = alt.Scale(
    domain=["success", "fail", "error", "skipped"],
    range=["#28a745", "#dc3545", "#dc3545", "#6c757d"],
)
_CHAIN_COLOR = "#6f42c1"
# Timeline height: 18px per lane, kept between 90px and 380px.
_LANE_PX, _TIMELINE_MIN_PX, _TIMELINE_MAX_PX = 18, 90, 380


def _render_timeline(invocation_id: str, details):
    """Models packed into lanes by start and end time, the longest dependency
    chain and each model's share of the run's model time."""
    df = get_invocation_models(invocation_id)
    df = df[df["UNIQUE_ID"].notna()].drop_duplicates("UNIQUE_ID", keep="last").copy()
    if df.empty:
        ui.empty_state("No model runs in this invocation")
        return
    df["EXECUTION_TIME"] = df["EXECUTION_TIME"].astype(float).fillna(0.0)
    total_time = df["EXECUTION_TIME"].sum()
    edges = list(get_run_model_edges(invocation_id)[["CHILD", "PARENT"]].itertuples(index=False, name=None))

    timed = _recorded_times(df, details)
    recorded = not timed.empty
    if recorded:
        start = dict(zip(timed["UNIQUE_ID"], timed["START_SEC"]))
        end = dict(zip(timed["UNIQUE_ID"], timed["END_SEC"]))
        wall = timed["END_SEC"].max()
        wall_help = "Run start to the last model finishing"
    else:
        start, end = replay_schedule(dict(zip(df["UNIQUE_ID"], df["EXECUTION_TIME"])), edges)
        wall = details.get("DURATION_SECONDS")
        wall = None if is_missing(wall) else float(wall)
        wall_help = "Run start to finish as recorded by dbt, including tests and hooks"
    chain = longest_chain(start, end, edges)

    parallelism = total_time / wall if wall else None
    ui.metric_row([
        ("Total model time", format_duration(total_time) or "0s"),
        ("Wall clock", format_duration(wall) or "N/A", {"help": wall_help}),
        ("Avg parallelism", f"{parallelism:.1f}x" if parallelism else "N/A"),
    ])

    x_max = (wall if recorded else max(end.values(), default=0)) / 60
    if recorded:
        _render_lanes(timed, set(chain), len(df), x_max)
    else:
        st.info(
            "dbt did not record start and end times for the models in this run, so there is no "
            "timeline. The chain below is estimated from dependencies and execution times."
        )
    _render_chain(df, chain, start, end, recorded, wall, x_max)
    _render_run_share(df, total_time, set(chain))


def _recorded_times(df: pd.DataFrame, details) -> pd.DataFrame:
    """Models with recorded start and end, as seconds from the run start."""
    starts = pd.to_datetime(df["EXECUTE_STARTED_AT"], utc=True, errors="coerce", format="ISO8601")
    ends = pd.to_datetime(df["EXECUTE_COMPLETED_AT"], utc=True, errors="coerce", format="ISO8601")
    ok = starts.notna() & ends.notna()
    if not ok.any():
        return df.iloc[0:0]
    timed = df[ok].copy()
    timed["START"] = starts[ok].dt.tz_localize(None)
    timed["END"] = ends[ok].dt.tz_localize(None)
    origin = timed["START"].min()
    run_start = pd.to_datetime(details.get("RUN_STARTED_AT"), utc=True, errors="coerce")
    if not pd.isna(run_start):
        origin = min(origin, run_start.tz_localize(None))
    timed["START_SEC"] = (timed["START"] - origin).dt.total_seconds()
    timed["END_SEC"] = (timed["END"] - origin).dt.total_seconds()
    return timed


def _duration_label(seconds: float) -> str:
    return f"{seconds:.1f}s" if seconds < 60 else format_duration(seconds)


def _bar_tooltip(start_title: str):
    return [
        alt.Tooltip("NAME:N", title="Model"),
        alt.Tooltip("STATUS:N", title="Status"),
        alt.Tooltip("START_LABEL:N", title=start_title),
        alt.Tooltip("DURATION:N", title="Duration"),
    ]


def _render_lanes(timed: pd.DataFrame, chain_ids: set, n_models: int, x_max: float):
    """Lane chart: each bar goes in the first lane free at its start."""
    data = timed[["UNIQUE_ID", "NAME", "STATUS", "START", "START_SEC", "END_SEC", "EXECUTION_TIME"]].copy()
    data["LANE"] = pack_lanes(data["START_SEC"].tolist(), data["END_SEC"].tolist())
    lanes = int(data["LANE"].max()) + 1
    data["LANE_END"] = data["LANE"] + 0.8
    data["LANE_MID"] = data["LANE"] + 0.4
    data["START_MIN"] = data["START_SEC"] / 60
    data["END_MIN"] = data["END_SEC"] / 60
    data["START_LABEL"] = data["START"].dt.strftime("%H:%M:%S")
    data["DURATION"] = data["EXECUTION_TIME"].map(_duration_label)
    on_chain = data["UNIQUE_ID"].isin(chain_ids)
    data = data.drop(columns=["UNIQUE_ID", "START", "START_SEC", "END_SEC"])

    missing = n_models - len(data)
    st.caption(
        f"{len(data):,} models in {lanes:,} lanes (the most models running at once). "
        f"Violet marks the longest chain."
        + (f" {missing:,} models without recorded times are not shown." if missing else "")
    )
    x_scale = alt.Scale(domain=[0, x_max], nice=False)
    y_scale = alt.Scale(domain=[0, lanes], reverse=True, nice=False)
    tooltip = _bar_tooltip("Start (UTC)")
    bars = alt.Chart(data).mark_rect().encode(
        x=alt.X("START_MIN:Q", title="Minutes from run start", scale=x_scale),
        x2="END_MIN:Q",
        y=alt.Y("LANE:Q", axis=None, scale=y_scale),
        y2="LANE_END:Q",
        color=alt.Color("STATUS:N", scale=_STATUS_SCALE, legend=alt.Legend(title=None, orient="bottom")),
        tooltip=tooltip,
    )
    chain_marks = alt.Chart(data[on_chain.values]).mark_rule(
        color=_CHAIN_COLOR, strokeWidth=3, strokeCap="round",
    ).encode(
        x=alt.X("START_MIN:Q", scale=x_scale),
        x2="END_MIN:Q",
        y=alt.Y("LANE_MID:Q", scale=y_scale),
        tooltip=tooltip,
    )
    height = min(max(lanes * _LANE_PX, _TIMELINE_MIN_PX), _TIMELINE_MAX_PX)
    st.altair_chart(alt.layer(bars, chain_marks).properties(height=height))


def _render_chain(df, chain, start, end, recorded: bool, wall, x_max: float):
    """The chain of dependent models that ends the run, as a strip and a table."""
    st.markdown("**Longest chain**")
    if not chain:
        ui.empty_state("No dependency chain found for this run")
        return

    rows = df.set_index("UNIQUE_ID").loc[chain, ["NAME", "STATUS", "EXECUTION_TIME"]].reset_index()
    rows["STEP"] = range(1, len(rows) + 1)
    rows["START_MIN"] = [start[u] / 60 for u in chain]
    rows["END_MIN"] = [end[u] / 60 for u in chain]
    rows["WAITED"] = [float("nan")] + [start[u] - end[p] for p, u in zip(chain, chain[1:])]
    chain_time = rows["EXECUTION_TIME"].sum()
    last_end = end[chain[-1]]

    if recorded:
        st.caption(
            f"{len(chain)} models, ending with the last model to finish. Each step is the parent in this "
            f"run that finished last before the next model started. Model time on the chain: "
            f"{format_duration(chain_time) or '0s'} of the {format_duration(last_end)} to its end; the rest is "
            f"time before and between steps. Dependencies come from the current manifest."
        )
    else:
        share = f", {chain_time / wall * 100:.0f}% of the wall clock" if wall else ""
        st.caption(
            f"Estimated: the path of dependent models in this run with the most execution time end to end "
            f"({len(chain)} models, {format_duration(chain_time) or '0s'}{share}). It leaves out tests, "
            f"snapshots and waiting between models. Dependencies come from the current manifest."
        )

    strip = rows.assign(
        START_LABEL=rows["START_MIN"].map("{:.1f} min".format),
        DURATION=rows["EXECUTION_TIME"].map(_duration_label),
    )
    x_title = "Minutes from run start" if recorded else "Minutes (estimated)"
    chart = alt.Chart(strip).mark_rect(color=_CHAIN_COLOR, stroke="white", strokeWidth=1).encode(
        x=alt.X("START_MIN:Q", title=x_title, scale=alt.Scale(domain=[0, max(x_max, last_end / 60)], nice=False)),
        x2="END_MIN:Q",
        tooltip=_bar_tooltip("Start (from run start)" if recorded else "Start (estimated)"),
    ).properties(height=44)
    st.altair_chart(chart)

    rows["STATUS_LABEL"] = rows["STATUS"].map(status_label)
    columns = {
        "STEP": st.column_config.NumberColumn("#", width="small"),
        "NAME": st.column_config.TextColumn("Model", width="large"),
        "STATUS_LABEL": "Status",
        "START_MIN": st.column_config.NumberColumn("Start" if recorded else "Start (est.)", format="%.1f min"),
        "EXECUTION_TIME": ui.seconds_column("Duration"),
    }
    if recorded:
        columns["WAITED"] = ui.seconds_column(
            "Waited", help="Time between the previous step finishing and this model starting",
        )
    selected = ui.table(rows, key="run_chain_table", columns=columns)
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])


def _render_run_share(df: pd.DataFrame, total_time: float, chain_ids: set):
    """Top models by execution time with their share of the run's model time."""
    st.markdown("**Share of run time**")
    if total_time <= 0:
        ui.empty_state("No model time recorded for this run")
        return
    top = df.nlargest(RUN_SHARE_TOP_N, "EXECUTION_TIME").copy()
    top["SHARE"] = top["EXECUTION_TIME"] / total_time * 100
    top["ON_CHAIN"] = top["UNIQUE_ID"].isin(chain_ids)
    st.caption(
        f"Top {len(top)} of {len(df):,} models by execution time: {top['SHARE'].sum():.0f}% of the "
        f"run's {format_duration(total_time) or '0s'} of model time."
    )
    selected = ui.table(
        top,
        key="run_share_table",
        columns={
            "NAME": st.column_config.TextColumn("Model", width="large"),
            "SCHEMA_NAME": "Schema",
            "EXECUTION_TIME": ui.seconds_column("Duration"),
            "SHARE": st.column_config.ProgressColumn(
                "Share of run time", format="%.1f%%", min_value=0, max_value=float(top["SHARE"].max()),
            ),
            "ON_CHAIN": st.column_config.CheckboxColumn("On chain"),
        },
    )
    if selected is not None:
        nav.open_model(selected["UNIQUE_ID"])
