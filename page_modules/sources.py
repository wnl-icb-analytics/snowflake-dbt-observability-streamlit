"""Sources page - is a stale model down to dbt or to its source feed?

Feed freshness from DATA_LAKE.META, content dates from REPORTING, and which
dbt models read each source table. render_upstream_sources() is the model
detail section listing the sources a model reads.
"""

import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import format_hours
from config import (
    CONTENT_FRESHNESS_TABLE,
    FEED_CADENCE_DAYS,
    FEED_LATE_MIN_HOURS,
    FEED_LATE_MULTIPLIER,
    FEED_NOT_UPDATING_DAYS,
    ROW_COUNT_HISTORY_TABLE,
    SNAPSHOT_STALE_HOURS,
    SOURCE_FRESHNESS_TABLE,
)
from services import sources_service as src

STATUS_LABELS = {
    src.LATE: "🟠 Late",
    src.NOT_UPDATING: "⚫ Not updating",
    src.UNKNOWN: "⚪ Unknown",
    src.FRESH: "🟢 Fresh",
    src.NOT_TRACKED: "Not tracked",
}
CONTENT_LABELS = {"current": "🟢 Current", "past_expected": "🟠 Past expected", "breached": "🔴 Breached"}
ALL_STATUSES = "All statuses"

META_UNAVAILABLE = (
    f"The app's role can't read {SOURCE_FRESHNESS_TABLE} or {ROW_COUNT_HISTORY_TABLE}, "
    "so source freshness is not shown."
)
CONTENT_UNAVAILABLE = f"The app's role can't read {CONTENT_FRESHNESS_TABLE}, so content dates are not shown."

SHORT_RULE = (
    f"Late = no new rows for more than {FEED_LATE_MULTIPLIER}x the typical gap between updates "
    f"(at least {FEED_LATE_MIN_HOURS}h). Not updating = no new rows for over {FEED_NOT_UPDATING_DAYS} days "
    "and past the late threshold, if known."
)
STATUS_RULE = (
    f"{SHORT_RULE} Unknown = views only, or fewer than two updates to learn the gap from. "
    "New rows = a row-count change in the hourly snapshot; a rewrite with the same row count does not count. "
    f"Typical gap = median gap between updates over the last {FEED_CADENCE_DAYS} days."
)


def _days_column(label: str, **kwargs):
    return st.column_config.NumberColumn(label, format="%.1f d", **kwargs)


def _static_table(df: pd.DataFrame, *, columns: dict, height="auto"):
    """Read-only table; columns maps source column -> column_config or label."""
    shown, config = ui.display_frame(df, columns)
    st.dataframe(
        shown,
        column_order=list(columns),
        column_config=config,
        hide_index=True,
        width="stretch",
        height=height,
    )


def _labelled(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["STATUS_LABEL"] = df["STATUS"].map(STATUS_LABELS)
    return df


def _with_content(df: pd.DataFrame, content, schemas: pd.Series | None = None) -> pd.DataFrame:
    """Add COMPLETE_UP_TO by data-lake schema (default: the DATA_LAKE_SCHEMA column)."""
    if content is None:
        return df.assign(COMPLETE_UP_TO=pd.NaT)
    dates = content.drop_duplicates("DATA_LAKE_SCHEMA").set_index("DATA_LAKE_SCHEMA")["COMPLETE_UP_TO"]
    schemas = df["DATA_LAKE_SCHEMA"] if schemas is None else schemas
    return df.assign(COMPLETE_UP_TO=schemas.map(dates))


# --- page -------------------------------------------------------------------------

def render():
    ui.page_header(
        "Sources",
        "Source feeds in the data lake: when each last had new rows, how often it normally does, "
        "what date its data is complete up to, and which dbt models read it.",
    )

    feeds = src.get_feeds()
    objects = src.get_objects() if feeds is not None else None
    content = src.get_content_freshness()
    tables = src.get_source_tables(objects)

    if feeds is None:
        st.info(META_UNAVAILABLE)
    else:
        _snapshot_note(feeds)
        ui.metric_row([
            ("Feeds", len(feeds)),
            ("Late", int((feeds["STATUS"] == src.LATE).sum())),
            ("Not updating", int((feeds["STATUS"] == src.NOT_UPDATING).sum())),
            ("dbt source tables tracked", f"{int(tables['TRACKED'].sum())} / {len(tables)}"),
        ])

    tab_feeds, tab_content, tab_dbt = st.tabs(["Feeds", "Content", "dbt sources"])
    with tab_feeds:
        _render_feeds(feeds, objects, tables, content)
    with tab_content:
        _render_content(content)
    with tab_dbt:
        _render_dbt_sources(tables, feeds)


def _snapshot_note(feeds: pd.DataFrame):
    snapshot_at = feeds["SNAPSHOT_AT"].max()
    age = feeds["SNAPSHOT_AGE_HOURS"].min()
    if pd.isna(snapshot_at):
        return
    st.caption(
        f"Snapshot of source objects taken {snapshot_at:%Y-%m-%d %H:%M} London time "
        f"({format_hours(age)} ago); it refreshes hourly."
    )
    if age > SNAPSHOT_STALE_HOURS:
        st.warning(
            f"The snapshot is {format_hours(age)} old (over {SNAPSHOT_STALE_HOURS}h), so recent loads may be "
            "missing and ages overstated."
        )


def _render_feeds(feeds, objects, tables, content):
    if feeds is None:
        ui.empty_state(META_UNAVAILABLE)
        return

    col_search, col_status = st.columns([3, 2])
    with col_search:
        search = st.text_input("Search", placeholder="Feed or source database", key="sources_search")
    with col_status:
        status = st.selectbox(
            "Status", [ALL_STATUSES, src.LATE, src.NOT_UPDATING, src.UNKNOWN, src.FRESH], key="sources_status"
        )
    st.caption(STATUS_RULE)

    df = _labelled(_with_content(feeds, content))
    df = ui.contains(df, ["DATA_LAKE_SCHEMA", "SOURCE_DATABASE"], search)
    if status != ALL_STATUSES:
        df = df[df["STATUS"] == status]
    if df.empty:
        ui.empty_state("No feeds match the filters")
        return

    selected = ui.table(
        df,
        key="feeds_table",
        height=600,
        noun="feeds",
        columns={
            "STATUS_LABEL": "Status",
            "DATA_LAKE_SCHEMA": "Feed",
            "SOURCE_DATABASE": "Source database",
            "OBJECTS": st.column_config.NumberColumn("Objects"),
            "LAST_ROW_CHANGE": ui.datetime_column("Last new rows"),
            "AGE_DAYS": _days_column("Age", help="Time since the last new rows"),
            "TYPICAL_GAP_DAYS": _days_column("Typical gap"),
            "UPDATES": st.column_config.NumberColumn("Updates", help=f"Updates in the last {FEED_CADENCE_DAYS} days"),
            "LATE_AFTER_DAYS": _days_column("Late after"),
            "COMPLETE_UP_TO": st.column_config.DateColumn("Complete up to", help=f"From {CONTENT_FRESHNESS_TABLE}"),
            "NEWEST_ALTERED": ui.datetime_column("Newest altered"),
            "OLDEST_ALTERED": ui.datetime_column("Oldest altered"),
        },
    )
    if selected is not None:
        _render_feed_objects(selected, objects, tables)


def _render_feed_objects(feed, objects, tables):
    st.subheader(f"{feed['DATA_LAKE_SCHEMA']} objects")
    if objects is None:
        ui.empty_state(META_UNAVAILABLE)
        return
    df = objects[objects["DATA_LAKE_SCHEMA"] == feed["DATA_LAKE_SCHEMA"]]
    tracked = tables[tables["TRACKED"]]
    dbt_names = (tracked["SOURCE_NAME"] + "." + tracked["NAME"]).groupby(tracked["DATA_LAKE_OBJECT_FQN"]).agg(", ".join)
    df = _labelled(src.sort_by_status(df, "SOURCE_OBJECT")).assign(DBT_SOURCE=lambda d: d["DATA_LAKE_OBJECT_FQN"].map(dbt_names))
    st.caption(f"{len(df):,} objects in {feed['SOURCE_DATABASE']}; status per object uses the same rule as feeds.")
    _static_table(
        df,
        columns={
            "STATUS_LABEL": "Status",
            "SOURCE_OBJECT": st.column_config.TextColumn("Object", width="large"),
            "TABLE_TYPE": "Type",
            "ROW_COUNT": st.column_config.NumberColumn("Rows", format="localized"),
            "LAST_ROW_CHANGE": ui.datetime_column("Last new rows"),
            "AGE_DAYS": _days_column("Age"),
            "TYPICAL_GAP_DAYS": _days_column("Typical gap"),
            "LAST_ALTERED": ui.datetime_column("Last altered"),
            "DBT_SOURCE": "dbt source",
        },
    )


def _render_content(content):
    if content is None:
        ui.empty_state(CONTENT_UNAVAILABLE)
        return
    st.caption(
        f"From {CONTENT_FRESHNESS_TABLE}: the date each feed's data is complete up to, read from the data itself. "
        "Expected-by and breach-by dates are set per feed in that model."
    )
    df = content.copy()
    df["STATUS_LABEL"] = df["FRESHNESS_STATUS"].map(
        lambda s: CONTENT_LABELS.get(s, str(s).replace("_", " ").capitalize() if pd.notna(s) else "")
    )
    _static_table(
        df,
        columns={
            "STATUS_LABEL": "Status",
            "SOURCE_SCHEMA": "Feed",
            "COMPLETE_UP_TO": st.column_config.DateColumn("Complete up to"),
            "LATEST_CONTENT_DATE": st.column_config.DateColumn("Latest record"),
            "CONTENT_AGE_DAYS": st.column_config.NumberColumn("Age", format="%d d", help="Days since Complete up to"),
            "EXPECTED_BY_DATE": st.column_config.DateColumn("Expected by"),
            "BREACH_BY_DATE": st.column_config.DateColumn("Breach by"),
            "SIGNAL_DETAIL": st.column_config.TextColumn("Signal", width="large"),
        },
    )


def _render_dbt_sources(tables: pd.DataFrame, feeds):
    if feeds is None:
        st.info(META_UNAVAILABLE)
    summary = src.summarise_sources(tables, feeds)
    st.caption(
        f"dbt sources matched to tracked objects by relation name (database.schema.table, case-insensitive): "
        f"{int(tables['TRACKED'].sum())} of {len(tables)} tables. Select a source to list its tables, then a table "
        "to list the models that read it."
    )
    search = st.text_input("Search", placeholder="Source or table", key="dbt_sources_search")
    if search:
        matching = ui.contains(tables, ["SOURCE_NAME", "NAME", "RELATION_NAME"], search)["SOURCE_NAME"].unique()
        summary = summary[summary["SOURCE_NAME"].isin(matching)]

    tracked = _labelled(src.sort_by_status(summary[summary["TRACKED_TABLES"] > 0], "SOURCE_NAME"))
    if tracked.empty:
        ui.empty_state("No tracked dbt sources match" if search else "No dbt sources map to tracked objects")
    else:
        selected = ui.table(
            tracked,
            key="dbt_sources_table",
            noun="tracked sources",
            columns={
                "STATUS_LABEL": st.column_config.TextColumn("Feed status", help="Worst status of the feeds it reads"),
                "SOURCE_NAME": "Source",
                "LOCATION": "Location",
                "TABLES": st.column_config.NumberColumn("Tables"),
                "TRACKED_TABLES": st.column_config.NumberColumn("Tracked"),
                "LATE": st.column_config.NumberColumn("Late tables"),
                "NOT_UPDATING": st.column_config.NumberColumn("Not updating tables"),
                "LAST_ROW_CHANGE": ui.datetime_column("Last new rows"),
            },
        )
        if selected is not None:
            _render_source_tables(tables, selected["SOURCE_NAME"], "tracked")

    st.subheader("Not tracked")
    untracked = summary[summary["TRACKED_TABLES"] < summary["TABLES"]].assign(
        UNTRACKED=lambda d: d["TABLES"] - d["TRACKED_TABLES"]
    )
    st.caption(
        f"dbt sources with tables not in {SOURCE_FRESHNESS_TABLE}, so their freshness is not measured "
        f"({int(untracked['UNTRACKED'].sum())} tables)."
    )
    if untracked.empty:
        ui.empty_state("Every dbt source table is tracked", ok=not search)
        return
    selected = ui.table(
        untracked,
        key="untracked_sources_table",
        noun="sources",
        columns={
            "SOURCE_NAME": "Source",
            "LOCATION": "Location",
            "TABLES": st.column_config.NumberColumn("Tables"),
            "UNTRACKED": st.column_config.NumberColumn("Not tracked"),
        },
    )
    if selected is not None:
        _render_source_tables(tables, selected["SOURCE_NAME"], "untracked")


def _render_source_tables(tables: pd.DataFrame, source_name: str, prefix: str):
    st.markdown(f"**{source_name} tables**")
    df = _labelled(src.sort_by_status(tables[tables["SOURCE_NAME"] == source_name], "NAME"))
    selected = ui.table(
        df,
        key=f"{prefix}_source_tables_{source_name}",
        noun="tables",
        columns={
            "STATUS_LABEL": "Status",
            "NAME": st.column_config.TextColumn("Table", width="large"),
            "ROW_COUNT": st.column_config.NumberColumn("Rows", format="localized"),
            "LAST_ROW_CHANGE": ui.datetime_column("Last new rows"),
            "AGE_DAYS": _days_column("Age"),
            "TYPICAL_GAP_DAYS": _days_column("Typical gap"),
            "LAST_ALTERED": ui.datetime_column("Last altered"),
            "MODELS": st.column_config.NumberColumn("Models", help="Models that read this table directly"),
            "RELATION_NAME": st.column_config.TextColumn("Relation", width="medium"),
        },
    )
    if selected is not None:
        _render_readers(selected)


def _render_readers(source_table):
    label = f"{source_table['SOURCE_NAME']}.{source_table['NAME']}"
    st.markdown(f"**Models reading {label}**")
    readers = src.get_direct_readers(source_table["UNIQUE_ID"])
    if readers.empty:
        ui.empty_state(f"No models read {label} directly")
        return
    selected = ui.table(
        readers,
        key=f"source_readers_{source_table['UNIQUE_ID']}",
        noun="models",
        columns={
            "CHILD_NAME": "Model",
            "CHILD_SCHEMA": "Schema",
            "CHILD_MATERIALIZATION": "Materialization",
        },
    )
    if selected is not None:
        nav.open_model(selected["CHILD_ID"])


# --- model detail section -----------------------------------------------------------

def render_upstream_sources(unique_id: str):
    """dbt sources a model reads (directly or through other models) with their
    freshness, to tell a stale feed from a dbt problem."""
    st.subheader("Upstream sources")
    source_ids = src.get_upstream_source_ids(unique_id)
    if not source_ids:
        ui.empty_state("This model reads no dbt sources, directly or through other models")
        return

    objects = src.get_objects()
    tables = src.get_source_tables(objects)
    df = tables[tables["UNIQUE_ID"].isin(source_ids)].copy()
    df["DIRECT"] = df["UNIQUE_ID"].map(source_ids)
    # Untracked DATA_LAKE sources can still have a content date for their schema.
    in_data_lake = df["DATABASE_NAME"].str.upper() == "DATA_LAKE"
    schemas = df["DATA_LAKE_SCHEMA"].fillna(df["SCHEMA_NAME"].str.upper().where(in_data_lake))
    df = _labelled(_with_content(df, src.get_content_freshness(), schemas))
    df = src.sort_by_status(df, "SOURCE_NAME", "NAME")

    if objects is None:
        st.info(META_UNAVAILABLE)
        df["STATUS_LABEL"] = ""
    counts = df["STATUS"].value_counts()
    summary = ", ".join(
        f"{counts[s]} {s.lower()}" for s in (src.LATE, src.NOT_UPDATING, src.NOT_TRACKED) if counts.get(s)
    )
    st.caption(
        f"{len(df)} source tables read directly or through other models"
        + (f": {summary}." if objects is not None and summary else ".")
        + f" {SHORT_RULE} The Sources page has the full rule."
    )
    _static_table(
        df,
        columns={
            "STATUS_LABEL": "Status",
            "SOURCE_NAME": "Source",
            "NAME": st.column_config.TextColumn("Table", width="medium"),
            "DATA_LAKE_SCHEMA": "Feed",
            "LAST_ROW_CHANGE": ui.datetime_column("Last new rows"),
            "AGE_DAYS": _days_column("Age"),
            "TYPICAL_GAP_DAYS": _days_column("Typical gap"),
            "COMPLETE_UP_TO": st.column_config.DateColumn("Feed complete up to"),
            "LAST_ALTERED": ui.datetime_column("Last altered"),
            "DIRECT": st.column_config.CheckboxColumn("Direct", help="Read by this model itself"),
        },
    )
