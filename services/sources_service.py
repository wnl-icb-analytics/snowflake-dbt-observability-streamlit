"""Source feed freshness from DATA_LAKE.META, content freshness from REPORTING,
and dbt source lineage.

A feed is a data-lake schema (one source database each). New rows = a change
in row count, recorded hourly in ROW_COUNT_HISTORY; a table rewritten with the
same row count has no new rows. SQL returns UTC; timestamps are converted to
SOURCE_TIMEZONE (tz-aware) here. The META and REPORTING readers return None
when the app's role can't read them.
"""

import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException

from config import (
    CONTENT_FRESHNESS_TABLE,
    ELEMENTARY_SCHEMA,
    FEED_CADENCE_DAYS,
    FEED_LATE_MIN_HOURS,
    FEED_LATE_MULTIPLIER,
    FEED_NOT_UPDATING_DAYS,
    FEED_UPDATE_WINDOW_HOURS,
    ROW_COUNT_HISTORY_TABLE,
    SOURCE_FRESHNESS_TABLE,
    SOURCE_TIMEZONE,
)
from database import run_query

FRESH = "Fresh"
LATE = "Late"
NOT_UPDATING = "Not updating"
UNKNOWN = "Unknown"
NOT_TRACKED = "Not tracked"
# Most to least urgent, for sorting and for a source's worst status.
STATUS_ORDER = [LATE, NOT_UPDATING, UNKNOWN, FRESH, NOT_TRACKED]

# Snowflake codes for "does not exist or not authorized" and "insufficient privileges".
_ACCESS_ERROR_CODES = {2003, 3001}


def _read(query: str, params: tuple = ()):
    """run_query, or None when the role can't read an object in the query."""
    try:
        return run_query(query, params)
    except SnowparkSQLException as exc:
        if getattr(exc, "sql_error_code", None) in _ACCESS_ERROR_CODES:
            return None
        raise


def _utc(ltz_expr: str) -> str:
    """TIMESTAMP_LTZ -> UTC TIMESTAMP_NTZ."""
    return f"CONVERT_TIMEZONE('UTC', {ltz_expr})::TIMESTAMP_NTZ"


def _localise(df: pd.DataFrame, *columns) -> pd.DataFrame:
    """UTC-naive columns -> tz-aware SOURCE_TIMEZONE."""
    for col in columns:
        df[col] = pd.to_datetime(df[col]).dt.tz_localize("UTC").dt.tz_convert(SOURCE_TIMEZONE)
    return df


def _cadence_ctes(grain: str) -> str:
    """CTEs keyed by grain_key (a ROW_COUNT_HISTORY column):
    last_change = latest row-count change (UTC);
    cadence = updates and median hours between them in the last FEED_CADENCE_DAYS."""
    return f"""
    history_start AS (
        SELECT MIN(observed_at_utc) AS started_at FROM {ROW_COUNT_HISTORY_TABLE}
    ),
    changes AS (
        -- Row-count changes, timed by LAST_ALTERED. Skips each object's first
        -- capture, which was altered before the history began.
        SELECT DISTINCT h.{grain} AS grain_key, {_utc('h.last_altered')} AS changed_at
        FROM {ROW_COUNT_HISTORY_TABLE} h
        JOIN history_start s ON {_utc('h.last_altered')} > s.started_at
    ),
    last_change AS (
        SELECT grain_key, MAX(changed_at) AS last_row_change
        FROM changes
        GROUP BY grain_key
    ),
    update_starts AS (
        -- One update = a run of changes less than {FEED_UPDATE_WINDOW_HOURS}h apart.
        SELECT grain_key, changed_at
        FROM changes
        WHERE changed_at >= DATEADD(day, -{FEED_CADENCE_DAYS}, SYSDATE())
        QUALIFY LAG(changed_at) OVER (PARTITION BY grain_key ORDER BY changed_at) IS NULL
             OR DATEDIFF(second, LAG(changed_at) OVER (PARTITION BY grain_key ORDER BY changed_at), changed_at)
                > {FEED_UPDATE_WINDOW_HOURS * 3600}
    ),
    gaps AS (
        SELECT
            grain_key,
            DATEDIFF(second, LAG(changed_at) OVER (PARTITION BY grain_key ORDER BY changed_at), changed_at) / 3600 AS gap_hours
        FROM update_starts
    ),
    cadence AS (
        SELECT grain_key, COUNT(*) AS updates, MEDIAN(gap_hours) AS typical_gap_hours
        FROM gaps
        GROUP BY grain_key
    )"""


# --- status ---------------------------------------------------------------------

def late_after_hours(typical_gap_hours):
    """Hours without new rows before a feed or table counts as late."""
    if pd.isna(typical_gap_hours):
        return float("nan")
    return max(FEED_LATE_MULTIPLIER * float(typical_gap_hours), FEED_LATE_MIN_HOURS)


def freshness_status(age_hours, typical_gap_hours, has_tables: bool = True) -> str:
    """Fresh / Late / Not updating / Unknown from hours since the last row-count
    change and the typical gap between updates. Unknown = views only (no row
    counts) or fewer than two updates to learn the gap from."""
    if not has_tables or pd.isna(age_hours):
        return UNKNOWN
    long_quiet = age_hours > FEED_NOT_UPDATING_DAYS * 24
    if pd.isna(typical_gap_hours):
        return NOT_UPDATING if long_quiet else UNKNOWN
    if age_hours <= late_after_hours(typical_gap_hours):
        return FRESH
    return NOT_UPDATING if long_quiet else LATE


def _with_status(df: pd.DataFrame, has_tables) -> pd.DataFrame:
    df = df.copy()
    for col in ("AGE_HOURS", "TYPICAL_GAP_HOURS"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["LATE_AFTER_HOURS"] = df["TYPICAL_GAP_HOURS"].map(late_after_hours)
    df["STATUS"] = [
        freshness_status(age, gap, bool(tables))
        for age, gap, tables in zip(df["AGE_HOURS"], df["TYPICAL_GAP_HOURS"], has_tables)
    ]
    df["AGE_DAYS"] = df["AGE_HOURS"] / 24
    df["TYPICAL_GAP_DAYS"] = df["TYPICAL_GAP_HOURS"] / 24
    df["LATE_AFTER_DAYS"] = df["LATE_AFTER_HOURS"] / 24
    return df


def sort_by_status(df: pd.DataFrame, *then) -> pd.DataFrame:
    rank = df["STATUS"].map({s: i for i, s in enumerate(STATUS_ORDER)})
    return df.assign(_rank=rank).sort_values(["_rank", *then]).drop(columns="_rank")


def worst_status(statuses) -> str:
    present = set(statuses)
    return next((s for s in STATUS_ORDER if s in present), NOT_TRACKED)


# --- META / REPORTING -------------------------------------------------------------

def get_feeds():
    """One row per feed (data-lake schema) with status, or None if unreadable."""
    query = f"""
    WITH {_cadence_ctes('data_lake_schema')},
    feeds AS (
        SELECT
            source_database,
            data_lake_schema,
            COUNT(*) AS objects,
            COUNT_IF(table_type = 'BASE TABLE') AS tables,
            -- A view's LAST_ALTERED is its DDL; use tables when there are any.
            {_utc("COALESCE(MAX(IFF(table_type = 'BASE TABLE', last_altered, NULL)), MAX(last_altered))")} AS newest_altered,
            {_utc("COALESCE(MIN(IFF(table_type = 'BASE TABLE', last_altered, NULL)), MIN(last_altered))")} AS oldest_altered,
            MAX(snapshot_timestamp_utc) AS snapshot_at
        FROM {SOURCE_FRESHNESS_TABLE}
        GROUP BY source_database, data_lake_schema
    )
    SELECT
        f.source_database,
        f.data_lake_schema,
        f.objects,
        f.tables,
        lc.last_row_change,
        f.newest_altered,
        f.oldest_altered,
        -- Feeds missing from the history fall back to their newest LAST_ALTERED.
        DATEDIFF(second, COALESCE(lc.last_row_change, f.newest_altered), SYSDATE()) / 3600 AS age_hours,
        c.updates,
        c.typical_gap_hours,
        f.snapshot_at,
        DATEDIFF(second, f.snapshot_at, SYSDATE()) / 3600 AS snapshot_age_hours
    FROM feeds f
    LEFT JOIN last_change lc ON lc.grain_key = f.data_lake_schema
    LEFT JOIN cadence c ON c.grain_key = f.data_lake_schema
    """
    df = _read(query)
    if df is None:
        return None
    df = _localise(_with_status(df, df["TABLES"] > 0), "LAST_ROW_CHANGE", "NEWEST_ALTERED", "OLDEST_ALTERED", "SNAPSHOT_AT")
    return sort_by_status(df, "SOURCE_DATABASE", "DATA_LAKE_SCHEMA").reset_index(drop=True)


def get_objects():
    """One row per tracked source object with status, or None if unreadable.
    RELATION_KEY = upper-cased DATABASE.SCHEMA.OBJECT without quotes."""
    query = f"""
    WITH {_cadence_ctes('data_lake_object_fqn')}
    SELECT
        o.source_database,
        o.source_schema,
        o.source_object,
        o.data_lake_schema,
        o.data_lake_object_fqn,
        UPPER(REPLACE(o.data_lake_object_fqn, '"', '')) AS relation_key,
        o.table_type,
        o.row_count,
        lc.last_row_change,
        {_utc('o.last_altered')} AS last_altered,
        DATEDIFF(second, COALESCE(lc.last_row_change, {_utc('o.last_altered')}), SYSDATE()) / 3600 AS age_hours,
        c.updates,
        c.typical_gap_hours
    FROM {SOURCE_FRESHNESS_TABLE} o
    LEFT JOIN last_change lc ON lc.grain_key = o.data_lake_object_fqn
    LEFT JOIN cadence c ON c.grain_key = o.data_lake_object_fqn
    """
    df = _read(query)
    if df is None:
        return None
    return _localise(_with_status(df, df["TABLE_TYPE"] == "BASE TABLE"), "LAST_ROW_CHANGE", "LAST_ALTERED")


def get_content_freshness():
    """'Data complete up to' per feed from the SOURCE_CONTENT_FRESHNESS dbt
    model, or None if unreadable. DATA_LAKE_SCHEMA joins to get_feeds()."""
    query = f"""
    SELECT
        source_schema,
        UPPER(SPLIT_PART(source_schema, '.', -1)) AS data_lake_schema,
        COALESCE(consensus_content_date, content_date) AS complete_up_to,
        latest_content_date,
        content_age_days,
        expected_by_date,
        breach_by_date,
        freshness_status,
        COALESCE(consensus_signal_detail, signal_detail) AS signal_detail
    FROM {CONTENT_FRESHNESS_TABLE}
    ORDER BY
        CASE freshness_status WHEN 'breached' THEN 0 WHEN 'past_expected' THEN 1 ELSE 2 END,
        source_schema
    """
    df = _read(query)
    if df is None:
        return None
    df = df.copy()
    for col in ("COMPLETE_UP_TO", "LATEST_CONTENT_DATE", "EXPECTED_BY_DATE", "BREACH_BY_DATE"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# --- dbt sources and lineage ----------------------------------------------------------

def get_dbt_sources():
    """dbt source tables with RELATION_KEY for matching tracked objects."""
    query = f"""
    SELECT
        unique_id,
        source_name,
        name,
        database_name,
        schema_name,
        relation_name,
        UPPER(REPLACE(relation_name, '"', '')) AS relation_key
    FROM {ELEMENTARY_SCHEMA}.dbt_sources
    ORDER BY source_name, name
    """
    return run_query(query)


def get_lineage_edges():
    """(child, parent) node pairs from models and snapshots, with child details."""
    query = f"""
    WITH nodes AS (
        SELECT unique_id, name, schema_name, materialization, depends_on_nodes, 'model' AS kind
        FROM {ELEMENTARY_SCHEMA}.dbt_models
        UNION ALL
        SELECT unique_id, name, schema_name, materialization, depends_on_nodes, 'snapshot' AS kind
        FROM {ELEMENTARY_SCHEMA}.dbt_snapshots
    )
    SELECT
        n.unique_id AS child_id,
        n.name AS child_name,
        n.schema_name AS child_schema,
        n.materialization AS child_materialization,
        n.kind AS child_kind,
        f.value::string AS parent_id
    FROM nodes n,
         LATERAL FLATTEN(input => PARSE_JSON(n.depends_on_nodes)) f
    """
    return run_query(query)


_OBJECT_COLUMNS = [
    "DATA_LAKE_SCHEMA", "DATA_LAKE_OBJECT_FQN", "TABLE_TYPE", "ROW_COUNT", "LAST_ROW_CHANGE",
    "LAST_ALTERED", "AGE_DAYS", "TYPICAL_GAP_DAYS", "LATE_AFTER_DAYS", "STATUS",
]


def get_source_tables(objects):
    """dbt source tables joined to the tracked object each reads (by relation
    name), with STATUS (NOT_TRACKED when unmatched) and MODELS = number of
    models reading the table directly. objects: get_objects() or None."""
    df = get_dbt_sources().copy()
    if objects is not None:
        tracked = objects.sort_values("LAST_ALTERED", ascending=False).drop_duplicates("RELATION_KEY")
        df = df.merge(tracked[["RELATION_KEY", *_OBJECT_COLUMNS]], on="RELATION_KEY", how="left")
    else:
        for col in _OBJECT_COLUMNS:
            df[col] = None
        _localise(df, "LAST_ROW_CHANGE", "LAST_ALTERED")
    df["TRACKED"] = df["DATA_LAKE_OBJECT_FQN"].notna()
    df["STATUS"] = df["STATUS"].where(df["TRACKED"], NOT_TRACKED)

    edges = get_lineage_edges()
    readers = edges[(edges["CHILD_KIND"] == "model") & edges["PARENT_ID"].str.startswith("source.")]
    counts = readers.groupby("PARENT_ID")["CHILD_ID"].nunique()
    df["MODELS"] = df["UNIQUE_ID"].map(counts).fillna(0).astype(int)
    return df


def summarise_sources(tables: pd.DataFrame, feeds) -> pd.DataFrame:
    """One row per dbt source (SOURCE_NAME): table, tracked, late and
    not-updating counts, newest row change, and the worst status of the feeds
    it maps to."""
    feed_status = {} if feeds is None else dict(zip(feeds["DATA_LAKE_SCHEMA"], feeds["STATUS"]))
    rows = []
    for source_name, group in tables.groupby("SOURCE_NAME", sort=True):
        schemas = sorted(group["DATA_LAKE_SCHEMA"].dropna().unique())
        locations = sorted((group["DATABASE_NAME"] + "." + group["SCHEMA_NAME"]).dropna().unique())
        rows.append({
            "SOURCE_NAME": source_name,
            "LOCATION": ", ".join(locations),
            "FEEDS": ", ".join(schemas),
            "TABLES": len(group),
            "TRACKED_TABLES": int(group["TRACKED"].sum()),
            "LATE": int((group["STATUS"] == LATE).sum()),
            "NOT_UPDATING": int((group["STATUS"] == NOT_UPDATING).sum()),
            "LAST_ROW_CHANGE": group["LAST_ROW_CHANGE"].max(),
            "STATUS": worst_status(feed_status.get(s) for s in schemas),
        })
    columns = ["SOURCE_NAME", "LOCATION", "FEEDS", "TABLES", "TRACKED_TABLES", "LATE", "NOT_UPDATING", "LAST_ROW_CHANGE", "STATUS"]
    summary = pd.DataFrame(rows, columns=columns)
    summary["LAST_ROW_CHANGE"] = pd.to_datetime(summary["LAST_ROW_CHANGE"], utc=True).dt.tz_convert(SOURCE_TIMEZONE)
    return summary


def get_direct_readers(source_id: str) -> pd.DataFrame:
    """Models that read the dbt source table directly."""
    edges = get_lineage_edges()
    readers = edges[(edges["PARENT_ID"] == source_id) & (edges["CHILD_KIND"] == "model")]
    return readers.drop_duplicates("CHILD_ID").sort_values("CHILD_NAME").reset_index(drop=True)


def get_upstream_source_ids(unique_id: str) -> dict:
    """{source id: read directly by the node} for every dbt source the node
    reads, directly or through models and snapshots."""
    edges = get_lineage_edges()
    parents = {}
    for child, parent in zip(edges["CHILD_ID"], edges["PARENT_ID"]):
        parents.setdefault(child, []).append(parent)
    direct = {p for p in parents.get(unique_id, []) if p.startswith("source.")}
    seen, stack, sources = {unique_id}, [unique_id], {}
    while stack:
        for parent in parents.get(stack.pop(), []):
            if parent in seen:
                continue
            seen.add(parent)
            if parent.startswith("source."):
                sources[parent] = parent in direct
            else:
                stack.append(parent)
    return sources
