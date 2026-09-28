"""Model run queries."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS, SLOW_MODEL_MIN_SECONDS


def get_models_summary(days: int = DEFAULT_LOOKBACK_DAYS):
    """All models in dbt_models with latest status, avg execution time and a
    slow flag (top 10% by avg time and at least SLOW_MODEL_MIN_SECONDS)."""
    query = f"""
    WITH run_stats AS (
        SELECT
            unique_id,
            status,
            execution_time,
            generated_at,
            ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) as rn,
            AVG(execution_time) OVER (PARTITION BY unique_id) as avg_execution_time,
            COUNT(*) OVER (PARTITION BY unique_id) as run_count
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE generated_at >= DATEADD(day, -{days}, SYSDATE())
        AND resource_type = 'model'
    ),
    latest_runs AS (
        SELECT * FROM run_stats WHERE rn = 1
    ),
    percentiles AS (
        SELECT COALESCE(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY avg_execution_time), 999999) as p90
        FROM latest_runs
    )
    SELECT
        m.unique_id,
        m.name,
        m.schema_name,
        m.database_name,
        m.materialization,
        COALESCE(m.original_path, m.path) as model_path,
        COALESCE(r.status, 'no_runs') as latest_status,
        r.generated_at as last_run,
        r.avg_execution_time,
        COALESCE(r.run_count, 0) as run_count,
        CASE WHEN r.avg_execution_time > p.p90 AND r.avg_execution_time >= {SLOW_MODEL_MIN_SECONDS} THEN TRUE ELSE FALSE END as is_slow
    FROM {ELEMENTARY_SCHEMA}.dbt_models m
    LEFT JOIN latest_runs r ON m.unique_id = r.unique_id
    CROSS JOIN percentiles p
    ORDER BY
        CASE WHEN r.status IN ('fail', 'error') THEN 0 ELSE 1 END,
        r.avg_execution_time DESC NULLS LAST,
        m.name
    """
    return run_query(query)


def get_model_run_history(unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Run history for a model with error messages and the row count logged
    by the same invocation."""
    query = f"""
    WITH runs AS (
        SELECT
            invocation_id,
            name,
            status,
            execution_time,
            generated_at,
            message
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE unique_id = ?
        AND generated_at >= DATEADD(day, -{days}, SYSDATE())
    ),
    row_counts AS (
        -- A model published to several databases logs one row per copy;
        -- keep the latest per invocation.
        SELECT rc.invocation_id, rc.row_count
        FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG rc
        JOIN (SELECT DISTINCT invocation_id, LOWER(name) as model_name FROM runs) r
          ON rc.invocation_id = r.invocation_id AND LOWER(rc.model_name) = r.model_name
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rc.invocation_id ORDER BY rc.recorded_at DESC) = 1
    )
    SELECT r.*, rc.row_count
    FROM runs r
    LEFT JOIN row_counts rc ON rc.invocation_id = r.invocation_id
    ORDER BY r.generated_at DESC
    """
    return run_query(query, (unique_id,))


def get_model_compiled_code(unique_id: str):
    """Compiled SQL from the most recent run that captured it."""
    query = f"""
    SELECT compiled_code
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    WHERE unique_id = ?
    AND compiled_code IS NOT NULL
    ORDER BY generated_at DESC
    LIMIT 1
    """
    return run_query(query, (unique_id,))


def get_model_details(unique_id: str):
    """Metadata of a model, snapshot or seed (materialization 'snapshot' or
    'seed' for the latter two)."""
    columns = "unique_id, name, schema_name, database_name, alias, description, owner, tags, meta, package_name, original_path, path"
    query = f"""
    SELECT {columns}, materialization FROM {ELEMENTARY_SCHEMA}.dbt_models WHERE unique_id = ?
    UNION ALL
    SELECT {columns}, 'snapshot' FROM {ELEMENTARY_SCHEMA}.dbt_snapshots WHERE unique_id = ?
    UNION ALL
    SELECT {columns}, 'seed' FROM {ELEMENTARY_SCHEMA}.dbt_seeds WHERE unique_id = ?
    """
    return run_query(query, (unique_id,) * 3)


def get_model_execution_trend(unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get execution time trend for charting. Excludes skipped/error runs with 0 or null times."""
    query = f"""
    SELECT
        -- generated_at is UTC; bucket by London day.
        DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'Europe/London', TRY_TO_TIMESTAMP_NTZ(generated_at))) as run_date,
        AVG(execution_time) as avg_time,
        MAX(execution_time) as max_time,
        MIN(execution_time) as min_time,
        COUNT(*) as run_count
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    WHERE unique_id = ?
    AND TRY_TO_TIMESTAMP(generated_at) >= DATEADD(day, -{days}, SYSDATE())
    AND execution_time > 0
    AND status = 'success'
    GROUP BY run_date
    ORDER BY run_date
    """
    return run_query(query, (unique_id,))


def get_model_by_name(model_name: str):
    """Get model unique_id by name."""
    query = f"""
    SELECT unique_id, name, schema_name
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    WHERE LOWER(name) = LOWER(?)
    LIMIT 1
    """
    return run_query(query, (model_name,))


def get_model_row_count_history(model_name: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Row count per invocation for a model from ROW_COUNT_LOG."""
    query = f"""
    SELECT
        model_name,
        row_count,
        run_started_at,
        recorded_at
    FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG
    WHERE LOWER(model_name) = LOWER(?)
    AND run_started_at >= DATEADD(day, -{days}, SYSDATE())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY invocation_id ORDER BY recorded_at DESC) = 1
    ORDER BY run_started_at DESC
    """
    return run_query(query, (model_name,))


def get_model_latest_row_count(model_name: str):
    """Get the most recent row count for a model with change from previous."""
    query = f"""
    WITH per_run AS (
        SELECT model_name, row_count, run_started_at
        FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG
        WHERE LOWER(model_name) = LOWER(?)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY invocation_id ORDER BY recorded_at DESC) = 1
    ),
    recent AS (
        SELECT
            model_name,
            row_count,
            run_started_at,
            LAG(row_count) OVER (ORDER BY run_started_at) as prev_row_count
        FROM per_run
    )
    SELECT
        model_name,
        row_count,
        run_started_at,
        prev_row_count,
        CASE WHEN prev_row_count > 0
             THEN row_count - prev_row_count
             ELSE NULL END as row_change,
        CASE WHEN prev_row_count > 0
             THEN ((row_count - prev_row_count)::FLOAT / prev_row_count) * 100
             ELSE NULL END as change_pct
    FROM recent
    ORDER BY run_started_at DESC
    LIMIT 1
    """
    return run_query(query, (model_name,))


def get_growth_summary(days: int = DEFAULT_LOOKBACK_DAYS):
    """Per-model row count growth over the range: latest vs earliest count in
    the window, plus a daily series (last count each day) for sparklines."""
    query = f"""
    WITH log AS (
        SELECT
            LOWER(model_name) as model_key,
            model_name,
            schema_name,
            row_count,
            run_started_at
        FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG
        WHERE run_started_at >= DATEADD(day, -{days}, SYSDATE())
    ),
    daily AS (
        -- run_started_at is UTC; bucket by London day.
        SELECT model_key, DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'Europe/London', run_started_at)) as day, row_count
        FROM log
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_key, day ORDER BY run_started_at DESC) = 1
    ),
    series AS (
        SELECT model_key, ARRAY_AGG(row_count) WITHIN GROUP (ORDER BY day) as trend
        FROM daily
        GROUP BY model_key
    ),
    bounds AS (
        SELECT
            model_key,
            MAX_BY(model_name, run_started_at) as model_name,
            MAX_BY(schema_name, run_started_at) as schema_name,
            MAX_BY(row_count, run_started_at) as latest_row_count,
            MIN_BY(row_count, run_started_at) as earliest_row_count,
            MAX(run_started_at) as last_recorded
        FROM log
        GROUP BY model_key
    )
    SELECT
        b.model_name,
        b.schema_name,
        m.unique_id,
        b.latest_row_count,
        b.earliest_row_count,
        b.latest_row_count - b.earliest_row_count as row_change,
        CASE
            WHEN b.earliest_row_count > 0
            THEN ((b.latest_row_count - b.earliest_row_count) / b.earliest_row_count) * 100
            ELSE NULL
        END as change_pct,
        b.last_recorded,
        s.trend
    FROM bounds b
    JOIN series s ON s.model_key = b.model_key
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON LOWER(m.name) = b.model_key
    ORDER BY ABS(COALESCE(change_pct, 0)) DESC, b.model_name
    """
    return run_query(query)
