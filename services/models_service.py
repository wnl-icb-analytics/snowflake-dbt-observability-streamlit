"""Model run queries."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS, DEFAULT_PAGE_SIZE, SLOW_MODEL_MIN_SECONDS


def get_models_summary(
    days: int = DEFAULT_LOOKBACK_DAYS,
    search: str = "",
    show_all: bool = False,
    limit: int = 500,
    offset: int = 0,
):
    """
    Get model summary with latest status and avg execution time.
    By default shows only models with issues (failed or slow).
    When show_all=True, shows ALL models from dbt_models table.
    """
    # Normalize search to handle both forward and back slashes
    normalized_search = search.replace("\\", "/") if search else ""
    search_filter = f"AND (LOWER(m.unique_id) LIKE LOWER('%{normalized_search}%') OR LOWER(m.name) LIKE LOWER('%{normalized_search}%') OR LOWER(REPLACE(COALESCE(m.original_path, m.path, ''), '\\\\', '/')) LIKE LOWER('%{normalized_search}%'))" if search else ""
    search_filter_run = f"AND (LOWER(r.unique_id) LIKE LOWER('%{normalized_search}%') OR LOWER(r.name) LIKE LOWER('%{normalized_search}%') OR LOWER(REPLACE(COALESCE(m.original_path, m.path, ''), '\\\\', '/')) LIKE LOWER('%{normalized_search}%'))" if search else ""

    if show_all:
        # Show all models, including those without recent runs
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
            WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
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
            COALESCE(m.original_path, m.path) as model_path,
            COALESCE(r.status, 'no_runs') as latest_status,
            r.generated_at as last_run,
            r.avg_execution_time,
            COALESCE(r.run_count, 0) as run_count,
            CASE WHEN r.avg_execution_time > p.p90 AND r.avg_execution_time >= {SLOW_MODEL_MIN_SECONDS} THEN TRUE ELSE FALSE END as is_slow
        FROM {ELEMENTARY_SCHEMA}.dbt_models m
        LEFT JOIN latest_runs r ON m.unique_id = r.unique_id
        CROSS JOIN percentiles p
        WHERE 1=1
        {search_filter}
        ORDER BY
            CASE WHEN r.status IN ('fail', 'error') THEN 0 ELSE 1 END,
            r.avg_execution_time DESC NULLS LAST,
            m.name
        LIMIT {limit} OFFSET {offset}
        """
    else:
        # Show only models with issues (failed or slow) from recent runs
        query = f"""
        WITH model_stats AS (
            SELECT
                r.unique_id,
                r.name,
                m.schema_name,
                m.database_name,
                r.status,
                r.execution_time,
                r.generated_at,
                ROW_NUMBER() OVER (PARTITION BY r.unique_id ORDER BY r.generated_at DESC) as rn,
                AVG(r.execution_time) OVER (PARTITION BY r.unique_id) as avg_execution_time,
                COUNT(*) OVER (PARTITION BY r.unique_id) as run_count
            FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
            JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON r.unique_id = m.unique_id
            WHERE r.generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            AND r.resource_type = 'model'
            {search_filter_run}
        ),
        percentiles AS (
            SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY avg_execution_time) as p90
            FROM (SELECT DISTINCT unique_id, AVG(execution_time) as avg_execution_time
                  FROM {ELEMENTARY_SCHEMA}.dbt_run_results
                  WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
                  AND resource_type = 'model'
                  GROUP BY unique_id)
        )
        SELECT
            ms.unique_id,
            ms.name,
            ms.schema_name,
            ms.database_name,
            ms.status as latest_status,
            ms.generated_at as last_run,
            ms.avg_execution_time,
            ms.run_count,
            CASE WHEN ms.avg_execution_time > p.p90 AND ms.avg_execution_time >= {SLOW_MODEL_MIN_SECONDS} THEN TRUE ELSE FALSE END as is_slow
        FROM model_stats ms
        CROSS JOIN percentiles p
        WHERE ms.rn = 1
        AND (ms.status IN ('fail', 'error') OR (ms.avg_execution_time > p.p90 AND ms.avg_execution_time >= {SLOW_MODEL_MIN_SECONDS}))
        ORDER BY
            CASE WHEN ms.status IN ('fail', 'error') THEN 0 ELSE 1 END,
            ms.avg_execution_time DESC NULLS LAST
        LIMIT {limit} OFFSET {offset}
        """
    return run_query(query)


def get_models_count(search: str = ""):
    """Get total count of all models in dbt_models table."""
    search_filter = f"WHERE LOWER(unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    SELECT COUNT(*) as total
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    {search_filter}
    """
    return run_query(query)


def get_model_run_history(unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get run history for a specific model including error messages and compiled code."""
    query = f"""
    SELECT
        unique_id,
        name,
        status,
        execution_time,
        generated_at,
        compile_started_at,
        compile_completed_at,
        execute_started_at,
        execute_completed_at,
        message,
        compiled_code
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    WHERE unique_id = '{unique_id}'
    AND generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY generated_at DESC
    """
    return run_query(query)


def get_model_details(unique_id: str):
    """Get model metadata."""
    query = f"""
    SELECT
        unique_id,
        name,
        schema_name,
        database_name,
        alias,
        description,
        owner,
        tags,
        package_name,
        original_path,
        path,
        materialization
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    WHERE unique_id = '{unique_id}'
    """
    return run_query(query)


def get_model_execution_trend(unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get execution time trend for charting. Excludes skipped/error runs with 0 or null times."""
    query = f"""
    SELECT
        DATE_TRUNC('day', TRY_TO_TIMESTAMP(generated_at)) as run_date,
        AVG(execution_time) as avg_time,
        MAX(execution_time) as max_time,
        MIN(execution_time) as min_time,
        COUNT(*) as run_count
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    WHERE unique_id = '{unique_id}'
    AND TRY_TO_TIMESTAMP(generated_at) >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    AND execution_time > 0
    AND status = 'success'
    GROUP BY DATE_TRUNC('day', TRY_TO_TIMESTAMP(generated_at))
    ORDER BY run_date
    """
    return run_query(query)


def get_model_by_name(model_name: str):
    """Get model unique_id by name."""
    query = f"""
    SELECT unique_id, name, schema_name
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    WHERE LOWER(name) = LOWER('{model_name}')
    LIMIT 1
    """
    return run_query(query)


def get_schema_list():
    """Get list of schemas for filtering."""
    query = f"""
    SELECT DISTINCT schema_name
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    WHERE schema_name IS NOT NULL
    ORDER BY schema_name
    """
    return run_query(query)


def get_model_paths():
    """Get all unique model paths for folder navigation."""
    query = f"""
    SELECT DISTINCT
        COALESCE(original_path, path) as model_path
    FROM {ELEMENTARY_SCHEMA}.dbt_models
    WHERE COALESCE(original_path, path) IS NOT NULL
    ORDER BY model_path
    """
    return run_query(query)


def get_model_row_count_history(model_name: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get row count history for a model from ROW_COUNT_LOG table."""
    query = f"""
    SELECT
        model_name,
        row_count,
        run_started_at,
        recorded_at
    FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG
    WHERE LOWER(model_name) = LOWER('{model_name}')
    AND run_started_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY run_started_at DESC
    """
    return run_query(query)


def get_model_latest_row_count(model_name: str):
    """Get the most recent row count for a model with change from previous."""
    query = f"""
    WITH recent AS (
        SELECT
            model_name,
            row_count,
            run_started_at,
            LAG(row_count) OVER (ORDER BY run_started_at) as prev_row_count
        FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG
        WHERE LOWER(model_name) = LOWER('{model_name}')
        ORDER BY run_started_at DESC
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
    LIMIT 1
    """
    return run_query(query)
