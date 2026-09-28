"""Model runtime queries for the Performance page."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS


def get_runtime_summary(days: int = DEFAULT_LOOKBACK_DAYS):
    """Total and average execution time of successful model runs."""
    query = f"""
    SELECT
        SUM(execution_time) as total_execution_time,
        COUNT(*) as total_runs,
        AVG(execution_time) as avg_execution_time,
        COUNT(DISTINCT unique_id) as models_run
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    AND status = 'success'
    AND resource_type = 'model'
    """
    return run_query(query)


def get_model_runtimes(days: int = DEFAULT_LOOKBACK_DAYS):
    """Per-model runtime stats ranked by total time, with the daily average
    execution time as a series for sparklines. One query for all models."""
    query = f"""
    WITH runs AS (
        SELECT
            unique_id,
            name,
            execution_time,
            DATE_TRUNC('day', TRY_TO_TIMESTAMP(generated_at)) as run_date
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND status = 'success'
        AND resource_type = 'model'
    ),
    daily AS (
        SELECT unique_id, run_date, AVG(execution_time) as avg_time
        FROM runs
        GROUP BY unique_id, run_date
    ),
    series AS (
        SELECT unique_id, ARRAY_AGG(ROUND(avg_time, 1)) WITHIN GROUP (ORDER BY run_date) as trend
        FROM daily
        GROUP BY unique_id
    ),
    stats AS (
        SELECT
            unique_id,
            ANY_VALUE(name) as name,
            SUM(execution_time) as total_time,
            AVG(execution_time) as avg_time,
            MAX(execution_time) as max_time,
            COUNT(*) as run_count
        FROM runs
        GROUP BY unique_id
    )
    SELECT
        s.unique_id,
        s.name,
        m.schema_name,
        s.total_time,
        s.avg_time,
        s.max_time,
        s.run_count,
        t.trend
    FROM stats s
    JOIN series t ON t.unique_id = s.unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON m.unique_id = s.unique_id
    ORDER BY s.total_time DESC
    """
    return run_query(query)
