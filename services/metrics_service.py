"""Metrics queries for dashboard KPIs."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS
from services.jobs_service import hide_compile_show_sql, job_columns_sql, run_counts_columns_sql, run_counts_sql


def get_last_run_time():
    """Time of the latest model, seed, snapshot or test result (UTC text)."""
    query = f"""
    SELECT MAX(generated_at) as last_run_time
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results
    """
    return run_query(query)


def get_recent_runs(limit: int = 10, include_compile_show: bool = False):
    """Get most recent dbt invocations with job, trigger, model (with seed and
    snapshot) and test status counts and warehouse info. Local compile and
    show invocations only when asked."""
    query = f"""
    WITH {run_counts_sql()}
    SELECT
        i.invocation_id,
        i.created_at,
        i.run_started_at,
        i.run_completed_at,
        i.command,
        i.target_name,
        i.dbt_user,
        i.selected,
        {job_columns_sql()},
        TRY_PARSE_JSON(i.target_adapter_specific_fields):warehouse::VARCHAR as warehouse,
        TIMESTAMPDIFF('second', TRY_TO_TIMESTAMP(i.run_started_at), TRY_TO_TIMESTAMP(i.run_completed_at)) as duration_seconds,
        {run_counts_columns_sql()}
    FROM {ELEMENTARY_SCHEMA}.dbt_invocations i
    LEFT JOIN run_stats s ON i.invocation_id = s.invocation_id
    LEFT JOIN test_stats t ON i.invocation_id = t.invocation_id
    {"" if include_compile_show else "WHERE " + hide_compile_show_sql()}
    ORDER BY i.created_at DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_project_totals():
    """Get total counts of models and tests in the project (not just recent runs)."""
    query = f"""
    SELECT
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.dbt_models) as total_models,
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.dbt_tests) as total_tests
    """
    return run_query(query)


def get_total_execution_time(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get total runtime from invocation durations (not query execution sum)."""
    query = f"""
    SELECT SUM(
        TIMESTAMPDIFF('second', TRY_TO_TIMESTAMP(run_started_at), TRY_TO_TIMESTAMP(run_completed_at))
    ) as total_time
    FROM {ELEMENTARY_SCHEMA}.dbt_invocations
    WHERE created_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    AND run_started_at IS NOT NULL
    AND run_completed_at IS NOT NULL
    """
    return run_query(query)
