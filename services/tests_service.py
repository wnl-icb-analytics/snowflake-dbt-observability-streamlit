"""Test result queries."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS, DEFAULT_PAGE_SIZE, FLAKY_TEST_THRESHOLD


def get_tests_summary(
    days: int = DEFAULT_LOOKBACK_DAYS,
    search: str = "",
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
):
    """
    Get test summary with pass rate and flaky detection.
    Joins with dbt_tests for cleaner display names.
    """
    search_filter = f"AND LOWER(r.test_unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    WITH test_stats AS (
        SELECT
            r.test_unique_id,
            r.test_name,
            r.test_type,
            r.table_name,
            r.schema_name,
            r.status,
            r.detected_at,
            ROW_NUMBER() OVER (PARTITION BY r.test_unique_id ORDER BY r.detected_at DESC) as rn,
            COUNT(*) OVER (PARTITION BY r.test_unique_id) as total_runs,
            SUM(CASE WHEN r.status = 'pass' THEN 1 ELSE 0 END) OVER (PARTITION BY r.test_unique_id) as pass_count
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        {search_filter}
    )
    SELECT
        s.test_unique_id,
        s.test_name,
        COALESCE(t.short_name, s.test_name) as short_name,
        COALESCE(t.test_namespace, s.test_type) as test_namespace,
        s.test_type,
        s.table_name,
        s.schema_name,
        s.status as latest_status,
        s.detected_at as last_run,
        s.total_runs,
        s.pass_count,
        ROUND(s.pass_count::FLOAT / NULLIF(s.total_runs, 0), 3) as pass_rate,
        CASE
            WHEN (1 - s.pass_count::FLOAT / NULLIF(s.total_runs, 0)) >= {FLAKY_TEST_THRESHOLD}
            AND s.total_runs >= 3
            THEN TRUE ELSE FALSE
        END as is_flaky
    FROM test_stats s
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON s.test_unique_id = t.unique_id
    WHERE s.rn = 1
    ORDER BY (s.pass_count::FLOAT / NULLIF(s.total_runs, 0)) ASC NULLS LAST, s.total_runs DESC
    LIMIT {limit} OFFSET {offset}
    """
    return run_query(query)


def get_test_run_history(test_unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get run history for a specific test."""
    query = f"""
    SELECT
        test_unique_id,
        test_name,
        status,
        detected_at,
        test_results_description,
        test_results_query
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results
    WHERE test_unique_id = '{test_unique_id}'
    AND detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY detected_at DESC
    """
    return run_query(query)


def get_test_run_history_ascending(test_unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get run history for a specific test in chronological order."""
    query = f"""
    SELECT
        test_unique_id,
        test_name,
        status,
        detected_at,
        test_results_description,
        test_results_query
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results
    WHERE test_unique_id = '{test_unique_id}'
    AND detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY detected_at ASC
    """
    return run_query(query)


def get_models_without_tests():
    """Get models that have no associated tests."""
    query = f"""
    WITH tested_models AS (
        SELECT DISTINCT table_name
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results
    )
    SELECT
        m.unique_id,
        m.name,
        m.schema_name,
        m.database_name
    FROM {ELEMENTARY_SCHEMA}.dbt_models m
    LEFT JOIN tested_models t ON LOWER(m.name) = LOWER(t.table_name)
    WHERE t.table_name IS NULL
    ORDER BY m.schema_name, m.name
    """
    return run_query(query)


def get_flaky_tests(days: int = DEFAULT_LOOKBACK_DAYS, limit: int = 20):
    """Get tests with high failure rates (flaky tests)."""
    query = f"""
    WITH test_stats AS (
        SELECT
            r.test_unique_id,
            r.test_name,
            r.table_name,
            r.schema_name,
            COUNT(*) as total_runs,
            SUM(CASE WHEN r.status = 'pass' THEN 1 ELSE 0 END) as pass_count,
            SUM(CASE WHEN r.status IN ('fail', 'error') THEN 1 ELSE 0 END) as fail_count
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        GROUP BY r.test_unique_id, r.test_name, r.table_name, r.schema_name
        HAVING total_runs >= 3
    )
    SELECT
        s.test_unique_id,
        s.test_name,
        COALESCE(t.short_name, s.test_name) as short_name,
        COALESCE(t.test_namespace, '') as test_namespace,
        s.table_name,
        s.schema_name,
        s.total_runs,
        s.pass_count,
        s.fail_count,
        ROUND(s.fail_count::FLOAT / s.total_runs, 3) as failure_rate
    FROM test_stats s
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON s.test_unique_id = t.unique_id
    WHERE s.fail_count::FLOAT / s.total_runs >= {FLAKY_TEST_THRESHOLD}
    ORDER BY failure_rate DESC, s.total_runs DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_tests_for_model(model_name: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get tests associated with a specific model with latest status."""
    query = f"""
    WITH test_runs AS (
        SELECT
            test_unique_id,
            test_name,
            test_type,
            schema_name,
            status,
            detected_at,
            ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY detected_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results
        WHERE LOWER(table_name) = LOWER('{model_name}')
        AND detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    )
    SELECT
        test_unique_id,
        test_name,
        test_type,
        schema_name,
        status as latest_status
    FROM test_runs
    WHERE rn = 1
    ORDER BY
        CASE WHEN status IN ('fail', 'error') THEN 0 ELSE 1 END,
        test_name
    """
    return run_query(query)


def get_tests_count(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """Get total count of tests with runs in the time period."""
    search_filter = f"AND LOWER(test_unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    SELECT COUNT(DISTINCT test_unique_id) as total
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results
    WHERE detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    {search_filter}
    """
    return run_query(query)


def get_test_details(test_unique_id: str):
    """Get metadata for a specific test, joining with dbt_tests for richer info."""
    query = f"""
    SELECT
        r.test_unique_id,
        r.test_name,
        COALESCE(t.short_name, r.test_name) as short_name,
        COALESCE(t.test_namespace, r.test_type) as test_namespace,
        r.test_type,
        r.table_name,
        r.schema_name,
        r.database_name,
        r.column_name,
        r.test_params,
        t.test_column_name,
        t.severity,
        t.description,
        t.parent_model_unique_id,
        t.tags,
        t.original_path
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    WHERE r.test_unique_id = '{test_unique_id}'
    ORDER BY r.detected_at DESC
    LIMIT 1
    """
    return run_query(query)
