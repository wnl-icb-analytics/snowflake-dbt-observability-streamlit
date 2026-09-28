"""Test result queries."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS, FLAKY_TEST_THRESHOLD


def get_tests_summary(days: int = DEFAULT_LOOKBACK_DAYS):
    """
    Get test summary with pass rate and flaky detection.
    Only includes tests in the current manifest.
    """
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
    JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON s.test_unique_id = t.unique_id
    WHERE s.rn = 1
    ORDER BY (s.pass_count::FLOAT / NULLIF(s.total_runs, 0)) ASC NULLS LAST, s.total_runs DESC
    """
    return run_query(query)


def get_test_run_history(test_unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get run history for a specific test."""
    query = f"""
    SELECT
        test_unique_id,
        test_name,
        invocation_id,
        status,
        failures,
        detected_at,
        test_results_description,
        test_results_query
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results
    WHERE test_unique_id = ?
    AND detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY detected_at DESC
    """
    return run_query(query, (test_unique_id,))


def get_models_without_tests():
    """Get models that have no associated tests."""
    query = f"""
    WITH tested_models AS (
        SELECT DISTINCT parent_model_unique_id
        FROM {ELEMENTARY_SCHEMA}.dbt_tests
        WHERE parent_model_unique_id IS NOT NULL
    )
    SELECT
        m.unique_id,
        m.name,
        m.schema_name,
        m.database_name,
        COALESCE(m.original_path, m.path) as model_path
    FROM {ELEMENTARY_SCHEMA}.dbt_models m
    LEFT JOIN tested_models t ON m.unique_id = t.parent_model_unique_id
    WHERE t.parent_model_unique_id IS NULL
    ORDER BY m.schema_name, m.name
    """
    return run_query(query)


def get_flaky_tests(days: int = DEFAULT_LOOKBACK_DAYS, limit: int = 200):
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
    JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON s.test_unique_id = t.unique_id
    WHERE s.fail_count::FLOAT / s.total_runs >= {FLAKY_TEST_THRESHOLD}
    ORDER BY failure_rate DESC, s.total_runs DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_tests_for_model(unique_id: str, days: int = DEFAULT_LOOKBACK_DAYS):
    """Tests defined on a model (dbt_tests.parent_model_unique_id) with their
    latest status in the range; tests without runs in the range are included."""
    query = f"""
    WITH model_tests AS (
        SELECT
            unique_id as test_unique_id,
            COALESCE(short_name, name) as test_name,
            COALESCE(test_namespace, type) as test_namespace,
            test_column_name,
            severity
        FROM {ELEMENTARY_SCHEMA}.dbt_tests
        WHERE parent_model_unique_id = ?
    ),
    latest AS (
        SELECT r.test_unique_id, r.status, r.detected_at
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        JOIN model_tests t ON r.test_unique_id = t.test_unique_id
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.test_unique_id ORDER BY r.detected_at DESC) = 1
    )
    SELECT
        t.test_unique_id,
        t.test_name,
        t.test_namespace,
        t.test_column_name,
        t.severity,
        l.status as latest_status,
        l.detected_at as last_run
    FROM model_tests t
    LEFT JOIN latest l ON l.test_unique_id = t.test_unique_id
    ORDER BY
        CASE WHEN l.status IN ('fail', 'error') THEN 0 WHEN l.status = 'warn' THEN 1 ELSE 2 END,
        t.test_name
    """
    return run_query(query, (unique_id,))


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
    WHERE r.test_unique_id = ?
    ORDER BY r.detected_at DESC
    LIMIT 1
    """
    return run_query(query, (test_unique_id,))
