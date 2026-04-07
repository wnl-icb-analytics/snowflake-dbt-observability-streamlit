"""Metrics queries for dashboard KPIs."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS


def get_dashboard_kpis(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get main dashboard KPIs in a single query."""
    query = f"""
    WITH test_base AS (
        SELECT
            test_unique_id,
            table_name,
            status,
            detected_at
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results
        WHERE detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ),
    test_latest AS (
        SELECT
            table_name,
            status,
            ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY detected_at DESC) as rn
        FROM test_base
    ),
    active_test_areas AS (
        SELECT COUNT(DISTINCT table_name) as failed_tests
        FROM test_latest
        WHERE rn = 1
        AND status IN ('fail', 'error')
        AND table_name IS NOT NULL
    ),
    model_base AS (
        SELECT
            name,
            status,
            execution_time,
            generated_at
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND resource_type = 'model'
    ),
    model_latest AS (
        SELECT
            name,
            status,
            execution_time,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY generated_at DESC) as rn
        FROM model_base
    ),
    active_models AS (
        SELECT COUNT(DISTINCT name) as failed_models
        FROM model_latest
        WHERE rn = 1
        AND status IN ('fail', 'error')
    ),
    last_run AS (
        SELECT MAX(generated_at) as last_run_time
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    )
    SELECT
        (SELECT failed_tests FROM active_test_areas) as failed_tests,
        (SELECT COUNT(DISTINCT test_unique_id) FROM test_base) as total_tests_run,
        (SELECT failed_models FROM active_models) as failed_models,
        (SELECT COUNT(DISTINCT name) FROM model_base) as total_models_run,
        (SELECT AVG(execution_time) FROM model_latest WHERE rn = 1) as avg_execution_time,
        (SELECT last_run_time FROM last_run) as last_run_time
    """
    return run_query(query)


def get_recent_runs(limit: int = 10):
    """Get most recent dbt invocations with run stats and warehouse info."""
    query = f"""
    WITH run_stats AS (
        SELECT
            invocation_id,
            COUNT(*) as total_models,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status IN ('fail', 'error') THEN 1 ELSE 0 END) as fail_count,
            SUM(execution_time) as total_time
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE resource_type = 'model'
        GROUP BY invocation_id
    ),
    test_stats AS (
        SELECT
            invocation_id,
            COUNT(*) as total_tests,
            SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as tests_passed,
            SUM(CASE WHEN status IN ('fail', 'error') THEN 1 ELSE 0 END) as tests_failed,
            SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) as tests_warned
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results
        GROUP BY invocation_id
    )
    SELECT
        i.invocation_id,
        i.created_at,
        i.run_started_at,
        i.run_completed_at,
        i.command,
        i.target_name,
        i.dbt_user,
        i.selected,
        TRY_PARSE_JSON(i.target_adapter_specific_fields):warehouse::VARCHAR as warehouse,
        COALESCE(s.total_models, 0) as models_run,
        COALESCE(s.success_count, 0) as success_count,
        COALESCE(s.fail_count, 0) as fail_count,
        COALESCE(s.total_time, 0) as total_time,
        TIMESTAMPDIFF('second', TRY_TO_TIMESTAMP(i.run_started_at), TRY_TO_TIMESTAMP(i.run_completed_at)) as duration_seconds,
        COALESCE(t.total_tests, 0) as tests_run,
        COALESCE(t.tests_passed, 0) as tests_passed,
        COALESCE(t.tests_failed, 0) as tests_failed,
        COALESCE(t.tests_warned, 0) as tests_warned
    FROM {ELEMENTARY_SCHEMA}.dbt_invocations i
    LEFT JOIN run_stats s ON i.invocation_id = s.invocation_id
    LEFT JOIN test_stats t ON i.invocation_id = t.invocation_id
    ORDER BY i.created_at DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_top_failures(limit: int = 5, days: int = DEFAULT_LOOKBACK_DAYS):
    """Get current failures (latest run is failing) for 'needs attention' section."""
    query = f"""
    WITH test_latest AS (
        SELECT
            r.test_unique_id as unique_id,
            COALESCE(t.short_name, r.test_name) as name,
            'test' as type,
            r.detected_at as failed_at,
            r.status,
            r.schema_name,
            COALESCE(t.test_namespace, r.test_type) as test_namespace,
            r.table_name as model_name,
            m.unique_id as tested_model_id,
            COALESCE(m.original_path, m.path) as model_path,
            ROW_NUMBER() OVER (PARTITION BY r.test_unique_id ORDER BY r.detected_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON r.table_name = m.name
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ),
    model_latest AS (
        SELECT
            r.unique_id,
            r.name,
            'model' as type,
            r.generated_at as failed_at,
            r.status,
            m.schema_name,
            NULL as test_namespace,
            NULL as model_name,
            r.unique_id as tested_model_id,
            COALESCE(m.original_path, m.path) as model_path,
            ROW_NUMBER() OVER (PARTITION BY r.unique_id ORDER BY r.generated_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON r.unique_id = m.unique_id
        WHERE r.generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND r.resource_type = 'model'
    )
    SELECT unique_id, name, type, failed_at, schema_name, test_namespace, model_name, tested_model_id, model_path
    FROM (
        SELECT unique_id, name, type, failed_at, schema_name, test_namespace, model_name, tested_model_id, model_path
        FROM test_latest WHERE rn = 1 AND status IN ('fail', 'error')
        UNION ALL
        SELECT unique_id, name, type, failed_at, schema_name, test_namespace, model_name, tested_model_id, model_path
        FROM model_latest WHERE rn = 1 AND status IN ('fail', 'error')
    )
    ORDER BY failed_at DESC
    LIMIT {limit}
    """
    return run_query(query)


def get_project_totals():
    """Get total counts of models and tests in the project (not just recent runs)."""
    query = f"""
    SELECT
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.dbt_models) as total_models,
        (SELECT COUNT(DISTINCT test_unique_id) FROM {ELEMENTARY_SCHEMA}.elementary_test_results) as total_tests
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
