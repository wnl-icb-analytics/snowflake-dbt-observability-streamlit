"""Alert queries - test and model failures with smart filtering."""

from database import run_query
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS


def get_current_test_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """
    Get test failures where the most recent run is a failure.
    Joins with dbt_tests for cleaner display names.
    """
    search_filter = f"AND LOWER(r.test_unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    WITH ranked AS (
        SELECT
            r.test_unique_id,
            r.test_name,
            r.test_type,
            r.status,
            r.detected_at,
            r.database_name,
            r.schema_name,
            r.table_name,
            r.column_name,
            r.test_results_description,
            r.test_results_query,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_unique_id
                ORDER BY r.detected_at DESC
            ) as rn
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        {search_filter}
    )
    SELECT
        r.test_unique_id,
        r.test_name,
        COALESCE(t.short_name, r.test_name) as short_name,
        COALESCE(t.test_namespace, r.test_type) as test_namespace,
        t.test_column_name,
        t.parent_model_unique_id,
        r.test_type,
        r.status,
        r.detected_at,
        r.database_name,
        r.schema_name,
        r.table_name,
        r.column_name,
        r.test_results_description,
        r.test_results_query
    FROM ranked r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    WHERE r.rn = 1 AND r.status IN ('fail', 'error')
    ORDER BY r.detected_at DESC
    """
    return run_query(query)


def get_current_model_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """
    Get model failures where the most recent run failed.
    """
    search_filter = f"AND LOWER(r.unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    WITH ranked AS (
        SELECT
            r.unique_id,
            r.name,
            r.status,
            r.execution_time,
            r.generated_at,
            m.database_name,
            m.schema_name,
            r.compile_started_at,
            r.compile_completed_at,
            r.execute_started_at,
            r.execute_completed_at,
            r.message,
            ROW_NUMBER() OVER (
                PARTITION BY r.unique_id
                ORDER BY r.generated_at DESC
            ) as rn
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON r.unique_id = m.unique_id
        WHERE r.generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND r.resource_type = 'model'
        {search_filter}
    )
    SELECT *
    FROM ranked
    WHERE rn = 1 AND status IN ('fail', 'error')
    ORDER BY generated_at DESC
    """
    return run_query(query)


def get_alert_counts(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get summary counts of current failures."""
    query = f"""
    WITH test_ranked AS (
        SELECT
            test_unique_id,
            status,
            ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY detected_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results
        WHERE detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ),
    model_ranked AS (
        SELECT
            unique_id,
            status,
            ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND resource_type = 'model'
    )
    SELECT
        (SELECT COUNT(*) FROM test_ranked WHERE rn = 1 AND status IN ('fail', 'error')) as failed_tests,
        (SELECT COUNT(*) FROM model_ranked WHERE rn = 1 AND status IN ('fail', 'error')) as failed_models
    """
    return run_query(query)


def get_historical_test_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """Get all test failures in time period (not just current failures)."""
    search_filter = f"AND LOWER(r.test_unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    SELECT
        r.test_unique_id,
        r.test_name,
        COALESCE(t.short_name, r.test_name) as short_name,
        COALESCE(t.test_namespace, r.test_type) as test_namespace,
        r.test_type,
        r.status,
        r.detected_at,
        r.schema_name,
        r.table_name,
        r.test_results_description
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    AND r.status IN ('fail', 'error', 'warn')
    {search_filter}
    ORDER BY r.detected_at DESC
    LIMIT 200
    """
    return run_query(query)


def get_historical_model_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """Get all model failures in time period (not just current failures)."""
    search_filter = f"AND LOWER(r.unique_id) LIKE LOWER('%{search}%')" if search else ""

    query = f"""
    SELECT
        r.unique_id,
        r.name,
        r.status,
        r.execution_time,
        r.generated_at,
        m.schema_name,
        r.message
    FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON r.unique_id = m.unique_id
    WHERE r.generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    AND r.resource_type = 'model'
    AND r.status IN ('fail', 'error')
    {search_filter}
    ORDER BY r.generated_at DESC
    LIMIT 200
    """
    return run_query(query)


def get_historical_alert_counts(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get counts of all failures in time period."""
    query = f"""
    SELECT
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.elementary_test_results
         WHERE detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
         AND status IN ('fail', 'error', 'warn')) as failed_tests,
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.dbt_run_results
         WHERE generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
         AND resource_type = 'model'
         AND status IN ('fail', 'error')) as failed_models
    """
    return run_query(query)


def get_project_test_status_history(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get project-wide test status history for trend and resolution analysis."""
    query = f"""
    SELECT
        r.test_unique_id,
        COALESCE(t.short_name, r.test_name) as short_name,
        r.table_name,
        r.status,
        r.detected_at
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY r.test_unique_id, r.detected_at ASC
    """
    return run_query(query)


def get_current_issue_summary(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get combined current model and test issues for homepage summary."""
    query = f"""
    WITH model_base AS (
        SELECT
            r.name as object_name,
            'Model' as issue_type,
            r.status,
            r.generated_at as event_at,
            r.message
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        WHERE r.resource_type = 'model'
        AND r.generated_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ),
    model_latest AS (
        SELECT
            object_name,
            status as current_status
        FROM model_base
        QUALIFY ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY event_at DESC) = 1
    ),
    model_last_success AS (
        SELECT
            object_name,
            MAX(event_at) as last_success_at
        FROM model_base
        WHERE status = 'success'
        GROUP BY object_name
    ),
    model_agg AS (
        SELECT
            b.object_name,
            b.issue_type,
            COUNT_IF(b.status IN ('fail', 'error')) as failure_count,
            COUNT(*) as total_runs,
            MIN(
                CASE
                    WHEN b.status IN ('fail', 'error')
                     AND b.event_at > COALESCE(s.last_success_at, TO_TIMESTAMP('1970-01-01'))
                    THEN b.event_at
                END
            ) as first_issue_at,
            MAX(CASE WHEN b.status IN ('fail', 'error') THEN b.event_at END) as last_issue_at,
            ANY_VALUE(b.message) as sample_message
        FROM model_base b
        LEFT JOIN model_last_success s ON b.object_name = s.object_name
        GROUP BY b.object_name, b.issue_type
    ),
    test_base AS (
        SELECT
            r.table_name as object_name,
            'Test Area' as issue_type,
            CONCAT(
                COALESCE(r.table_name, ''),
                '||',
                COALESCE(COALESCE(t.short_name, r.test_name), '')
            ) as logical_test_key,
            r.test_unique_id,
            r.status,
            r.detected_at as event_at,
            COALESCE(t.short_name, r.test_name) as test_name
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
        WHERE r.detected_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        AND r.table_name IS NOT NULL
    ),
    test_latest AS (
        SELECT
            object_name,
            logical_test_key,
            status as current_status
        FROM test_base
        QUALIFY ROW_NUMBER() OVER (PARTITION BY logical_test_key ORDER BY event_at DESC) = 1
    ),
    test_last_pass AS (
        SELECT
            object_name,
            logical_test_key,
            MAX(event_at) as last_pass_at
        FROM test_base
        WHERE status = 'pass'
        GROUP BY object_name, logical_test_key
    ),
    test_agg_per_check AS (
        SELECT
            b.object_name,
            b.issue_type,
            b.logical_test_key,
            ANY_VALUE(b.test_name) as test_name,
            COUNT_IF(b.status IN ('fail', 'error')) as failure_count,
            COUNT(*) as total_runs,
            MIN(
                CASE
                    WHEN b.status <> 'pass'
                     AND b.event_at > COALESCE(p.last_pass_at, TO_TIMESTAMP('1970-01-01'))
                    THEN b.event_at
                END
            ) as first_issue_at,
            MAX(CASE WHEN b.status IN ('fail', 'error') THEN b.event_at END) as last_issue_at
        FROM test_base b
        LEFT JOIN test_last_pass p
            ON b.object_name = p.object_name
           AND b.logical_test_key = p.logical_test_key
        GROUP BY b.object_name, b.issue_type, b.logical_test_key
    ),
    test_filtered AS (
        SELECT
            a.object_name,
            a.issue_type,
            a.logical_test_key,
            a.test_name,
            a.failure_count,
            a.total_runs,
            a.first_issue_at,
            a.last_issue_at,
            l.current_status
        FROM test_agg_per_check a
        JOIN test_latest l
            ON a.object_name = l.object_name
           AND a.logical_test_key = l.logical_test_key
        WHERE a.failure_count > 0
          AND l.current_status <> 'pass'
          AND NOT (a.failure_count = 1 AND l.current_status = 'pass')
    ),
    test_agg AS (
        SELECT
            object_name,
            issue_type,
            SUM(failure_count) as failure_count,
            COUNT(*) as affected_checks,
            COUNT_IF(current_status IN ('fail', 'error')) as currently_failing_checks,
            MIN(first_issue_at) as first_issue_at,
            MAX(last_issue_at) as last_issue_at,
            ANY_VALUE(test_name) as sample_message
        FROM test_filtered
        GROUP BY object_name, issue_type
    )
    SELECT
        a.object_name,
        a.issue_type,
        l.current_status,
        a.failure_count,
        a.total_runs,
        NULL as affected_checks,
        a.first_issue_at,
        a.last_issue_at,
        a.sample_message
    FROM model_agg a
    JOIN model_latest l USING (object_name)
    WHERE l.current_status IN ('fail', 'error')
      AND a.failure_count > 0

    UNION ALL

    SELECT
        object_name,
        issue_type,
        CASE
            WHEN currently_failing_checks > 0 THEN 'fail'
            ELSE 'skipped'
        END as current_status,
        failure_count,
        NULL as total_runs,
        affected_checks,
        first_issue_at,
        last_issue_at,
        sample_message
    FROM test_agg
    ORDER BY failure_count DESC, last_issue_at DESC
    """
    return run_query(query)


def get_latest_run_issues():
    """Get model/test issues from the most recent build invocation only."""
    query = f"""
    WITH latest_invocation AS (
        SELECT invocation_id, created_at, command
        FROM {ELEMENTARY_SCHEMA}.dbt_invocations
        WHERE LOWER(command) LIKE '%build%'
        ORDER BY created_at DESC
        LIMIT 1
    ),
    model_issues AS (
        SELECT
            r.name as object_name,
            'Model' as issue_type,
            r.status as current_status,
            1 as issue_count,
            i.created_at as event_at,
            r.message as summary
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
        WHERE r.resource_type = 'model'
          AND r.status IN ('fail', 'error')
    ),
    test_issues AS (
        SELECT
            COALESCE(r.table_name, COALESCE(t.short_name, r.test_name)) as object_name,
            'Test' as issue_type,
            CASE WHEN r.status = 'warn' THEN 'warn' ELSE 'fail' END as current_status,
            COUNT(*) as issue_count,
            i.created_at as event_at,
            ANY_VALUE(COALESCE(t.short_name, r.test_name)) as summary
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
        WHERE r.status IN ('fail', 'error', 'warn')
        GROUP BY 1, 2, 3, 5
    )
    SELECT *
    FROM (
        SELECT * FROM model_issues
        UNION ALL
        SELECT * FROM test_issues
    )
    ORDER BY
        CASE current_status WHEN 'fail' THEN 0 WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END,
        issue_type,
        object_name
    """
    return run_query(query)
