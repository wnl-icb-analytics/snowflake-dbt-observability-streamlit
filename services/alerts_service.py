"""Alert queries - failure history, the latest build and downstream impact.

Time windows on UTC columns (generated_at, detected_at) use SYSDATE(), which is UTC;
CURRENT_TIMESTAMP() is in the account timezone (Europe/London)."""

import json

from database import run_query, search_clause
from config import ELEMENTARY_SCHEMA, DEFAULT_LOOKBACK_DAYS


def get_historical_test_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """Get all test failures in time period (not just current failures)."""
    search_filter, params = search_clause(["r.test_unique_id", "r.table_name"], search)

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
    WHERE r.detected_at >= DATEADD(day, -{days}, SYSDATE())
    AND r.status IN ('fail', 'error', 'warn')
    {search_filter}
    ORDER BY r.detected_at DESC
    LIMIT 200
    """
    return run_query(query, params)


def get_historical_model_failures(days: int = DEFAULT_LOOKBACK_DAYS, search: str = ""):
    """Get all model failures in time period (not just current failures)."""
    search_filter, params = search_clause(["r.unique_id"], search)

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
    WHERE r.generated_at >= DATEADD(day, -{days}, SYSDATE())
    AND r.resource_type IN ('model', 'seed', 'snapshot')
    AND r.status IN ('fail', 'error')
    {search_filter}
    ORDER BY r.generated_at DESC
    LIMIT 200
    """
    return run_query(query, params)


def get_historical_alert_counts(days: int = DEFAULT_LOOKBACK_DAYS):
    """Get counts of all failures in time period."""
    query = f"""
    SELECT
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.elementary_test_results
         WHERE detected_at >= DATEADD(day, -{days}, SYSDATE())
         AND status IN ('fail', 'error', 'warn')) as failed_tests,
        (SELECT COUNT(*) FROM {ELEMENTARY_SCHEMA}.dbt_run_results
         WHERE generated_at >= DATEADD(day, -{days}, SYSDATE())
         AND resource_type IN ('model', 'seed', 'snapshot')
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
        r.detected_at,
        t.unique_id IS NOT NULL as is_current
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    WHERE r.detected_at >= DATEADD(day, -{days}, SYSDATE())
    ORDER BY r.test_unique_id, r.detected_at ASC
    """
    return run_query(query)


def get_latest_run_issues():
    """Model, seed, snapshot and test issues from the most recent build
    invocation only. event_at is UTC."""
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
            TRY_TO_TIMESTAMP_NTZ(r.generated_at) as event_at,
            r.message as summary,
            r.unique_id as unique_id,
            r.resource_type
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
        WHERE r.resource_type IN ('model', 'seed', 'snapshot')
          AND r.status IN ('fail', 'error')
    ),
    test_issues AS (
        SELECT
            COALESCE(r.table_name, COALESCE(t.short_name, r.test_name)) as object_name,
            'Test' as issue_type,
            CASE WHEN r.status = 'warn' THEN 'warn' ELSE 'fail' END as current_status,
            COUNT(*) as issue_count,
            MAX(r.detected_at) as event_at,
            ANY_VALUE(COALESCE(t.short_name, r.test_name)) as summary,
            NULL as unique_id,
            'test' as resource_type
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
        WHERE r.status IN ('fail', 'error', 'warn')
        GROUP BY 1, 2, 3
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


def get_latest_build_summary():
    """Model, seed and snapshot status breakdown for the most recent build
    invocation (a model 'warn' built with warnings and counts as ok). Used to
    surface skip count and ground failures with impact."""
    query = f"""
    WITH latest_invocation AS (
        SELECT invocation_id, created_at, run_started_at, command, selected
        FROM {ELEMENTARY_SCHEMA}.dbt_invocations
        WHERE LOWER(command) LIKE '%build%'
        ORDER BY created_at DESC
        LIMIT 1
    ),
    model_agg AS (
        SELECT
            COUNT_IF(r.status IN ('success', 'warn')) as success_count,
            COUNT_IF(r.status IN ('fail', 'error')) as failed_count,
            COUNT_IF(r.status = 'skipped') as skipped_count,
            COUNT(*) as total_count
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
        WHERE r.resource_type IN ('model', 'seed', 'snapshot')
    ),
    test_agg AS (
        SELECT
            COUNT_IF(r.status IN ('fail', 'error')) as test_failed_count,
            COUNT_IF(r.status = 'warn') as test_warned_count
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        JOIN latest_invocation i ON r.invocation_id = i.invocation_id
    )
    SELECT
        i.invocation_id,
        i.created_at,
        i.run_started_at,
        i.command,
        i.selected,
        m.success_count,
        m.failed_count,
        m.skipped_count,
        m.total_count,
        t.test_failed_count,
        t.test_warned_count
    FROM latest_invocation i, model_agg m, test_agg t
    """
    return run_query(query)


def get_downstream_skips(invocation_id: str):
    """Per-failing-model blast radius: count of downstream models skipped in the
    same invocation. Walks the model DAG (dbt_models.depends_on_nodes) from each
    errored model and intersects with models skipped in that run.

    Note: a skipped model downstream of several failures counts for each root, so
    these are blast-radius figures and do not sum to the run's total skip count.
    """
    query = f"""
    WITH edges AS (
        SELECT m.unique_id AS child, f.value::string AS parent
        FROM {ELEMENTARY_SCHEMA}.dbt_models m,
             LATERAL FLATTEN(input => PARSE_JSON(m.depends_on_nodes)) f
    ),
    run AS (
        SELECT unique_id, status
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE invocation_id = ?
          AND resource_type IN ('model', 'seed', 'snapshot')
    ),
    errored AS (SELECT unique_id FROM run WHERE status IN ('error', 'fail')),
    skipped AS (SELECT unique_id FROM run WHERE status = 'skipped'),
    downstream(root, node, depth) AS (
        SELECT unique_id, unique_id, 0 FROM errored
        UNION ALL
        SELECT d.root, e.child, d.depth + 1
        FROM downstream d
        JOIN edges e ON e.parent = d.node
        WHERE d.depth < 50
    )
    SELECT d.root AS unique_id, COUNT(DISTINCT s.unique_id) AS downstream_skipped
    FROM downstream d
    JOIN skipped s ON s.unique_id = d.node
    GROUP BY d.root
    """
    return run_query(query, (invocation_id,))


def get_downstream_model_counts(unique_ids):
    """Transitive count of models that depend on each given model (static DAG
    impact / dependents), walking dbt_models.depends_on_nodes."""
    ids = sorted({str(u) for u in unique_ids if u})
    if not ids:
        return run_query("SELECT NULL AS unique_id, 0 AS downstream_count WHERE 1=0")
    query = f"""
    WITH edges AS (
        SELECT m.unique_id AS child, f.value::string AS parent
        FROM {ELEMENTARY_SCHEMA}.dbt_models m,
             LATERAL FLATTEN(input => PARSE_JSON(m.depends_on_nodes)) f
    ),
    roots AS (SELECT value::string AS unique_id FROM TABLE(FLATTEN(input => PARSE_JSON(?)))),
    downstream(root, node, depth) AS (
        SELECT unique_id, unique_id, 0 FROM roots
        UNION ALL
        SELECT d.root, e.child, d.depth + 1
        FROM downstream d
        JOIN edges e ON e.parent = d.node
        WHERE d.depth < 50
    )
    SELECT root AS unique_id,
           COUNT(DISTINCT CASE WHEN node <> root THEN node END) AS downstream_count
    FROM downstream
    GROUP BY root
    """
    return run_query(query, (json.dumps(ids),))


def get_latest_build_test_results():
    """Per-test failures/warnings from the most recent build, with the detail
    needed to see what broke (accepted values, failing-row count, sample rows,
    query)."""
    query = f"""
    WITH latest_invocation AS (
        SELECT invocation_id, created_at
        FROM {ELEMENTARY_SCHEMA}.dbt_invocations
        WHERE LOWER(command) LIKE '%build%'
        ORDER BY created_at DESC
        LIMIT 1
    )
    SELECT
        r.test_unique_id,
        COALESCE(t.short_name, r.test_short_name, r.test_name) as test_name,
        COALESCE(t.test_namespace, r.test_sub_type, r.test_type) as test_namespace,
        r.table_name,
        r.column_name,
        r.status,
        r.failures,
        r.failed_row_count,
        r.test_params,
        r.result_rows,
        r.test_results_description,
        r.test_results_query,
        r.detected_at,
        t.original_path,
        t.description as test_description,
        t.severity,
        m.database_name as model_database,
        m.schema_name as model_schema,
        COALESCE(m.alias, m.name) as model_relation
    FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
    JOIN latest_invocation i ON r.invocation_id = i.invocation_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON r.test_unique_id = t.unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON m.unique_id = t.parent_model_unique_id
    WHERE r.status IN ('fail', 'error', 'warn')
    ORDER BY
        CASE r.status WHEN 'error' THEN 0 WHEN 'fail' THEN 0 ELSE 1 END,
        r.table_name,
        test_name
    """
    return run_query(query)
