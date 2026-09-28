"""Open-issue signals for Home: failing, warning and skipped nodes and tests,
stale outputs and row-count drops.

Issues are read from the latest results of the nodes and tests in the current
manifest, over all history (not the sidebar range). Every query takes as_of (a
UTC timestamp string) to replay a past moment; None means now. Times returned
are UTC.
"""

from config import (
    ELEMENTARY_SCHEMA,
    DEFAULT_LOOKBACK_DAYS,
    ROW_DROP_PCT,
    STALE_GAP_MULTIPLIER,
    STALE_LOOKBACK_DAYS,
    STALE_MIN_AGE_HOURS,
    STALE_MIN_BUILDS,
)
from database import run_query
from services.jobs_service import job_columns_sql, trigger_sql

# Resource types dbt builds as relations and reports in dbt_run_results.
BUILT_TYPES = "('model', 'seed', 'snapshot')"

_CLOCK = "clock AS (SELECT COALESCE(TRY_TO_TIMESTAMP_NTZ(?), SYSDATE()) AS now_utc)"

# Current manifest nodes that dbt builds (models, snapshots, seeds).
_NODES = f"""
    nodes AS (
        SELECT unique_id, name, 'model' AS resource_type, materialization, package_name
        FROM {ELEMENTARY_SCHEMA}.dbt_models
        UNION ALL
        SELECT unique_id, name, 'snapshot', 'snapshot', package_name
        FROM {ELEMENTARY_SCHEMA}.dbt_snapshots
        UNION ALL
        SELECT unique_id, name, 'seed', 'seed', package_name
        FROM {ELEMENTARY_SCHEMA}.dbt_seeds
    )"""


def get_node_issues(days: int = DEFAULT_LOOKBACK_DAYS, as_of: str | None = None):
    """Models, seeds and snapshots with an open issue. STATUS is the latest
    result that was not skipped when it is fail, error or warn (a skip does
    not fix a failure), else 'skipped' when the latest result was skipped.
    A model 'warn' still built the relation.

    STREAK_*: first result of the open streak (first fail/error since the last
    success or warn; for warnings, first warn since the last success) with the
    trigger, commit and GitHub run of its invocation. LATEST_*: the latest
    result, which may be a skip. FAILURES_IN_RANGE counts fail/error results in
    the last `days` days."""
    query = f"""
    WITH {_CLOCK},
    {_NODES},
    results AS (
        SELECT
            r.unique_id,
            r.invocation_id,
            r.status,
            r.message,
            TRY_TO_TIMESTAMP_NTZ(r.generated_at) AS result_at,
            r.created_at
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        JOIN nodes n ON n.unique_id = r.unique_id
        CROSS JOIN clock c
        WHERE r.resource_type IN {BUILT_TYPES}
          AND TRY_TO_TIMESTAMP_NTZ(r.generated_at) <= c.now_utc
    ),
    latest AS (
        SELECT *
        FROM results
        QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY result_at DESC, created_at DESC) = 1
    ),
    latest_real AS (
        SELECT *
        FROM results
        WHERE status <> 'skipped'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY result_at DESC, created_at DESC) = 1
    ),
    open_nodes AS (
        SELECT
            l.unique_id,
            IFF(lr.status IN ('fail', 'error', 'warn'), lr.status, l.status) AS status,
            l.status AS latest_status,
            l.result_at AS latest_at,
            l.invocation_id AS latest_invocation_id,
            lr.status AS last_real_status,
            lr.result_at AS last_real_at,
            lr.message
        FROM latest l
        LEFT JOIN latest_real lr ON lr.unique_id = l.unique_id
        WHERE lr.status IN ('fail', 'error', 'warn') OR l.status = 'skipped'
    ),
    agg AS (
        SELECT
            r.unique_id,
            MAX(IFF(r.status = 'success', r.result_at, NULL)) AS last_success_at,
            MAX(IFF(r.status IN ('success', 'warn'), r.result_at, NULL)) AS last_built_at,
            COUNT_IF(r.status IN ('fail', 'error') AND r.result_at >= DATEADD(day, -{days}, c.now_utc)) AS failures_in_range
        FROM results r
        JOIN open_nodes o ON o.unique_id = r.unique_id
        CROSS JOIN clock c
        GROUP BY r.unique_id
    ),
    streak AS (
        SELECT r.unique_id, r.invocation_id, r.result_at
        FROM results r
        JOIN open_nodes o ON o.unique_id = r.unique_id
        JOIN agg a ON a.unique_id = r.unique_id
        WHERE (o.status IN ('fail', 'error') AND r.status IN ('fail', 'error')
               AND r.result_at > COALESCE(a.last_built_at, '1970-01-01'::TIMESTAMP_NTZ))
           OR (o.status = 'warn' AND r.status = 'warn'
               AND r.result_at > COALESCE(a.last_success_at, '1970-01-01'::TIMESTAMP_NTZ))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.unique_id ORDER BY r.result_at, r.created_at) = 1
    )
    SELECT
        n.unique_id,
        n.name,
        n.resource_type,
        n.materialization,
        o.status,
        o.latest_status,
        o.latest_at,
        o.latest_invocation_id,
        o.last_real_status,
        o.last_real_at,
        o.message,
        a.failures_in_range,
        s.result_at AS streak_started_at,
        s.invocation_id AS streak_invocation_id,
        i.run_started_at AS streak_run_started_at,
        {job_columns_sql('i')},
        NULLIF(i.git_sha, '') AS git_sha,
        NULLIF(i.job_run_url, '') AS job_run_url
    FROM open_nodes o
    JOIN nodes n ON n.unique_id = o.unique_id
    JOIN agg a ON a.unique_id = o.unique_id
    LEFT JOIN streak s ON s.unique_id = o.unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_invocations i ON i.invocation_id = s.invocation_id
    ORDER BY
        CASE WHEN o.status IN ('fail', 'error') THEN 0 WHEN o.status = 'warn' THEN 1 ELSE 2 END,
        s.result_at,
        n.name
    """
    return run_query(query, (as_of,))


def get_test_issues(days: int = DEFAULT_LOOKBACK_DAYS, as_of: str | None = None):
    """Tests with an open issue, with the columns the test issue cards need.
    STATUS follows the rule of get_node_issues: the latest result that was not
    skipped when it is fail, error or warn, else 'skipped' when the latest
    result was skipped. Detail columns come from that result.

    STREAK_*: first fail/error since the last pass or warn (for warnings, first
    warn since the last pass) with the trigger, commit and GitHub run of its
    invocation. LATEST_*: the latest result, which may be a skip."""
    query = f"""
    WITH {_CLOCK},
    results AS (
        SELECT r.test_unique_id, r.invocation_id, r.status, r.detected_at, r.created_at
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results r
        JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON t.unique_id = r.test_unique_id
        CROSS JOIN clock c
        WHERE r.detected_at <= c.now_utc
    ),
    latest AS (
        SELECT *
        FROM results
        QUALIFY ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY detected_at DESC, created_at DESC) = 1
    ),
    latest_real AS (
        SELECT *
        FROM results
        WHERE status <> 'skipped'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY detected_at DESC, created_at DESC) = 1
    ),
    open_tests AS (
        SELECT
            l.test_unique_id,
            IFF(lr.status IN ('fail', 'error', 'warn'), lr.status, l.status) AS status,
            l.status AS latest_status,
            l.detected_at AS latest_at,
            l.invocation_id AS latest_invocation_id,
            lr.status AS last_real_status,
            lr.detected_at AS last_real_at,
            COALESCE(lr.invocation_id, l.invocation_id) AS detail_invocation_id
        FROM latest l
        LEFT JOIN latest_real lr ON lr.test_unique_id = l.test_unique_id
        WHERE lr.status IN ('fail', 'error', 'warn') OR l.status = 'skipped'
    ),
    agg AS (
        SELECT
            r.test_unique_id,
            MAX(IFF(r.status = 'pass', r.detected_at, NULL)) AS last_pass_at,
            MAX(IFF(r.status IN ('pass', 'warn'), r.detected_at, NULL)) AS last_ok_at,
            COUNT_IF(r.status IN ('fail', 'error') AND r.detected_at >= DATEADD(day, -{days}, c.now_utc)) AS failures_in_range
        FROM results r
        JOIN open_tests o ON o.test_unique_id = r.test_unique_id
        CROSS JOIN clock c
        GROUP BY r.test_unique_id
    ),
    streak AS (
        SELECT r.test_unique_id, r.invocation_id, r.detected_at
        FROM results r
        JOIN open_tests o ON o.test_unique_id = r.test_unique_id
        JOIN agg a ON a.test_unique_id = r.test_unique_id
        WHERE (o.status IN ('fail', 'error') AND r.status IN ('fail', 'error')
               AND r.detected_at > COALESCE(a.last_ok_at, '1970-01-01'::TIMESTAMP_NTZ))
           OR (o.status = 'warn' AND r.status = 'warn'
               AND r.detected_at > COALESCE(a.last_pass_at, '1970-01-01'::TIMESTAMP_NTZ))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.test_unique_id ORDER BY r.detected_at, r.created_at) = 1
    )
    SELECT
        o.test_unique_id,
        COALESCE(t.short_name, d.test_short_name, d.test_name) AS test_name,
        COALESCE(t.test_namespace, d.test_sub_type, d.test_type) AS test_namespace,
        d.table_name,
        d.column_name,
        o.status,
        o.latest_status,
        o.latest_at,
        o.latest_invocation_id,
        o.last_real_status,
        o.last_real_at,
        d.failures,
        d.failed_row_count,
        d.test_params,
        d.result_rows,
        d.test_results_description,
        d.test_results_query,
        d.detected_at,
        t.original_path,
        t.severity,
        t.parent_model_unique_id,
        m.database_name AS model_database,
        m.schema_name AS model_schema,
        COALESCE(m.alias, m.name) AS model_relation,
        a.failures_in_range,
        s.detected_at AS streak_started_at,
        s.invocation_id AS streak_invocation_id,
        i.run_started_at AS streak_run_started_at,
        {job_columns_sql('i')},
        NULLIF(i.git_sha, '') AS git_sha,
        NULLIF(i.job_run_url, '') AS job_run_url
    FROM open_tests o
    JOIN {ELEMENTARY_SCHEMA}.elementary_test_results d
      ON d.test_unique_id = o.test_unique_id AND d.invocation_id = o.detail_invocation_id
    JOIN {ELEMENTARY_SCHEMA}.dbt_tests t ON t.unique_id = o.test_unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON m.unique_id = t.parent_model_unique_id
    JOIN agg a ON a.test_unique_id = o.test_unique_id
    LEFT JOIN streak s ON s.test_unique_id = o.test_unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_invocations i ON i.invocation_id = s.invocation_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.test_unique_id ORDER BY d.created_at DESC) = 1
    ORDER BY
        CASE WHEN o.status IN ('fail', 'error') THEN 0 WHEN o.status = 'warn' THEN 1 ELSE 2 END,
        d.table_name,
        test_name
    """
    return run_query(query, (as_of,))


def get_stale_outputs(as_of: str | None = None):
    """Tables, incremental models and snapshots whose last successful build is
    overdue against their own scheduled cadence (thresholds in config:
    STALE_*). Excluded: views, ephemeral models and semantic views (no stored
    data), seeds (CSV files that change only on deploy) and elementary's own
    models (written by run hooks, not builds)."""
    query = f"""
    WITH {_CLOCK},
    {_NODES},
    outputs AS (
        SELECT *
        FROM nodes
        WHERE materialization IN ('table', 'incremental', 'snapshot')
          AND COALESCE(package_name, '') <> 'elementary'
    ),
    builds AS (
        -- A model 'warn' still built the relation.
        SELECT DISTINCT
            r.unique_id,
            r.invocation_id,
            TRY_TO_TIMESTAMP_NTZ(r.generated_at) AS built_at,
            {trigger_sql('i')} IN ('schedule', 'manual') AS is_scheduled
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results r
        JOIN outputs o ON o.unique_id = r.unique_id
        LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_invocations i ON i.invocation_id = r.invocation_id
        CROSS JOIN clock c
        WHERE r.resource_type IN {BUILT_TYPES}
          AND r.status IN ('success', 'warn')
          AND TRY_TO_TIMESTAMP_NTZ(r.generated_at) <= c.now_utc
    ),
    last_build AS (
        SELECT unique_id, MAX(built_at) AS last_success_at
        FROM builds
        GROUP BY unique_id
    ),
    build_days AS (
        -- One row per London day with a scheduled build, in the lookback before the last success.
        SELECT DISTINCT
            b.unique_id,
            DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'Europe/London', b.built_at)) AS build_day
        FROM builds b
        JOIN last_build l ON l.unique_id = b.unique_id
        WHERE b.is_scheduled
          AND b.built_at >= DATEADD(day, -{STALE_LOOKBACK_DAYS}, l.last_success_at)
    ),
    cadence AS (
        SELECT unique_id, COUNT(*) AS scheduled_build_days, MEDIAN(gap_hours) AS typical_gap_hours
        FROM (
            SELECT
                unique_id,
                DATEDIFF('hour', LAG(build_day) OVER (PARTITION BY unique_id ORDER BY build_day), build_day) AS gap_hours
            FROM build_days
        )
        GROUP BY unique_id
    )
    SELECT
        o.unique_id,
        o.name,
        o.resource_type,
        o.materialization,
        l.last_success_at,
        k.typical_gap_hours,
        k.scheduled_build_days,
        DATEDIFF('second', l.last_success_at, c.now_utc) / 3600 AS hours_since_success,
        GREATEST({STALE_GAP_MULTIPLIER} * k.typical_gap_hours, {STALE_MIN_AGE_HOURS}) AS stale_after_hours
    FROM outputs o
    JOIN last_build l ON l.unique_id = o.unique_id
    JOIN cadence k ON k.unique_id = o.unique_id
    CROSS JOIN clock c
    WHERE k.scheduled_build_days >= {STALE_MIN_BUILDS}
      AND DATEDIFF('second', l.last_success_at, c.now_utc) / 3600
          > GREATEST({STALE_GAP_MULTIPLIER} * k.typical_gap_hours, {STALE_MIN_AGE_HOURS})
    ORDER BY hours_since_success DESC, o.name
    """
    return run_query(query, (as_of,))


def get_row_count_drops(as_of: str | None = None):
    """Current models whose latest logged row count is 0 after a non-zero run,
    or fell more than ROW_DROP_PCT against the previous run. Only the latest run
    counts. A model published to several databases logs one row per copy; the
    latest recorded copy per invocation is kept, as on model detail."""
    query = f"""
    WITH {_CLOCK},
    models AS (
        SELECT unique_id, name, LOWER(name) AS model_key
        FROM {ELEMENTARY_SCHEMA}.dbt_models
    ),
    per_run AS (
        SELECT m.unique_id, m.name, rc.invocation_id, rc.run_started_at, rc.recorded_at, rc.row_count
        FROM {ELEMENTARY_SCHEMA}.ROW_COUNT_LOG rc
        JOIN models m ON LOWER(rc.model_name) = m.model_key
        CROSS JOIN clock c
        WHERE rc.run_started_at <= c.now_utc
        QUALIFY ROW_NUMBER() OVER (PARTITION BY m.unique_id, rc.invocation_id ORDER BY rc.recorded_at DESC) = 1
    ),
    ranked AS (
        SELECT
            *,
            LAG(row_count) OVER (PARTITION BY unique_id ORDER BY run_started_at, recorded_at) AS previous_row_count,
            ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY run_started_at DESC, recorded_at DESC) AS rn
        FROM per_run
    )
    SELECT
        r.unique_id,
        r.name,
        r.invocation_id,
        r.run_started_at,
        r.previous_row_count,
        r.row_count,
        (r.row_count - r.previous_row_count) / r.previous_row_count * 100 AS change_pct,
        {job_columns_sql('i')},
        NULLIF(i.git_sha, '') AS git_sha,
        NULLIF(i.job_run_url, '') AS job_run_url
    FROM ranked r
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_invocations i ON i.invocation_id = r.invocation_id
    WHERE r.rn = 1
      AND r.previous_row_count > 0
      AND r.row_count < r.previous_row_count * (1 - {ROW_DROP_PCT} / 100)
    ORDER BY change_pct, r.name
    """
    return run_query(query, (as_of,))
