"""Model runtime queries for the Performance page and run timeline helpers."""

import heapq
from collections import defaultdict, deque

from database import run_query
from config import (
    DEFAULT_LOOKBACK_DAYS,
    ELEMENTARY_SCHEMA,
    SLOWDOWN_BASELINE_DAYS,
    SLOWDOWN_MIN_EXTRA_SECONDS,
    SLOWDOWN_MIN_PRIOR_RUNS,
    SLOWDOWN_RATIO,
)


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


def get_slowdowns(days: int = DEFAULT_LOOKBACK_DAYS):
    """Models whose latest successful run (in the last `days`) took at least
    SLOWDOWN_RATIO x the median of their successful runs in the
    SLOWDOWN_BASELINE_DAYS before it, and at least SLOWDOWN_MIN_EXTRA_SECONDS
    longer. Needs SLOWDOWN_MIN_PRIOR_RUNS prior runs."""
    query = f"""
    WITH runs AS (
        SELECT
            unique_id,
            name,
            invocation_id,
            execution_time,
            generated_at,
            TRY_TO_TIMESTAMP(generated_at) as ran_at,
            ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) as rn
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE resource_type = 'model'
        AND status = 'success'
        AND generated_at >= DATEADD(day, -{days + SLOWDOWN_BASELINE_DAYS}, CURRENT_TIMESTAMP())
    ),
    latest AS (
        SELECT * FROM runs
        WHERE rn = 1 AND ran_at >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ),
    baseline AS (
        SELECT r.unique_id, MEDIAN(r.execution_time) as median_time, COUNT(*) as prior_runs
        FROM runs r
        JOIN latest l ON l.unique_id = r.unique_id
        WHERE r.rn > 1 AND r.ran_at >= DATEADD(day, -{SLOWDOWN_BASELINE_DAYS}, l.ran_at)
        GROUP BY r.unique_id
    )
    SELECT
        l.unique_id,
        l.name,
        m.schema_name,
        l.invocation_id,
        l.generated_at,
        l.execution_time as latest_time,
        b.median_time,
        l.execution_time / NULLIF(b.median_time, 0) as ratio,
        l.execution_time - b.median_time as extra_time,
        b.prior_runs
    FROM latest l
    JOIN baseline b ON b.unique_id = l.unique_id
    LEFT JOIN {ELEMENTARY_SCHEMA}.dbt_models m ON m.unique_id = l.unique_id
    WHERE b.prior_runs >= {SLOWDOWN_MIN_PRIOR_RUNS}
    AND l.execution_time >= {SLOWDOWN_RATIO} * b.median_time
    AND l.execution_time - b.median_time >= {SLOWDOWN_MIN_EXTRA_SECONDS}
    ORDER BY extra_time DESC
    """
    return run_query(query)


def get_run_model_edges(invocation_id: str):
    """Dependencies between models of one run (child depends on parent), from
    depends_on_nodes of the current manifest. A path through nodes outside the
    run (snapshots, unselected or ephemeral models) counts as a direct
    dependency, as dbt keeps that order when it runs a subset."""
    query = f"""
    WITH run AS (
        SELECT DISTINCT unique_id
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results
        WHERE invocation_id = ?
        AND resource_type = 'model'
    ),
    node_edges AS (
        SELECT m.unique_id as child, f.value::string as parent
        FROM {ELEMENTARY_SCHEMA}.dbt_models m,
             LATERAL FLATTEN(input => PARSE_JSON(m.depends_on_nodes)) f
        UNION
        SELECT s.unique_id, f.value::string
        FROM {ELEMENTARY_SCHEMA}.dbt_snapshots s,
             LATERAL FLATTEN(input => PARSE_JSON(s.depends_on_nodes)) f
    ),
    flagged AS (
        SELECT e.child, e.parent, r.unique_id IS NOT NULL as parent_in_run
        FROM node_edges e
        LEFT JOIN run r ON r.unique_id = e.parent
    ),
    up (child, parent, parent_in_run, depth) AS (
        SELECT f.child, f.parent, f.parent_in_run, 1
        FROM flagged f
        JOIN run r ON r.unique_id = f.child
        UNION ALL
        SELECT u.child, f.parent, f.parent_in_run, u.depth + 1
        FROM up u
        JOIN flagged f ON f.child = u.parent
        WHERE NOT u.parent_in_run AND u.depth < 20
    )
    SELECT DISTINCT child, parent
    FROM up
    WHERE parent_in_run AND child <> parent
    """
    return run_query(query, (invocation_id,))


# --- run timeline (pure functions, no queries) -------------------------------

def pack_lanes(starts, ends) -> list:
    """Lane index per bar. Bars are taken in start order and put in the
    lowest-numbered lane free at their start, so the lane count equals the
    most bars running at once."""
    order = sorted(range(len(starts)), key=lambda i: (starts[i], ends[i]))
    lanes = [0] * len(starts)
    busy = []  # (end, lane)
    free = []  # lane numbers
    count = 0
    for i in order:
        while busy and busy[0][0] <= starts[i]:
            heapq.heappush(free, heapq.heappop(busy)[1])
        if free:
            lane = heapq.heappop(free)
        else:
            lane = count
            count += 1
        lanes[i] = lane
        heapq.heappush(busy, (ends[i], lane))
    return lanes


def _parents(edges) -> dict:
    """edges: iterable of (child, parent) -> {child: {parents}}."""
    parents = defaultdict(set)
    for child, parent in edges:
        if child != parent:
            parents[child].add(parent)
    return parents


def replay_schedule(durations: dict, edges):
    """(start, end) dicts from starting each model when its last parent in the
    run ends, with no thread limit. Used when a run has no recorded times;
    models on a dependency cycle are left out."""
    parents = _parents((c, p) for c, p in edges if c in durations and p in durations)
    children = defaultdict(list)
    for child, ps in parents.items():
        for p in ps:
            children[p].append(child)
    waiting = {u: len(parents.get(u, ())) for u in durations}
    queue = deque(u for u, n in waiting.items() if n == 0)
    start, end = {}, {}
    while queue:
        u = queue.popleft()
        start[u] = max((end[p] for p in parents.get(u, ())), default=0.0)
        end[u] = start[u] + (durations[u] or 0.0)
        for c in children[u]:
            waiting[c] -= 1
            if waiting[c] == 0:
                queue.append(c)
    return start, end


def longest_chain(start: dict, end: dict, edges, tolerance: float = 1.0) -> list:
    """Unique ids of the chain that ends the run, first to last: from the
    last-finishing model, step to the parent in the run that finished last
    before it started (within `tolerance` seconds), until there is none."""
    if not end:
        return []
    parents = _parents(edges)
    current = max(end, key=end.get)
    chain, seen = [current], {current}
    while True:
        options = [
            p for p in parents.get(current, ())
            if p in end and p not in seen and end[p] <= start[current] + tolerance
        ]
        if not options:
            break
        current = max(options, key=end.get)
        chain.append(current)
        seen.add(current)
    return chain[::-1]
