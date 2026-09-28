"""Jobs: which GitHub Actions job (or local run) produced each dbt invocation,
invocations grouped into job runs, and schedule checks (start delay, missing
runs) against config.JOB_SCHEDULES. Also the per-invocation status counts
that decide a run's outcome, shared with the Runs and Home queries.

Elementary's job_name/job_id are empty, so the job comes from the trigger
(cause_category) and the dbt selection. All times are naive UTC
(run_started_at), as are the cron schedules; the Jobs page converts them to
Europe/London for display.
"""

from datetime import timedelta

import pandas as pd

from components.formatting import run_failed, to_local
from config import (
    DBT_REPO_URL,
    ELEMENTARY_SCHEMA,
    JOB_GRACE_HOURS,
    JOB_SCHEDULES,
    JOB_SLOT_WINDOW_HOURS,
)
from database import run_query

# Display order and labels. Keys for scheduled jobs match the workflow's
# run_type names (GitHub shows runs as "dbt <run_type>").
JOB_LABELS = {
    "daily": "Daily build",
    "weekly": "Weekly build",
    "monthly-full-refresh": "Monthly full refresh",
    "sdl-intraday": "SDL intraday",
    "snapshots": "Snapshots",
    "deploy": "Deploy",
    "manual": "Manual",
    "local": "Local",
    "other": "Other",
}
TRIGGER_LABELS = {
    "schedule": "Schedule",
    "push": "Push to main",
    "manual": "Manual",
    "local": "Local",
    "snowflake-task": "Snowflake task",
}

# Before 2026-08-07 scheduled runs ran as Snowflake tasks: this target, no cause.
SNOWFLAKE_TASK_TARGET = "snowflake-prod"

# Failure issues opened by report_dbt_failures.py. sdl-intraday opens none.
_SCHEDULED_ISSUE_LABEL = "dbt-scheduled-run-failure"
_DEPLOY_ISSUE_LABEL = "dbt-run-failure"
ISSUE_LABELS = {
    "daily": _SCHEDULED_ISSUE_LABEL,
    "weekly": _SCHEDULED_ISSUE_LABEL,
    "monthly-full-refresh": _SCHEDULED_ISSUE_LABEL,
    "snapshots": _SCHEDULED_ISSUE_LABEL,
    "deploy": _DEPLOY_ISSUE_LABEL,
    "manual": _DEPLOY_ISSUE_LABEL,  # dbt-deploy.yml dispatch (full build)
}


def job_label(job) -> str:
    return JOB_LABELS.get(job, str(job or "Other").replace("-", " ").capitalize())


def trigger_label(trigger) -> str:
    return TRIGGER_LABELS.get(trigger, str(trigger or "").replace("_", " ").capitalize())


# --- classification (SQL) ------------------------------------------------------

def job_type_sql(alias: str = "i") -> str:
    """SQL CASE giving the job of a dbt_invocations row. Rules, in order:
    push -> deploy; no cause (and not a Snowflake task) -> local; snapshot
    command -> snapshots; selection containing tag:daily -> daily, sdl_wnl ->
    sdl-intraday, staging with full refresh -> monthly-full-refresh, staging
    -> weekly; other workflow_dispatch -> manual; else other."""
    a = alias
    return f"""CASE
        WHEN {a}.cause_category = 'push' THEN 'deploy'
        WHEN {a}.cause_category IS NULL AND COALESCE({a}.target_name, '') <> '{SNOWFLAKE_TASK_TARGET}' THEN 'local'
        WHEN {a}.command = 'snapshot' THEN 'snapshots'
        WHEN CONTAINS({a}.selected, 'tag:daily') THEN 'daily'
        WHEN CONTAINS({a}.selected, 'sdl_wnl') THEN 'sdl-intraday'
        WHEN CONTAINS({a}.selected, 'staging') AND {a}.full_refresh THEN 'monthly-full-refresh'
        WHEN CONTAINS({a}.selected, 'staging') THEN 'weekly'
        WHEN {a}.cause_category = 'workflow_dispatch' THEN 'manual'
        ELSE 'other'
    END"""


def trigger_sql(alias: str = "i") -> str:
    """SQL CASE giving what started the invocation."""
    a = alias
    return f"""CASE
        WHEN {a}.cause_category = 'schedule' THEN 'schedule'
        WHEN {a}.cause_category = 'push' THEN 'push'
        WHEN {a}.cause_category = 'workflow_dispatch' THEN 'manual'
        WHEN {a}.cause_category IS NULL AND {a}.target_name = '{SNOWFLAKE_TASK_TARGET}' THEN 'snowflake-task'
        WHEN {a}.cause_category IS NULL THEN 'local'
        ELSE LOWER({a}.cause_category)
    END"""


def job_columns_sql(alias: str = "i") -> str:
    """Select-list fragment adding JOB_TYPE and TRIGGER_TYPE."""
    return f"{job_type_sql(alias)} AS job_type,\n        {trigger_sql(alias)} AS trigger_type"


def hide_compile_show_sql(alias: str = "i") -> str:
    """SQL condition dropping local compile and show invocations."""
    return f"NOT (({job_type_sql(alias)}) = 'local' AND COALESCE({alias}.command, '') IN ('compile', 'show'))"


# --- run counts (SQL) ----------------------------------------------------------

# Resource types dbt builds as relations and reports in dbt_run_results.
BUILT_TYPES = "('model', 'seed', 'snapshot')"


def run_counts_sql(scope: str = "") -> str:
    """CTEs run_stats (models, seeds and snapshots by status) and test_stats
    (tests by status), one row per invocation_id. scope: optional
    'JOIN <cte> ON <cte>.invocation_id = x.invocation_id' limiting the rows
    read (x is the source alias). A run fails when either has a fail or error
    (components.formatting.run_failed)."""
    return f"""run_stats AS (
        SELECT
            x.invocation_id,
            COUNT(*) AS models_run,
            COUNT_IF(x.status = 'success') AS success_count,
            COUNT_IF(x.status IN ('fail', 'error')) AS fail_count,
            COUNT_IF(x.status = 'skipped') AS skipped_count
        FROM {ELEMENTARY_SCHEMA}.dbt_run_results x
        {scope}
        WHERE x.resource_type IN {BUILT_TYPES}
        GROUP BY x.invocation_id
    ),
    test_stats AS (
        SELECT
            x.invocation_id,
            COUNT(*) AS tests_run,
            COUNT_IF(x.status = 'pass') AS tests_passed,
            COUNT_IF(x.status IN ('fail', 'error')) AS tests_failed,
            COUNT_IF(x.status = 'warn') AS tests_warned
        FROM {ELEMENTARY_SCHEMA}.elementary_test_results x
        {scope}
        GROUP BY x.invocation_id
    )"""


def run_counts_columns_sql() -> str:
    """Select-list fragment with the run_counts_sql counts, 0 when none;
    run_stats joined as s and test_stats as t."""
    cols = [("s", c) for c in ("models_run", "success_count", "fail_count", "skipped_count")]
    cols += [("t", c) for c in ("tests_run", "tests_passed", "tests_failed", "tests_warned")]
    return ",\n        ".join(f"COALESCE({a}.{c}, 0) AS {c}" for a, c in cols)


# --- GitHub links ----------------------------------------------------------------

def commit_url(sha) -> str | None:
    return f"{DBT_REPO_URL}/commit/{sha}" if isinstance(sha, str) and sha.strip() else None


def run_links(row) -> list[tuple[str, str]]:
    """(label, url) links for an invocation or job run: GitHub run, commit and
    open failure issues for its job. Local runs have none."""
    def text(col):
        value = row.get(col)
        return "" if value is None or pd.isna(value) else str(value).strip()

    links = []
    run_url, run_id, sha = text("JOB_RUN_URL"), text("JOB_RUN_ID"), text("GIT_SHA")
    if run_url:
        links.append((f"GitHub run {run_id}".strip(), run_url))
    if sha:
        links.append((f"commit {sha[:7]}", commit_url(sha)))
    label = ISSUE_LABELS.get(text("JOB_TYPE"))
    if run_url and label:
        links.append(("open failure issues", f"{DBT_REPO_URL}/issues?q=is%3Aissue+is%3Aopen+label%3A{label}"))
    return links


# --- data --------------------------------------------------------------------------

def get_job_invocations(days: int):
    """Invocations started in the last days + 1 days (UTC) with job, trigger,
    GitHub fields and model (with seed and snapshot) and test counts; local
    compile and show left out. The extra day lets runs early in the range find
    their slots."""
    query = f"""
    WITH inv AS (
        SELECT
            i.invocation_id,
            NULLIF(i.job_run_id, '') AS job_run_id,
            NULLIF(i.job_run_url, '') AS job_run_url,
            NULLIF(i.git_sha, '') AS git_sha,
            i.command,
            LEFT(i.selected, 300) AS selected,
            {job_columns_sql()},
            TRY_TO_TIMESTAMP_NTZ(i.run_started_at) AS started_at,
            TRY_TO_TIMESTAMP_NTZ(i.run_completed_at) AS completed_at
        FROM {ELEMENTARY_SCHEMA}.dbt_invocations i
        WHERE TRY_TO_TIMESTAMP_NTZ(i.run_started_at) >= DATEADD(day, ?, SYSDATE())
          AND {hide_compile_show_sql()}
    ),
    {run_counts_sql("JOIN inv ON inv.invocation_id = x.invocation_id")}
    SELECT
        inv.*,
        TIMESTAMPDIFF('second', inv.started_at, inv.completed_at) AS duration_seconds,
        {run_counts_columns_sql()}
    FROM inv
    LEFT JOIN run_stats s ON s.invocation_id = inv.invocation_id
    LEFT JOIN test_stats t ON t.invocation_id = inv.invocation_id
    ORDER BY inv.started_at
    """
    return run_query(query, (-(int(days) + 1),))


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_localize(None)


# --- job runs ----------------------------------------------------------------------

def group_job_runs(inv: pd.DataFrame) -> pd.DataFrame:
    """One row per job run: invocations sharing a GitHub run id (a deploy's
    compile + build) or one invocation without one. Counts, outcome and
    duration come from the main invocation (the latest that is not a compile);
    STARTED_AT is the first invocation's start."""
    df = inv.copy()
    df["STARTED_AT"] = pd.to_datetime(df["STARTED_AT"])
    df["RUN_KEY"] = df["JOB_RUN_ID"].where(df["JOB_RUN_ID"].notna(), df["INVOCATION_ID"])
    df["_COMPILE"] = df["COMMAND"].eq("compile")
    df = df.sort_values(["RUN_KEY", "_COMPILE", "STARTED_AT"], ascending=[True, True, False])
    main = df.drop_duplicates("RUN_KEY").drop(columns=["STARTED_AT", "_COMPILE"])
    firsts = df.groupby("RUN_KEY").agg(STARTED_AT=("STARTED_AT", "min"), INVOCATIONS=("INVOCATION_ID", "size"))
    runs = main.merge(firsts, left_on="RUN_KEY", right_index=True)
    runs = runs.rename(columns={"INVOCATION_ID": "MAIN_INVOCATION_ID"})
    counts = ["FAIL_COUNT", "TESTS_FAILED", "TESTS_WARNED", "SKIPPED_COUNT", "SUCCESS_COUNT"]
    runs[counts] = runs[counts].fillna(0).astype(int)
    runs["FAILED"] = [run_failed(row) for _, row in runs.iterrows()]
    runs["DURATION_MIN"] = pd.to_numeric(runs["DURATION_SECONDS"], errors="coerce") / 60
    return runs.sort_values("STARTED_AT", ascending=False).reset_index(drop=True)


# --- schedules ---------------------------------------------------------------------

def expected_slots(job: str, start, end) -> list[pd.Timestamp]:
    """Scheduled start times of job in [start, end] (UTC)."""
    sched = JOB_SCHEDULES[job]
    weekdays, month_days = sched.get("weekdays"), sched.get("days")
    skip_days = sched.get("skip_days", ())
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    slots = []
    day = start.normalize()
    while day <= end:
        if (
            (weekdays is None or day.weekday() in weekdays)
            and (month_days is None or day.day in month_days)
            and day.day not in skip_days
        ):
            slots.extend(t for t in (day + timedelta(hours=h) for h in sched["hours"]) if start <= t <= end)
        day += timedelta(days=1)
    return slots


def match_slots(starts: pd.Series, slots: list) -> pd.Series:
    """Slot filled by each run: in start order, each run fills the earliest
    empty slot at most JOB_SLOT_WINDOW_HOURS before it. NaT = no slot."""
    window = timedelta(hours=JOB_SLOT_WINDOW_HOURS)
    open_slots = sorted(slots)
    filled = pd.Series(pd.NaT, index=starts.index, dtype="datetime64[ns]")
    for idx, start in starts.sort_values().items():
        for slot in open_slots:
            if slot <= start <= slot + window:
                filled[idx] = slot
                open_slots.remove(slot)
                break
    return filled


def next_slot(job: str, after) -> pd.Timestamp | None:
    after = pd.Timestamp(after)
    upcoming = expected_slots(job, after, after + timedelta(days=40))
    return next((s for s in upcoming if s > after), None)


def job_report(days: int, now=None):
    """(runs, summary, missing) for the Jobs page.

    runs: job runs started in the range. Runs that filled a slot have SLOT and
    FILLED_BY ('Schedule' or 'Manual run'); DELAY_H is set for scheduled ones.
    summary: one row per job (every scheduled job, plus others with runs).
    missing: slots in the range with no run after JOB_GRACE_HOURS.
    Scheduled runs fill slots first; then manual runs of the job fill slots
    still empty in the same window (a re-run of a missed build)."""
    now = utc_now() if now is None else pd.Timestamp(now)
    range_start = now - timedelta(days=days)
    grace = timedelta(hours=JOB_GRACE_HOURS)

    runs = group_job_runs(get_job_invocations(days))
    runs["SLOT"] = pd.Series(pd.NaT, index=runs.index, dtype="datetime64[ns]")
    runs["FILLED_BY"] = pd.Series(None, index=runs.index, dtype=object)
    missing_rows, due = [], {}
    fetch_start = range_start - timedelta(days=1)
    for job in JOB_SCHEDULES:
        empty = expected_slots(job, fetch_start, now)
        for trigger, filled_by in (("schedule", "Schedule"), ("manual", "Manual run")):
            candidates = runs[(runs["JOB_TYPE"] == job) & (runs["TRIGGER_TYPE"] == trigger)]
            filled = match_slots(candidates["STARTED_AT"], empty).dropna()
            runs.loc[filled.index, "SLOT"] = filled
            runs.loc[filled.index, "FILLED_BY"] = filled_by
            taken = set(filled)
            empty = [s for s in empty if s not in taken]
        missing_rows += [{"JOB_TYPE": job, "SLOT": s} for s in empty if range_start <= s and s + grace <= now]
        waiting = [s for s in empty if s + grace > now]
        if waiting:
            due[job] = waiting[0]
    # Start delay measures the scheduler, so only scheduled runs have one.
    scheduled_fill = runs["FILLED_BY"] == "Schedule"
    runs["DELAY_H"] = (runs["STARTED_AT"] - runs["SLOT"]).where(scheduled_fill).dt.total_seconds() / 3600
    runs = runs[runs["STARTED_AT"] >= range_start].reset_index(drop=True)

    missing = pd.DataFrame(missing_rows, columns=["JOB_TYPE", "SLOT"])
    missing["JOB_LABEL"] = missing["JOB_TYPE"].map(job_label)
    missing = missing.sort_values("SLOT", ascending=False).reset_index(drop=True)

    jobs = list(JOB_SCHEDULES) + [j for j in JOB_LABELS if j not in JOB_SCHEDULES]
    jobs += sorted(set(runs["JOB_TYPE"]) - set(jobs))
    rows = []
    for job in jobs:
        job_runs = runs[runs["JOB_TYPE"] == job]
        if job_runs.empty and job not in JOB_SCHEDULES:
            continue
        rows.append(_summary_row(job, job_runs, missing, due.get(job), now))
    return runs, pd.DataFrame(rows), missing


def _summary_row(job, job_runs, missing, due_slot, now) -> dict:
    sched = JOB_SCHEDULES.get(job)
    row = {
        "JOB_TYPE": job,
        "JOB_LABEL": job_label(job),
        "SCHEDULE": f"{sched['label']} UTC" if sched else ("On push to main" if job == "deploy" else ""),
        "RUNS": len(job_runs),
        "SUCCESS_RATE": None,
        "LAST_STARTED": pd.NaT,
        "LAST_INVOCATION_ID": None,
        "LAST_DURATION_MIN": None,
        "MEDIAN_DURATION_MIN": None,
        "VS_MEDIAN_PCT": None,
        "LAST_DELAY_H": None,
        "MEDIAN_DELAY_H": None,
        "MISSING": int((missing["JOB_TYPE"] == job).sum()) if sched else None,
        "NEXT_EXPECTED": _next_expected_text(job, due_slot, now) if sched else "",
    }
    if job_runs.empty:
        return row
    last = job_runs.iloc[0]
    median = job_runs["DURATION_MIN"].median()
    row.update({
        "SUCCESS_RATE": 100 * (~job_runs["FAILED"]).mean(),
        "LAST_STARTED": last["STARTED_AT"],
        "LAST_INVOCATION_ID": last["MAIN_INVOCATION_ID"],
        "LAST_DURATION_MIN": last["DURATION_MIN"],
        "MEDIAN_DURATION_MIN": median,
        "VS_MEDIAN_PCT": 100 * (last["DURATION_MIN"] - median) / median if median else None,
    })
    if sched:
        delays = job_runs["DELAY_H"].dropna()
        slotted = job_runs[job_runs["DELAY_H"].notna()]
        row["LAST_DELAY_H"] = slotted.iloc[0]["DELAY_H"] if not slotted.empty else None
        row["MEDIAN_DELAY_H"] = delays.median() if not delays.empty else None
    return row


def _next_expected_text(job, due_slot, now) -> str:
    """Earliest slot still inside the grace period with no run yet ("due"),
    else the next scheduled time. Shown in Europe/London."""
    if due_slot is not None:
        return f"Due since {to_local(due_slot):%a %d %b %H:%M}"
    upcoming = next_slot(job, now)
    return f"{to_local(upcoming):%a %d %b %H:%M}" if upcoming is not None else ""
