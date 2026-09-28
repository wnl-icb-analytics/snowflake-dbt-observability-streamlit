"""Configuration constants for dbt observability dashboard."""

ELEMENTARY_SCHEMA = "DATA_LAKE__NCL.DBT_OBSERVABILITY"
WAREHOUSE = "WH_NCL_ENGINEERING_XS"
ROLE = "ENGINEER"

PAGE_CONFIG = {
    "page_title": "dbt Observability",
    "page_icon": ":bar_chart:",
    "layout": "wide",
    "initial_sidebar_state": "expanded",
}

# Shared time range (days), chosen in the sidebar
TIME_RANGE_OPTIONS = (7, 14, 30)
DEFAULT_LOOKBACK_DAYS = 7

# Cache TTL (seconds) - mainly for UI rerender efficiency
CACHE_TTL = 300

# dbt runs in GitHub Actions in this repo; used for run, commit and issue links.
DBT_REPO_URL = "https://github.com/wnl-icb-analytics/dbt-analytics"

# Scheduled dbt jobs. Mirrors the cron schedules in dbt-analytics
# .github/workflows/dbt-scheduled.yml: keep the two in step. Times are UTC.
# weekdays: Monday=0 ... Sunday=6 (None = every day). days: day of month
# (None = any). skip_days: days of month the workflow skips.
# The dev monthly full refresh is omitted: Elementary does not record dev runs.
JOB_SCHEDULES = {
    "daily": {"cron": "0 4 * * 0,2-6", "label": "Tue-Sun 04:00", "hours": (4,), "weekdays": (1, 2, 3, 4, 5, 6)},
    "weekly": {"cron": "0 4 * * 1", "label": "Mon 04:00, not the 1st", "hours": (4,), "weekdays": (0,), "skip_days": (1,)},
    "monthly-full-refresh": {"cron": "0 4 1 * *", "label": "1st 04:00", "hours": (4,), "days": (1,)},
    "sdl-intraday": {"cron": "0 7,12 * * *", "label": "Daily 07:00, 12:00", "hours": (7, 12)},
}
# GitHub starts scheduled runs hours after the cron time and Elementary records
# a run when it finishes: in the 30 days to 2026-09-28 runs were recorded
# 2.8-8.5h after their slot (04:00 jobs: 4.8-7.6h). A slot with no run counts
# as missing once this many hours have passed.
JOB_GRACE_HOURS = 9
# A scheduled run fills the earliest empty slot of its job in the preceding
# 16 hours. Kept under the 19h gap between sdl-intraday's 12:00 and next 07:00
# slots so a late run never fills the previous day's slot.
JOB_SLOT_WINDOW_HOURS = 16

# Thresholds
FLAKY_TEST_THRESHOLD = 0.2  # 20% failure rate = flaky
SLOW_MODEL_PERCENTILE = 90  # Top 10% by execution time = slow
SLOW_MODEL_MIN_SECONDS = 60  # Minimum 60s to be considered slow
GROWTH_HIGH_PCT = 50  # Row count up more than 50% over the range = high growth
GROWTH_SHRINK_PCT = -20  # Row count down more than 20% over the range = shrinking

# Source freshness (Sources page, upstream sources on model detail)
SOURCE_FRESHNESS_TABLE = "DATA_LAKE.META.SOURCE_FRESHNESS"  # hourly snapshot of source objects
ROW_COUNT_HISTORY_TABLE = "DATA_LAKE.META.ROW_COUNT_HISTORY"  # one row per object row-count change
CONTENT_FRESHNESS_TABLE = "REPORTING.DATA_QUALITY.SOURCE_CONTENT_FRESHNESS"  # data complete up to, per feed
SOURCE_TIMEZONE = "Europe/London"  # display zone for source timestamps
FEED_UPDATE_WINDOW_HOURS = 2  # row-count changes less than 2h apart count as one update
FEED_CADENCE_DAYS = 180  # typical gap = median gap between updates in the last 180 days
FEED_LATE_MULTIPLIER = 2  # late = no new rows for more than 2x the typical gap...
FEED_LATE_MIN_HOURS = 48  # ...and for more than 48h
FEED_NOT_UPDATING_DAYS = 90  # not updating = past the late threshold and no new rows for 90+ days
SNAPSHOT_STALE_HOURS = 3  # warn when the hourly snapshot is more than 3h old

# Slowdown: a model's latest successful run (within the selected range) compared
# with the median of its prior successful runs from the same job (command +
# selector) in the 30 days before it.
SLOWDOWN_BASELINE_DAYS = 30  # Prior runs from the 30 days before the latest run
SLOWDOWN_MIN_PRIOR_RUNS = 5  # Need at least 5 same-job prior runs, else not flagged
SLOWDOWN_RATIO = 2.0  # Latest at least 2x the median...
SLOWDOWN_MIN_EXTRA_SECONDS = 60  # ...and at least 60s longer than it
# A run with 10+ flagged models is shown as one callout and its models are
# hidden from the list by default. In the 45 days to 2026-09-28 daily builds
# flagged 15-104 models each (1-8% of their models) with total model time at
# 0.9-1.3x usual; other runs flagged at most 9, except one with 12.
SLOWDOWN_RUN_MIN_FLAGGED = 10

# Run detail: models listed in "Share of run time"
RUN_SHARE_TOP_N = 25

# Health signals on Home
# Stale output: a table, incremental model, snapshot or seed whose last successful
# build is older than STALE_GAP_MULTIPLIER x its typical gap between scheduled
# builds, and at least STALE_MIN_AGE_HOURS old. Typical gap = median gap between
# the distinct days with a scheduled (or manually dispatched) build in the
# STALE_LOOKBACK_DAYS before the last success. CI and local builds count as a
# refresh but not towards the cadence, so build-on-change models are not flagged.
STALE_GAP_MULTIPLIER = 2  # twice the usual gap = at least one scheduled build missed
STALE_MIN_AGE_HOURS = 72  # daily builds skip Mondays, so 48h would flag them on Tuesday mornings
STALE_LOOKBACK_DAYS = 60  # cadence window; long enough to cover weekly builds
STALE_MIN_BUILDS = 3  # scheduled build days needed before a cadence is trusted
# Row-count drop: the latest logged run of a model has 0 rows after a non-zero
# run, or fell more than ROW_DROP_PCT against the previous run.
ROW_DROP_PCT = 50  # run-to-run falls above 50% are rare (0-12 a week) and worth a look
