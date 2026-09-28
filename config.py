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

# Thresholds
FLAKY_TEST_THRESHOLD = 0.2  # 20% failure rate = flaky
SLOW_MODEL_PERCENTILE = 90  # Top 10% by execution time = slow
SLOW_MODEL_MIN_SECONDS = 60  # Minimum 60s to be considered slow
GROWTH_HIGH_PCT = 50  # Row count up more than 50% over the range = high growth
GROWTH_SHRINK_PCT = -20  # Row count down more than 20% over the range = shrinking
