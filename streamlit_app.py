"""dbt Observability Dashboard - Main entry point."""

import os

import streamlit as st

from config import PAGE_CONFIG

st.set_page_config(**PAGE_CONFIG)

from components import nav
from page_modules import (
    alerts,
    growth,
    home,
    model_detail,
    models,
    performance,
    run_detail,
    runs,
    test_detail,
    tests,
)

LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "dbt-logo.svg")

DETAIL_VIEWS = {
    "model": model_detail.render,
    "test": test_detail.render,
    "run": run_detail.render,
}


def page(render, title, icon, **kwargs):
    return nav.page(render, title, icon, DETAIL_VIEWS, **kwargs)


# Sidebar sections. Adding a page = one entry here (plus its import).
PAGES = {
    "Overview": [
        page(home.render, "Home", ":material/home:", default=True),
    ],
    "Monitoring": [
        page(alerts.render, "Alerts", ":material/notifications:"),
        page(runs.render, "Runs", ":material/history:"),
    ],
    "Project": [
        page(models.render, "Models", ":material/deployed_code:"),
        page(tests.render, "Tests", ":material/rule:"),
        page(growth.render, "Growth", ":material/trending_up:"),
        page(performance.render, "Performance", ":material/speed:"),
    ],
}


def main():
    st.logo(LOGO_PATH)
    current = st.navigation(PAGES)
    with st.sidebar:
        nav.time_range_control()
    current.run()


if __name__ == "__main__":
    main()
