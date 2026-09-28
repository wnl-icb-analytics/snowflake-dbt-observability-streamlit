"""Navigation: pages, detail views addressed by query params, shared time range.

Detail views (model, test, run) render in place on the current page when the
URL has ?model=<unique_id>, ?test=<test_unique_id> or ?run=<invocation_id>.
Each change of these params adds a browser history entry, so back returns to
the list. Streamlit 1.50 clears query params on page switches and has no
per-page hidden flag, so detail views are not separate pages.
"""

from typing import Callable

import streamlit as st

from config import DEFAULT_LOOKBACK_DAYS, TIME_RANGE_OPTIONS

DETAIL_PARAMS = ("model", "test", "run")
DAYS_PARAM = "days"
_DAYS_KEY = "time_range_days"
_LAST_DAYS_KEY = "_time_range_last"


# --- pages --------------------------------------------------------------------

def page(render: Callable, title: str, icon: str, detail_views: dict, *, default: bool = False):
    """st.Page that shows a detail view instead of render() when a detail
    param is in the URL. detail_views maps each DETAIL_PARAMS kind to a
    render(id) function. The URL path is the lower-cased title."""

    def body():
        target = detail_target()
        if target is None:
            render()
            return
        kind, value = target
        if st.button(f"Back to {title}", icon=":material/arrow_back:", type="tertiary"):
            close_detail()
        detail_views[kind](value)

    return st.Page(body, title=title, icon=icon, url_path=title.lower().replace(" ", "-"), default=default)


def detail_target():
    """(kind, id) of the detail view requested by the URL, or None."""
    for kind in DETAIL_PARAMS:
        value = st.query_params.get(kind)
        if value:
            return kind, value
    return None


def _set_detail(kind: str | None = None, value: str | None = None):
    params = {k: v for k, v in st.query_params.to_dict().items() if k not in DETAIL_PARAMS}
    if kind:
        params[kind] = str(value)
        params[DAYS_PARAM] = str(days())
    # One from_dict call = one history entry.
    st.query_params.from_dict(params)
    st.rerun()


def open_model(unique_id: str):
    _set_detail("model", unique_id)


def open_test(test_unique_id: str):
    _set_detail("test", test_unique_id)


def open_run(invocation_id: str):
    _set_detail("run", invocation_id)


def close_detail():
    _set_detail()


# --- shared time range --------------------------------------------------------

def time_range_control():
    """Sidebar range selector. The URL param wins (shared links, back button),
    then session state, then the default."""
    param = st.query_params.get(DAYS_PARAM, "")
    if param.isdigit() and int(param) in TIME_RANGE_OPTIONS:
        st.session_state[_DAYS_KEY] = int(param)
    elif st.session_state.get(_DAYS_KEY) not in TIME_RANGE_OPTIONS:
        st.session_state[_DAYS_KEY] = st.session_state.get(_LAST_DAYS_KEY, DEFAULT_LOOKBACK_DAYS)
    st.session_state[_LAST_DAYS_KEY] = st.session_state[_DAYS_KEY]

    st.segmented_control(
        "Time range",
        TIME_RANGE_OPTIONS,
        key=_DAYS_KEY,
        format_func=lambda d: f"{d}d",
        help="Applies to every page and detail view",
        on_change=_on_range_change,
        width="stretch",
    )


def _on_range_change():
    value = st.session_state.get(_DAYS_KEY)
    if value is None:
        # Clicking the active option deselects it; keep the previous range.
        st.session_state[_DAYS_KEY] = st.session_state.get(_LAST_DAYS_KEY, DEFAULT_LOOKBACK_DAYS)
        return
    st.query_params[DAYS_PARAM] = str(value)


def days() -> int:
    """Selected time range in days."""
    value = st.session_state.get(_DAYS_KEY)
    return value if value in TIME_RANGE_OPTIONS else DEFAULT_LOOKBACK_DAYS
