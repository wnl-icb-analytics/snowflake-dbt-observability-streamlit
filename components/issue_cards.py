"""Shared expandable cards for model and test failures.

Used by the Home page and the Run-detail view so both surfaces get the same
parsed-error / test-detail experience from a single implementation.
"""

import re
import json
import pandas as pd
import streamlit as st

from components import nav
from components.formatting import format_timestamp, format_when, is_missing, issue_status
from database import run_query
from services.jobs_service import commit_url, job_label


def _present(value) -> bool:
    """True when a dataframe value holds real content (not None/NaN/empty)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip() not in ("", "{}", "[]")


# --- model error parsing ----------------------------------------------------

_ERROR_CLASS_RE = re.compile(r"([A-Za-z][A-Za-z ]*?Error)\s+in\s+(?:model|test|seed|snapshot)", re.I)
_SNOWFLAKE_CODE_RE = re.compile(r"\[Snowflake\]\s*(\d+)\s*\(([0-9A-Za-z]+)\)")
_MISSING_OBJECT_RE = re.compile(r"Object '([^']+)' does not exist or not authorized", re.I)
_INVALID_IDENTIFIER_RE = re.compile(r"invalid identifier '([^']+)'", re.I)
_COMPILED_LOC_RE = re.compile(r"\(in (compiled/[^)]+)\)")


def categorize_error(message: str) -> str:
    """Map an error message to a short category label."""
    m = (message or "").lower()
    if "does not exist or not authorized" in m:
        return "Missing or unauthorized object"
    if "invalid identifier" in m:
        return "Invalid identifier"
    if "out of sync" in m or "on_schema_change" in m:
        return "Incremental schema drift"
    if "syntax error" in m or "compilation error" in m:
        return "SQL compilation error"
    return "Build failure"


def parse_model_error(message: str) -> dict:
    """Extract structured fields (error class, Snowflake code, offending object,
    compiled file location) from a raw dbt run message."""
    msg = message or ""
    out = {"category": categorize_error(msg)}
    for key, pattern in (
        ("error_class", _ERROR_CLASS_RE),
        ("missing_object", _MISSING_OBJECT_RE),
        ("invalid_identifier", _INVALID_IDENTIFIER_RE),
        ("compiled_location", _COMPILED_LOC_RE),
    ):
        m = pattern.search(msg)
        if m:
            out[key] = m.group(1)
    code = _SNOWFLAKE_CODE_RE.search(msg)
    if code:
        out["snowflake_code"] = f"{code.group(1)} ({code.group(2)})"
    return out


def where_it_broke(row) -> str | None:
    """Markdown line for the first failing run of an open streak: local time,
    job (Deploy for a push to main, Daily build, Manual, Local, ...), commit
    link and GitHub run link. Reads the STREAK_* and job columns of the health
    service queries; None when there is no streak."""
    when = row.get("STREAK_RUN_STARTED_AT")
    if is_missing(when):
        when = row.get("STREAK_STARTED_AT")
    if is_missing(when):
        return None
    parts = [f"Broke {format_when(when)}", job_label(row.get("JOB_TYPE") if _present(row.get("JOB_TYPE")) else "local")]
    sha = row.get("GIT_SHA")
    if _present(sha):
        parts.append(f"commit [{str(sha)[:7]}]({commit_url(sha)})")
    run_url = row.get("JOB_RUN_URL")
    if _present(run_url):
        parts.append(f"[GitHub run]({run_url})")
    return " · ".join(parts)


def _first_run_button(invocation_id, key):
    """Button to the run where the open streak started."""
    if _present(invocation_id) and st.button("Open first failing run", key=key, icon=":material/history:"):
        nav.open_run(str(invocation_id))


def render_model_error_card(object_name, message, unique_id, meta_line, key_prefix,
                            downstream_skipped=None, downstream_total=None,
                            broke=None, broke_invocation_id=None, resource_type="model"):
    """Expandable card for one failing model, seed or snapshot: parsed fields +
    full error + drill-in.
    downstream_skipped = models skipped downstream in a specific run (run blast radius).
    downstream_total = models that transitively depend on this one (static DAG impact).
    broke = where_it_broke() line; broke_invocation_id = run where the streak started."""
    parsed = parse_model_error(message)
    title = f"🔴 {object_name} — {parsed['category']}"
    if downstream_skipped:
        title += f" · {downstream_skipped} skipped downstream"
    elif downstream_total is not None:
        if downstream_total > 0:
            title += f" · {downstream_total} downstream affected"
        else:
            title += " · output stale, nothing downstream"
    with st.expander(title, expanded=False):
        if meta_line:
            st.caption(meta_line)
        if broke:
            st.caption(broke)
        if downstream_skipped:
            st.markdown(f"**Blast radius:** {downstream_skipped} downstream model(s) skipped in this run")
        if downstream_total is not None:
            if downstream_total > 0:
                st.markdown(
                    f"**Impact:** {downstream_total} other model(s) build on this one, "
                    "so they run on stale or broken data until it's fixed."
                )
            else:
                st.markdown(
                    "**Impact:** No other dbt models build on this one, so nothing else "
                    "in the project breaks. But its own table/view is now stale — anything "
                    "reading it directly (dashboards, reports, other queries) is affected."
                )
        if parsed.get("error_class"):
            st.markdown(f"**Error type:** {parsed['error_class']}")
        if parsed.get("snowflake_code"):
            st.markdown(f"**Snowflake code:** `{parsed['snowflake_code']}`")
        if parsed.get("missing_object"):
            st.markdown(f"**Missing / unauthorized object:** `{parsed['missing_object']}`")
        if parsed.get("invalid_identifier"):
            st.markdown(f"**Invalid identifier:** `{parsed['invalid_identifier']}`")
        if parsed.get("compiled_location"):
            st.markdown(f"**Compiled SQL:** `{parsed['compiled_location']}`")
        if message:
            st.markdown("**Full error:**")
            st.code(str(message), language="text")
        else:
            st.caption("No error message captured for this run.")
        if unique_id and st.button(f"View {resource_type}", key=f"{key_prefix}_view_{unique_id}"):
            nav.open_model(unique_id)
        _first_run_button(broke_invocation_id, f"{key_prefix}_first_run_{unique_id}")


# --- test cards -------------------------------------------------------------

def test_kind(test_name, namespace) -> str:
    """Short label for the kind of test (accepted_values, not_null, ...)."""
    name = (test_name or "").lower()
    for kind in ("accepted_values", "not_null", "relationships", "accepted_range", "unique"):
        if kind in name:
            return kind
    return test_name or namespace or "test"


def accepted_values(test_params):
    """Pull the accepted `values` list out of a test_params JSON string, if any."""
    if not test_params:
        return None
    try:
        data = json.loads(test_params) if isinstance(test_params, str) else test_params
    except (ValueError, TypeError):
        return None
    vals = data.get("values") if isinstance(data, dict) else None
    if isinstance(vals, list) and vals:
        return [str(v) for v in vals]
    return None


def _model_fqn(row):
    """Fully-qualified model relation from the joined dbt_models columns."""
    parts = [row.get("MODEL_DATABASE"), row.get("MODEL_SCHEMA"), row.get("MODEL_RELATION")]
    if all(_present(p) for p in parts):
        return ".".join(str(p) for p in parts)
    return None


def _inspect_query(kind, model_fqn, column, accepted):
    """Reconstruct a query to see the offending rows for the common test kinds.
    Elementary only stores this when store_failures is on, so we derive it."""
    if not model_fqn or not _present(column):
        return None
    if kind == "accepted_values" and accepted:
        vals = ", ".join("'" + v.replace("'", "''") + "'" for v in accepted)
        return (
            f"SELECT {column}, COUNT(*) AS failing_rows\n"
            f"FROM {model_fqn}\n"
            f"WHERE {column} IS NOT NULL AND {column} NOT IN ({vals})\n"
            f"GROUP BY {column}\nORDER BY failing_rows DESC;"
        )
    if kind == "not_null":
        return f"SELECT *\nFROM {model_fqn}\nWHERE {column} IS NULL\nLIMIT 100;"
    return None


def render_test_issue_card(row, key_prefix, note=None, broke=None, broke_invocation_id=None):
    """Expandable card for one failing/warning test: what it checks, how many
    rows failed, accepted values, source, and a button to pull the failing rows.
    note = extra caption line; broke = where_it_broke() line;
    broke_invocation_id = run where the failing streak started."""
    status = (row.get("STATUS") or "").lower()
    icon = "🟡" if status == "warn" else "🔴"
    kind = test_kind(row.get("TEST_NAME"), row.get("TEST_NAMESPACE"))
    loc = ".".join(p for p in [row.get("TABLE_NAME") or "", row.get("COLUMN_NAME") or ""] if p)
    title = f"{icon} {kind}" + (f" — {loc}" if loc else "")

    tuid = row.get("TEST_UNIQUE_ID")
    tuid = str(tuid) if (tuid is not None and pd.notna(tuid)) else None
    row_key = f"{key_prefix}_rows_{tuid or loc}"
    show_rows = st.session_state.get(row_key, False)

    accepted = accepted_values(row.get("TEST_PARAMS"))
    # A runnable query: elementary's stored one, else reconstruct it.
    stored_query = row.get("TEST_RESULTS_QUERY")
    if _present(stored_query):
        query_sql, reconstructed = str(stored_query), False
    else:
        query_sql = _inspect_query(kind, _model_fqn(row), row.get("COLUMN_NAME"), accepted)
        reconstructed = bool(query_sql)

    # Keep the card open across the rerun that a button click triggers.
    with st.expander(title, expanded=show_rows):
        meta = " · ".join(p for p in [
            issue_status(row.get("STATUS")),
            row.get("TEST_NAMESPACE") or "",
            (row.get("SEVERITY") or "").lower(),
            format_timestamp(row.get("DETECTED_AT")),
        ] if p)
        st.caption(meta)
        for line in (note, broke):
            if line:
                st.caption(line)

        failures = row.get("FAILURES")
        if not _present(failures):
            failures = row.get("FAILED_ROW_COUNT")
        if _present(failures):
            try:
                st.markdown(f"**Failing rows:** {int(failures)}")
            except (TypeError, ValueError):
                pass

        if accepted:
            st.markdown("**Accepted values:** " + ", ".join(f"`{v}`" for v in accepted))

        desc = row.get("TEST_RESULTS_DESCRIPTION")
        if desc and str(desc).strip().lower() not in ("", "warn", "fail", "error", "pass"):
            (st.warning if status == "warn" else st.error)(str(desc))

        result_rows = row.get("RESULT_ROWS")
        if _present(result_rows):
            st.markdown("**Sample failing rows:**")
            st.code(str(result_rows), language="json")

        if _present(row.get("ORIGINAL_PATH")):
            st.markdown(f"**Source:** `{row['ORIGINAL_PATH']}`")

        # Run the query on demand and show the offending rows inline.
        if query_sql:
            btn_cols = st.columns([1, 1, 3])
            with btn_cols[0]:
                if st.button("Show failing rows", key=f"{row_key}_show"):
                    st.session_state[row_key] = True
                    st.rerun()
            if show_rows:
                with btn_cols[1]:
                    if st.button("Hide", key=f"{row_key}_hide"):
                        st.session_state[row_key] = False
                        st.rerun()
                try:
                    with st.spinner("Querying failing rows..."):
                        res = run_query(query_sql)
                    if res.empty:
                        st.info("Query returned no rows.")
                    else:
                        st.caption(f"{len(res)} row(s)" + (" (showing first 500)" if len(res) > 500 else ""))
                        st.dataframe(res.head(500), width="stretch", hide_index=True)
                except Exception as e:
                    st.error(f"Could not run query: {e}")
                st.caption("Reconstructed query" if reconstructed else "Query captured by elementary")
                st.code(query_sql, language="sql")

        if tuid and st.button("View test", key=f"{key_prefix}_view_{tuid}"):
            nav.open_test(tuid)
        _first_run_button(broke_invocation_id, f"{key_prefix}_first_run_{tuid or loc}")
