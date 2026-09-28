"""Jobs page - dbt runs grouped by GitHub Actions job: outcome, duration,
start delay against the schedule, and missing scheduled runs."""

import altair as alt
import pandas as pd
import streamlit as st

from components import nav, ui
from components.formatting import format_hours, run_status_label, to_datetime
from config import JOB_GRACE_HOURS, JOB_SCHEDULES, JOB_SLOT_WINDOW_HOURS
from services.jobs_service import JOB_LABELS, commit_url, job_label, job_report, run_links, trigger_label

_JOB_KEY = "jobs_selected_job"
_OUTCOME_COLORS = {"Passed": "#28a745", "Warnings": "#ffc107", "Skipped": "#6c757d", "Failed": "#dc3545"}
_RUN_ID_TEXT = r"https://github\.com/.*/runs/(\d+)"
_COMMIT_TEXT = r"https://github\.com/.*/commit/(\w{7})"


def job_caption(details) -> str:
    """Job, trigger and GitHub links of one invocation, as markdown."""
    if details.get("TRIGGER_TYPE") == "local":
        return "Local run, not from GitHub Actions"
    parts = [
        f"Job: {job_label(details.get('JOB_TYPE'))}",
        f"Trigger: {trigger_label(details.get('TRIGGER_TYPE'))}",
    ]
    parts += [f"[{label}]({url})" for label, url in run_links(details)]
    return " · ".join(parts)


def _prepare(runs: pd.DataFrame) -> pd.DataFrame:
    df = runs.copy()
    df["OUTCOME_LABEL"] = pd.Series([run_status_label(row) for _, row in df.iterrows()], index=df.index, dtype=object)
    df["OUTCOME"] = df["OUTCOME_LABEL"].map(lambda label: label.split(" ", 1)[1])
    df["TRIGGER_LABEL"] = df["TRIGGER_TYPE"].map(trigger_label)
    df["SHORT_SHA"] = df["GIT_SHA"].map(lambda sha: sha[:7] if isinstance(sha, str) else None)
    df["COMMIT_URL"] = df["GIT_SHA"].map(commit_url)
    return df


def render():
    days = nav.days()
    ui.page_header(
        "Jobs",
        f"dbt runs grouped by GitHub Actions job over the last {days} days. Times are UK time; "
        "schedules are the cron times in UTC from dbt-scheduled.yml. Elementary records a run when it finishes.",
    )

    runs, summary, missing = job_report(days)
    runs = _prepare(runs)
    outcome_by_run = dict(zip(runs["MAIN_INVOCATION_ID"], runs["OUTCOME_LABEL"]))
    summary["LAST_OUTCOME"] = summary["LAST_INVOCATION_ID"].map(outcome_by_run)

    scheduled = runs[runs["TRIGGER_TYPE"] == "schedule"]
    github = runs[runs["TRIGGER_TYPE"] != "local"]
    median_delay = scheduled["DELAY_H"].median()
    ui.metric_row([
        ("Scheduled runs", len(scheduled)),
        ("Missing runs", len(missing), {"help": f"Scheduled slots with no run {JOB_GRACE_HOURS}h later"}),
        ("Failed job runs", int(github["FAILED"].sum()), {"help": "GitHub job runs with a failed model or test"}),
        ("Median start delay", format_hours(median_delay) if pd.notna(median_delay) else "N/A",
         {"help": "Scheduled runs: dbt start time minus the cron time"}),
    ])

    _render_summary(summary)
    _render_missing(missing)
    _render_history(runs)


def _render_summary(summary: pd.DataFrame):
    """Per-job table; selecting a row picks the job shown in Job history."""
    st.subheader("Jobs")
    st.caption(
        "Success = no failed model or test (warnings allowed). Delay = dbt start minus the scheduled time. "
        f"A slot is missing when no scheduled run starts within {JOB_SLOT_WINDOW_HOURS}h of it "
        f"and {JOB_GRACE_HOURS}h have passed; due slots are still inside that grace period. "
        "Select a job to see its history below."
    )
    selected = ui.table(
        to_datetime(summary.copy(), "LAST_STARTED"),
        key="jobs_summary",
        columns={
            "JOB_LABEL": "Job",
            "SCHEDULE": "Schedule",
            "RUNS": st.column_config.NumberColumn("Runs"),
            "SUCCESS_RATE": st.column_config.NumberColumn("Success", format="%.0f%%"),
            "LAST_STARTED": ui.datetime_column("Last run"),
            "LAST_OUTCOME": "Outcome",
            "LAST_DURATION_MIN": st.column_config.NumberColumn("Duration", format="%.1f min"),
            "MEDIAN_DURATION_MIN": st.column_config.NumberColumn("Median", format="%.1f min"),
            "VS_MEDIAN_PCT": st.column_config.NumberColumn("vs median", format="%+.0f%%"),
            "LAST_DELAY_H": st.column_config.NumberColumn("Delay", format="%.1f h"),
            "MEDIAN_DELAY_H": st.column_config.NumberColumn("Median delay", format="%.1f h"),
            "MISSING": st.column_config.NumberColumn("Missing"),
            "NEXT_EXPECTED": "Next expected",
        },
    )
    if selected is not None:
        st.session_state[_JOB_KEY] = selected["JOB_TYPE"]


def _render_missing(missing: pd.DataFrame):
    st.subheader("Missing runs")
    if missing.empty:
        ui.empty_state("No missing scheduled runs in this range", ok=True)
        return
    counts = missing["JOB_LABEL"].value_counts()
    st.caption(" · ".join(f"{job}: {n}" for job, n in counts.items()))
    df = to_datetime(missing.copy(), "SLOT")
    df["DAY"] = df["SLOT"].dt.strftime("%a")
    st.dataframe(
        df,
        column_order=["JOB_LABEL", "SLOT", "DAY"],
        column_config={
            "JOB_LABEL": "Job",
            "SLOT": ui.datetime_column("Scheduled"),
            "DAY": "Day",
        },
        hide_index=True,
        width="stretch",
        height=min(400, 38 + 35 * len(df)),
        key="jobs_missing",
    )


def _render_history(runs: pd.DataFrame):
    """Trend charts and run history of the job picked in the Jobs table
    (default: the first job with runs)."""
    job = st.session_state.get(_JOB_KEY)
    if job is None and not runs.empty:
        found = set(runs["JOB_TYPE"])
        job = next(j for j in list(JOB_LABELS) + sorted(found) if j in found)
    st.subheader(f"Job history: {job_label(job)}" if job else "Job history")
    # Schedule maths ran in UTC; show UK time from here on.
    df = to_datetime(runs[runs["JOB_TYPE"] == job].copy(), "STARTED_AT", "SLOT")
    if df.empty:
        ui.empty_state(f"No {job_label(job)} runs in this time range" if job else "No job runs in this time range")
        return
    is_scheduled = job in JOB_SCHEDULES

    median = df["DURATION_MIN"].median()
    if is_scheduled:
        chart_col, delay_col = st.columns(2)
        with chart_col:
            _trend_chart(df, "DURATION_MIN", "Duration (min)", median)
        with delay_col:
            _trend_chart(df[df["DELAY_H"].notna()], "DELAY_H", "Start delay (h)", df["DELAY_H"].median())
    else:
        _trend_chart(df, "DURATION_MIN", "Duration (min)", median)

    columns = {
        "OUTCOME_LABEL": "Outcome",
        "STARTED_AT": ui.datetime_column("Started"),
    }
    if is_scheduled:
        columns["SLOT"] = ui.datetime_column("Slot")
        columns["DELAY_H"] = st.column_config.NumberColumn("Delay", format="%.1f h")
    columns["DURATION_MIN"] = st.column_config.NumberColumn("Duration", format="%.1f min")
    columns["TRIGGER_LABEL"] = "Trigger"
    if job in ("local", "manual", "other"):
        columns["COMMAND"] = "Command"
        columns["SELECTED"] = st.column_config.TextColumn("Selection", width="medium")
    if df["GIT_SHA"].notna().any():
        columns["COMMIT_URL"] = st.column_config.LinkColumn("Commit", display_text=_COMMIT_TEXT)
    columns.update({
        "MODELS_RUN": st.column_config.NumberColumn("Models"),
        "FAIL_COUNT": st.column_config.NumberColumn("Failed"),
        "SKIPPED_COUNT": st.column_config.NumberColumn("Skipped"),
        "TESTS_FAILED": st.column_config.NumberColumn("Tests failed"),
        "TESTS_WARNED": st.column_config.NumberColumn("Warnings"),
    })
    if df["JOB_RUN_URL"].notna().any():
        columns["JOB_RUN_URL"] = st.column_config.LinkColumn("GitHub run", display_text=_RUN_ID_TEXT)

    selected = ui.table(df, key="job_runs_table", columns=columns, height=500, noun="job runs")
    if selected is not None:
        nav.open_run(selected["MAIN_INVOCATION_ID"])


def _trend_chart(df: pd.DataFrame, field: str, title: str, median):
    """Values per run over time, dots coloured by outcome, dashed median."""
    if df.empty:
        ui.empty_state(f"No data for {title.lower()}")
        return
    df = df[["STARTED_AT", field, "OUTCOME", "TRIGGER_LABEL", "SHORT_SHA"]].copy()
    # London wall-clock time, so the axis does not depend on the browser zone.
    df["STARTED_AT"] = df["STARTED_AT"].dt.tz_localize(None)
    x = alt.X("STARTED_AT:T", title="Started")
    y = alt.Y(f"{field}:Q", title=title)
    line = alt.Chart(df).mark_line(strokeWidth=2, color="#4a90d9").encode(x=x, y=y)
    dots = alt.Chart(df).mark_circle(size=70, opacity=1).encode(
        x=x,
        y=y,
        color=alt.Color(
            "OUTCOME:N",
            scale=alt.Scale(domain=list(_OUTCOME_COLORS), range=list(_OUTCOME_COLORS.values())),
            legend=alt.Legend(title="Outcome", orient="bottom"),
        ),
        tooltip=[
            alt.Tooltip("STARTED_AT:T", title="Started", format="%a %d %b %H:%M"),
            alt.Tooltip(f"{field}:Q", title=title, format=".1f"),
            alt.Tooltip("OUTCOME:N", title="Outcome"),
            alt.Tooltip("TRIGGER_LABEL:N", title="Trigger"),
            alt.Tooltip("SHORT_SHA:N", title="Commit"),
        ],
    )
    layers = [line, dots]
    if pd.notna(median):
        layers.append(
            alt.Chart(pd.DataFrame({"median": [median]}))
            .mark_rule(strokeDash=[4, 4], color="#888")
            .encode(y="median:Q", tooltip=[alt.Tooltip("median:Q", title="Median", format=".1f")])
        )
    st.altair_chart(alt.layer(*layers).properties(height=240, title=f"{title} · dashed line = median"))
