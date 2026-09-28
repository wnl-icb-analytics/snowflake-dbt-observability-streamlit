"""Snowflake connection utilities for Snowflake-native Streamlit."""

import streamlit as st
from snowflake.snowpark.context import get_active_session

from config import CACHE_TTL


@st.cache_resource
def get_session():
    """Get Snowflake session (cached for app lifetime)."""
    return get_active_session()


@st.cache_data(ttl=CACHE_TTL)
def run_query(query: str, params: tuple = ()):
    """Execute query and return results as pandas DataFrame.

    params fill `?` placeholders as bind variables; pass a tuple so the cache
    key is hashable and stable.
    """
    session = get_session()
    return session.sql(query, params=list(params) if params else None).to_pandas()


def search_clause(columns, search: str):
    """`AND (...)` fragment and bind params matching search as a literal,
    case-insensitive substring of any of the columns. Empty search -> ("", ())."""
    term = (search or "").strip().lower()
    if not term:
        return "", ()
    clause = " OR ".join(f"CONTAINS(LOWER({col}), ?)" for col in columns)
    return f"AND ({clause})", (term,) * len(columns)
