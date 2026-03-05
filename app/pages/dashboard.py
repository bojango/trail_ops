from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components

from app.pages.dashboard_shell import render_dashboard_shell


REACT_DEV_URL = "http://127.0.0.1:5173"


def render_dashboard_page() -> None:
    """Dashboard page.

    Phase React-0:
    - Prefer the React dashboard (iframe).
    - Fall back to the Streamlit shell dashboard if React is not running.
    """
    st.markdown('<div class="to-panel"><p class="to-panel-title">Dashboard</p></div>', unsafe_allow_html=True)

    # Attempt to embed React app.
    try:
        components.iframe(REACT_DEV_URL, height=980, scrolling=True)
        st.caption("If the dashboard is blank, start the React dev server (frontend).")
        return
    except Exception as e:
        st.warning("React dashboard not reachable. Showing Streamlit dashboard fallback.")
        st.exception(e)
        st.divider()
        render_dashboard_shell()
