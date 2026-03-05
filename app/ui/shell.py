from __future__ import annotations

from typing import Callable

import streamlit as st

from app.ui.state import (
    NAV_PAGES,
    RANGE_PRESETS,
    set_nav_page,
    get_nav_page,
    get_effective_date_range,
)


def _topbar() -> None:
    dr = get_effective_date_range()
    sport = st.session_state.get("sport_filter", "All")

    left, right = st.columns([1.15, 1.0], vertical_alignment="center")

    with left:
        st.markdown(
            """
            <div class="to-shell-topbar">
              <div class="to-brand">
                <div class="to-logo"></div>
                <div>
                  <p class="to-title">TrailOps</p>
                  <p class="to-subtitle">Mission console for training, terrain, and future planning</p>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        st.markdown(
            f"""
            <div class="to-shell-topbar">
              <div class="to-chip-row">
                <div class="to-chip to-chip-accent"><b>RANGE</b>{dr.start.isoformat()} → {dr.end.isoformat()}</div>
                <div class="to-chip"><b>SPORT</b>{sport}</div>
                <div class="to-chip"><b>MODE</b>LOCAL</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _nav_panel() -> None:
    st.markdown('<div class="to-panel"><p class="to-panel-title">Navigation</p>', unsafe_allow_html=True)

    active = get_nav_page()
    idx = NAV_PAGES.index(active) if active in NAV_PAGES else 0

    choice = st.radio(
        "Page",
        options=NAV_PAGES,
        index=idx,
        label_visibility="collapsed",
        key="nav_radio",
    )
    if choice != active:
        set_nav_page(choice)
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def _filters_panel() -> None:
    st.markdown('<div class="to-panel"><p class="to-panel-title">Global filters</p>', unsafe_allow_html=True)

    st.selectbox(
        "Date range preset",
        options=RANGE_PRESETS,
        index=RANGE_PRESETS.index(st.session_state.get("range_preset", "Last 7 days")),
        key="range_preset",
    )

    if st.session_state.get("range_preset") == "Custom":
        c1, c2 = st.columns(2)
        with c1:
            st.date_input("Start", key="custom_start")
        with c2:
            st.date_input("End", key="custom_end")

    st.selectbox(
        "Sport",
        options=["All", "Run", "Walk", "Hike", "Cycling", "Other"],
        index=0,
        key="sport_filter",
    )

    st.markdown("</div>", unsafe_allow_html=True)


def _ai_panel_placeholder() -> None:
    st.markdown(
        """
        <div class="to-panel">
          <p class="to-panel-title">AI / Coach (placeholder)</p>
          <p class="to-muted" style="margin:0;">
            Not wired yet. This space stays reserved so we don't have to rearrange
            the whole interface later.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_app_shell(body_renderer: Callable[[], None]) -> None:
    _topbar()
    st.write("")

    nav_col, main_col, right_col = st.columns([0.85, 2.25, 1.0], gap="large")

    with nav_col:
        _nav_panel()
        st.write("")
        _filters_panel()

    with main_col:
        body_renderer()

    with right_col:
        _ai_panel_placeholder()
