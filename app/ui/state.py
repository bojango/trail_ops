from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import streamlit as st


# ----------------------------
# Global nav + filters (Phase 0)
# ----------------------------
NAV_PAGES = ["Dashboard", "Activities", "Planning", "System"]

RANGE_PRESETS = [
    "Last 7 days",
    "Last 14 days",
    "This week to date",
    "Last week",
    "This month to date",
    "Last month",
    "Custom",
]


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date  # inclusive for UI


def _today() -> date:
    return date.today()


def compute_range(preset: str) -> DateRange:
    t = _today()

    if preset == "Last 7 days":
        return DateRange(start=t - timedelta(days=6), end=t)
    if preset == "Last 14 days":
        return DateRange(start=t - timedelta(days=13), end=t)

    if preset == "This week to date":
        # Monday as start of week (UK expectation)
        start = t - timedelta(days=t.weekday())
        return DateRange(start=start, end=t)

    if preset == "Last week":
        # previous Mon-Sun
        this_week_start = t - timedelta(days=t.weekday())
        last_week_end = this_week_start - timedelta(days=1)
        last_week_start = last_week_end - timedelta(days=6)
        return DateRange(start=last_week_start, end=last_week_end)

    if preset == "This month to date":
        start = t.replace(day=1)
        return DateRange(start=start, end=t)

    if preset == "Last month":
        first_this = t.replace(day=1)
        last_month_end = first_this - timedelta(days=1)
        start = last_month_end.replace(day=1)
        return DateRange(start=start, end=last_month_end)

    # Custom is handled by UI inputs; default to last 7
    return compute_range("Last 7 days")


def init_global_state() -> None:
    ss = st.session_state

    ss.setdefault("nav_page", "Dashboard")

    # Filters
    ss.setdefault("range_preset", "Last 7 days")
    ss.setdefault("custom_start", compute_range("Last 7 days").start)
    ss.setdefault("custom_end", compute_range("Last 7 days").end)
    ss.setdefault("sport_filter", "All")

    # Future: surface, smoothing, etc. Keep placeholders without wiring.
    ss.setdefault("surface_filter", "All")
    ss.setdefault("smooth_filter", "On")


def set_nav_page(page: str) -> None:
    if page in NAV_PAGES:
        st.session_state["nav_page"] = page


def get_nav_page() -> str:
    page = st.session_state.get("nav_page", "Dashboard")
    return page if page in NAV_PAGES else "Dashboard"


def get_effective_date_range() -> DateRange:
    preset = st.session_state.get("range_preset", "Last 7 days")
    if preset != "Custom":
        return compute_range(preset)

    # Custom
    start = st.session_state.get("custom_start", compute_range("Last 7 days").start)
    end = st.session_state.get("custom_end", compute_range("Last 7 days").end)

    if start > end:
        start, end = end, start

    return DateRange(start=start, end=end)
