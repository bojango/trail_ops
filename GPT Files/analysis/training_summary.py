from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

import streamlit as st
from app.db.database import get_connection

def _empty_category() -> Dict[str, float | int]:
    return {
        "distance_m": 0.0,
        "duration_s": 0.0,
        "elevation_m": 0.0,
        "count": 0,
    }


def _classify_sport(sport: str | None) -> str:
    if not sport:
        return "other"
    s = sport.lower()
    if "run" in s:
        return "running"
    if "walk" in s or "hike" in s:
        return "walking"
    return "other"


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


@st.cache_data(show_spinner=False)
def _query_period(days: int | None) -> Dict[str, Any]:
    """Aggregate totals by sport for a given lookback window.

    We alias aggregate expressions to avoid accidental use of column labels
    like 'SUM(distance_m)' as values.
    """
    conn = get_connection()

    summary: Dict[str, Any] = {
        "total_distance_m": 0.0,
        "total_duration_s": 0.0,
        "total_elevation_m": 0.0,
        "count": 0,
        "by_sport": {},
        "by_category": {
            "running": _empty_category(),
            "walking": _empty_category(),
            "other": _empty_category(),
        },
    }

    try:
        where_clause = ""
        params: list[str] = []

        if days is not None:
            now = datetime.utcnow()
            cutoff = now - timedelta(days=days)
            where_clause = "WHERE start_time >= ?"
            params.append(cutoff.isoformat())

        query = f"""
            SELECT
                sport_type AS sport_type,
                SUM(distance_m) AS dist_m,
                SUM(duration_s) AS dur_s,
                SUM(elevation_gain_m) AS elev_m,
                COUNT(*) AS cnt
            FROM workouts
            {where_clause}
            GROUP BY sport_type;
        """

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        for row in rows:
            # row is tuple-like (RowProxy); use indices for compatibility
            sport = row[0]
            dist = _safe_float(row[1])
            dur = _safe_float(row[2])
            elev = _safe_float(row[3])
            cnt = int(row[4] or 0)

            summary["total_distance_m"] += dist
            summary["total_duration_s"] += dur
            summary["total_elevation_m"] += elev
            summary["count"] += cnt

            summary["by_sport"][sport] = {
                "distance_m": dist,
                "duration_s": dur,
                "elevation_m": elev,
                "count": cnt,
            }

            cat = _classify_sport(sport)
            cat_bucket = summary["by_category"][cat]
            cat_bucket["distance_m"] += dist
            cat_bucket["duration_s"] += dur
            cat_bucket["elevation_m"] += elev
            cat_bucket["count"] += cnt

    finally:
        conn.close()

    return summary


@st.cache_data(show_spinner=False)
def get_training_summary() -> Dict[str, Any]:
    """Return summary buckets expected by training_dashboard."""
    lifetime = _query_period(None)
    return {
        "last_7": _query_period(7),
        "last_14": _query_period(14),
        "lifetime": lifetime,
        # Backward/extra aliases (harmless if unused)
        "all_time": lifetime,
        "last_30": _query_period(30),
    }
