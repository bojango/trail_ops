from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any

import streamlit as st

from db.database import get_connection


def _classify_sport(sport_type: str | None) -> str:
    """
    Group raw sport_type strings into coarse categories:
    - 'running'  : running, trail running, treadmill, etc.
    - 'walking'  : walking, hiking, indoor walking, stair stepper/climbing.
    - 'other'    : everything else (cycling, strength, etc).

    This is purely for distance/elevation summaries and can be refined later.
    """
    if not sport_type:
        return "other"

    s = sport_type.lower()

    if "run" in s:
        return "running"

    if (
        "walk" in s
        or "hike" in s
        or "stair" in s
        or "step" in s
    ):
        return "walking"

    return "other"


def _empty_category() -> Dict[str, float | int]:
    return {
        "distance_m": 0.0,
        "duration_s": 0.0,
        "elevation_m": 0.0,
        "count": 0,
    }


def _query_period(days: int | None) -> Dict[str, Any]:
    """
    Return summary stats for the last <days> days.
    If days is None, return stats for all time.

    Output in raw metric units (meters, seconds).
    """
    conn = get_connection()

    summary: Dict[str, Any] = {
        "days": days,
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
                sport_type,
                SUM(distance_m),
                SUM(duration_s),
                SUM(elevation_gain_m),
                COUNT(*)
            FROM workouts
            {where_clause}
            GROUP BY sport_type;
        """

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        for sport, dist, dur, elev, cnt in rows:
            dist = float(dist or 0)
            dur = float(dur or 0)
            elev = float(elev or 0)
            cnt = int(cnt or 0)

            # Overall totals
            summary["total_distance_m"] += dist
            summary["total_duration_s"] += dur
            summary["total_elevation_m"] += elev
            summary["count"] += cnt

            # Per-sport breakdown
            summary["by_sport"][sport] = {
                "distance_m": dist,
                "duration_s": dur,
                "elevation_m": elev,
                "count": cnt,
            }

            # Category breakdown (running / walking / other)
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
    """
    Return a dict containing:
    - last_7_days summary
    - last_14_days summary
    - lifetime summary (all workouts)

    Cached so we don't hammer the DB on every UI interaction.
    """
    return {
        "last_7": _query_period(7),
        "last_14": _query_period(14),
        "lifetime": _query_period(None),
    }