from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Optional

import sqlite3

from app.db.database import get_connection


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] if isinstance(r, tuple) else r.get('name') for r in rows}


def _parse_ymd(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _sport_like(sport: Optional[str]) -> Optional[str]:
    if not sport:
        return None
    s = sport.strip().lower()
    if s in ("all", ""):
        return None
    # Common UI shorthands
    if s == "run":
        return "running:%"
    if s == "walk":
        return "walking:%"
    if s == "hike":
        return "hiking:%"
    # Allow callers to pass a full prefix like "running:%" or "cycling:%"
    if ":" in s:
        return s if s.endswith("%") else (s + "%")
    return f"{s}:%"


def _row_to_dict(row: Any) -> dict:
    # RowProxy from app.db.database behaves like dict already; sqlite3.Row is mapping-like.
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return {k: row[k] for k in row.keys()}
    except Exception:
        # fallback: tuple rows (no keys)
        return {"_": list(row)}


def get_workouts_range(
    start: Optional[str] = None,
    end: Optional[str] = None,
    sport: Optional[str] = "all",
    limit: int = 300,
) -> dict:
    """Return workouts for a date range.

    Args:
        start/end: 'YYYY-MM-DD' inclusive bounds. If omitted, defaults to last 7 days.
        sport: 'all' or shorthand (run/walk/hike) or full prefix like 'running:%'.
        limit: max rows (hard limit enforced upstream too).
    """
    start_d = _parse_ymd(start)
    end_d = _parse_ymd(end)

    if start_d is None and end_d is None:
        end_d = date.today()
        start_d = end_d - timedelta(days=6)
    elif start_d is None and end_d is not None:
        start_d = end_d - timedelta(days=6)
    elif start_d is not None and end_d is None:
        end_d = start_d + timedelta(days=6)

    # Build ISO timestamps at day boundaries (DB stores ISO strings)
    start_ts = datetime.combine(start_d, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S")
    end_excl = datetime.combine(end_d + timedelta(days=1), datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S")

    sport_like = _sport_like(sport)

    conn = get_connection(row_factory="sqlite_row")
    try:
        where = "w.start_time >= ? AND w.start_time < ?"
        params: list[Any] = [start_ts, end_excl]

        if sport_like:
            where += " AND w.sport_type LIKE ?"
            params.append(sport_like)

        sql = f"""
            SELECT
                w.id,
                w.start_time,
                w.sport_type,
                w.distance_m,
                w.duration_s,
                w.moving_time_s,
                w.elevation_gain_m,
                w.avg_heart_rate,
                w.max_heart_rate,
                w.avg_gap_min_per_mile
            FROM workouts w
            WHERE {where}
            ORDER BY w.start_time DESC
            LIMIT ?
        """
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        items = [_row_to_dict(r) for r in rows]
        return {
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "sport": (sport or "all"),
            "count": len(items),
            "items": items,
        }
    finally:
        conn.close()


def get_workout_by_id(workout_id: int) -> dict:
    """Return a single workout with lightweight joined enrichment fields if available."""
    conn = get_connection(row_factory="sqlite_row")
    try:
        # Base workout
        w = conn.execute(
            """
            SELECT
                id, start_time, sport_type,
                distance_m, duration_s, moving_time_s,
                elevation_gain_m, avg_heart_rate, max_heart_rate,
                avg_gap_min_per_mile
            FROM workouts
            WHERE id = ?
            """,
            (int(workout_id),),
        ).fetchone()
        if not w:
            return {"detail": "not_found", "workout_id": int(workout_id)}

        out = _row_to_dict(w)

        # Optional enrichment: route context
        if _table_exists(conn, "workout_route_context"):
            cols = _cols(conn, "workout_route_context")
            # Prefer newer columns if present
            preferred = None
            for c in ("start_location_label", "location_label", "center_location_label"):
                if c in cols:
                    preferred = c
                    break
            if preferred:
                rc = conn.execute(
                    f"SELECT {preferred} AS location_label FROM workout_route_context WHERE workout_id = ?",
                    (int(workout_id),),
                ).fetchone()
                if rc and rc["location_label"]:
                    out["location_label"] = rc["location_label"]

        # Optional enrichment: surface summary (derive from stats columns)
        if _table_exists(conn, "workout_surface_stats"):
            ss = conn.execute(
                """
                SELECT road_m, paved_path_m, trail_m, track_m, grass_m, rock_m, forest_m, unknown_m
                FROM workout_surface_stats
                WHERE workout_id = ?
                """,
                (int(workout_id),),
            ).fetchone()
            if ss:
                d = _row_to_dict(ss)
                # Create a simple human summary (top 3 surfaces)
                pairs = [(k.replace("_m","").replace("paved_path","paved path"), float(v or 0.0)) for k, v in d.items() if k.endswith("_m")]
                pairs = [(k, v) for k, v in pairs if v > 0]
                pairs.sort(key=lambda x: x[1], reverse=True)
                total = sum(v for _, v in pairs) or 0.0
                if total > 0 and pairs:
                    top = []
                    for k, v in pairs[:4]:
                        pct = (v / total) * 100.0
                        top.append(f"{k} {pct:.0f}%")
                    out["surface_summary"] = ", ".join(top)

        # Optional enrichment: weather summary (if table exists and has a usable text column)
        if _table_exists(conn, "workout_weather"):
            cols = _cols(conn, "workout_weather")
            text_col = None
            for c in ("summary", "weather_summary", "raw_json"):
                if c in cols:
                    text_col = c
                    break
            if text_col:
                ww = conn.execute(
                    f"SELECT {text_col} AS weather_summary FROM workout_weather WHERE workout_id = ?",
                    (int(workout_id),),
                ).fetchone()
                if ww and ww["weather_summary"]:
                    out["weather_summary"] = ww["weather_summary"]

        # Optional enrichment: peaks summary (count hits)
        if _table_exists(conn, "workout_peak_hits"):
            ph = conn.execute(
                "SELECT COUNT(*) AS peak_count FROM workout_peak_hits WHERE workout_id = ?",
                (int(workout_id),),
            ).fetchone()
            if ph:
                out["peaks_count"] = int(ph["peak_count"] or 0)

        return out
    finally:
        conn.close()


def get_plot_samples(workout_id: int, max_points: int = 1000) -> dict:
    """Return plot samples for a workout (for charts).

    This endpoint is defensive: it returns whatever columns exist in workout_plot_samples.
    """
    conn = get_connection(row_factory="sqlite_row")
    try:
        if not _table_exists(conn, "workout_plot_samples"):
            return {"workout_id": int(workout_id), "count": 0, "items": []}

        cols = _cols(conn, "workout_plot_samples")
        # Pick a stable ordering column if present
        order_col = None
        for c in ("t_s", "t_sec", "t", "sample_index", "idx"):
            if c in cols:
                order_col = c
                break

        # Select a common subset if available; otherwise select *
        preferred = [c for c in (
            "t_s",
            "t_min",
            "pace_min_per_mile",
            "elevation_ft",
            "grade_pct",
            "heart_rate_bpm",
            "cadence_spm",
            "power_w",
            "lat",
            "lon",
        ) if c in cols]

        select_cols = ", ".join(preferred) if preferred else "*"
        sql = f"SELECT {select_cols} FROM workout_plot_samples WHERE workout_id = ?"
        if order_col:
            sql += f" ORDER BY {order_col} ASC"
        sql += " LIMIT ?"

        rows = conn.execute(sql, (int(workout_id), int(max_points))).fetchall()
        items = [_row_to_dict(r) for r in rows]
        return {"workout_id": int(workout_id), "count": len(items), "items": items, "columns": list(items[0].keys()) if items else preferred}
    finally:
        conn.close()
