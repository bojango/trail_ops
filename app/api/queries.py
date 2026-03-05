from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Optional

import sqlite3
import json
import re

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
    return {r[1] for r in rows}


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


def _choose_spark_metric(sport_type: str | None, elevation_gain_m: float | None) -> str:
    st = (sport_type or "").lower()
    elev_ft = (float(elevation_gain_m or 0.0) * 3.28084)

    # Running (outdoor): elevation if big vert, otherwise pace.
    if st == "running:generic":
        return "elevation" if elev_ft > 800 else "pace"
    if st == "running:indoor_running":
        return "hr"

    # Walking / hiking.
    if st == "walking:generic":
        return "elevation"
    if st == "walking:indoor_walking":
        return "hr"
    if "hiking" in st:
        return "elevation"

    # Cycling.
    if st.startswith("cycling:"):
        return "power"

    # Strength / HIIT / Stair climber.
    if st == "training:strength_training":
        return "hr"
    if st == "fitness_equipment:stair_climbing":
        return "hr"
    if st == "62:70":
        return "hr"

    # Default.
    return "hr"


def _downsample_points(points: list[list[float]], max_points: int) -> list[list[float]]:
    if max_points <= 0 or len(points) <= max_points:
        return points
    if max_points == 1:
        return [points[0]]

    n = len(points)
    out: list[list[float]] = []
    last_i = -1
    for j in range(max_points):
        i = round(j * (n - 1) / (max_points - 1))
        if i == last_i:
            continue
        out.append(points[i])
        last_i = i
    return out


def get_sparklines_batch(
    workout_ids: list[int],
    max_points: int = 60,
    metric_mode: str = "auto",
) -> dict:
    """Return downsampled per-workout sparkline series.

    Response shape:
    {
      "count": <n>,
      "sparklines": {
        "<workout_id>": { "metric": "hr|pace|elevation|power|gap", "points": [[t_min, value], ...] }
      }
    }
    """
    ids = [int(x) for x in (workout_ids or []) if str(x).strip().isdigit()]
    ids = ids[:500]  # hard cap

    if not ids:
        return {"count": 0, "sparklines": {}}

    conn = get_connection(row_factory="sqlite_row")
    try:
        # Pull minimal workout context for metric selection.
        q_marks = ",".join(["?"] * len(ids))
        w_sql = f"""
            SELECT id, sport_type, elevation_gain_m
            FROM workouts
            WHERE id IN ({q_marks})
        """
        w_rows = conn.execute(w_sql, ids).fetchall()
        workout_meta = {int(r["id"]): _row_to_dict(r) for r in w_rows}

        # Map metric -> plot_samples column
        metric_col = {
            "hr": "heart_rate_bpm",
            "pace": "pace_min_per_mile",
            "gap": "gap_min_per_mile",
            "elevation": "elevation_ft",
            "power": "power_w",
            "grade": "grade_pct",
            "cad": "cadence_spm",
            "pwr": "power_w",
        }

        out: dict[str, dict] = {}

        for wid in ids:
            meta = workout_meta.get(int(wid), {})
            chosen = "hr"
            if (metric_mode or "auto").lower() == "auto":
                chosen = _choose_spark_metric(meta.get("sport_type"), meta.get("elevation_gain_m"))
            else:
                chosen = str(metric_mode).lower()

            col = metric_col.get(chosen, metric_col["hr"])

            sql = f"""
                SELECT t_min, {col} AS v
                FROM workout_plot_samples
                WHERE workout_id = ?
                  AND v IS NOT NULL
                ORDER BY t_min ASC
            """

            rows = conn.execute(sql, (int(wid),)).fetchall()
            pts: list[list[float]] = []
            for r in rows:
                try:
                    t = float(r["t_min"])
                    v = float(r["v"])
                except Exception:
                    continue
                pts.append([t, v])

            pts = _downsample_points(pts, int(max_points))
            out[str(int(wid))] = {"metric": chosen, "points": pts}

        return {"count": len(out), "sparklines": out}
    finally:
        conn.close()


# --- Recent Activity context (Phase 3 wiring prep) ---
def get_workout_context(workout_id: int) -> dict[str, Any]:
    """Return small, UI-ready context for a workout.

    This is intentionally defensive: enrichment tables/columns may vary across versions.
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        wid = int(workout_id)

        # --- Location --------------------------------------------------------
        location_label = None
        start_lat = None
        start_lon = None
        if _table_exists(conn, "workout_route_context"):
            rc_cols = _cols(conn, "workout_route_context")
            # Prefer more specific labels first
            label_expr = None
            for c in ("start_location_label", "location_label", "start_locality", "locality", "center_location_label"):
                if c in rc_cols:
                    label_expr = c
                    break
            select_parts = []
            if label_expr:
                select_parts.append(f"COALESCE(NULLIF({label_expr},''), NULLIF(location_label,'')) AS start_location")
            else:
                select_parts.append("NULL AS start_location")
            if "start_lat" in rc_cols:
                select_parts.append("start_lat")
            else:
                select_parts.append("NULL AS start_lat")
            if "start_lon" in rc_cols:
                select_parts.append("start_lon")
            else:
                select_parts.append("NULL AS start_lon")

            loc = conn.execute(
                f"""
                SELECT {", ".join(select_parts)}
                FROM workout_route_context
                WHERE workout_id = ?
                """,
                (wid,),
            ).fetchone()

            if loc:
                location_label = loc["start_location"]
                start_lat = loc["start_lat"]
                start_lon = loc["start_lon"]

        # --- Surface ---------------------------------------------------------
        surface_primary = None
        if _table_exists(conn, "workout_surface_stats"):
            surf = conn.execute(
                """
                SELECT road_m, paved_path_m, trail_m, track_m, grass_m, rock_m, forest_m, unknown_m
                FROM workout_surface_stats
                WHERE workout_id = ?
                """,
                (wid,),
            ).fetchone()
            if surf:
                candidates = [
                    ("road", surf["road_m"]),
                    ("paved_path", surf["paved_path_m"]),
                    ("trail", surf["trail_m"]),
                    ("track", surf["track_m"]),
                    ("grass", surf["grass_m"]),
                    ("rock", surf["rock_m"]),
                    ("forest", surf["forest_m"]),
                    ("unknown", surf["unknown_m"]),
                ]
                best = None
                for k, v in candidates:
                    try:
                        vv = float(v) if v is not None else 0.0
                    except Exception:
                        vv = 0.0
                    if best is None or vv > best[1]:
                        best = (k, vv)
                if best and best[1] > 0:
                    surface_primary = best[0]

        # --- Weather ---------------------------------------------------------
        weather = None
        if _table_exists(conn, "workout_weather"):
            ww_cols = _cols(conn, "workout_weather")

            # Optional point_type filter (newer schema uses start/mid/end)
            # Optional point_type preference (newer schema uses start/center/end etc.)
            point_type_order = ""
            if "point_type" in ww_cols:
                point_type_order = " ORDER BY CASE point_type WHEN 'start' THEN 0 WHEN 'center' THEN 1 WHEN 'mid' THEN 1 WHEN 'end' THEN 2 ELSE 3 END"


            # Identify best-guess column names
            def pick(*names: str) -> Optional[str]:
                for n in names:
                    if n in ww_cols:
                        return n
                return None

            c_temp_c = pick("temp_c", "temperature_c", "air_temp_c")
            c_temp_f = pick("temp_f", "temperature_f", "air_temp_f")
            c_wind_kph = pick("wind_kph", "wind_speed_kph", "wind_kmh")
            c_wind_mph = pick("wind_mph", "wind_speed_mph")
            c_precip = pick("precip_mm", "precipitation_mm", "rain_mm")
            c_precip_sum = pick("precip_sum_mm", "precip_total_mm", "rain_sum_mm")
            c_cloud = pick("cloudcover_pct", "cloud_pct", "cloud_cover_pct")
            c_code = pick("weather_code", "code", "condition_code")
            c_desc = pick("summary", "weather_summary", "conditions", "condition_text", "description")

            c_moon_name = pick("moon_phase_name", "moon_phase")
            c_moon_illum = pick("moon_illumination", "moon_illum", "moon_illum_pct")

            select_cols = ["workout_id"]
            for c in (
                c_temp_c,
                c_temp_f,
                c_wind_kph,
                c_wind_mph,
                c_precip,
                c_precip_sum,
                c_cloud,
                c_code,
                c_desc,
                c_moon_name,
                c_moon_illum,
            ):
                if c and c not in select_cols:
                    select_cols.append(c)

            if len(select_cols) > 1:
                row = conn.execute(
                    f"SELECT {', '.join(select_cols)} FROM workout_weather WHERE workout_id = ? {point_type_order} LIMIT 1",
                    (wid,),
                ).fetchone()

                if row:
                    def _f(x):
                        try:
                            return float(x) if x is not None else None
                        except Exception:
                            return None

                    temp_c = _f(row[c_temp_c]) if c_temp_c else None
                    if temp_c is None and c_temp_f:
                        tf = _f(row[c_temp_f])
                        if tf is not None:
                            temp_c = (tf - 32.0) * (5.0 / 9.0)

                    wind_kph = _f(row[c_wind_kph]) if c_wind_kph else None
                    if wind_kph is None and c_wind_mph:
                        wm = _f(row[c_wind_mph])
                        if wm is not None:
                            wind_kph = wm * 1.60934

                    precip_mm = _f(row[c_precip]) if c_precip else None
                    if precip_mm is None and c_precip_sum:
                        precip_mm = _f(row[c_precip_sum])

                    cloud_pct = _f(row[c_cloud]) if c_cloud else None
                    code = None
                    if c_code:
                        try:
                            code = int(row[c_code]) if row[c_code] is not None else None
                        except Exception:
                            code = None

                    desc = str(row[c_desc]) if c_desc and row[c_desc] is not None else None

                    # Derive a simple icon "kind" for the UI
                    kind = "unknown"
                    p = precip_mm if precip_mm is not None else 0.0
                    w = wind_kph if wind_kph is not None else 0.0
                    c = cloud_pct if cloud_pct is not None else None

                    if p >= 2.0:
                        kind = "rain_heavy"
                    elif p >= 0.2:
                        kind = "rain_light"
                    elif w >= 30.0:
                        kind = "wind"
                    elif c is not None and c >= 70.0:
                        kind = "cloud"
                    elif c is not None and c <= 30.0:
                        kind = "sun"
                    else:
                        # If we have a description/code but no cloud %, keep it generic cloud
                        kind = "cloud" if (desc or code is not None) else "unknown"

                    weather = {
                        "temp_c": temp_c,
                        "wind_kph": wind_kph,
                        "precip_mm": precip_mm,
                        "cloud_pct": cloud_pct,
                        "code": code,
                        "desc": desc,
                        "moon_phase_name": row[c_moon_name] if c_moon_name else None,
                        "moon_illumination": row[c_moon_illum] if c_moon_illum else None,
                        "kind": kind,
                    }

        # --- Peaks / POIs (deduped) ------------------------------------------
        peaks_list: list[dict[str, Any]] = []
        if _table_exists(conn, "workout_peak_hits"):
            # NOTE: workout_peak_hits can contain multiple hits for the same named POI (or OSM duplicates).
            # We dedupe by (name, ele_m) and keep earliest-by-distance.
            peaks_rows = conn.execute(
                """
                SELECT
                  p.name AS name,
                  p.ele_m AS ele_m,
                  wph.distance_m AS distance_m
                FROM workout_peak_hits wph
                LEFT JOIN peaks p
                  ON p.peak_id = COALESCE(wph.peak_id, CAST(wph.peak_osm_id AS TEXT))
                WHERE wph.workout_id = ?
                ORDER BY wph.distance_m ASC
                LIMIT 200
                """,
                (wid,),
            ).fetchall()

            seen: set[tuple[str, Optional[int]]] = set()
            for r in peaks_rows or []:
                n = r["name"]
                if not n:
                    continue
                name = str(n).strip()
                ele_i: Optional[int] = None
                if r["ele_m"] is not None:
                    try:
                        ele_i = int(round(float(r["ele_m"])))
                    except Exception:
                        ele_i = None

                key = (name.lower(), ele_i)
                if key in seen:
                    continue
                seen.add(key)

                item: dict[str, Any] = {"name": name}
                if ele_i is not None:
                    item["ele_m"] = float(ele_i)
                if r["distance_m"] is not None:
                    try:
                        item["distance_m"] = float(r["distance_m"])
                    except Exception:
                        pass
                peaks_list.append(item)

                if len(peaks_list) >= 25:
                    break

        return {
            "workout_id": wid,
            "location": {"label": location_label, "start_lat": start_lat, "start_lon": start_lon},
            "surface": {"primary": surface_primary},
            "weather": weather,
            "peaks": {"count": len(peaks_list), "items": peaks_list},
        }
    finally:
        conn.close()


def get_peaks_dashboard(
    range_key: str = "30d",
    class_key: str = "wainwrights",
) -> dict:
    """Aggregate global peaks/POI stats for the Peaks dashboard.

    Data sources:
      - workout_peak_hits: per-workout hits with hit_type = '{proximity}:{kind}'
      - peaks: dimension table with DoBIH classifications in dobih_classifications (JSON array)

    Args:
        range_key: one of {'7d','30d','12m','all'} (used for the top stat window + rate).
        class_key: classification key, matched case-insensitively against dobih_classifications.
    """
    def _row_get(row, key, default=None):
        """Safe getter for sqlite3.Row / dict-like."""
        try:
            if hasattr(row, "keys") and key not in row.keys():
                return default
            val = row[key]
        except Exception:
            try:
                val = row.get(key, default)  # type: ignore[attr-defined]
            except Exception:
                return default
        return default if val is None else val

    rk = (range_key or "30d").strip().lower()
    ck = (class_key or "wainwrights").strip().lower()

    # Window length for "range" stats
    today = date.today()
    if rk == "7d":
        start_d = today - timedelta(days=6)
    elif rk == "30d":
        start_d = today - timedelta(days=29)
    elif rk == "12m":
        start_d = today - timedelta(days=365)
    elif rk == "all":
        start_d = None
    else:
        start_d = today - timedelta(days=29)
        rk = "30d"

    # Heatmap is always last 30 days (stable UI), independent of range_key
    heat_days = 30
    heat_start_d = today - timedelta(days=heat_days - 1)

    def _day_start_ts(d: date) -> str:
        return datetime.combine(d, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S")

    start_ts = _day_start_ts(start_d) if start_d else None
    end_excl = _day_start_ts(today + timedelta(days=1))

    heat_start_ts = _day_start_ts(heat_start_d)
    heat_end_excl = end_excl

    conn = get_connection(row_factory="sqlite_row")
    try:
        # Build lifetime visited peak set for class trackers
        visited_rows = conn.execute(
            "SELECT DISTINCT peak_osm_id FROM workout_peak_hits WHERE hit_type = 'bagged:peak'"
        ).fetchall()
        visited_set = {int(r['peak_osm_id']) for r in visited_rows if r['peak_osm_id'] is not None}

        # 1) Range stats for peaks (bagged:peak)
        range_where = ""
        range_params: list[Any] = []
        if start_ts:
            range_where = "AND w.start_time >= ? AND w.start_time < ?"
            range_params = [start_ts, end_excl]

        peaks_bagged = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type = 'bagged:peak'
            {range_where}
            """,
            range_params,
        ).fetchone()
        peaks_bagged_n = int(peaks_bagged["n"] or 0)

        unique_peaks = conn.execute(
            f"""
            SELECT COUNT(DISTINCT h.peak_osm_id) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type = 'bagged:peak'
            {range_where}
            """,
            range_params,
        ).fetchone()
        unique_peaks_n = int(unique_peaks["n"] or 0)

        lifetime_unique = conn.execute(
            """
            SELECT COUNT(DISTINCT peak_osm_id) AS n
            FROM workout_peak_hits
            WHERE hit_type = 'bagged:peak'
            """
        ).fetchone()
        lifetime_unique_n = int(lifetime_unique["n"] or 0)

        # repeats in range (bagged peak hits - unique peaks)
        repeats_n = max(0, peaks_bagged_n - unique_peaks_n)

        # Peak rate per week based on selected window
        if rk == "all":
            # Use last 90 days as a stable rolling proxy for "all"
            rate_days = 90
            rate_start = today - timedelta(days=rate_days - 1)
            rate_start_ts = _day_start_ts(rate_start)
            rate_hits = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM workout_peak_hits h
                JOIN workouts w ON w.id = h.workout_id
                WHERE h.hit_type = 'bagged:peak'
                  AND w.start_time >= ? AND w.start_time < ?
                """,
                [rate_start_ts, end_excl],
            ).fetchone()
            rate_hits_n = int(rate_hits["n"] or 0)
            peak_rate_per_week = (rate_hits_n / (rate_days / 7.0)) if rate_days > 0 else 0.0
            rate_window_label = f"{rate_days}d"
        else:
            # Use the selected window length
            window_days = 7 if rk == "7d" else (30 if rk == "30d" else 365)
            peak_rate_per_week = (peaks_bagged_n / (window_days / 7.0)) if window_days > 0 else 0.0
            rate_window_label = rk

        # 2) Heatmap: daily bagged peak hits over last 30 days
        heat_rows = conn.execute(
            """
            SELECT SUBSTR(w.start_time, 1, 10) AS day, COUNT(*) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type = 'bagged:peak'
              AND w.start_time >= ? AND w.start_time < ?
            GROUP BY day
            ORDER BY day ASC
            """,
            [heat_start_ts, heat_end_excl],
        ).fetchall()
        heat_by_day = {r["day"]: int(r["n"] or 0) for r in heat_rows}
        heat_days_list = []
        for i in range(heat_days):
            d = heat_start_d + timedelta(days=i)
            ds = d.isoformat()
            heat_days_list.append({"day": ds, "count": int(heat_by_day.get(ds, 0))})

        # 3) POI stats (bagged non-peak kinds)
        poi_range_where = ""
        poi_range_params: list[Any] = []
        if start_ts:
            poi_range_where = "AND w.start_time >= ? AND w.start_time < ?"
            poi_range_params = [start_ts, end_excl]

        pois_bagged = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type LIKE 'bagged:%' AND h.hit_type != 'bagged:peak'
            {poi_range_where}
            """,
            poi_range_params,
        ).fetchone()
        pois_bagged_n = int(pois_bagged["n"] or 0)

        unique_pois = conn.execute(
            f"""
            SELECT COUNT(DISTINCT h.peak_osm_id) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type LIKE 'bagged:%' AND h.hit_type != 'bagged:peak'
            {poi_range_where}
            """,
            poi_range_params,
        ).fetchone()
        unique_pois_n = int(unique_pois["n"] or 0)

        # Breakdown by POI kind (range)
        poi_kind_rows = conn.execute(
            f"""
            SELECT
              SUBSTR(h.hit_type, INSTR(h.hit_type, ':') + 1) AS kind,
              COUNT(*) AS n
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type LIKE 'bagged:%' AND h.hit_type != 'bagged:peak'
            {poi_range_where}
            GROUP BY kind
            ORDER BY n DESC
            """,
            poi_range_params,
        ).fetchall()
        poi_kinds = [{"kind": r["kind"], "count": int(r["n"] or 0)} for r in poi_kind_rows if r["kind"]]

        # 4) Near misses (ever): near hits where never bagged for that same kind
        # Peaks near misses
        near_peak_rows = conn.execute(
            """
            SELECT
              h.peak_osm_id,
              COUNT(*) AS near_hits,
              MAX(w.start_time) AS last_near
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type = 'near:peak'
              AND h.peak_osm_id NOT IN (
                SELECT peak_osm_id FROM workout_peak_hits WHERE hit_type = 'bagged:peak'
              )
            GROUP BY h.peak_osm_id
            ORDER BY near_hits DESC, last_near DESC
            LIMIT 25
            """
        ).fetchall()
        near_miss_peaks = []
        if near_peak_rows:
            # join names
            ids = [int(r["peak_osm_id"]) for r in near_peak_rows]
            placeholders = ",".join(["?"] * len(ids))
            names = conn.execute(
                f"SELECT peak_osm_id, name, ele_m FROM peaks WHERE peak_osm_id IN ({placeholders})",
                ids,
            ).fetchall()
            name_map = {int(r["peak_osm_id"]): {"name": r["name"], "ele_m": r["ele_m"]} for r in names}
            for r in near_peak_rows:
                pid = int(r["peak_osm_id"])
                meta = name_map.get(pid, {})
                near_miss_peaks.append(
                    {
                        "peak_osm_id": pid,
                        "name": meta.get("name") or "(unknown)",
                        "ele_m": float(meta.get("ele_m")) if meta.get("ele_m") is not None else None,
                        "near_hits": int(r["near_hits"] or 0),
                        "last_near": (str(r["last_near"])[:10] if ("last_near" in r.keys() and r["last_near"] is not None) else None),
                    }
                )

        # POI near misses (by kind)
        near_poi_rows = conn.execute(
            """
            WITH near_poi AS (
              SELECT
                h.peak_osm_id,
                SUBSTR(h.hit_type, INSTR(h.hit_type, ':') + 1) AS kind,
                COUNT(*) AS near_hits,
                MAX(w.start_time) AS last_near
              FROM workout_peak_hits h
              JOIN workouts w ON w.id = h.workout_id
              WHERE h.hit_type LIKE 'near:%' AND h.hit_type != 'near:peak'
              GROUP BY h.peak_osm_id, kind
            ),
            bagged_poi AS (
              SELECT DISTINCT
                peak_osm_id,
                SUBSTR(hit_type, INSTR(hit_type, ':') + 1) AS kind
              FROM workout_peak_hits
              WHERE hit_type LIKE 'bagged:%' AND hit_type != 'bagged:peak'
            )
            SELECT n.peak_osm_id, n.kind, n.near_hits, n.last_near
            FROM near_poi n
            LEFT JOIN bagged_poi b
              ON b.peak_osm_id = n.peak_osm_id AND b.kind = n.kind
            WHERE b.peak_osm_id IS NULL
            ORDER BY n.near_hits DESC, n.last_near DESC
            LIMIT 25
            """
        ).fetchall()
        near_miss_pois = []
        if near_poi_rows:
            ids = [int(r["peak_osm_id"]) for r in near_poi_rows]
            placeholders = ",".join(["?"] * len(ids))
            names = conn.execute(
                f"SELECT peak_osm_id, name, ele_m FROM peaks WHERE peak_osm_id IN ({placeholders})",
                ids,
            ).fetchall()
            name_map = {int(r["peak_osm_id"]): {"name": r["name"], "ele_m": r["ele_m"]} for r in names}
            for r in near_poi_rows:
                pid = int(r["peak_osm_id"])
                meta = name_map.get(pid, {})
                near_miss_pois.append(
                    {
                        "peak_osm_id": pid,
                        "kind": (_row_get(r, "kind") or "poi"),
                        "name": meta.get("name") or "(unknown)",
                        "ele_m": float(meta.get("ele_m")) if meta.get("ele_m") is not None else None,
                        "near_hits": int(r["near_hits"] or 0),
                        "last_near": (str(r["last_near"])[:10] if ("last_near" in r.keys() and r["last_near"] is not None) else None),
                    }
                )

        # 5) Class trackers + top visited (per class)

        def _table_exists(name: str) -> bool:
            try:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (name,),
                ).fetchone()
                return row is not None
            except Exception:
                return False

        def _first_col(table: str, candidates: list[str]) -> str | None:
            cols = _cols(conn, table)
            for c in candidates:
                if c in cols:
                    return c
            return None

        workout_start_col = _first_col("workouts", ["start_time", "started_at", "start_ts", "start"])
        # fallback to start_time if unknown (older DBs)
        if not workout_start_col:
            workout_start_col = "start_time"


        # Set of peak_osm_id values ever bagged (used by class trackers).
        visited_set: set[int] = set()
        try:
            _vrows = conn.execute(
                "SELECT DISTINCT peak_osm_id FROM workout_peak_hits WHERE hit_type = 'bagged:peak'"
            ).fetchall()
            visited_set = {int(r["peak_osm_id"]) for r in _vrows if r["peak_osm_id"] is not None}
        except Exception:
            visited_set = set()

        totals_by_class: dict[str, int] = {}
        ids_by_class: dict[str, set[int]] = {}

        if _table_exists("peak_classifications"):
            rows = conn.execute(
                "SELECT class_key, peak_osm_id FROM peak_classifications"
            ).fetchall()

            for r in rows:
                k = str(_row_get(r, "class_key", "") or "").strip().lower()
                if not k:
                    continue
                pid = int(_row_get(r, "peak_osm_id", 0) or 0)
                if pid <= 0:
                    continue
                ids_by_class.setdefault(k, set()).add(pid)

            for k, s in ids_by_class.items():
                totals_by_class[k] = len(s)

        else:
            # Legacy fallback: JSON / delimited string stored on peaks table
            dim_rows = conn.execute(
                "SELECT peak_osm_id, dobih_classifications FROM peaks"
            ).fetchall()

            for r in dim_rows:
                pid = int(_row_get(r, "peak_osm_id", 0) or 0)
                if pid <= 0:
                    continue

                cls_raw = _row_get(r, "dobih_classifications")
                cls_list: list[str] = []
                if cls_raw:
                    if isinstance(cls_raw, str):
                        s = cls_raw.strip()
                        try:
                            v = json.loads(s)
                            if isinstance(v, list):
                                cls_list = [str(x) for x in v if str(x).strip()]
                            elif isinstance(v, str) and v.strip():
                                cls_list = [v.strip()]
                        except Exception:
                            parts = re.split(r"[\|,;\/]+", s)
                            cls_list = [p.strip() for p in parts if p.strip()]
                    elif isinstance(cls_raw, (list, tuple)):
                        cls_list = [str(x).strip() for x in cls_raw if str(x).strip()]

                for c in cls_list:
                    key = c.strip().lower()
                    if not key:
                        continue
                    ids_by_class.setdefault(key, set()).add(pid)

            for k, s in ids_by_class.items():
                totals_by_class[k] = len(s)

        # Prefer stable ordering: selected class first, then others by total desc.
        class_keys = sorted(totals_by_class.keys(), key=lambda k: (-totals_by_class.get(k, 0), k))
        if ck in class_keys:
            class_keys.remove(ck)
            class_keys = [ck] + class_keys
        class_keys = class_keys[:8]  # keep UI lightweight

        class_trackers: list[dict[str, Any]] = []
        for k in class_keys:
            total = int(totals_by_class.get(k, 0) or 0)
            ids = ids_by_class.get(k, set())
            done = len(ids.intersection(visited_set)) if ids else 0
            class_trackers.append({"key": k, "label": k, "done": int(done), "total": int(total)})

        # Top visited peaks within selected class (all-time). If no mapping, fall back to overall peaks.
        class_ids = ids_by_class.get(ck, None)
        where_in = ""
        params_tv: list[Any] = []
        if class_ids:
            ids = list(class_ids)
            placeholders = ",".join(["?"] * len(ids))
            where_in = f"AND h.peak_osm_id IN ({placeholders})"
            params_tv = ids

        top_rows = conn.execute(
            f'''
            SELECT
              h.peak_osm_id,
              COUNT(*) AS hits,
              MAX(w.{workout_start_col}) AS last_visit
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type = 'bagged:peak'
            {where_in}
            GROUP BY h.peak_osm_id
            ORDER BY hits DESC, last_visit DESC
            LIMIT 12
            ''',
            params_tv,
        ).fetchall()

        top_peaks: list[dict[str, Any]] = []
        if top_rows:
            ids = [int(r["peak_osm_id"]) for r in top_rows]
            placeholders = ",".join(["?"] * len(ids))
            meta_rows = conn.execute(
                f"SELECT peak_osm_id, name, ele_m FROM peaks WHERE peak_osm_id IN ({placeholders})",
                ids,
            ).fetchall()
            meta_map = {int(r["peak_osm_id"]): {"name": r["name"], "ele_m": r["ele_m"]} for r in meta_rows}
            for r in top_rows:
                pid = int(r["peak_osm_id"])
                meta = meta_map.get(pid, {})
                top_peaks.append(
                    {
                        "peak_osm_id": pid,
                        "name": meta.get("name") or "(unknown)",
                        "ele_m": float(meta.get("ele_m")) if meta.get("ele_m") is not None else None,
                        "count": int(r["hits"] or 0),
                        "last": (str(_row_get(r, "last_visit"))[:10] if _row_get(r, "last_visit") else None),
                    }
                )

        # Top visited POIs (all-time, non-peak kinds)
        top_poi_rows = conn.execute(
            f'''
            SELECT
              h.peak_osm_id,
              SUBSTR(h.hit_type, INSTR(h.hit_type, ':') + 1) AS kind,
              COUNT(*) AS hits,
              MAX(w.{workout_start_col}) AS last_visit
            FROM workout_peak_hits h
            JOIN workouts w ON w.id = h.workout_id
            WHERE h.hit_type LIKE 'bagged:%' AND h.hit_type != 'bagged:peak'
            GROUP BY h.peak_osm_id, kind
            ORDER BY hits DESC, last_visit DESC
            LIMIT 12
            '''
        ).fetchall()

        top_pois: list[dict[str, Any]] = []
        if top_poi_rows:
            ids = [int(r["peak_osm_id"]) for r in top_poi_rows]
            placeholders = ",".join(["?"] * len(ids))
            meta_rows = conn.execute(
                f"SELECT peak_osm_id, name, ele_m FROM peaks WHERE peak_osm_id IN ({placeholders})",
                ids,
            ).fetchall()
            meta_map = {int(r["peak_osm_id"]): {"name": r["name"], "ele_m": r["ele_m"]} for r in meta_rows}
            for r in top_poi_rows:
                pid = int(r["peak_osm_id"])
                meta = meta_map.get(pid, {})
                top_pois.append(
                    {
                        "peak_osm_id": pid,
                        "kind": str(_row_get(r, "kind") or "poi"),
                        "name": meta.get("name") or "(unknown)",
                        "ele_m": float(meta.get("ele_m")) if meta.get("ele_m") is not None else None,
                        "count": int(r["hits"] or 0),
                        "last": (str(_row_get(r, "last_visit"))[:10] if _row_get(r, "last_visit") else None),
                    }
                )
        return {
            "range": rk,
            "class": ck,
            "stats": {
                "peaks_bagged": peaks_bagged_n,
                "unique_peaks": unique_peaks_n,
                "repeats": repeats_n,
                "lifetime_unique": lifetime_unique_n,
                "peak_rate_per_week": float(peak_rate_per_week),
                "rate_window": rate_window_label,
                "pois_bagged": pois_bagged_n,
                "unique_pois": unique_pois_n,
            },
            "heatmap": {
                "days": heat_days_list,
            },
            "class_trackers": class_trackers,
            "top_visited": top_peaks,
            "top_visited_pois": top_pois,
            "poi_kinds": poi_kinds,
            "near_misses": {
                "peaks": near_miss_peaks,
                "pois": near_miss_pois,
            },
        }
    finally:
        conn.close()


def get_peak_item(peak_osm_id: int) -> dict:
    """Return diagnostics for a single peak/POI entity by peak_osm_id."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            p.peak_osm_id,
            p.peak_id,
            p.name,
            p.ele_m,
            p.lat,
            p.lon,
            p.source,
            p.tags_json,
            p.dobih_classifications
        FROM peaks p
        WHERE p.peak_osm_id = ?
        """,
        (peak_osm_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"found": False, "peak_osm_id": peak_osm_id}

    tags = {}
    try:
        if row["tags_json"]:
            tags = json.loads(row["tags_json"]) if isinstance(row["tags_json"], str) else (row["tags_json"] or {})
    except Exception:
        tags = {}

    # derive kind/type from tags
    kind = "poi"
    poi_type = None
    if tags.get("natural") == "peak":
        kind = "peak"
        poi_type = "peak"
    elif tags.get("man_made"):
        kind = "poi"
        poi_type = tags.get("man_made")
    elif tags.get("tourism"):
        kind = "poi"
        poi_type = tags.get("tourism")
    elif tags.get("historic"):
        kind = "poi"
        poi_type = tags.get("historic")
    elif tags.get("mountain_pass") == "yes":
        kind = "poi"
        poi_type = "mountain_pass"

    # hits summary
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN hit_type='bagged' THEN 1 ELSE 0 END) AS bagged_count,
            SUM(CASE WHEN hit_type='near' THEN 1 ELSE 0 END) AS near_count,
            MAX(CASE WHEN hit_type='bagged' THEN created_at ELSE NULL END) AS last_bagged,
            MAX(CASE WHEN hit_type='near' THEN created_at ELSE NULL END) AS last_near
        FROM workout_peak_hits
        WHERE peak_osm_id = ?
        """,
        (peak_osm_id,),
    )
    h = cur.fetchone() or {}
    def _row_get(r, key, default=None):
        try:
            return r[key]
        except Exception:
            return default

    bagged_count = int(_row_get(h, "bagged_count", 0) or 0)
    near_count = int(_row_get(h, "near_count", 0) or 0)
    last_bagged = _row_get(h, "last_bagged")
    last_near = _row_get(h, "last_near")

    # recent workouts for this entity
    cur.execute(
        """
        SELECT
            w.id AS workout_id,
            w.started_at,
            w.sport,
            h.hit_type,
            h.distance_m
        FROM workout_peak_hits h
        JOIN workouts w ON w.id = h.workout_id
        WHERE h.peak_osm_id = ?
        ORDER BY w.started_at DESC
        LIMIT 25
        """,
        (peak_osm_id,),
    )
    recent = []
    for r in cur.fetchall() or []:
        recent.append({
            "workout_id": r["workout_id"],
            "started_at": r["started_at"],
            "sport": r["sport"],
            "hit_type": r["hit_type"],
            "distance_m": r["distance_m"],
        })

    # classifications
    classes = []
    try:
        if row["dobih_classifications"]:
            classes = json.loads(row["dobih_classifications"]) if isinstance(row["dobih_classifications"], str) else (row["dobih_classifications"] or [])
    except Exception:
        classes = []

    return {
        "found": True,
        "peak_osm_id": row["peak_osm_id"],
        "peak_id": row["peak_id"],
        "name": row["name"] or "(unknown)",
        "ele_m": row["ele_m"],
        "lat": row["lat"],
        "lon": row["lon"],
        "source": row["source"],
        "kind": kind,
        "poi_type": poi_type,
        "tags": tags,
        "dobih_classifications": classes,
        "bagged_count": bagged_count,
        "near_count": near_count,
        "last_bagged": (str(last_bagged)[:19] if last_bagged else None),
        "last_near": (str(last_near)[:19] if last_near else None),
        "recent_workouts": recent,
    }

def rename_peak_item(peak_osm_id: int, kind: str, new_name: str) -> dict[str, Any]:
    """Rename a peak/POI in the peaks table so future hits display the chosen name."""
    if peak_osm_id is None:
        return {"ok": False, "error": "missing peak_osm_id"}
    name = (new_name or "").strip()
    if not name:
        return {"ok": False, "error": "empty name"}
    conn = get_connection()
    try:
        cols = _cols(conn, "peaks")
        if "name" not in cols:
            return {"ok": False, "error": "peaks.name column missing"}
        cur = conn.execute("UPDATE peaks SET name = ? WHERE peak_osm_id = ?", (name, int(peak_osm_id)))
        conn.commit()
        return {"ok": True, "updated": cur.rowcount, "peak_osm_id": int(peak_osm_id), "name": name, "kind": kind}
    finally:
        conn.close()
