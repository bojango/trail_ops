from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from app.db.database import get_connection
from app.ui.state import get_effective_date_range

from app.training_dashboard import (
    format_sport_label,
    get_workout_detail,
    get_workout_plot_samples_if_allowed,
    _plot_series,
    _render_route_map,
)

_M_TO_MI = 0.000621371
_M_TO_FT = 3.28084


def _apply_matplotlib_dark_defaults() -> None:
    """Make legacy matplotlib plots less blinding against a dark UI.

    This affects charts produced by legacy helpers like _plot_series().
    """
    try:
        import matplotlib as mpl

        mpl.rcParams["figure.facecolor"] = (0, 0, 0, 0)      # transparent
        mpl.rcParams["axes.facecolor"] = (0, 0, 0, 0)        # transparent
        mpl.rcParams["savefig.facecolor"] = (0, 0, 0, 0)
        mpl.rcParams["text.color"] = "white"
        mpl.rcParams["axes.labelcolor"] = "white"
        mpl.rcParams["xtick.color"] = "white"
        mpl.rcParams["ytick.color"] = "white"
        mpl.rcParams["axes.edgecolor"] = (1, 1, 1, 0.20)
        mpl.rcParams["grid.color"] = (1, 1, 1, 0.15)
        mpl.rcParams["axes.grid"] = True
    except Exception:
        # If matplotlib isn't available for some reason, ignore.
        pass


def _ui_sport_to_like(ui_sport: str) -> str | None:
    s = (ui_sport or "").strip().lower()
    if s in ("all", ""):
        return None
    if s == "run":
        return "running:%"
    if s == "walk":
        return "walking:%"
    if s == "hike":
        return "hiking:%"
    return f"{s}:%"


def _load_workouts(dr_start: str, dr_end_exclusive: str, sport_like: str | None, limit: int = 300) -> pd.DataFrame:
    conn = get_connection(row_factory="tuple")
    try:
        params: list[Any] = [dr_start, dr_end_exclusive]
        where = "start_time >= ? AND start_time < ?"
        if sport_like:
            where += " AND sport_type LIKE ?"
            params.append(sport_like)

        params.append(limit)

        sql = f"""
            SELECT
                id,
                start_time,
                sport_type,
                distance_m,
                duration_s,
                elevation_gain_m,
                avg_heart_rate,
                max_heart_rate,
                moving_time_s,
                avg_gap_min_per_mile
            FROM workouts
            WHERE {where}
            ORDER BY start_time DESC
            LIMIT ?;
        """
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    if df is None or df.empty:
        return pd.DataFrame()

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True).dt.tz_convert(None)
    for c in ["distance_m", "duration_s", "elevation_gain_m", "avg_heart_rate", "max_heart_rate", "moving_time_s", "avg_gap_min_per_mile"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["distance_mi"] = df["distance_m"] * _M_TO_MI
    df["elevation_gain_ft"] = df["elevation_gain_m"] * _M_TO_FT
    df["duration_min"] = df["duration_s"] / 60.0
    df["moving_time_min"] = df["moving_time_s"] / 60.0
    df["moving_pace_min_per_mile"] = (df["moving_time_s"] / 60.0) / df["distance_mi"].replace(0, pd.NA)

    return df


def render_dashboard_shell() -> None:
    _apply_matplotlib_dark_defaults()

    dr = get_effective_date_range()
    start_dt = datetime.combine(dr.start, datetime.min.time())
    end_excl = datetime.combine(dr.end + timedelta(days=1), datetime.min.time())

    ui_sport = st.session_state.get("sport_filter", "All")
    sport_like = _ui_sport_to_like(ui_sport)

    df = _load_workouts(
        dr_start=start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        dr_end_exclusive=end_excl.strftime("%Y-%m-%dT%H:%M:%S"),
        sport_like=sport_like,
        limit=300,
    )

    # Header panel
    st.markdown(
        f"""
        <div class="to-panel">
          <p class="to-panel-title">Dashboard</p>
          <div class="to-chip-row" style="justify-content:flex-start; gap:10px;">
            <div class="to-chip to-chip-accent"><b>RANGE</b>{dr.start.isoformat()} → {dr.end.isoformat()}</div>
            <div class="to-chip"><b>SPORT</b>{ui_sport}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")

    # Stat pills
    total_dist_mi = float(df["distance_mi"].fillna(0).sum()) if not df.empty else 0.0
    total_elev_ft = float(df["elevation_gain_ft"].fillna(0).sum()) if not df.empty else 0.0
    total_time_hr = float(df["duration_min"].fillna(0).sum()) / 60.0 if not df.empty else 0.0
    total_count = int(len(df)) if not df.empty else 0

    st.markdown(
        f"""
        <div class="to-panel">
          <div class="to-statgrid">
            <div class="to-stat"><div class="k">Distance</div><div class="v">{total_dist_mi:.1f} mi</div></div>
            <div class="to-stat"><div class="k">Elevation</div><div class="v">{total_elev_ft:,.0f} ft</div></div>
            <div class="to-stat"><div class="k">Time</div><div class="v">{total_time_hr:.1f} hr</div></div>
            <div class="to-stat"><div class="k">Workouts</div><div class="v">{total_count}</div></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.write("")

    col_left, col_right = st.columns([2.15, 1.0], gap="large")

    # Right panel
    with col_right:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Recent activity</p>', unsafe_allow_html=True)

        if df.empty:
            st.info("No workouts found for this filter range.")
            st.markdown("</div>", unsafe_allow_html=True)
            return

        records = df.to_dict(orient="records")

        def option_label(rec: dict) -> str:
            dt_str = str(rec.get("start_time", ""))[:16].replace("T", " ")
            sport_label = format_sport_label(rec.get("sport_type"))
            dist = rec.get("distance_mi")
            try:
                dist_val = float(dist) if dist is not None and pd.notna(dist) else None
            except Exception:
                dist_val = None
            dist_str = f"{dist_val:.2f} mi" if dist_val is not None else "n/a"
            return f"{dt_str} · {sport_label} · {dist_str}"

        selected_rec = st.selectbox(
            "Select workout",
            options=records,
            format_func=option_label,
            key="shell_selected_workout",
            label_visibility="collapsed",
        )

        workout_id = int(selected_rec["id"])
        detail = get_workout_detail(workout_id)

        # Compact key/value block (HTML)
        def kv(k: str, v: str) -> str:
            return f'<div class="row"><div class="k">{k}</div><div class="v">{v}</div></div>'

        parts = []
        parts.append(kv("Workout", str(workout_id)))
        parts.append(kv("Sport", format_sport_label(detail.get("sport_type"))))
        parts.append(kv("Start", str(detail.get("start_time"))))

        if detail.get("distance_m") is not None:
            parts.append(kv("Distance", f"{(float(detail.get('distance_m')) * _M_TO_MI):.2f} mi"))
        if detail.get("elevation_gain_m") is not None:
            parts.append(kv("Elev gain", f"{(float(detail.get('elevation_gain_m')) * _M_TO_FT):.0f} ft"))
        if detail.get("moving_time_s") is not None:
            parts.append(kv("Moving time", f"{int(detail.get('moving_time_s'))//60:d} min"))
        if detail.get("avg_gap_min_per_mile") is not None:
            parts.append(kv("Avg GAP", f"{float(detail.get('avg_gap_min_per_mile')):.2f} min/mi"))

        loc = detail.get("location_name") or detail.get("location")
        if loc:
            parts.append(kv("Location", str(loc)))
        if detail.get("surface_summary"):
            parts.append(kv("Surface", str(detail.get("surface_summary"))))
        if detail.get("peaks_summary"):
            parts.append(kv("Peaks", str(detail.get("peaks_summary"))))
        if detail.get("weather_summary"):
            parts.append(kv("Weather", str(detail.get("weather_summary"))))

        st.markdown(f'<div class="to-kv">{"".join(parts)}</div>', unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

    # Left
    with col_left:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Route map</p>', unsafe_allow_html=True)
        try:
            _render_route_map(workout_id)
        except Exception:
            st.info("Route map unavailable for this workout.")
        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")

        st.markdown('<div class="to-panel"><p class="to-panel-title">Telemetry</p>', unsafe_allow_html=True)
        samples_df = get_workout_plot_samples_if_allowed(workout_id, detail.get("sport_type"), detail.get("start_time"))
        if samples_df is None or getattr(samples_df, "empty", True):
            st.info("No chart-ready telemetry found for this workout (plot samples missing or not allowed).")
        else:
            c1, c2 = st.columns(2)
            with c1:
                _plot_series(samples_df, "t_min", "pace_min_per_mile", "Pace", "min/mi")
            with c2:
                _plot_series(samples_df, "t_min", "elevation_ft", "Elevation", "ft")

            c3, c4 = st.columns(2)
            with c3:
                if "grade_pct" in samples_df.columns and samples_df["grade_pct"].notna().any():
                    _plot_series(samples_df, "t_min", "grade_pct", "Grade", "%")
                else:
                    st.write("Grade: N/A")
            with c4:
                _plot_series(samples_df, "t_min", "heart_rate_bpm", "Heart rate", "bpm")

            c5, c6 = st.columns(2)
            with c5:
                _plot_series(samples_df, "t_min", "cadence_spm", "Cadence", "spm")
            with c6:
                if "power_w" in samples_df.columns and samples_df["power_w"].notna().any():
                    _plot_series(samples_df, "t_min", "power_w", "Power", "W")
                else:
                    st.write("Power: N/A")

        st.markdown("</div>", unsafe_allow_html=True)
