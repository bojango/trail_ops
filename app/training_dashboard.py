from __future__ import annotations

"""TrailOps · Training Dashboard (render module)

This file is now import-safe.
- The unified Streamlit entrypoint is app/main_app.py.
- main_app calls render_training_dashboard() inside a tab.

You *can* still run this file directly for development, but the preferred
run pattern is:
    streamlit run app/main_app.py
"""

import datetime
import math
from datetime import timezone, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import folium
from branca.element import MacroElement, Template

try:
    from streamlit_folium import st_folium
except Exception:
    st_folium = None

import pydeck as pdk
import plotly.graph_objects as go

from app.db.database import get_connection
from app.analysis.training_summary import get_training_summary



from app.analysis.route_context import get_route_context, get_surface_stats, get_peak_hits, get_geo_areas, get_weather, get_peak_visits
from app.analysis.map_points import (
    get_available_map_levels,
    get_map_points,
    get_markers,
    add_marker,
    delete_marker,
)
# ----------------------------
# Sport label helpers
# ----------------------------
SPORT_LABEL_OVERRIDES = {
    "running:generic": "Run (outdoor)",
    "running:indoor_running": "Run (indoor)",
    "cycling:indoor_cycling": "Cycling (indoor)",
    "cycling:road": "Cycling (road)",
    "walking:walking": "Walk",
    "hiking:hiking": "Hike",
    "fitness_equipment:stair_climbing": "Stair climber",
    "generic:generic": "Cooldown",
    # HIIT appears as a numeric code in some HealthFit exports
    "62:70": "HIIT",
    "62 (70)": "HIIT",
    "62(70)": "HIIT",
}


def format_sport_label(raw: str) -> str:
    if not raw:
        return "Unknown"

    raw_s = str(raw).strip()

    # Exact overrides first
    if raw_s in SPORT_LABEL_OVERRIDES:
        return SPORT_LABEL_OVERRIDES[raw_s]

    # Try last segment after ':' (some sources prefix the code)
    last = raw_s.split(":")[-1].strip()
    if last in SPORT_LABEL_OVERRIDES:
        return SPORT_LABEL_OVERRIDES[last]

    # Try no-space variant
    last_nospace = last.replace(" ", "")
    if last_nospace in SPORT_LABEL_OVERRIDES:
        return SPORT_LABEL_OVERRIDES[last_nospace]

    parts = raw_s.split(":")

    def prettify(s: str) -> str:
        s = s.replace("_", " ")
        return s[:1].upper() + s[1:] if s else s

    parts = [prettify(p) for p in parts]
    return parts[0] if len(parts) == 1 else f"{parts[0]} ({parts[1]})"


def classify_sport_for_totals(raw: str | None) -> str:
    if not raw:
        return "other"
    s = raw.lower()
    if "stair" in s or "step" in s:
        return "stair"
    if "walk" in s or "hike" in s:
        return "walking"
    if "run" in s:
        return "running"
    return "other"


# ----------------------------
# Date range helper
# ----------------------------
def get_date_range(mode: str) -> tuple[datetime.date | None, datetime.date | None]:
    today = datetime.date.today()
    this_monday = today - datetime.timedelta(days=today.weekday())
    first_of_this_month = today.replace(day=1)

    if mode == "Last 7 days":
        return today - datetime.timedelta(days=6), today
    if mode == "Last 14 days":
        return today - datetime.timedelta(days=13), today
    if mode == "This week":
        return this_monday, today
    if mode == "Last week":
        last_week_monday = this_monday - datetime.timedelta(days=7)
        last_week_sunday = this_monday - datetime.timedelta(days=1)
        return last_week_monday, last_week_sunday
    if mode == "This month":
        return first_of_this_month, today
    if mode == "Last month":
        last_month_end = first_of_this_month - datetime.timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end
    if mode == "Lifetime":
        return None, None

    return None, None



# ----------------------------
# Centralised dtype normalisation
# ----------------------------
_NUMERIC_COLS_DEFAULT = [
    "distance_m",
    "duration_s",
    "elevation_gain_m",
    "avg_heart_rate",
    "max_heart_rate",
    "avg_gap_min_per_mile",
    "moving_time_s",
    "stationary_time_s",
    "moving_pace_min_per_mile",
]

_DATETIME_COLS_DEFAULT = ["start_time", "end_time", "created_at", "computed_at"]


def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    """Coerce a Series to numeric safely, handling common SQLite TEXT pitfalls."""
    if s is None:
        return s
    # If it's already numeric, this is cheap.
    if pd.api.types.is_numeric_dtype(s):
        return s

    # Normalise stringy numbers: strip, remove commas, keep blanks as NaN.
    s2 = s.astype("string")
    s2 = s2.str.replace(",", "", regex=False).str.strip()
    s2 = s2.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "NaN": pd.NA})
    return pd.to_numeric(s2, errors="coerce")


def normalise_df_types(
    df: pd.DataFrame,
    numeric_cols: list[str] | None = None,
    datetime_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Normalise dtypes for any DataFrame that came from SQLite.

    This is the single choke-point that prevents 'object' dtypes from exploding
    later when the dashboard does arithmetic or datetime comparisons.
    """
    if df is None or df.empty:
        return df

    numeric_cols = numeric_cols or _NUMERIC_COLS_DEFAULT
    datetime_cols = datetime_cols or _DATETIME_COLS_DEFAULT

    for c in datetime_cols:
        if c in df.columns:
            # Parse as UTC, then drop timezone to keep comparisons consistent (naive UTC)
            _dt = pd.to_datetime(df[c], errors="coerce", utc=True)
            try:
                _dt = _dt.dt.tz_convert(None)
            except Exception:
                pass
            df[c] = _dt

    for c in numeric_cols:
        if c in df.columns:
            df[c] = _coerce_numeric_series(df[c])

    return df


# ----------------------------
# Cached DB access helpers
# ----------------------------
@st.cache_data(show_spinner=False)
def get_recent_workouts(limit: int = 200) -> pd.DataFrame:
    conn = get_connection()
    # get_connection() sets row_factory for dict-like access; pandas needs the default.
    conn.row_factory = None
    try:
        df = pd.read_sql_query(
            """
            SELECT
                id,
                start_time,
                sport_type,
                distance_m,
                duration_s,
                elevation_gain_m,
                avg_heart_rate,
                max_heart_rate
            FROM workouts
            ORDER BY start_time DESC
            LIMIT ?;
            """,
            conn,
            params=(limit,),
        )
    finally:
        conn.close()

    if df is None or df.empty:
        return pd.DataFrame()

    df = normalise_df_types(
        df,
        numeric_cols=["distance_m", "duration_s", "elevation_gain_m", "avg_heart_rate", "max_heart_rate"],
        datetime_cols=["start_time"],
    )

    # Hard guard against pathological row_factory / header-row corruption.
    # If pandas was fed sqlite Row objects, it can sometimes yield rows like {'id':'id', 'start_time':'start_time', ...}.
    df["id"] = pd.to_numeric(df.get("id"), errors="coerce")
    df = df[df["id"].notna()].copy()
    df["id"] = df["id"].astype(int)
    if "start_time" in df.columns:
        df = df[df["start_time"].notna()].copy()
    if "sport_type" in df.columns:
        bad = df["sport_type"].astype(str).str.strip().str.lower() == "sport_type"
        df = df[~bad].copy()

    # Derived display columns (robust even if some rows are NaN)
    df["distance_mi"] = (df["distance_m"] / 1609.344).round(2)
    df["duration_min"] = (df["duration_s"] / 60.0).round(1)
    df["elevation_gain_ft"] = (df["elevation_gain_m"] * 3.28084).round(0)

    df = df[
        [
            "start_time",
            "sport_type",
            "distance_mi",
            "duration_min",
            "elevation_gain_ft",
            "avg_heart_rate",
            "max_heart_rate",
            "id",
        ]
    ]

    # Final guard: ensure computed cols are numeric for downstream formatting
    df = normalise_df_types(
        df,
        numeric_cols=["distance_mi", "duration_min", "elevation_gain_ft", "avg_heart_rate", "max_heart_rate"],
        datetime_cols=["start_time"],
    )
    return df


@st.cache_data(show_spinner=False)
def get_workout_detail(workout_id: int) -> dict | None:
    conn = get_connection()
    # TrailOps uses a custom RowProxy row_factory globally. Tuple-unpacking a RowProxy iterates column
    # names (e.g. 'distance_m') instead of values. Force tuple rows for this query.
    conn.row_factory = None
    try:
        cursor = conn.execute(
            """
            SELECT
                id,
                start_time,
                end_time,
                sport_type,
                distance_m,
                duration_s,
                elevation_gain_m,
                avg_heart_rate,
                max_heart_rate,
                notes,
                avg_gap_min_per_mile,
                moving_time_s,
                stationary_time_s,
                moving_pace_min_per_mile
            FROM workouts
            WHERE id = ?;
            """,
            (workout_id,),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    (
        wid,
        start_time,
        end_time,
        sport_type,
        distance_m,
        duration_s,
        elevation_gain_m,
        avg_hr,
        max_hr,
        notes,
        avg_gap_min_per_mile,
        moving_time_s,
        stationary_time_s,
        moving_pace_min_per_mile,
    ) = row

    distance_m = float(distance_m or 0.0)
    duration_s = float(duration_s or 0.0)
    elevation_m = float(elevation_gain_m or 0.0)

    distance_mi = distance_m / 1609.344
    elevation_ft = elevation_m * 3.28084

    pace_min_per_mile = None
    if distance_mi > 0:
        pace_min_per_mile = (duration_s / 60) / distance_mi

    return {
        "id": wid,
        "start_time": start_time,
        "end_time": end_time,
        "sport_type": sport_type,
        "distance_m": distance_m,
        "distance_mi": distance_mi,
        "duration_s": duration_s,
        "elevation_m": elevation_m,
        "elevation_ft": elevation_ft,
        "avg_heart_rate": avg_hr,
        "max_heart_rate": max_hr,
        "pace_min_per_mile": pace_min_per_mile,
        "notes": notes,
        "avg_gap_min_per_mile": avg_gap_min_per_mile,
        "moving_time_s": moving_time_s,
        "stationary_time_s": stationary_time_s,
        "moving_pace_min_per_mile": moving_pace_min_per_mile,
    }


def _is_allowed_detailed_sport(sport_type: str) -> bool:
    s = sport_type.lower()
    # Allow: running*, walking*, hiking*, and trail variants
    if s.startswith("running") or s.startswith("walking") or s.startswith("hiking"):
        return True
    if "trail" in s and ("run" in s or "walk" in s or "hike" in s):
        return True
    return False


@st.cache_data(show_spinner=False)
def get_workout_plot_samples_if_allowed(
    workout_id: int,
    sport_type: str | None,
    start_time_str: str | None,
) -> pd.DataFrame:
    """
    Load chart-ready samples from workout_plot_samples ONLY IF:
    - sport_type is running OR walking OR hiking (incl trail variants)
    - AND workout is within last 30 days
    """
    if not sport_type or not start_time_str:
        return pd.DataFrame()

    if not _is_allowed_detailed_sport(sport_type):
        return pd.DataFrame()

    try:
        dt = datetime.datetime.fromisoformat(start_time_str)
    except Exception:
        return pd.DataFrame()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    now_utc = datetime.datetime.now(timezone.utc)
    if dt < (now_utc - timedelta(days=30)):
        return pd.DataFrame()

    conn = get_connection()
    conn.row_factory = None
    try:
        df = pd.read_sql_query(
            """
            SELECT
                t_min,
                pace_min_per_mile,
                gap_min_per_mile,
                grade_pct,
                elevation_ft,
                heart_rate_bpm,
                cadence_spm,
                power_w
            FROM workout_plot_samples
            WHERE workout_id = ?
            ORDER BY t_min ASC;
            """,
            conn,
            params=(workout_id,),
        )
    finally:
        conn.close()

    if df is None or df.empty:
        return pd.DataFrame()

    df = normalise_df_types(
        df,
        numeric_cols=[
            "t_min",
            "pace_min_per_mile",
            "gap_min_per_mile",
            "grade_pct",
            "elevation_ft",
            "heart_rate_bpm",
            "cadence_spm",
            "power_w",
        ],
        datetime_cols=[],
    )

    return df


def _plot_series(df: pd.DataFrame, x: str, y: str, title: str, y_label: str) -> None:
    if df is None or df.empty or x not in df.columns or y not in df.columns:
        st.info(f"No {title.lower()} data available.")
        return

    clean = df[[x, y]].dropna()
    if clean.empty:
        st.info(f"No {title.lower()} data available.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scattergl(
            x=clean[x],
            y=clean[y],
            mode="lines",
            name=title,
            hovertemplate=f"Time: %{{x:.1f}} min<br>{y_label}: %{{y:.2f}}<extra></extra>",
        )
    )
    fig.update_layout(
        title=dict(text=title, x=0.0, xanchor="left"),
        height=260,
        margin=dict(l=10, r=10, t=30, b=10),
        hovermode="x",
    )
    fig.update_xaxes(title_text="Time (min)")
    fig.update_yaxes(title_text=y_label)

    # Streamlit deprecation: use width='stretch'
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def _format_min_per_mile(v: float | None) -> str:
    if v is None or not pd.notna(v) or v <= 0:
        return "N/A"
    mins = int(v)
    secs = int(round((v - mins) * 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins}:{secs:02d} min/mi"

def _format_hhmmss(seconds: int | float | None) -> str:
    if seconds is None or not pd.notna(seconds):
        return "N/A"
    try:
        s = int(seconds)
    except Exception:
        return "N/A"
    if s < 0:
        s = 0
    return str(datetime.timedelta(seconds=s))



def _distance_weighted_avg_gap(samples_df: pd.DataFrame) -> float | None:
    """Compute distance-weighted mean GAP using time deltas and raw pace for distance."""
    if samples_df is None or samples_df.empty:
        return None
    needed = {"t_min", "pace_min_per_mile", "gap_min_per_mile"}
    if not needed.issubset(samples_df.columns):
        return None

    df = samples_df[["t_min", "pace_min_per_mile", "gap_min_per_mile"]].copy()
    df = df.dropna(subset=["t_min", "pace_min_per_mile", "gap_min_per_mile"])
    if df.empty:
        return None

    df = df.sort_values("t_min")
    dt_min = df["t_min"].diff().fillna(0.0)

    # Distance (mi) for each segment ~ dt(min) / pace(min/mi)
    seg_dist_mi = dt_min / df["pace_min_per_mile"]

    mask = (dt_min > 0) & seg_dist_mi.notna() & (seg_dist_mi > 0)
    if not mask.any():
        return None

    w = seg_dist_mi[mask]
    g = df.loc[mask, "gap_min_per_mile"]
    total = float(w.sum())
    if total <= 0:
        return None

    return float((g * w).sum() / total)




# ----------------------------
# Route map (pydeck)
# ----------------------------
CARTO_LIGHT_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
CARTO_DARK_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"


def _compute_view_state(pts: pd.DataFrame) -> pdk.ViewState:
    if pts is None or pts.empty:
        return pdk.ViewState(latitude=51.5074, longitude=-0.1278, zoom=10)

    lat = pts["latitude_deg"].astype(float)
    lon = pts["longitude_deg"].astype(float)

    lat0 = float(lat.mean())
    lon0 = float(lon.mean())

    lat_span = float(lat.max() - lat.min())
    lon_span = float(lon.max() - lon.min())
    span = max(lat_span, lon_span, 1e-6)

    if span < 0.01:
        zoom = 14
    elif span < 0.03:
        zoom = 13
    elif span < 0.08:
        zoom = 12
    elif span < 0.2:
        zoom = 11
    else:
        zoom = 10

    return pdk.ViewState(latitude=lat0, longitude=lon0, zoom=zoom, pitch=0)



# ----------------------------
# Map colour helpers (Map v2 Phase 2.0)
# ----------------------------
def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    r, g, b = rgb
    r = 0 if r < 0 else 255 if r > 255 else r
    g = 0 if g < 0 else 255 if g > 255 else g
    b = 0 if b < 0 else 255 if b > 255 else b
    return f"#{r:02x}{g:02x}{b:02x}"


def _interp_rgb(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = _clamp01(float(t))
    return (
        int(round(a[0] + (b[0] - a[0]) * t)),
        int(round(a[1] + (b[1] - a[1]) * t)),
        int(round(a[2] + (b[2] - a[2]) * t)),
    )


def _safe_avg2(a: float | int | None, b: float | int | None) -> float | None:
    """Return the mean of two values without numpy warnings.

    - If both are non-finite/None -> None
    - If one is finite -> that value
    - If both finite -> arithmetic mean
    """
    try:
        fa = float(a) if a is not None else float('nan')
    except Exception:
        fa = float('nan')
    try:
        fb = float(b) if b is not None else float('nan')
    except Exception:
        fb = float('nan')
    a_ok = np.isfinite(fa)
    b_ok = np.isfinite(fb)
    if not a_ok and not b_ok:
        return None
    if a_ok and not b_ok:
        return fa
    if b_ok and not a_ok:
        return fb
    return (fa + fb) / 2.0


def _colour_for_pace_min_per_mile(pace: float, vmin: float, vmax: float) -> str:
    # Slow = red, fast = green
    if not np.isfinite(pace) or not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
        return "#ff7a00"  # fallback orange
    # pace smaller is faster -> invert
    t = 1.0 - ((pace - vmin) / (vmax - vmin))
    red = (255, 0, 51)  # brighter red
    green = (40, 167, 69)
    return _rgb_to_hex(_interp_rgb(red, green, t))


def _colour_for_grade(grade: float, cap: float = 0.15) -> str:
    """Colour by grade (rise/run).

    Downhill = blue
    Flat = purple
    Uphill = red
    """
    if not np.isfinite(grade):
        return "#ff7a00"
    if cap <= 0:
        cap = 0.15
    g = float(max(-cap, min(cap, grade)))
    t = (g + cap) / (2.0 * cap)  # 0..1
    blue = (0, 102, 255)  # brighter blue
    purple = (111, 66, 193)
    red = (255, 0, 51)  # brighter red
    if t < 0.5:
        return _rgb_to_hex(_interp_rgb(blue, purple, t / 0.5))
    return _rgb_to_hex(_interp_rgb(purple, red, (t - 0.5) / 0.5))



def _add_distance_markers(
    fmap: folium.Map,
    draw_df: pd.DataFrame,
    unit: str = "mi",
    max_markers: int = 80,
) -> None:
    """Add subtle distance markers snapped to nearest route points.

    - unit: "mi" or "km"
    - Uses draw_df columns: lat, lon, distance_m (required)
    """
    if draw_df is None or draw_df.empty:
        return
    if "distance_m" not in draw_df.columns:
        return

    dist_m = pd.to_numeric(draw_df["distance_m"], errors="coerce")
    if dist_m.isna().all():
        return

    # Ensure increasing-ish
    dist_m = dist_m.ffill()
    if dist_m.isna().all():
        return

    total_m = float(dist_m.max())
    if not np.isfinite(total_m) or total_m <= 0:
        return

    step_m = 1609.344 if unit == "mi" else 1000.0
    label_unit = "mi" if unit == "mi" else "km"

    n_max = int(total_m // step_m)
    if n_max <= 0:
        return
    if n_max > max_markers:
        # Avoid visual clutter on ultra routes or dense data
        n_max = max_markers

    # Use a simple DivIcon: tiny dot + label
    for n in range(1, n_max + 1):
        target = n * step_m
        # nearest index where distance >= target
        try:
            idxs = dist_m[dist_m >= target].index
            if len(idxs) == 0:
                continue
            i = int(idxs[0])
        except Exception:
            continue

        try:
            lat = float(draw_df.iloc[i]["lat"])
            lon = float(draw_df.iloc[i]["lon"])
        except Exception:
            continue

        html = (
            "<div style='position: relative; transform: translate(-50%, -50%);'>"
            "<div style='width:8px;height:8px;border-radius:50%;background:rgba(20,20,20,0.85);"
            "border:1px solid rgba(255,255,255,0.9);'></div>"
            f"<div style='position:absolute; left:12px; top:-1px; font-size:11px; line-height:11px;"
            "color:rgba(20,20,20,0.95); text-shadow: 0 0 3px rgba(255,255,255,0.9);"
            "font-weight:600; white-space:nowrap;'>"
            f"{n}{label_unit}</div></div>"
        )


        folium.Marker(
            location=[lat, lon],
            icon=folium.DivIcon(html=html),
            tooltip=f"{n} {label_unit}",
        ).add_to(fmap)


class ZoomToRouteControl(MacroElement):
    """Leaflet control button to zoom the map back to the route bounds."""

    def __init__(self, bounds: list[list[float]], position: str = "topleft"):
        super().__init__()
        self._name = "ZoomToRouteControl"
        self.bounds = bounds
        self.position = position
        self._template = Template(
            """{% macro script(this, kwargs) %}
            (function() {
              var map = {{ this._parent.get_name() }};
              var bounds = {{ this.bounds | tojson }};
              var control = L.control({position: {{ this.position | tojson }}});
              control.onAdd = function(mapObj) {
                var div = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
                div.style.backgroundColor = 'white';
                div.style.width = '34px';
                div.style.height = '34px';
                div.style.display = 'flex';
                div.style.alignItems = 'center';
                div.style.justifyContent = 'center';
                div.style.cursor = 'pointer';
                div.style.boxShadow = '0 1px 4px rgba(0,0,0,0.25)';
                div.style.fontSize = '18px';
                div.style.userSelect = 'none';
                div.title = 'Zoom to route';
                div.innerHTML = '⌂';
                L.DomEvent.disableClickPropagation(div);
                L.DomEvent.on(div, 'click', function(e) {
                  map.fitBounds(bounds, {padding: [20, 20]});
                });
                return div;
              };
              control.addTo(map);
            })();
            {% endmacro %}"""
        )


def add_zoom_to_route_button(fmap: folium.Map, bounds: list[list[float]]) -> None:
    """Attach the zoom-to-route Leaflet control."""
    if not bounds or len(bounds) != 2:
        return
    ZoomToRouteControl(bounds=bounds).add_to(fmap)


_M_PER_MI = 1609.344
_KM_PER_MI = 1.609344
_FT_PER_M = 3.28084


def _format_hms(seconds: float | int | None) -> str:
    if seconds is None or not np.isfinite(float(seconds)) or seconds < 0:
        return "N/A"
    s = int(round(float(seconds)))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h:d}:{m:02d}:{sec:02d}"
    return f"{m:d}:{sec:02d}"


def _format_pace(min_per_unit: float | int | None, unit_label: str) -> str:
    if min_per_unit is None:
        return "N/A"
    try:
        v = float(min_per_unit)
    except Exception:
        return "N/A"
    if not np.isfinite(v) or v <= 0:
        return "N/A"
    whole = int(math.floor(v))
    sec = int(round((v - whole) * 60))
    if sec == 60:
        whole += 1
        sec = 0
    return f"{whole:d}:{sec:02d} / {unit_label}"



def _render_route_map(
    workout_id: int,
    plot_samples_df: pd.DataFrame | None = None,
    sport_type: str | None = None,
    start_time_str: str | None = None,
) -> None:
    """Render route map using Folium (Leaflet) + OSM/CARTO tiles.

    Map v2 Phase 2.3:
    - Add elevation profile under the map
    - Add a cursor (desktop-first) that places a dot on the map
    - Show stats at the cursor point (distance, elevation, time, pace, HR)
    """
    st.markdown("#### Route map")
    # --- Route context (computed offline via backfill_route_context.py) ---
    try:
        rc = get_route_context(workout_id)
    except Exception:
        rc = None

    if rc:
        def _rc_first(*names: str) -> str | None:
            for n in names:
                v = getattr(rc, n, None)
                if v is None:
                    continue
                if isinstance(v, str):
                    v = v.strip()
                    if not v:
                        continue
                return v
            return None

        # Support both the newer split fields (start_/center_) and older flat fields (location_label/locality/etc.)
        loc = _rc_first(
            "start_location_label",
            "start_locality",
            "center_location_label",
            "center_locality",
            "location_label",
            "locality",
            "district",
            "county",
            "region",
            "country",
        )
        if loc:
            st.caption(f"📍 {loc}")

    # Protected areas / regions (start-point based)
    try:
        areas = get_geo_areas(workout_id)
    except Exception:
        areas = []

    if areas:
        protected = [a for a in areas if a.area_type == "protected_area"]
        regions = [a for a in areas if a.area_type == "region"]

        if protected:
            labels = []
            for a in protected[:6]:
                if getattr(a, "designation", None):
                    labels.append(f"{a.name} ({a.designation})")
                else:
                    labels.append(a.name)
            if labels:
                st.caption("🏞 " + " · ".join(labels))

        if regions:
            st.caption("🗺 " + " · ".join([a.name for a in regions[:6]]))

    # Weather + lunar phase (start vs highest point)
    try:
        wrow = get_weather(workout_id)
    except Exception:
        wrow = None

    if wrow:
        start_bits = []
        if wrow.start_temp_c is not None:
            start_bits.append(f"{wrow.start_temp_c:.0f}°C")
        if wrow.start_precip_mm is not None:
            start_bits.append(f"{wrow.start_precip_mm:.1f}mm rain")
        if wrow.start_wind_kph is not None:
            start_bits.append(f"{wrow.start_wind_kph:.0f}kph wind")

        high_bits = []
        if wrow.high_temp_c is not None:
            high_bits.append(f"{wrow.high_temp_c:.0f}°C")
        if wrow.high_precip_mm is not None:
            high_bits.append(f"{wrow.high_precip_mm:.1f}mm rain")
        if wrow.high_wind_kph is not None:
            high_bits.append(f"{wrow.high_wind_kph:.0f}kph wind")

        moon = ""
        if getattr(wrow, "moon_phase_name", None):
            if getattr(wrow, "moon_illumination", None) is not None:
                moon = f" · 🌙 {wrow.moon_phase_name} ({wrow.moon_illumination*100:.0f}% lit)"
            else:
                moon = f" · 🌙 {wrow.moon_phase_name}"

        if start_bits or high_bits:
            st.caption(
                "🌦 Start: " + (", ".join(start_bits) if start_bits else "N/A") +
                " | High point: " + (", ".join(high_bits) if high_bits else "N/A") +
                moon
            )

    # Surface breakdown
    try:
        ss = get_surface_stats(workout_id)
    except Exception:
        ss = None

    if ss:
        surface_vals = {
            "Road": float(ss.road_m),
            "Paved path": float(ss.paved_path_m),
            "Trail": float(ss.trail_m),
            "Track": float(ss.track_m),
            "Grass": float(ss.grass_m),
            "Unknown": float(ss.unknown_m),
        }
        total_m = sum(surface_vals.values())
        if total_m > 0:
            top = sorted(surface_vals.items(), key=lambda kv: kv[1], reverse=True)
            parts = [f"{k} {v/total_m*100:.0f}%" for k, v in top if v > 0]
            st.caption("Surface: " + ", ".join(parts[:6]))

    # Peaks (near-visited within threshold distance)
    try:
        peaks = get_peak_visits(workout_id)
    except Exception:
        peaks = []

    if peaks:
        with st.expander(f"⛰ Peaks visited ({len(peaks)})", expanded=False):
            for p in peaks[:50]:
                name = p.get("name") or f"OSM {p.get('peak_osm_id')}"
                ele = f"{p.get('ele_m'):.0f} m" if p.get("ele_m") is not None else "ele N/A"
                visits = int(p.get("visits") or 0)
                first_seen = p.get("first_seen")

                badge = ""
                if visits <= 1 and first_seen:
                    badge = " · 🆕 first visit"
                elif visits > 1:
                    badge = f" · 🔁 {visits} visits"

                st.write(f"**{name}** · {ele}{badge}")


    try:
        available_levels = get_available_map_levels(workout_id)
        if not available_levels:
            st.info(
                "No map points found for this workout. "
                "Run the map backfill script once to generate simplified routes."
            )
            return

        # v1.1: always use default/first level for the *drawn* route
        level = int(available_levels[0])

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            basemap = st.selectbox(
                "Basemap",
                ["OSM (default)", "Carto Light", "Carto Dark"],
                key=f"folium_basemap_{workout_id}",
            )
        with col2:
            route_style = st.selectbox(
                "Route style",
                ["Normal", "Elevation", "Pace"],
                index=0,
                key=f"route_style_{workout_id}",
            )
        with col3:
            marker_mode = st.selectbox(
                "Markers",
                ["Off", "Miles", "KM"],
                index=1,
                key=f"route_markers_{workout_id}",
            )

        pts = get_map_points(workout_id, level=level)
        if pts is None or pts.empty:
            st.info("No points available for this workout.")
            return

        if st_folium is None:
            st.warning(
                "Map rendering needs two packages. Install them in your .venv, then restart:\n\n"
                "pip install folium streamlit-folium"
            )
            return

        # Prefer full-detail points for cursor/profile if available (Phase 2.3),
        # while keeping the existing drawn route level unchanged.
        cursor_level = 0 if 0 in [int(x) for x in available_levels] else level
        cursor_pts = pts if cursor_level == level else get_map_points(workout_id, level=cursor_level)
        if cursor_pts is None or cursor_pts.empty:
            cursor_pts = pts

        def _coerce_route_points(_pts: pd.DataFrame) -> pd.DataFrame:
            # Flexible column detection (keep metric columns alongside lat/lon)
            lat_col = None
            lon_col = None
            for c in ["latitude_deg", "lat", "latitude"]:
                if c in _pts.columns:
                    lat_col = c
                    break
            for c in ["longitude_deg", "lon", "longitude"]:
                if c in _pts.columns:
                    lon_col = c
                    break

            if lat_col is None or lon_col is None:
                return pd.DataFrame()

            needed_cols = [lat_col, lon_col]
            for c in ["seconds_since_start", "distance_m", "elevation_m", "seq"]:
                if c in _pts.columns and c not in needed_cols:
                    needed_cols.append(c)

            df0 = _pts[needed_cols].copy()
            df0["lat"] = pd.to_numeric(df0[lat_col], errors="coerce")
            df0["lon"] = pd.to_numeric(df0[lon_col], errors="coerce")
            df0 = df0.dropna(subset=["lat", "lon"])

            if df0.empty:
                return pd.DataFrame()

            # Heuristics:
            # 1) If values look like FIT semicircles (abs >> degrees), convert to degrees.
            # 2) If lat/lon appear swapped, swap them.
            def _looks_like_semicircles(s: pd.Series, deg_limit: float) -> bool:
                med = float(pd.to_numeric(s, errors="coerce").abs().median())
                return med > deg_limit * 5  # generous

            if _looks_like_semicircles(df0["lat"], 90) or _looks_like_semicircles(df0["lon"], 180):
                factor = 180.0 / (2**31)
                df0["lat"] = df0["lat"] * factor
                df0["lon"] = df0["lon"] * factor

            lat_bad = (df0["lat"].abs() > 90).mean()
            lon_bad = (df0["lon"].abs() > 180).mean()
            if lat_bad > 0.5 and lon_bad < 0.5:
                df0[["lat", "lon"]] = df0[["lon", "lat"]]

            df0.loc[df0["lat"].abs() > 90, "lat"] = pd.NA
            df0.loc[df0["lon"].abs() > 180, "lon"] = pd.NA
            df0 = df0.dropna(subset=["lat", "lon"])
            return df0.reset_index(drop=True)

        df = _coerce_route_points(pts)
        cursor_df = _coerce_route_points(cursor_pts)

        if df.empty:
            st.warning(f"Map points missing lat/lon columns. Columns: {list(pts.columns)}")
            return
        if cursor_df.empty:
            # Fall back gracefully
            cursor_df = df.copy()

        if len(df) < 2:
            st.info("Not enough valid GPS points to draw a route for this workout.")
            return

        # Use provided plot samples if supplied (preferred), otherwise try loading if allowed.
        if plot_samples_df is None or plot_samples_df.empty:
            plot_df = get_workout_plot_samples_if_allowed(workout_id, sport_type, start_time_str)
        else:
            plot_df = plot_samples_df

        # Containers let us compute widgets first while keeping the UI order:
        # controls -> map -> elevation profile
        map_container = st.container()
        profile_container = st.container()

        # -----------------------------
        # Phase 2.3: Cursor + profile UI
        # -----------------------------
        cursor_point = None
        cursor_distance_unit = None
        units_mode = "mi/ft"

        with profile_container:
            st.markdown("#### Elevation profile")

            units_mode = st.selectbox(
                "Units",
                ["mi/ft", "km/m"],
                index=0,
                key=f"route_units_{workout_id}",
            )

            # Need distance + elevation for the profile; if missing, we still show the map.
            dist_m = pd.to_numeric(cursor_df.get("distance_m"), errors="coerce")
            elev_m = pd.to_numeric(cursor_df.get("elevation_m"), errors="coerce")
            t_s = pd.to_numeric(cursor_df.get("seconds_since_start"), errors="coerce")

            if dist_m is None or dist_m.isna().all() or elev_m is None or elev_m.isna().all():
                st.info("No elevation profile data available for this workout.")
            else:
                if units_mode == "km/m":
                    x = (dist_m / 1000.0).to_numpy()
                    y = elev_m.to_numpy()
                    x_label = "Distance (km)"
                    y_label = "Elevation (m)"
                    pace_unit = "km"
                    dist_unit = "km"
                    elev_unit = "m"
                else:
                    x = (dist_m / _M_PER_MI).to_numpy()
                    y = (elev_m * _FT_PER_M).to_numpy()
                    x_label = "Distance (mi)"
                    y_label = "Elevation (ft)"
                    pace_unit = "mi"
                    dist_unit = "mi"
                    elev_unit = "ft"

                # Clean arrays
                ok = np.isfinite(x) & np.isfinite(y)
                x = x[ok]
                y = y[ok]

                if len(x) < 2:
                    st.info("Not enough data to render an elevation profile.")
                else:
                    
                    max_x = float(np.nanmax(x))

                    # Build hover-enriched stats per point (pace/HR/power come from plot samples via nearest time).
                    # This keeps the profile buttery-smooth (no Streamlit reruns) while still showing rich info.
                    time_str = []
                    pace_str = []
                    hr_str = []
                    power_str = []

                    t_sec = pd.to_numeric(t_s, errors="coerce").to_numpy()
                    t_sec = np.where(np.isfinite(t_sec), t_sec, np.nan)

                    # Prepare plot samples lookup by time (seconds)
                    if plot_df is None or plot_df.empty:
                        plot_t_sec = None
                    else:
                        plot_t_min = pd.to_numeric(plot_df.get("t_min"), errors="coerce").to_numpy()
                        plot_t_sec = np.where(np.isfinite(plot_t_min), plot_t_min * 60.0, np.nan)
                        # Ensure ascending for searchsorted
                        order = np.argsort(plot_t_sec)
                        plot_t_sec = plot_t_sec[order]
                        plot_pace_mi = pd.to_numeric(plot_df.get("pace_min_per_mile"), errors="coerce").to_numpy()[order]
                        plot_hr = pd.to_numeric(plot_df.get("heart_rate_bpm"), errors="coerce").to_numpy()[order]
                        plot_power = pd.to_numeric(plot_df.get("power_w"), errors="coerce").to_numpy()[order]

                    def _nearest_plot_idx(ts: float) -> int | None:
                        if plot_t_sec is None or not np.isfinite(ts):
                            return None
                        # Guard: if all NaN
                        if np.isnan(plot_t_sec).all():
                            return None
                        i = int(np.searchsorted(plot_t_sec, ts))
                        candidates = []
                        if 0 <= i < len(plot_t_sec):
                            candidates.append(i)
                        if 0 <= i - 1 < len(plot_t_sec):
                            candidates.append(i - 1)
                        best = None
                        best_dt = None
                        for j in candidates:
                            if not np.isfinite(plot_t_sec[j]):
                                continue
                            dt = abs(float(plot_t_sec[j]) - float(ts))
                            if best is None or dt < best_dt:
                                best = j
                                best_dt = dt
                        return best

                    for i in range(len(cursor_df)):
                        ts = float(t_sec[i]) if i < len(t_sec) and np.isfinite(t_sec[i]) else np.nan
                        time_str.append(_format_hms(ts))

                        j = _nearest_plot_idx(ts)
                        if j is None:
                            pace_str.append("N/A")
                            hr_str.append("N/A")
                            power_str.append("N/A")
                        else:
                            # Pace conversion for km view
                            pace_mi = float(plot_pace_mi[j]) if np.isfinite(plot_pace_mi[j]) else np.nan
                            if units_mode == "km/m":
                                pace_val = (pace_mi / _KM_PER_MI) if np.isfinite(pace_mi) else np.nan
                                pace_str.append(_format_pace(pace_val, "km"))
                            else:
                                pace_str.append(_format_pace(pace_mi, "mi"))

                            hr = float(plot_hr[j]) if np.isfinite(plot_hr[j]) else np.nan
                            hr_str.append(f"{int(round(hr))} bpm" if np.isfinite(hr) else "N/A")

                            pw = float(plot_power[j]) if np.isfinite(plot_power[j]) else np.nan
                            power_str.append(f"{int(round(pw))} W" if np.isfinite(pw) else "N/A")

                    # Align all arrays with the cleaned x/y mask
                    time_arr = np.array(time_str, dtype=object)[ok]
                    pace_arr = np.array(pace_str, dtype=object)[ok]
                    hr_arr = np.array(hr_str, dtype=object)[ok]
                    power_arr = np.array(power_str, dtype=object)[ok]

                    custom = np.stack([time_arr, pace_arr, hr_arr, power_arr], axis=1)

                    fig = go.Figure()
                    fig.add_trace(
                        go.Scatter(
                            x=x,
                            y=y,
                            mode="lines",
                            name="Elevation",
                            customdata=custom,
                            hovertemplate=(
                                f"Distance: %{{x:.2f}} {dist_unit}<br>"
                                f"Elevation: %{{y:.0f}} {elev_unit}<br>"
                                "Time: %{customdata[0]}<br>"
                                "Pace: %{customdata[1]}<br>"
                                "HR: %{customdata[2]}<br>"
                                "Power: %{customdata[3]}"
                                "<extra></extra>"
                            ),
                        )
                    )

                    fig.update_layout(
                        height=230,
                        margin=dict(l=10, r=10, t=10, b=10),
                        xaxis_title=x_label,
                        yaxis_title=y_label,
                        showlegend=False,
                        hovermode="x",
                        dragmode=False,
                    )

                    # Keep the profile static (no zoom/pan) and avoid the extra axis spike label.
                    fig.update_xaxes(
                        fixedrange=True,
                        showspikes=True,
                        spikedash="dot",
                        spikesnap="cursor",
                        spikemode="across",
                        spikethickness=1,
                    )
                    fig.update_yaxes(fixedrange=True)

                    st.plotly_chart(
                        fig,
                        width="stretch",
                        config={
                            "displayModeBar": False,
                            "scrollZoom": False,
                            "doubleClick": False,
                            "staticPlot": False,
                        },
                    )

        # -----------------------------
        # Render map (existing behaviour + cursor marker)
        # -----------------------------
        with map_container:
            lat0 = float(df["lat"].mean())
            lon0 = float(df["lon"].mean())

            tiles = "OpenStreetMap"
            attr = None
            if basemap == "Carto Light":
                tiles = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
                attr = "© OpenStreetMap contributors © CARTO"
            elif basemap == "Carto Dark":
                tiles = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                attr = "© OpenStreetMap contributors © CARTO"

            fmap = folium.Map(
                location=[lat0, lon0],
                zoom_start=13,
                tiles=tiles,
                attr=attr,
                control_scale=True,
            )

            # Optional safety: metric styles can be heavier, so cap point count
            draw_df = df.reset_index(drop=True)
            if route_style != "Normal" and len(draw_df) > 2500:
                stride = int(math.ceil(len(draw_df) / 2500.0))
                draw_df = draw_df.iloc[::stride].reset_index(drop=True)
                if (
                    len(draw_df) >= 2
                    and (draw_df.iloc[-1]["lat"] != df.iloc[-1]["lat"] or draw_df.iloc[-1]["lon"] != df.iloc[-1]["lon"])
                ):
                    draw_df = pd.concat([draw_df, df.iloc[[-1]][draw_df.columns]], ignore_index=True)

            path = draw_df[["lat", "lon"]].values.tolist()

            if route_style == "Normal":
                folium.PolyLine(path, color="#ff7a00", weight=4, opacity=0.9).add_to(fmap)
            elif route_style == "Elevation":
                # Colour by grade (segment elevation change), smoothed lightly
                elev = pd.to_numeric(draw_df.get("elevation_m"), errors="coerce")
                dist = pd.to_numeric(draw_df.get("distance_m"), errors="coerce")
                if elev is None or dist is None or elev.isna().all() or dist.isna().all():
                    folium.PolyLine(path, color="#ff7a00", weight=4, opacity=0.9).add_to(fmap)
                else:
                    de = elev.diff()
                    dd = dist.diff()
                    grade = (de / dd).replace([np.inf, -np.inf], np.nan)
                    grade_s = grade.rolling(window=3, center=True, min_periods=1).median()

                    for i in range(len(draw_df) - 1):
                        seg = [path[i], path[i + 1]]
                        g_avg = _safe_avg2(grade_s.iloc[i], grade_s.iloc[i + 1])
                        g = float(g_avg) if g_avg is not None else float("nan")
                        colour = _colour_for_grade(g, cap=0.15)
                        folium.PolyLine(seg, color=colour, weight=5, opacity=0.95).add_to(fmap)

            elif route_style == "Pace":
                # Colour by pace (min/mile), smoothed lightly
                t = pd.to_numeric(draw_df.get("seconds_since_start"), errors="coerce")
                dist = pd.to_numeric(draw_df.get("distance_m"), errors="coerce")
                if t is None or dist is None or t.isna().all() or dist.isna().all():
                    folium.PolyLine(path, color="#ff7a00", weight=4, opacity=0.9).add_to(fmap)
                else:
                    dt = t.diff()
                    dd = dist.diff()
                    pace = ((dt / 60.0) / (dd / _M_PER_MI)).replace([np.inf, -np.inf], np.nan)
                    pace_s = pace.rolling(window=3, center=True, min_periods=1).median()

                    # Robust scaling (ignore spikes)
                    q = pace_s.dropna()
                    if len(q) < 5:
                        vmin = float(np.nanmin(pace_s.to_numpy()))
                        vmax = float(np.nanmax(pace_s.to_numpy()))
                    else:
                        vmin = float(q.quantile(0.05))
                        vmax = float(q.quantile(0.95))
                    if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
                        vmin, vmax = 6.0, 14.0  # sensible fallback

                    for i in range(len(draw_df) - 1):
                        seg = [path[i], path[i + 1]]
                        p_avg = _safe_avg2(pace_s.iloc[i], pace_s.iloc[i + 1])
                        p = float(p_avg) if p_avg is not None else float("nan")
                        colour = _colour_for_pace_min_per_mile(p, vmin=vmin, vmax=vmax)
                        folium.PolyLine(seg, color=colour, weight=5, opacity=0.95).add_to(fmap)
            else:
                folium.PolyLine(path, color="#ff7a00", weight=4, opacity=0.9).add_to(fmap)

            # Cursor marker (Phase 2.3)
            try:
                if cursor_point is not None:
                    clat = pd.to_numeric(cursor_point.get("lat"), errors="coerce")
                    clon = pd.to_numeric(cursor_point.get("lon"), errors="coerce")
                    if clat is not None and clon is not None and np.isfinite(float(clat)) and np.isfinite(float(clon)):
                        folium.CircleMarker(
                            location=[float(clat), float(clon)],
                            radius=6,
                            weight=2,
                            fill=True,
                            fill_opacity=1.0,
                        ).add_to(fmap)
            except Exception:
                pass

            # Distance markers (Map v2 Phase 2.1)
            try:
                if marker_mode == "Miles":
                    _add_distance_markers(fmap, draw_df, unit="mi")
                elif marker_mode == "KM":
                    _add_distance_markers(fmap, draw_df, unit="km")
            except Exception:
                # Markers are optional; never break the map for them
                pass

            # Map v2 Phase 2.2: Zoom-to-route control button
            try:
                lats = [p[0] for p in path]
                lons = [p[1] for p in path]
                path_bounds = [[min(lats), min(lons)], [max(lats), max(lons)]]
                add_zoom_to_route_button(fmap, path_bounds)
            except Exception:
                pass

            try:
                fmap.fit_bounds(path)
            except Exception:
                pass

            # Start/finish markers
            start_pt = draw_df.iloc[0]
            end_pt = draw_df.iloc[-1]
            folium.Marker([float(start_pt["lat"]), float(start_pt["lon"])], tooltip="Start").add_to(fmap)
            folium.Marker([float(end_pt["lat"]), float(end_pt["lon"])], tooltip="Finish").add_to(fmap)

            # Render (square). Avoid feeding pan/zoom events back into Streamlit to reduce reruns.
            folium_key = f"folium_map_{workout_id}_{level}_{basemap}_{route_style}"
            try:
                st_folium(fmap, height=720, width=720, returned_objects=[], key=folium_key)
            except TypeError:
                # Older streamlit-folium versions may not support returned_objects/key
                st_folium(fmap, height=720, width=720)

    except Exception as e:
        st.warning(f"Map failed to render (charts will still work). Details: {e}")
        return

def _render_marker_controls(workout_id: int, pts: pd.DataFrame) -> None:
    """UI to add/delete custom markers for a workout.

    Stored in workout_map_markers via analysis.map_points helpers.
    """
    st.caption("Custom markers are stored locally in your DB. Start/Finish are automatic.")

    if pts is None or pts.empty:
        st.info("No route points loaded for this workout/level, so you can't place markers.")
        return

    # Determine seq range
    if "seq" in pts.columns and pts["seq"].notna().any():
        seq_min = int(pd.to_numeric(pts["seq"], errors="coerce").dropna().min())
        seq_max = int(pd.to_numeric(pts["seq"], errors="coerce").dropna().max())
    else:
        seq_min, seq_max = 0, max(0, len(pts) - 1)

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_a:
        seq_choice = st.number_input(
            "Point seq",
            min_value=int(seq_min),
            max_value=int(seq_max),
            value=int(seq_min),
            step=1,
            key=f"mk_seq_{workout_id}",
        )
    with col_b:
        label = st.text_input("Label", value="Note", key=f"mk_label_{workout_id}")
    with col_c:
        kind = st.selectbox(
            "Kind",
            ["poi", "aid", "warning", "photo", "custom"],
            index=0,
            key=f"mk_kind_{workout_id}",
        )

    if st.button("Add marker", key=f"mk_add_{workout_id}"):
        # Choose closest row by seq if available
        if "seq" in pts.columns:
            sub = pts[pd.to_numeric(pts["seq"], errors="coerce") == int(seq_choice)]
            row = sub.iloc[0] if not sub.empty else pts.iloc[0]
        else:
            idx = int(seq_choice)
            idx = max(0, min(idx, len(pts) - 1))
            row = pts.iloc[idx]

        add_marker(
            workout_id=workout_id,
            label=(label.strip() or "Note"),
            kind=str(kind),
            latitude_deg=float(row["latitude_deg"]),
            longitude_deg=float(row["longitude_deg"]),
            seq=int(seq_choice),
        )
        st.success("Marker added. Rerun/refresh to see it on the map.")

    markers_df = get_markers(workout_id)
    if markers_df is None or markers_df.empty:
        st.write("No custom markers yet.")
        return

    st.dataframe(
        markers_df[["id", "label", "kind", "seq", "latitude_deg", "longitude_deg", "created_at"]],
        width="stretch",
    )

    del_id = st.selectbox(
        "Delete marker (id)",
        options=markers_df["id"].astype(int).tolist(),
        key=f"mk_del_{workout_id}",
    )
    if st.button("Delete selected marker", key=f"mk_del_btn_{workout_id}"):
        delete_marker(int(del_id))
        st.success("Marker deleted. Rerun/refresh to remove it from the map.")




def render_training_dashboard(shell_mode: bool = False) -> None:
    """Render the training dashboard.

    shell_mode=True:
      - No standalone title/caption.
      - Uses global filters from the app shell.
      - Layout aligns with the mission-console shell (Phase 1).
    """
    if shell_mode:
        _render_training_dashboard_shell()
    else:
        render_training_dashboard_legacy()


def _render_training_dashboard_shell() -> None:
    """Phase 1 mission-console layout for the Dashboard page."""
    # Imports here to avoid coupling when running legacy dashboard alone.
    from app.ui.state import get_effective_date_range

    # Session state (reuse existing keys)
    if "selected_workout_id" not in st.session_state:
        st.session_state["selected_workout_id"] = None
    if "selected_workout_detail" not in st.session_state:
        st.session_state["selected_workout_detail"] = None
    if "selected_workout_samples" not in st.session_state:
        st.session_state["selected_workout_samples"] = None

    dr = get_effective_date_range()
    start_dt = pd.Timestamp(dr.start).tz_localize("UTC")
    end_dt_excl = (pd.Timestamp(dr.end) + pd.Timedelta(days=1)).tz_localize("UTC")

    # Load recent workouts (we’ll filter by date + sport)
    recent_df = get_recent_workouts(limit=600)
    if recent_df.empty:
        st.info("No workouts found yet. Run FIT ingestion in System.")
        return

    recent_df["start_time_dt"] = pd.to_datetime(recent_df["start_time"], utc=True, errors="coerce")
    recent_df = recent_df[recent_df["start_time_dt"].notna()]
    recent_df = recent_df[(recent_df["start_time_dt"] >= start_dt) & (recent_df["start_time_dt"] < end_dt_excl)]

    # Sport filter from shell
    ui_sport = str(st.session_state.get("sport_filter", "All"))
    ui_map = {"Run": "running", "Walk": "walking", "Hike": "hiking"}
    if ui_sport in ui_map:
        key = ui_map[ui_sport]
        recent_df = recent_df[recent_df["sport_type"].astype(str).str.lower().str.startswith(key)]
    # else All/Other/Cycling etc = keep all in-range for now (Phase 2 makes this DB-driven)

    # Telemetry strip (current range)
    m_to_mi = 1 / 1609.344
    m_to_ft = 3.28084
    total_dist_m = float(recent_df["distance_m"].fillna(0).sum())
    total_elev_m = float(recent_df["elevation_m"].fillna(0).sum()) if "elevation_m" in recent_df else 0.0
    total_time_s = float(recent_df["duration_s"].fillna(0).sum()) if "duration_s" in recent_df else 0.0

    dist_mi = total_dist_m * m_to_mi
    elev_ft = total_elev_m * m_to_ft
    time_hr = total_time_s / 3600 if total_time_s else 0.0
    count = int(len(recent_df))

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
    t1, t2, t3, t4 = st.columns(4, gap="large")
    with t1:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Distance</p>', unsafe_allow_html=True)
        st.metric("", f"{dist_mi:.1f} mi")
        st.markdown("</div>", unsafe_allow_html=True)
    with t2:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Elevation</p>', unsafe_allow_html=True)
        st.metric("", f"{elev_ft:,.0f} ft")
        st.markdown("</div>", unsafe_allow_html=True)
    with t3:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Time</p>', unsafe_allow_html=True)
        st.metric("", f"{time_hr:.1f} hr")
        st.markdown("</div>", unsafe_allow_html=True)
    with t4:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Workouts</p>', unsafe_allow_html=True)
        st.metric("", f"{count}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.write("")
    col_left, col_right = st.columns([2.15, 1.0], gap="large")

    # Right rail: activity selection + details
    with col_right:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Recent activity</p>', unsafe_allow_html=True)

        # Build select options
        records = recent_df.sort_values("start_time_dt", ascending=False).to_dict("records")

        def option_label(rec) -> str:
            dt_str = str(rec.get("start_time", ""))[:16].replace("T", " ")
            sport_label = format_sport_label(rec.get("sport_type"))
            dist_m = rec.get("distance_m") or 0
            dist_str = f"{float(dist_m) * m_to_mi:.2f} mi"
            return f"{dt_str} · {sport_label} · {dist_str}"

        selected_rec = st.selectbox(
            "Select a workout",
            options=records,
            format_func=option_label,
            label_visibility="collapsed",
        )

        if selected_rec:
            new_id = int(selected_rec["id"])
            if new_id != st.session_state["selected_workout_id"]:
                st.session_state["selected_workout_id"] = new_id
                detail = get_workout_detail(new_id)
                st.session_state["selected_workout_detail"] = detail
                if detail is not None:
                    samples_df = get_workout_plot_samples(new_id)
                    st.session_state["selected_workout_samples"] = samples_df

        detail = st.session_state.get("selected_workout_detail")
        samples_df = st.session_state.get("selected_workout_samples")

        st.markdown("</div>", unsafe_allow_html=True)
        st.write("")

        if detail:
            st.markdown('<div class="to-panel"><p class="to-panel-title">Workout details</p>', unsafe_allow_html=True)
            st.write(f"**Date/time:** {detail.get('start_time')}")
            st.write(f"**Sport:** {format_sport_label(detail.get('sport_type'))}")
            st.write(f"**Distance:** {detail.get('distance_mi', 0):.2f} mi")
            st.write(f"**Elevation gain:** {detail.get('elevation_ft', 0):.0f} ft")
            st.write(f"**Moving time:** {_format_hhmmss(detail.get('moving_time_s'))}")
            mp = detail.get("moving_pace_min_per_mile")
            if mp is not None and pd.notna(mp) and mp > 0:
                st.write(f"**Moving pace:** {_format_min_per_mile(float(mp))}")
            gap_val = detail.get("avg_gap_min_per_mile")
            if str(detail.get("sport_type", "")).lower().startswith("running") and gap_val is not None and pd.notna(gap_val):
                st.write(f"**GAP:** {_format_min_per_mile(float(gap_val))}")
            if detail.get("notes"):
                st.markdown("**Notes:**")
                st.write(detail["notes"])
            st.markdown("</div>", unsafe_allow_html=True)

    # Left rail: map + charts
    with col_left:
        st.markdown('<div class="to-panel"><p class="to-panel-title">Route</p>', unsafe_allow_html=True)
        if detail:
            _render_route_map(int(detail["id"]), plot_samples_df=samples_df, sport_type=detail.get("sport_type"), start_time_str=detail.get("start_time"))
        else:
            st.info("Select a workout to view the route map.")
        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")

        st.markdown('<div class="to-panel"><p class="to-panel-title">Telemetry</p>', unsafe_allow_html=True)
        if samples_df is None or getattr(samples_df, "empty", True):
            st.info("No chart-ready telemetry found for this workout (plot samples missing).")
        else:
            # Reuse the existing chart block by calling the same code paths:
            st.markdown("#### Time-series analysis (interactive)")
            col1, col2 = st.columns(2)
            with col1:
                _render_series(samples_df, "pace_min_per_mile", "Pace", unit="min/mi", invert_y=True)
            with col2:
                _render_series(samples_df, "elevation_m", "Elevation", unit="m")

            col3, col4 = st.columns(2)
            with col3:
                _render_series(samples_df, "grade", "Grade", unit="%",)
            with col4:
                _render_series(samples_df, "heart_rate_bpm", "Heart rate", unit="bpm")

            col5, col6 = st.columns(2)
            with col5:
                _render_series(samples_df, "cadence_spm", "Cadence", unit="spm")
            with col6:
                _render_series(samples_df, "power_w", "Power", unit="W")
        st.markdown("</div>", unsafe_allow_html=True)
def render_training_dashboard_legacy() -> None:
    st.title("TrailOps · Training Dashboard")
    st.caption(
        "Summaries, recent workouts, and interactive charts. "
        "Detailed telemetry: running/walking/hiking workouts in the last 30 days."
    )

    # Session state
    if "selected_workout_id" not in st.session_state:
        st.session_state["selected_workout_id"] = None
    if "selected_workout_detail" not in st.session_state:
        st.session_state["selected_workout_detail"] = None
    if "selected_workout_samples" not in st.session_state:
        st.session_state["selected_workout_samples"] = None

    # Summaries
    summary = get_training_summary()
    last_7 = summary["last_7"]
    last_14 = summary["last_14"]
    lifetime = summary["lifetime"]

    m_to_mi = 1 / 1609.344
    m_to_ft = 3.28084

    def fmt(summary_dict):
        cat = summary_dict["by_category"]
        running = cat["running"]
        walking = cat["walking"]

        total_dist_m = summary_dict["total_distance_m"]
        total_elev_m = summary_dict["total_elevation_m"]

        return {
            "total_distance_mi": round(total_dist_m * m_to_mi, 2),
            "running_distance_mi": round(running["distance_m"] * m_to_mi, 2),
            "walking_distance_mi": round(walking["distance_m"] * m_to_mi, 2),
            "total_elevation_ft": round(total_elev_m * m_to_ft),
            "running_elevation_ft": round(running["elevation_m"] * m_to_ft),
            "walking_elevation_ft": round(walking["elevation_m"] * m_to_ft),
            "duration_hr": round(summary_dict["total_duration_s"] / 3600, 2),
            "count": summary_dict["count"],
        }

    s7 = fmt(last_7)
    s14 = fmt(last_14)
    slife = fmt(lifetime)

    col_t1, col_t2, col_t3 = st.columns(3)

    with col_t1:
        st.markdown("### Last 7 Days")
        st.write(
            f"**Distance:** {s7['total_distance_mi']} mi  \n"
            f"• Running: {s7['running_distance_mi']} mi  \n"
            f"• Walk/Hike: {s7['walking_distance_mi']} mi"
        )
        st.write(f"**Elevation:** {s7['total_elevation_ft']} ft")
        st.write(f"**Time:** {s7['duration_hr']} hr")
        st.write(f"**Workouts:** {s7['count']}")

    with col_t2:
        st.markdown("### Last 14 Days")
        st.write(
            f"**Distance:** {s14['total_distance_mi']} mi  \n"
            f"• Running: {s14['running_distance_mi']} mi  \n"
            f"• Walk/Hike: {s14['walking_distance_mi']} mi"
        )
        st.write(f"**Elevation:** {s14['total_elevation_ft']} ft")
        st.write(f"**Time:** {s14['duration_hr']} hr")
        st.write(f"**Workouts:** {s14['count']}")

    with col_t3:
        st.markdown("### Lifetime")
        st.write(
            f"**Distance:** {slife['total_distance_mi']} mi  \n"
            f"• Running: {slife['running_distance_mi']} mi  \n"
            f"• Walk/Hike: {slife['walking_distance_mi']} mi"
        )
        st.write(f"**Elevation:** {slife['total_elevation_ft']} ft")
        st.write(f"**Time:** {slife['duration_hr']} hr")
        st.write(f"**Workouts:** {slife['count']}")

    st.markdown("---")

    # Recent workouts list + filters
    st.subheader("Recent workouts (from database)")

    recent_df = get_recent_workouts(limit=200)
    if recent_df.empty:
        st.info("No workouts found yet. Run FIT ingestion in the System / Ingestion app.")
        return

    st.markdown("**Date range**")
    date_mode = st.radio(
        "Preset date range",
        [
            "Last 7 days",
            "Last 14 days",
            "This week",
            "Last week",
            "This month",
            "Last month",
            "Lifetime",
            "Custom",
        ],
        index=0,
        horizontal=True,
    )

    custom_start = custom_end = None
    if date_mode == "Custom":
        today = datetime.date.today()
        col_dr1, col_dr2 = st.columns(2)
        with col_dr1:
            custom_start = st.date_input("Start date", value=today - datetime.timedelta(days=7), key="recent_start")
        with col_dr2:
            custom_end = st.date_input("End date", value=today, key="recent_end")

    if date_mode == "Custom":
        start_date = custom_start
        end_date = custom_end
    else:
        start_date, end_date = get_date_range(date_mode)

    df_filtered = recent_df.copy()
    if start_date is not None and end_date is not None:
        # Pandas can be picky about comparing datetime64 to python date objects.
        # Compare using Timestamps instead, and include the full end_date day.
        df_filtered["start_time"] = pd.to_datetime(df_filtered["start_time"], errors="coerce", utc=True)
        try:
            df_filtered["start_time"] = df_filtered["start_time"].dt.tz_convert(None)
        except Exception:
            pass
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        mask = (df_filtered["start_time"] >= start_ts) & (df_filtered["start_time"] <= end_ts)
        df_filtered = df_filtered[mask]

    if df_filtered.empty:
        st.info("No workouts found in the selected date range.")
        return

    sport_options = sorted(df_filtered["sport_type"].dropna().unique().tolist())

    # Preserve sport filter selections across date range changes
    prev_selected = st.session_state.get("selected_sports")
    if prev_selected is None:
        default_selected = sport_options
    else:
        default_selected = [s for s in prev_selected if s in sport_options]
        if not default_selected:
            default_selected = sport_options

    selected_sports = st.multiselect(
        "Filter by sport type",
        options=sport_options,
        default=default_selected,
        format_func=lambda v: format_sport_label(v),
        key="selected_sports_multiselect",
    )
    st.session_state["selected_sports"] = selected_sports

    if selected_sports:
        filtered_df = df_filtered[df_filtered["sport_type"].isin(selected_sports)]
    else:
        filtered_df = df_filtered.iloc[0:0]

    if filtered_df.empty:
        st.info("No workouts match the selected filters.")
        return

    # Totals
    tmp = filtered_df.copy()
    tmp["category"] = tmp["sport_type"].apply(classify_sport_for_totals)

    running_df = tmp[tmp["category"] == "running"]
    walking_df = tmp[tmp["category"] == "walking"]
    stair_df = tmp[tmp["category"] == "stair"]

    total_dist_mi = float(filtered_df["distance_mi"].sum())
    total_elev_ft = float(filtered_df["elevation_gain_ft"].sum())
    total_duration_hr = float(filtered_df["duration_min"].sum()) / 60.0
    total_count = len(filtered_df)

    run_dist_mi = float(running_df["distance_mi"].sum()) if not running_df.empty else 0.0
    walk_dist_mi = float(walking_df["distance_mi"].sum()) if not walking_df.empty else 0.0
    run_elev_ft = float(running_df["elevation_gain_ft"].sum()) if not running_df.empty else 0.0
    walk_elev_ft = float(walking_df["elevation_gain_ft"].sum()) if not walking_df.empty else 0.0
    stair_elev_ft = float(stair_df["elevation_gain_ft"].sum()) if not stair_df.empty else 0.0

    st.markdown("#### Totals for selected date range")
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    with col_s1:
        st.metric("Total distance (mi)", f"{total_dist_mi:.1f}")
    with col_s2:
        st.metric("Total elevation (ft)", f"{int(round(total_elev_ft))}")
    with col_s3:
        st.metric("Total time (hr)", f"{total_duration_hr:.2f}")
    with col_s4:
        st.metric("Workouts", total_count)

    st.markdown("##### Breakdown by activity")
    col_b1, col_b2, col_b3, col_b4, col_b5 = st.columns(5)
    with col_b1:
        st.metric("Run distance (mi)", f"{run_dist_mi:.1f}")
    with col_b2:
        st.metric("Walk/Hike distance (mi)", f"{walk_dist_mi:.1f}")
    with col_b3:
        st.metric("Run elevation (ft)", f"{int(round(run_elev_ft))}")
    with col_b4:
        st.metric("Walk/Hike elevation (ft)", f"{int(round(walk_elev_ft))}")
    with col_b5:
        st.metric("Stair elevation (ft)", f"{int(round(stair_elev_ft))}")

    st.dataframe(filtered_df, width="stretch")

    # Workout selection
    records = filtered_df.to_dict(orient="records")

    def option_label(rec: dict) -> str:
        dt = rec["start_time"]
        try:
            dt_str = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            dt_str = str(dt)
        sport_label = format_sport_label(rec["sport_type"])
        dist = rec.get('distance_mi')
        try:
            dist_val = float(dist) if dist is not None and pd.notna(dist) else None
        except Exception:
            dist_val = None
        dist_str = f"{dist_val:.2f} mi" if dist_val is not None else "n/a"
        return f"{dt_str} · {sport_label} · {dist_str}"

    selected_rec = st.selectbox(
        "Select a workout for details",
        options=records,
        format_func=option_label,
    )

    if selected_rec:
        new_id = int(selected_rec["id"])
        if new_id != st.session_state["selected_workout_id"]:
            st.session_state["selected_workout_id"] = new_id
            detail = get_workout_detail(new_id)
            st.session_state["selected_workout_detail"] = detail

            if detail is not None:
                samples_df = get_workout_plot_samples_if_allowed(
                    new_id,
                    detail["sport_type"],
                    detail["start_time"],
                )
            else:
                samples_df = pd.DataFrame()

            st.session_state["selected_workout_samples"] = samples_df

    detail = st.session_state["selected_workout_detail"]
    samples_df = st.session_state["selected_workout_samples"]

    if detail is None:
        st.warning("Could not load details for this workout.")
        return
    st.markdown("### Workout details")

    col_d1, col_d2, col_d3 = st.columns(3)
    with col_d1:
        st.write(f"**Date/time:** {detail['start_time']}")
        st.write(f"**Sport:** {format_sport_label(detail['sport_type'])}")
        st.write(f"**Distance:** {detail['distance_mi']:.2f} mi")
        st.write(f"**Elevation gain:** {detail['elevation_ft']:.0f} ft")

    with col_d2:
        # Moving first (Strava-style)
        st.write(f"**Moving time:** {_format_hhmmss(detail.get('moving_time_s'))}")
        mp = detail.get("moving_pace_min_per_mile")
        if mp is not None and pd.notna(mp) and mp > 0:
            st.write(f"**Moving pace:** {_format_min_per_mile(float(mp))}")
        else:
            st.write("**Moving pace:** N/A")

        st.write(f"**Stationary time:** {_format_hhmmss(detail.get('stationary_time_s'))}")

    with col_d3:
        st.write(f"**Total time:** {_format_hhmmss(detail['duration_s'])}")

        if detail["pace_min_per_mile"]:
            st.write(f"**Total pace:** {_format_min_per_mile(float(detail['pace_min_per_mile']))}")
        else:
            st.write("**Total pace:** n/a")

        gap_val = detail.get("avg_gap_min_per_mile")
        if detail.get("sport_type", "").lower().startswith("running") and gap_val is not None and pd.notna(gap_val):
            st.write(f"**GAP:** {_format_min_per_mile(float(gap_val))}")
        else:
            st.write("**GAP:** N/A")


    if detail.get("notes"):
        st.markdown("**Notes:**")
        st.write(detail["notes"])
    # Route map
    _render_route_map(int(detail['id']), plot_samples_df=samples_df, sport_type=detail.get('sport_type'), start_time_str=detail.get('start_time'))


    # Charts
    if samples_df is None or samples_df.empty:
        st.info(
            "No chart-ready telemetry found for this workout. "
            "After ingestion, run the backfill script once to generate plot samples "
            "or add plot-sample generation into ingestion."
        )
        return

    st.markdown("#### Time-series analysis (interactive)")

    col1, col2 = st.columns(2)
    with col1:
        _plot_series(samples_df, "t_min", "pace_min_per_mile", "Pace", "min/mi")
    with col2:
        _plot_series(samples_df, "t_min", "elevation_ft", "Elevation", "ft")

    col3, col4 = st.columns(2)
    with col3:
        if "grade_pct" in samples_df.columns and samples_df["grade_pct"].notna().any():
            _plot_series(samples_df, "t_min", "grade_pct", "Grade", "%")
        else:
            st.write("Grade: N/A")
    with col4:
        _plot_series(samples_df, "t_min", "heart_rate_bpm", "Heart rate", "bpm")

    col5, col6 = st.columns(2)
    with col5:
        _plot_series(samples_df, "t_min", "cadence_spm", "Cadence", "spm")
    with col6:
        if "power_w" in samples_df.columns and samples_df["power_w"].notna().any():
            _plot_series(samples_df, "t_min", "power_w", "Power", "W")
        else:
            st.write("Power: N/A")

if __name__ == "__main__":
    import streamlit as st
    st.set_page_config(page_title="TrailOps · Training Dashboard", page_icon="⛰️", layout="wide")
    render_training_dashboard()
