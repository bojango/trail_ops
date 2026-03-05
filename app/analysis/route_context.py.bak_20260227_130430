from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone

def _now_utc_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string (seconds precision)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

from dataclasses import dataclass
from typing import Any, Optional
import numpy as np
import pandas as pd
import requests
from app.db.database import get_connection
from app.analysis.map_points import get_available_map_levels, get_map_points
from app.analysis.weather import fetch_weather_for_activity

DEFAULT_USER_AGENT = "TrailOps/0.1 (local personal dashboard)"
USER_AGENT = os.environ.get("TRAILOPS_USER_AGENT", DEFAULT_USER_AGENT)

NOMINATIM_REVERSE_URL = os.environ.get("TRAILOPS_NOMINATIM_REVERSE_URL", "https://nominatim.openstreetmap.org/reverse")
OVERPASS_URL = os.environ.get("TRAILOPS_OVERPASS_URL", "https://overpass-api.de/api/interpreter")
# Comma-separated list of Overpass endpoints (we rotate on failures / rate limits)
OVERPASS_URLS = [u.strip() for u in os.environ.get(
    "TRAILOPS_OVERPASS_URLS",
    "https://overpass-api.de/api/interpreter,https://overpass.private.coffee/api/interpreter,https://overpass.osm.ch/api/interpreter,https://overpass.openstreetmap.ru/api/interpreter",
).split(",") if u.strip()]

OVERPASS_TIMEOUT_S = float(os.environ.get("TRAILOPS_OVERPASS_TIMEOUT_S", "120"))
OVERPASS_MAX_RETRIES = int(os.environ.get("TRAILOPS_OVERPASS_MAX_RETRIES", "6"))
OVERPASS_MAX_BACKOFF_S = float(os.environ.get("TRAILOPS_OVERPASS_MAX_BACKOFF_S", "30"))

NOMINATIM_MIN_DELAY_S = float(os.environ.get("TRAILOPS_NOMINATIM_MIN_DELAY_S", "1.0"))
OVERPASS_MIN_DELAY_S = float(os.environ.get("TRAILOPS_OVERPASS_MIN_DELAY_S", "1.0"))

OVERPASS_AROUND_M = float(os.environ.get("TRAILOPS_OVERPASS_AROUND_M", "25"))
MAX_OVERPASS_POINTS = int(os.environ.get("TRAILOPS_MAX_OVERPASS_POINTS", "25"))

PEAK_BBOX_PAD_DEG = float(os.environ.get("TRAILOPS_PEAK_BBOX_PAD_DEG", "0.01"))
PEAK_BAGGED_M = float(os.environ.get("TRAILOPS_PEAK_BAGGED_M", "70"))
PEAK_NEAR_M = float(os.environ.get("TRAILOPS_PEAK_NEAR_M", "250"))
SURFACE_VERSION = "v2"
GEOCODE_VERSION = "v2"

# Optional extra weather sampling points (midpoint etc.)
WEATHER_EXTRA_POINTS = os.environ.get("TRAILOPS_WEATHER_EXTRA_POINTS", "0").strip() in ("1","true","TRUE","yes","YES")


@dataclass(frozen=True)
class GeoArea:
    workout_id: int
    area_type: str
    name: str
    designation: str | None = None


@dataclass(frozen=True)
class WeatherSummary:
    workout_id: int
    start_temp_c: float | None = None
    start_precip_mm: float | None = None
    start_wind_kph: float | None = None
    start_weather_code: int | None = None
    high_temp_c: float | None = None
    high_precip_mm: float | None = None
    high_wind_kph: float | None = None
    high_weather_code: int | None = None
    moon_phase_name: str | None = None
    moon_illumination: float | None = None
    source: str | None = None
    provider: str | None = None
    weather_version: str | None = None

@dataclass(frozen=True)
class RouteContext:
    workout_id: int
    start_lat: float
    start_lon: float
    center_lat: float
    center_lon: float
    location_label: str | None
    locality: str | None
    district: str | None
    county: str | None
    region: str | None
    country: str | None
    country_code: str | None
    provider: str
    raw_json: dict[str, Any]


@dataclass(frozen=True)
class SurfaceStats:
    workout_id: int
    road_m: float
    paved_path_m: float
    trail_m: float
    track_m: float
    grass_m: float
    rock_m: float
    forest_m: float
    unknown_m: float
    detail_json: dict[str, float]
    provider: str
    raw_json: dict[str, Any]


@dataclass(frozen=True)
class PeakHit:
    peak_id: str
    name: str | None
    lat: float
    lon: float
    ele_m: float | None
    distance_m: float


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _downsample_points(pts: pd.DataFrame, max_points: int) -> pd.DataFrame:
    if pts.empty or len(pts) <= max_points:
        return pts
    idx = [int(round(i)) for i in list(np.linspace(0, len(pts) - 1, max_points))]
    idx = sorted(set(max(0, min(len(pts) - 1, i)) for i in idx))
    return pts.iloc[idx].reset_index(drop=True)


def _normalize_latlon_df(pts: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure latitude_deg / longitude_deg are valid degrees.

    Supports:
    - degrees (already in [-90..90], [-180..180])
    - FIT semicircles (Apple Watch / FIT raw)
    - E7 scaled integers (rare here, but handled defensively)

    Returns a filtered copy with numeric lat/lon and valid bounds.
    """
    if pts is None or pts.empty:
        return pd.DataFrame()

    out = pts.copy()

    # Coerce to numeric (some rows can come back as strings due to drift)
    out["latitude_deg"] = pd.to_numeric(out["latitude_deg"], errors="coerce")
    out["longitude_deg"] = pd.to_numeric(out["longitude_deg"], errors="coerce")
    out = out.dropna(subset=["latitude_deg", "longitude_deg"])
    if out.empty:
        return pd.DataFrame()

    lat_abs_max = float(out["latitude_deg"].abs().max())
    lon_abs_max = float(out["longitude_deg"].abs().max())

    # If clearly out-of-range, try semicircles first (your DB shows values ~6e8)
    if lat_abs_max > 90.0 or lon_abs_max > 180.0:
        # Only attempt if magnitudes look like encoded integers
        if lat_abs_max > 1000.0 or lon_abs_max > 1000.0:
            out["latitude_deg"] = out["latitude_deg"].astype(float) * 180.0 / (2**31)
            out["longitude_deg"] = out["longitude_deg"].astype(float) * 180.0 / (2**31)

            # Re-check; if still out-of-range, fall back to E7 scaling
            lat_abs_max2 = float(out["latitude_deg"].abs().max())
            lon_abs_max2 = float(out["longitude_deg"].abs().max())
            if lat_abs_max2 > 90.0 or lon_abs_max2 > 180.0:
                out["latitude_deg"] = out["latitude_deg"].astype(float) / 1e7
                out["longitude_deg"] = out["longitude_deg"].astype(float) / 1e7

    # Final bounds filter
    out = out[(out["latitude_deg"].abs() <= 90.0) & (out["longitude_deg"].abs() <= 180.0)]
    if out.empty:
        return pd.DataFrame()

    return out


def _pick_best_route_points(workout_id: int) -> pd.DataFrame:
    levels = get_available_map_levels(workout_id)
    if not levels:
        return pd.DataFrame()

    level = min(int(l) for l in levels)
    pts = get_map_points(workout_id, level=level)
    if pts is None or pts.empty:
        return pd.DataFrame()

    pts = _normalize_latlon_df(pts)
    return pts
def _route_center(pts: pd.DataFrame) -> tuple[float, float]:
    return float(pts["latitude_deg"].mean()), float(pts["longitude_deg"].mean())


def _headers() -> dict[str, str]:
    return {"User-Agent": USER_AGENT}


def _raise_http_error(resp: requests.Response, context: str) -> None:
    txt = (resp.text or "")[:600].replace("\n", " ").replace("\r", " ")
    raise RuntimeError(f"{context} HTTP {resp.status_code}: {txt}")


def _get_json_with_retries(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    params=None,
    data=None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """HTTP JSON fetch with retries, backoff, and optional Overpass endpoint rotation.

    - Retries on 429/502/503/504.
    - If the target is an Overpass endpoint, rotates through OVERPASS_URLS.
    - Uses Retry-After header on 429 when present.
    """
    method_u = method.upper()
    timeout_s = float(timeout) if timeout is not None else float(OVERPASS_TIMEOUT_S)

    # Decide whether to rotate endpoints
    overpass_set = set(OVERPASS_URLS) | {OVERPASS_URL}
    rotate = url in overpass_set and bool(OVERPASS_URLS)

    last_err: Exception | None = None

    for attempt in range(1, OVERPASS_MAX_RETRIES + 1):
        # Base politeness delay (especially important for Overpass)
        if rotate:
            time.sleep(max(0.0, OVERPASS_MIN_DELAY_S))

        # Pick endpoint
        use_url = url
        if rotate:
            use_url = OVERPASS_URLS[(attempt - 1) % len(OVERPASS_URLS)]

        try:
            if method_u == "GET":
                resp = requests.get(use_url, params=params, headers=headers, timeout=timeout_s)
            else:
                resp = requests.post(use_url, params=params, data=data, headers=headers, timeout=timeout_s)

            if resp.status_code in (429, 502, 503, 504):
                # Respect Retry-After if provided (seconds)
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        wait = float(ra)
                        time.sleep(min(wait, OVERPASS_MAX_BACKOFF_S))
                    except Exception:
                        pass

                # Exponential backoff
                backoff = min((2 ** (attempt - 1)), OVERPASS_MAX_BACKOFF_S)
                time.sleep(backoff)
                last_err = RuntimeError(f"HTTP {resp.status_code} from {use_url}")
                continue

            if resp.status_code >= 400:
                _raise_http_error(resp, f"Request to {use_url}")

            return resp.json()

        except Exception as e:
            # Exponential backoff on network/JSON errors too
            backoff = min((2 ** (attempt - 1)), OVERPASS_MAX_BACKOFF_S)
            time.sleep(backoff)
            last_err = e
            continue

    raise RuntimeError(f"Failed request to {url}: {last_err}")

def _nominatim_reverse(lat: float, lon: float) -> dict[str, Any]:
    time.sleep(NOMINATIM_MIN_DELAY_S)
    params = {"format": "jsonv2", "lat": lat, "lon": lon, "zoom": 18, "addressdetails": 1}
    return _get_json_with_retries("GET", NOMINATIM_REVERSE_URL, headers=_headers(), params=params, timeout=30)


def _format_location_label(addr: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    locality = addr.get("hamlet") or addr.get("village") or addr.get("town") or addr.get("city") or addr.get("suburb")
    district = addr.get("city_district") or addr.get("district") or addr.get("municipality")
    county = addr.get("county")
    region = addr.get("state") or addr.get("region")
    country = addr.get("country")
    cc = addr.get("country_code")
    parts = [p for p in [locality, district or county, region, country] if p]
    label = ", ".join(parts) if parts else None
    return label, {"locality": locality, "district": district, "county": county, "region": region, "country": country, "country_code": cc}


def _get_workout_timeinfo(workout_id: int) -> tuple[str | None, float | None]:
    conn = get_connection()
    try:
        row = conn.execute("SELECT start_time, duration_s FROM workouts WHERE id = ?", (workout_id,)).fetchone()
        if not row:
            return None, None
        return (str(row["start_time"]) if row["start_time"] else None), (float(row["duration_s"]) if row["duration_s"] is not None else None)
    finally:
        conn.close()


def get_route_context(workout_id: int) -> RouteContext | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM workout_route_context WHERE workout_id = ?", (workout_id,)).fetchone()
        if not row:
            return None
        raw = {}
        if row["raw_json"]:
            try:
                raw = json.loads(row["raw_json"])
            except Exception:
                raw = {}
        return RouteContext(
            workout_id=int(row["workout_id"]),
            start_lat=float(row["start_lat"]),
            start_lon=float(row["start_lon"]),
            center_lat=float(row["center_lat"]),
            center_lon=float(row["center_lon"]),
            location_label=row["location_label"],
            locality=row["locality"],
            district=row["district"],
            county=row["county"],
            region=row["region"],
            country=row["country"],
            country_code=row["country_code"],
            provider=row["provider"] or "nominatim",
            raw_json=raw,
        )
    finally:
        conn.close()




def _table_exists(conn, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)).fetchone()
    return row is not None

def _table_columns(conn, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r["name"]) for r in rows}
    except Exception:
        return set()


def _upsert_row(conn, table: str, pk_col: str, data: dict[str, Any]) -> None:
    """
    Insert or replace, but only with columns that actually exist in the live SQLite table.

    This makes the enrichment resilient to schema drift while we stabilise.
    """
    cols = _table_columns(conn, table)
    if not cols:
        raise RuntimeError(f"Table not found or unreadable: {table}")

    # Only keep columns that exist
    filtered = {k: v for k, v in data.items() if k in cols}

    # Must include the PK
    if pk_col not in filtered:
        raise RuntimeError(f"Missing required PK column '{pk_col}' for {table}")

    col_names = list(filtered.keys())
    placeholders = ", ".join(["?"] * len(col_names))
    col_sql = ", ".join(col_names)

    sql = f"INSERT OR REPLACE INTO {table} ({col_sql}) VALUES ({placeholders})"
    conn.execute(sql, tuple(filtered[c] for c in col_names))


def _insert_weather_row(conn, workout_id: int, wp: Any) -> None:
    """Insert/replace a weather point, but only into columns that exist.

    This protects us from schema drift (your DB currently has both legacy 'start_*' columns
    and the newer per-point columns).
    """
    cols = _table_columns(conn, "workout_weather")
    data: dict[str, Any] = {
        "workout_id": workout_id,
        "point_type": getattr(wp, "point_type", None),
        "lat": getattr(wp, "lat", None),
        "lon": getattr(wp, "lon", None),
        "ele_m": getattr(wp, "ele_m", None),
        "obs_time_utc": getattr(wp, "obs_time_utc", None),
        "temp_c": getattr(wp, "temp_c", None),
        "wind_kph": getattr(wp, "wind_kph", None),
        "precip_mm": getattr(wp, "precip_mm", None),
        "weather_code": getattr(wp, "weather_code", None),
        "precip_sum_mm": getattr(wp, "precip_sum_mm", None),
        "cloudcover_pct": getattr(wp, "cloudcover_pct", None),
        "rel_humidity_pct": getattr(wp, "rel_humidity_pct", None),
        "wind_dir_deg": getattr(wp, "wind_dir_deg", None),
        "dewpoint_c": getattr(wp, "dewpoint_c", None),
        "moon_phase": getattr(wp, "moon_phase", None),
        "moon_illumination": getattr(wp, "moon_illumination", None),
        "provider": getattr(wp, "provider", None),
        "raw_json": json.dumps(getattr(wp, "raw_json", None)) if isinstance(getattr(wp, "raw_json", None), (dict, list)) else getattr(wp, "raw_json", None),
    }
    filtered = {k: v for k, v in data.items() if k in cols}
    col_names = list(filtered.keys())
    placeholders = ", ".join(["?"] * len(col_names))
    sql = f"INSERT OR REPLACE INTO workout_weather ({', '.join(col_names)}) VALUES ({placeholders})"
    conn.execute(sql, tuple(filtered[c] for c in col_names))





def _insert_weather_point(conn, workout_id: int, wp: Any) -> None:
    """Insert/replace a per-point weather row into workout_weather_points.

    This table uses (workout_id, point_type) as a composite primary key, so multiple points
    (start/high/mid/end) can safely coexist. Columns are filtered to those that exist.
    """
    cols = _table_columns(conn, "workout_weather_points")
    data: dict[str, Any] = {
        "workout_id": workout_id,
        "point_type": getattr(wp, "point_type", None),
        "lat": getattr(wp, "lat", None),
        "lon": getattr(wp, "lon", None),
        "ele_m": getattr(wp, "ele_m", None),
        "obs_time_utc": getattr(wp, "obs_time_utc", None),
        "temp_c": getattr(wp, "temp_c", None),
        "wind_kph": getattr(wp, "wind_kph", None),
        "precip_mm": getattr(wp, "precip_mm", None),
        "weather_code": getattr(wp, "weather_code", None),
        "precip_sum_mm": getattr(wp, "precip_sum_mm", None),
        "cloudcover_pct": getattr(wp, "cloudcover_pct", None),
        "rel_humidity_pct": getattr(wp, "rel_humidity_pct", None),
        "wind_dir_deg": getattr(wp, "wind_dir_deg", None),
        "dewpoint_c": getattr(wp, "dewpoint_c", None),
        "moon_phase": getattr(wp, "moon_phase", None),
        "moon_illumination": getattr(wp, "moon_illumination", None),
        "provider": getattr(wp, "provider", None),
        "raw_json": json.dumps(getattr(wp, "raw_json", None)) if isinstance(getattr(wp, "raw_json", None), (dict, list)) else getattr(wp, "raw_json", None),
    }
    filtered = {k: v for k, v in data.items() if k in cols}
    col_names = list(filtered.keys())
    placeholders = ", ".join(["?"] * len(col_names))
    sql = f"INSERT OR REPLACE INTO workout_weather_points ({', '.join(col_names)}) VALUES ({placeholders})"
    conn.execute(sql, tuple(filtered[c] for c in col_names))


def _upsert_legacy_weather_summary(conn, workout_id: int, wp_start: Any, wp_high: Any, *, start_time_utc: str | None, end_time_utc: str | None) -> None:
    """Maintain the legacy workout_weather row for backwards compatibility.

    Many existing dashboards read 'start_*' and 'high_*' columns from workout_weather.
    We keep that behaviour, while detailed per-point rows live in workout_weather_points.
    """
    cols = _table_columns(conn, "workout_weather")
    # Combine raw_json into a single blob for legacy row
    raw_obj = {
        "start": getattr(wp_start, "raw_json", None),
        "high": getattr(wp_high, "raw_json", None),
    }
    data: dict[str, Any] = {
        "workout_id": workout_id,
        "start_lat": getattr(wp_start, "lat", None),
        "start_lon": getattr(wp_start, "lon", None),
        "start_time_utc": start_time_utc,
        "end_time_utc": end_time_utc,
        "start_temp_c": getattr(wp_start, "temp_c", None),
        "start_precip_mm": getattr(wp_start, "precip_mm", None),
        "start_wind_kph": getattr(wp_start, "wind_kph", None),
        "start_weather_code": getattr(wp_start, "weather_code", None),
        "high_lat": getattr(wp_high, "lat", None),
        "high_lon": getattr(wp_high, "lon", None),
        "high_ele_m": getattr(wp_high, "ele_m", None),
        "high_temp_c": getattr(wp_high, "temp_c", None),
        "high_precip_mm": getattr(wp_high, "precip_mm", None),
        "high_wind_kph": getattr(wp_high, "wind_kph", None),
        "high_weather_code": getattr(wp_high, "weather_code", None),
        "provider": getattr(wp_start, "provider", None),
        "raw_json": json.dumps(raw_obj),
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    filtered = {k: v for k, v in data.items() if k in cols}
    col_names = list(filtered.keys())
    placeholders = ", ".join(["?"] * len(col_names))
    sql = f"INSERT OR REPLACE INTO workout_weather ({', '.join(col_names)}) VALUES ({placeholders})"
    conn.execute(sql, tuple(filtered[c] for c in col_names))

def compute_and_store_route_context(workout_id: int, *, force: bool = False) -> RouteContext | None:
    if not force:
        existing = get_route_context(workout_id)
        if existing is not None:
            return existing

    pts = _pick_best_route_points(workout_id)
    if pts.empty:
        return None

    start_lat = float(pts.iloc[0]["latitude_deg"])
    start_lon = float(pts.iloc[0]["longitude_deg"])
    center_lat, center_lon = _route_center(pts)

    geo_start = _nominatim_reverse(start_lat, start_lon)
    start_addr = (geo_start.get("address") or {})
    start_label, start_fields = _format_location_label(start_addr)
    start_location_label = geo_start.get("display_name")

    geo_center = _nominatim_reverse(center_lat, center_lon)
    center_addr = (geo_center.get("address") or {})
    _center_label, center_fields = _format_location_label(center_addr)
    center_location_label = geo_center.get("display_name")

    # Prefer a useful "suburb" for dense areas, but keep locality as hamlet/village/town/city
    start_suburb = start_addr.get("suburb") or start_addr.get("neighbourhood") or start_addr.get("quarter")
    center_suburb = center_addr.get("suburb") or center_addr.get("neighbourhood") or center_addr.get("quarter")

    rc = RouteContext(
        workout_id=workout_id,
        start_lat=start_lat,
        start_lon=start_lon,
        center_lat=center_lat,
        center_lon=center_lon,
        location_label=start_label,
        locality=start_fields.get("locality"),
        district=start_fields.get("district"),
        county=start_fields.get("county"),
        region=start_fields.get("region"),
        country=start_fields.get("country"),
        country_code=start_fields.get("country_code"),
        provider="nominatim",
        raw_json={"start": geo_start, "center": geo_center},
    )

    conn = get_connection()
    try:
        now_ts = time.strftime("%Y-%m-%d %H:%M:%S")
        data: dict[str, Any] = {
            # Core
            "workout_id": rc.workout_id,
            "start_lat": rc.start_lat,
            "start_lon": rc.start_lon,
            "center_lat": rc.center_lat,
            "center_lon": rc.center_lon,
            "provider": rc.provider,
            "geocode_version": GEOCODE_VERSION,
            "raw_json": json.dumps(rc.raw_json),
            "computed_at": now_ts,

            # Rich "start/center" fields (newer schema)
            "start_location_label": start_location_label,
            "start_locality": start_fields.get("locality"),
            "start_suburb": start_suburb,
            "start_region": start_fields.get("region"),
            "start_country": start_fields.get("country"),
            "start_country_code": start_fields.get("country_code"),
            "center_location_label": center_location_label,
            "center_locality": center_fields.get("locality"),
            "center_suburb": center_suburb,
            "center_region": center_fields.get("region"),
            "center_country": center_fields.get("country"),
            "center_country_code": center_fields.get("country_code"),

            # Flattened label hierarchy (older schema + dashboard friendly)
            "location_label": rc.location_label,
            "locality": rc.locality,
            "district": rc.district,
            "county": rc.county,
            "region": rc.region,
            "country": rc.country,
            "country_code": rc.country_code,
        }

        _upsert_row(conn, "workout_route_context", "workout_id", data)
        conn.commit()
    finally:
        conn.close()

    return rc

def _compute_and_store_weather(workout_id: int, pts: pd.DataFrame) -> None:
    """Compute weather points for a workout and persist them.

    Writes:
      1) workout_weather_points: one row per point_type (start/high today)
      2) workout_weather: legacy summary row with start_* and high_* columns for dashboard compatibility
    """
    start_time, duration_s = _get_workout_timeinfo(workout_id)
    if not start_time:
        return

    # Start point
    start_lat = float(pts.iloc[0]["latitude_deg"])
    start_lon = float(pts.iloc[0]["longitude_deg"])

    # Highest elevation point (fallback to start)
    high_lat, high_lon, high_ele = start_lat, start_lon, None
    if "elevation_m" in pts.columns and pts["elevation_m"].notna().any():
        idx = int(pts["elevation_m"].fillna(-1e9).idxmax())
        high_lat = float(pts.loc[idx, "latitude_deg"])
        high_lon = float(pts.loc[idx, "longitude_deg"])
        try:
            high_ele = float(pts.loc[idx, "elevation_m"])
        except Exception:
            high_ele = None

    wp_start = fetch_weather_for_activity(
        point_type="start",
        lat=start_lat,
        lon=start_lon,
        ele_m=None,
        start_time_iso=start_time,
        duration_s=duration_s,
    )
    wp_high = fetch_weather_for_activity(
        point_type="high",
        lat=high_lat,
        lon=high_lon,
        ele_m=high_ele,
        start_time_iso=start_time,
        duration_s=duration_s,
    )

    conn = get_connection()
    try:
        # 1) Robust per-point table (one row per workout_id+point_type)
        for wp in (wp_start, wp_high):
            _insert_weather_point(conn, workout_id, wp)

        # 2) Legacy single-row summary for dashboards that still read workout_weather.start_*/high_*
        _upsert_legacy_weather_summary(
            conn,
            workout_id,
            wp_start,
            wp_high,
            start_time_utc=start_time,
            end_time_utc=None,
        )

        conn.commit()
    finally:
        conn.close()


def compute_and_store_weather(workout_id: int, *, force: bool = False) -> bool | None:
    """Fetch and store weather context for a workout.

    Returns:
      - True when weather rows were written (or already exist)
      - None when the workout has no usable GPS points
    """
    if not force:
        conn = get_connection()
        try:
            # Prefer the per-point table if present
            try:
                row = conn.execute(
                    "SELECT 1 FROM workout_weather_points WHERE workout_id = ? LIMIT 1",
                    (workout_id,),
                ).fetchone()
            except Exception:
                row = conn.execute(
                    "SELECT 1 FROM workout_weather WHERE workout_id = ? LIMIT 1",
                    (workout_id,),
                ).fetchone()
            if row:
                return True
        finally:
            conn.close()

    pts = _pick_best_route_points(workout_id)
    if pts.empty:
        return None

    _compute_and_store_weather(workout_id, pts)
    return True

def get_surface_stats(workout_id: int) -> SurfaceStats | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM workout_surface_stats WHERE workout_id = ?", (workout_id,)).fetchone()
        if not row:
            return None
        raw = {}
        if row["raw_json"]:
            try:
                raw = json.loads(row["raw_json"])
            except Exception:
                raw = {}
        detail = {}
        if row["detail_json"]:
            try:
                detail = json.loads(row["detail_json"])
            except Exception:
                detail = {}
        return SurfaceStats(
            workout_id=int(row["workout_id"]),
            road_m=float(row["road_m"] or 0.0),
            paved_path_m=float(row["paved_path_m"] or 0.0),
            trail_m=float(row["trail_m"] or 0.0),
            track_m=float(row["track_m"] or 0.0),
            grass_m=float(row["grass_m"] or 0.0),
            rock_m=float(row["rock_m"] or 0.0),
            forest_m=float(row["forest_m"] or 0.0),
            unknown_m=float(row["unknown_m"] or 0.0),
            detail_json=detail,
            provider=row["provider"] or "overpass",
            raw_json=raw,
        )
    finally:
        conn.close()


def _classify_way(tags: dict[str, Any]) -> tuple[str, str]:
    highway = (tags.get("highway") or "").lower()
    surface = (tags.get("surface") or "").lower()
    tracktype = (tags.get("tracktype") or "").lower()
    landuse = (tags.get("landuse") or "").lower()
    natural = (tags.get("natural") or "").lower()

    if surface:
        detail = f"surface:{surface}"
    elif tracktype:
        detail = f"tracktype:{tracktype}"
    elif highway:
        detail = f"highway:{highway}"
    elif landuse:
        detail = f"landuse:{landuse}"
    elif natural:
        detail = f"natural:{natural}"
    else:
        detail = "unknown"

    if highway in ("motorway","trunk","primary","secondary","tertiary","residential","unclassified","service"):
        return "road", detail
    if highway in ("footway","cycleway","pedestrian","path"):
        if surface in ("asphalt","paved","paving_stones","concrete"):
            return "paved_path", detail
        return "trail", detail
    if highway == "track":
        return "track", detail
    if surface == "grass":
        return "grass", detail
    if natural == "wood" or landuse == "forest":
        return "forest", detail
    if natural in ("bare_rock","scree","rock"):
        return "rock", detail
    return "unknown", detail


def _overpass_ways_for_points(pts: pd.DataFrame) -> dict[str, Any]:
    pts2 = _downsample_points(pts, MAX_OVERPASS_POINTS)
    parts = []
    for _, r in pts2.iterrows():
        lat = float(r["latitude_deg"])
        lon = float(r["longitude_deg"])
        parts.append(f'way(around:{int(OVERPASS_AROUND_M)},{lat:.7f},{lon:.7f})["highway"];')
    q = "[out:json][timeout:60];(" + "".join(parts) + ");out body;>;out skel qt;"
    time.sleep(OVERPASS_MIN_DELAY_S)
    return _get_json_with_retries("POST", OVERPASS_URL, headers=_headers(), data={"data": q}, timeout=60)


def _compute_surface_stats(pts: pd.DataFrame) -> tuple[dict[str, float], dict[str, float], dict[str, Any]]:
    if pts.empty or len(pts) < 2:
        return {}, {}, {}

    data = _overpass_ways_for_points(pts)
    elements = data.get("elements") or []

    nodes = {e["id"]: e for e in elements if e.get("type") == "node"}
    ways = []
    for e in elements:
        if e.get("type") != "way":
            continue
        nds = e.get("nodes") or []
        coords = []
        for nid in nds:
            n = nodes.get(nid)
            if n and "lat" in n and "lon" in n:
                coords.append((float(n["lat"]), float(n["lon"])))
        if not coords:
            continue
        clat = sum(c[0] for c in coords) / len(coords)
        clon = sum(c[1] for c in coords) / len(coords)
        ways.append((clat, clon, e.get("tags") or {}))

    buckets = {k: 0.0 for k in ["road","paved_path","trail","track","grass","rock","forest","unknown"]}
    detail_totals: dict[str, float] = {}

    for i in range(len(pts) - 1):
        a = pts.iloc[i]
        b = pts.iloc[i + 1]
        lat1, lon1 = float(a["latitude_deg"]), float(a["longitude_deg"])
        lat2, lon2 = float(b["latitude_deg"]), float(b["longitude_deg"])
        seg_m = _haversine_m(lat1, lon1, lat2, lon2)
        if seg_m <= 0:
            continue
        mid_lat, mid_lon = (lat1 + lat2) / 2.0, (lon1 + lon2) / 2.0

        best_tags = None
        best_d = None
        for wlat, wlon, tags in ways:
            d = _haversine_m(mid_lat, mid_lon, wlat, wlon)
            if best_d is None or d < best_d:
                best_d = d
                best_tags = tags

        if best_tags is None or (best_d is not None and best_d > 60.0):
            buckets["unknown"] += seg_m
            detail_totals["unknown"] = detail_totals.get("unknown", 0.0) + seg_m
        else:
            coarse, detail = _classify_way(best_tags)
            buckets[coarse] += seg_m
            detail_totals[detail] = detail_totals.get(detail, 0.0) + seg_m

    return buckets, detail_totals, {"overpass_elements": len(elements), "matched_ways": len(ways)}


def compute_and_store_surface_stats(workout_id: int, *, force: bool = False) -> SurfaceStats | None:
    if not force:
        existing = get_surface_stats(workout_id)
        if existing is not None:
            return existing

    pts = _pick_best_route_points(workout_id)
    if pts.empty:
        return None

    buckets, detail, meta = _compute_surface_stats(pts)
    ss = SurfaceStats(
        workout_id=workout_id,
        road_m=buckets.get("road", 0.0),
        paved_path_m=buckets.get("paved_path", 0.0),
        trail_m=buckets.get("trail", 0.0),
        track_m=buckets.get("track", 0.0),
        grass_m=buckets.get("grass", 0.0),
        rock_m=buckets.get("rock", 0.0),
        forest_m=buckets.get("forest", 0.0),
        unknown_m=buckets.get("unknown", 0.0),
        detail_json=detail,
        provider="overpass",
        raw_json=meta,
    )

    conn = get_connection()
    try:
        conn.execute(
            '''
            INSERT OR REPLACE INTO workout_surface_stats
            (workout_id, road_m, paved_path_m, trail_m, track_m, grass_m, rock_m, forest_m, unknown_m, detail_json, provider, surface_version, raw_json, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                workout_id,
                ss.road_m,
                ss.paved_path_m,
                ss.trail_m,
                ss.track_m,
                ss.grass_m,
                ss.rock_m,
                ss.forest_m,
                ss.unknown_m,
                json.dumps(ss.detail_json),
                ss.provider,
                SURFACE_VERSION,
                json.dumps(ss.raw_json),
                _now_utc_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return ss


def _overpass_peaks_for_points(pts: pd.DataFrame, within_m: float) -> dict[str, Any]:
    pts2 = _downsample_points(pts, MAX_OVERPASS_POINTS)
    parts = []
    for _, r in pts2.iterrows():
        lat = float(r["latitude_deg"])
        lon = float(r["longitude_deg"])
        parts.append(f'node(around:{int(within_m)},{lat:.7f},{lon:.7f})["natural"="peak"];')
    q = "[out:json][timeout:60];(" + "".join(parts) + ");out body;"
    time.sleep(OVERPASS_MIN_DELAY_S)
    return _get_json_with_retries("POST", OVERPASS_URL, headers=_headers(), data={"data": q}, timeout=60)


def _peak_id_col(conn) -> str:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(workout_peak_hits)").fetchall()}
    # preferred
    if "peak_id" in cols:
        return "peak_id"
    # common legacy fallbacks
    for cand in ("osm_id", "peak_osm_id", "node_id", "peak_node_id"):
        if cand in cols:
            return cand
    return "peak_id"

def get_peak_hits(workout_id: int) -> list[PeakHit]:
    conn = get_connection()
    try:
        peak_col = _peak_id_col(conn)
        rows = conn.execute(
            f'''
            SELECT h.{peak_col} AS peak_id, p.name, p.lat, p.lon, p.ele_m, h.distance_m
            FROM workout_peak_hits h
            JOIN peaks p ON p.peak_id = h.{peak_col}
            WHERE h.workout_id = ?
            ORDER BY h.distance_m ASC
            ''',
            (workout_id,),
        ).fetchall()
        return [
            PeakHit(
                peak_id=str(r["peak_id"]),
                name=r.get("name"),
                lat=float(r["lat"]),
                lon=float(r["lon"]),
                ele_m=float(r["ele_m"]) if r.get("ele_m") is not None else None,
                distance_m=float(r.get("distance_m") or 0.0),
            )
            for r in rows
        ]
    finally:
        conn.close()





def get_weather(workout_id: int) -> WeatherSummary | None:
    """Return the legacy per-activity weather summary row.

    The dashboard expects the legacy 'start_*' and 'high_*' columns plus moon fields.
    We keep this as a simple read wrapper over workout_weather, and stay resilient to
    schema drift by only selecting columns that exist.
    """
    conn = get_connection()
    try:
        if not _table_exists(conn, "workout_weather"):
            return None

        cols = _table_columns(conn, "workout_weather")
        wanted = [
            "workout_id",
            "start_temp_c", "start_precip_mm", "start_wind_kph", "start_weather_code",
            "high_temp_c", "high_precip_mm", "high_wind_kph", "high_weather_code",
            "moon_phase_name", "moon_illumination",
            "source", "provider", "weather_version",
        ]
        sel = [c for c in wanted if c in cols]
        if not sel:
            return None

        row = conn.execute(
            f"SELECT {', '.join(sel)} FROM workout_weather WHERE workout_id = ?",
            (workout_id,),
        ).fetchone()
        if not row:
            return None

        d = {k: row[k] for k in sel}
        d.setdefault("workout_id", workout_id)
        return WeatherSummary(**d)
    finally:
        conn.close()


def get_geo_areas(workout_id: int) -> list[GeoArea]:
    """Return protected areas / regions for UI chips.

    If the geo-area table isn't present (or hasn't been populated yet), return [].
    This is intentionally best-effort and should never break dashboard import.
    """
    conn = get_connection()
    try:
        # Try the most likely table name first.
        table = "workout_geo_areas"
        if not _table_exists(conn, table):
            return []

        cols = _table_columns(conn, table)
        # Minimal fields expected by training_dashboard.py
        name_col = "name" if "name" in cols else None
        type_col = "area_type" if "area_type" in cols else None
        desig_col = "designation" if "designation" in cols else None

        if not (name_col and type_col):
            return []

        sel = ["workout_id", type_col, name_col]
        if desig_col:
            sel.append(desig_col)

        rows = conn.execute(
            f"SELECT {', '.join(sel)} FROM {table} WHERE workout_id = ? ORDER BY {type_col}, {name_col}",
            (workout_id,),
        ).fetchall()

        out: list[GeoArea] = []
        for r in rows:
            out.append(
                GeoArea(
                    workout_id=int(r["workout_id"]),
                    area_type=str(r[type_col]),
                    name=str(r[name_col]),
                    designation=str(r[desig_col]) if desig_col and r.get(desig_col) is not None else None,
                )
            )
        return out
    finally:
        conn.close()


def get_peak_visits(workout_id: int) -> list[dict[str, Any]]:
    """Return peak visits for the dashboard expander.

    This is a convenience wrapper that uses workout_peak_hits + peaks.
    We treat 'bagged' as the "visited" set if hit_type exists; otherwise we include all hits.
    Returned dict keys match what training_dashboard.py expects (best-effort).
    """
    conn = get_connection()
    try:
        if not _table_exists(conn, "workout_peak_hits") or not _table_exists(conn, "peaks"):
            return []

        peak_col = _peak_id_col(conn)
        cols = _table_columns(conn, "workout_peak_hits")
        has_hit_type = "hit_type" in cols

        where_hit = "AND h.hit_type = 'bagged'" if has_hit_type else ""

        # Visits count + first seen across workouts for each peak
        q = f'''
        WITH this AS (
            SELECT DISTINCT h.{peak_col} AS peak_id
            FROM workout_peak_hits h
            WHERE h.workout_id = ?
            {where_hit}
        ),
        visits AS (
            SELECT h2.{peak_col} AS peak_id,
                   COUNT(DISTINCT h2.workout_id) AS visits,
                   MIN(w.start_time_utc) AS first_seen
            FROM workout_peak_hits h2
            JOIN workouts w ON w.id = h2.workout_id
            WHERE h2.{peak_col} IN (SELECT peak_id FROM this)
            {"AND h2.hit_type = 'bagged'" if has_hit_type else ""}
            GROUP BY h2.{peak_col}
        )
        SELECT
            p.peak_osm_id AS peak_osm_id,
            p.peak_id     AS peak_id_text,
            p.name        AS name,
            p.ele_m       AS ele_m,
            v.visits      AS visits,
            v.first_seen  AS first_seen
        FROM visits v
        JOIN peaks p ON p.peak_id = v.peak_id
        ORDER BY p.ele_m DESC NULLS LAST, v.visits DESC;
        '''
        rows = conn.execute(q, (workout_id,)).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "peak_osm_id": r.get("peak_osm_id"),
                    "peak_id": r.get("peak_id_text"),
                    "name": r.get("name"),
                    "ele_m": r.get("ele_m"),
                    "visits": r.get("visits"),
                    "first_seen": r.get("first_seen"),
                }
            )
        return out
    finally:
        conn.close()


def compute_and_store_peak_hits(
    workout_id: int,
    *,
    force: bool = False,
    bagged_m: float | None = None,
    near_m: float | None = None,
) -> list[PeakHit] | None:
    """Compute and store peak/POI hits for a workout.

    Notes:
    - We fetch *candidates* from Overpass using a bbox around the full route (fast + complete-ish),
      then compute minimum point-to-point distance locally to decide which were "bagged" vs "near".
    - We store the OSM node id in BOTH:
        - peaks.peak_osm_id (INTEGER primary key)
        - peaks.peak_id (TEXT, e.g. "osm_node:123")
      and we store both identifiers in workout_peak_hits to keep joins/back-compat working.
    - hit_type:
        - "bagged" if within bagged_m (default PEAK_BAGGED_M)
        - "near"   if within near_m   (default PEAK_NEAR_M)
    """
    bagged_m = float(PEAK_BAGGED_M if bagged_m is None else bagged_m)
    near_m = float(PEAK_NEAR_M if near_m is None else near_m)

    if not force:
        existing = get_peak_hits(workout_id)
        if existing:
            return existing

    pts = _pick_best_route_points(workout_id)
    if pts.empty:
        return None

    # Build bbox around entire route (with padding).
    min_lat = float(pts["latitude_deg"].min()) - float(PEAK_BBOX_PAD_DEG)
    max_lat = float(pts["latitude_deg"].max()) + float(PEAK_BBOX_PAD_DEG)
    min_lon = float(pts["longitude_deg"].min()) - float(PEAK_BBOX_PAD_DEG)
    max_lon = float(pts["longitude_deg"].max()) + float(PEAK_BBOX_PAD_DEG)

    # Reasonable mountain-related POIs we can expand later:
    # - peaks/hills (the obvious)
    # - saddles / passes
    # - cairns
    # - viewpoints
    # - trig points / survey points
    # - notable rocks
    # - monuments/memorials (common on summits)
    q = (
        f'[out:json][timeout:60];('
        f'node["natural"="peak"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["natural"="hill"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["natural"="saddle"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["mountain_pass"="yes"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["tourism"="viewpoint"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["man_made"="cairn"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["man_made"="survey_point"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["survey_point"="trig_point"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["natural"="rock"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["historic"="monument"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f'node["historic"="memorial"]({min_lat},{min_lon},{max_lat},{max_lon});'
        f');out body;'
    )

    time.sleep(OVERPASS_MIN_DELAY_S)
    data = _get_json_with_retries("POST", OVERPASS_URL, headers=_headers(), data={"data": q}, timeout=60)
    elements = data.get("elements") or []

    # Downsample route points for local distance checks.
    pts_small = _downsample_points(pts, 800)

    def _classify(tags: dict[str, Any]) -> str:
        natural = (tags.get("natural") or "").strip()
        if natural in ("peak", "hill", "saddle", "rock"):
            return natural
        if (tags.get("mountain_pass") or "").strip() == "yes":
            return "mountain_pass"
        if (tags.get("tourism") or "").strip() == "viewpoint":
            return "viewpoint"
        man_made = (tags.get("man_made") or "").strip()
        if man_made in ("cairn", "survey_point"):
            return man_made
        if (tags.get("survey_point") or "").strip() == "trig_point":
            return "trig_point"
        historic = (tags.get("historic") or "").strip()
        if historic in ("monument", "memorial"):
            return historic
        return "poi"

    # Build candidate list: (osm_id_int, peak_id_text, name, lat, lon, ele_m, wikidata, wikipedia, tags, kind)
    candidates: list[tuple[int, str, str | None, float, float, float | None, str | None, str | None, dict[str, Any], str]] = []
    for e in elements:
        if e.get("type") != "node":
            continue
        osm_id = e.get("id")
        if osm_id is None:
            continue
        try:
            osm_id_int = int(osm_id)
        except Exception:
            continue

        tags = e.get("tags") or {}
        name = tags.get("name")
        ele = tags.get("ele")
        wikidata = tags.get("wikidata")
        wikipedia = tags.get("wikipedia")

        try:
            lat = float(e.get("lat"))
            lon = float(e.get("lon"))
        except Exception:
            continue

        ele_m: float | None
        try:
            ele_m = float(str(ele).split()[0]) if ele is not None else None
        except Exception:
            ele_m = None

        peak_id = f"osm_node:{osm_id_int}"
        kind = _classify(tags)
        candidates.append((osm_id_int, peak_id, name, lat, lon, ele_m, wikidata, wikipedia, tags, kind))

    hits: list[tuple[int, str, str | None, float, float, float | None, float, str, str | None, str | None, dict[str, Any]]] = []
    for osm_id_int, peak_id, name, lat, lon, ele_m, wd, wp, tags, kind in candidates:
        best: float | None = None
        for _, r in pts_small.iterrows():
            d = _haversine_m(lat, lon, float(r["latitude_deg"]), float(r["longitude_deg"]))
            if best is None or d < best:
                best = d

        if best is None:
            continue

        if best <= bagged_m:
            hit_type = "bagged"
        elif best <= near_m:
            hit_type = "near"
        else:
            continue

        # Encode kind into hit_type if you want later, without changing schema:
        # e.g. "bagged:peak", "near:viewpoint"
        hit_type = f"{hit_type}:{kind}"
        hits.append((osm_id_int, peak_id, name, lat, lon, ele_m, float(best), hit_type, wd, wp, tags))

    now_utc = _now_utc_iso()

    conn = get_connection()
    try:
        if force:
            conn.execute("DELETE FROM workout_peak_hits WHERE workout_id=?", (workout_id,))

        for osm_id_int, peak_id, name, lat, lon, ele_m, dist_m, hit_type, wd, wp, tags in hits:
            # Upsert POI record
            conn.execute(
                '''
                INSERT OR REPLACE INTO peaks (
                    peak_osm_id, name, ele_m, lat, lon, wikidata, wikipedia,
                    updated_at, peak_id, source, tags_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (osm_id_int, name, ele_m, lat, lon, wd, wp, now_utc, peak_id, "overpass", json.dumps(tags)),
            )

            # Upsert hit record (FK-safe)
            conn.execute(
                '''
                INSERT OR REPLACE INTO workout_peak_hits (
                    workout_id, peak_osm_id, distance_m, hit_type, hit_lat, hit_lon, created_at, peak_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (workout_id, osm_id_int, dist_m, hit_type, lat, lon, now_utc, peak_id),
            )

        conn.commit()
    finally:
        conn.close()

    return get_peak_hits(workout_id)
