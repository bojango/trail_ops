from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

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

NOMINATIM_MIN_DELAY_S = float(os.environ.get("TRAILOPS_NOMINATIM_MIN_DELAY_S", "1.0"))
OVERPASS_MIN_DELAY_S = float(os.environ.get("TRAILOPS_OVERPASS_MIN_DELAY_S", "1.0"))

OVERPASS_SURFACE_AROUND_M = float(os.environ.get("TRAILOPS_OVERPASS_AROUND_M", "25"))
OVERPASS_PEAK_AROUND_M = float(os.environ.get("TRAILOPS_PEAK_AROUND_M", "250"))
MAX_OVERPASS_POINTS = int(os.environ.get("TRAILOPS_MAX_OVERPASS_POINTS", "25"))

SURFACE_VERSION = "v2"
GEOCODE_VERSION = "v2"


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

    # Try from most-detailed to least until we get usable points.
    for level in sorted(int(l) for l in levels):
        pts = get_map_points(workout_id, level=level)
        if pts is None or pts.empty:
            continue
        pts = _normalize_latlon_df(pts)
        if not pts.empty:
            return pts

    return pd.DataFrame()

def _route_center(pts: pd.DataFrame) -> tuple[float, float]:
    return float(pts["latitude_deg"].mean()), float(pts["longitude_deg"].mean())


def _headers() -> dict[str, str]:
    return {"User-Agent": USER_AGENT}


def _raise_http_error(resp: requests.Response, context: str) -> None:
    txt = (resp.text or "")[:600].replace("\n", " ").replace("\r", " ")
    raise RuntimeError(f"{context} HTTP {resp.status_code}: {txt}")


def _get_json_with_retries(method: str, url: str, *, headers: dict[str, str], params=None, data=None, timeout: int = 60) -> dict[str, Any]:
    backoff = [0.0, 1.0, 2.0, 4.0, 8.0, 16.0]
    last_err: Exception | None = None
    for wait_s in backoff:
        if wait_s:
            time.sleep(wait_s)
        try:
            if method.upper() == "GET":
                resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            else:
                resp = requests.post(url, params=params, data=data, headers=headers, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                continue
            if resp.status_code >= 400:
                _raise_http_error(resp, f"Request to {url}")
            return resp.json()
        except Exception as e:
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


def compute_and_store_route_context(workout_id: int, *, force: bool = False) -> RouteContext | None:
    """
    Compute and store route context for a workout.

    Stability rule:
    - If usable GPS points exist, we ALWAYS write a workout_route_context row with start/center coords.
    - Reverse-geocoding (Nominatim) is best-effort; failures must NOT block row creation.
    """
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

    now_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # 1) Write base row first (coords only). This guarantees 100% rows for GPS workouts.
    conn = get_connection()
    try:
        base_data: dict[str, Any] = {
            "workout_id": workout_id,
            "start_lat": start_lat,
            "start_lon": start_lon,
            "center_lat": center_lat,
            "center_lon": center_lon,
            "provider": "nominatim",
            "geocode_version": GEOCODE_VERSION,
            "computed_at": now_ts,
        }
        _upsert_row(conn, "workout_route_context", "workout_id", base_data)
        conn.commit()
    finally:
        conn.close()

    # 2) Best-effort geocoding. Never let this prevent the row from existing.
    geo_start: dict[str, Any] = {}
    geo_center: dict[str, Any] = {}

    start_addr: dict[str, Any] = {}
    center_addr: dict[str, Any] = {}

    start_label: str | None = None
    start_fields: dict[str, Any] = {}
    start_location_label: str | None = None

    center_location_label: str | None = None
    center_fields: dict[str, Any] = {}

    try:
        geo_start = _nominatim_reverse(start_lat, start_lon)
        start_addr = (geo_start.get("address") or {})
        start_location_label = geo_start.get("display_name")
        start_label, start_fields = _format_location_label(start_addr)
    except Exception as e:
        geo_start = {"error": str(e)}
        start_addr = {}

    try:
        geo_center = _nominatim_reverse(center_lat, center_lon)
        center_addr = (geo_center.get("address") or {})
        center_location_label = geo_center.get("display_name")
        _tmp_label, center_fields = _format_location_label(center_addr)
    except Exception as e:
        geo_center = {"error": str(e)}
        center_addr = {}

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

    # 3) Update the row with whatever geocode data we got (or error payloads).
    conn = get_connection()
    try:
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
    start_time, duration_s = _get_workout_timeinfo(workout_id)
    if not start_time:
        return

    start_lat = float(pts.iloc[0]["latitude_deg"])
    start_lon = float(pts.iloc[0]["longitude_deg"])

    high_lat, high_lon, high_ele = start_lat, start_lon, None
    if "elevation_m" in pts.columns and pts["elevation_m"].notna().any():
        idx = int(pts["elevation_m"].fillna(-1e9).idxmax())
        high_lat = float(pts.loc[idx, "latitude_deg"])
        high_lon = float(pts.loc[idx, "longitude_deg"])
        try:
            high_ele = float(pts.loc[idx, "elevation_m"])
        except Exception:
            high_ele = None

    wp_start = fetch_weather_for_activity(point_type="start", lat=start_lat, lon=start_lon, ele_m=None, start_time_iso=start_time, duration_s=duration_s)
    wp_high = fetch_weather_for_activity(point_type="high", lat=high_lat, lon=high_lon, ele_m=high_ele, start_time_iso=start_time, duration_s=duration_s)

    conn = get_connection()
    try:
        for wp in (wp_start, wp_high):
            conn.execute(
                '''
                INSERT OR REPLACE INTO workout_weather
                (workout_id, point_type, lat, lon, ele_m, obs_time_utc, temp_c, wind_kph, precip_mm, weather_code, precip_sum_mm,
                 moon_phase, moon_illumination, provider, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    workout_id,
                    wp.point_type,
                    wp.lat,
                    wp.lon,
                    wp.ele_m,
                    wp.obs_time_utc,
                    wp.temp_c,
                    wp.wind_kph,
                    wp.precip_mm,
                    wp.weather_code,
                    wp.precip_sum_mm,
                    wp.moon_phase,
                    wp.moon_illumination,
                    wp.provider,
                    json.dumps(wp.raw_json),
                ),
            )
        conn.commit()
    finally:
        conn.close()



def compute_and_store_weather(workout_id: int, *, force: bool = False) -> bool | None:
    """Fetch and store weather context for a workout.

    Returns:
      - True when weather rows were written
      - None when the workout has no usable GPS points
    """
    if not force:
        conn = get_connection()
        try:
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
        parts.append(f'way(around:{int(within_m)},{lat:.7f},{lon:.7f})["highway"];')
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

    now_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_connection()
    try:
        data: dict[str, Any] = {
            "workout_id": ss.workout_id,
            "road_m": ss.road_m,
            "paved_path_m": ss.paved_path_m,
            "trail_m": ss.trail_m,
            "track_m": ss.track_m,
            "grass_m": ss.grass_m,
            "rock_m": ss.rock_m,
            "forest_m": ss.forest_m,
            "unknown_m": ss.unknown_m,
            "detail_json": json.dumps(ss.detail_json),
            "provider": ss.provider,
            "surface_version": SURFACE_VERSION,
            "raw_json": json.dumps(ss.raw_json),
            "computed_at": now_ts,
        }
        _upsert_row(conn, "workout_surface_stats", "workout_id", data)
        conn.commit()
    finally:
        conn.close()

    return ss



def _overpass_peaks_for_points(pts: pd.DataFrame, within_m: float | None = None) -> dict[str, Any]:
    pts2 = _downsample_points(pts, MAX_OVERPASS_POINTS)
    if within_m is None:
        within_m = OVERPASS_PEAK_AROUND_M
    parts = []
    for _, r in pts2.iterrows():
        lat = float(r["latitude_deg"])
        lon = float(r["longitude_deg"])
        parts.append(f'node(around:{int(within_m)},{lat:.7f},{lon:.7f})["natural"="peak"];')
    q = "[out:json][timeout:60];(" + "".join(parts) + ");out body;"
    time.sleep(OVERPASS_MIN_DELAY_S)
    return _get_json_with_retries("POST", OVERPASS_URL, headers=_headers(), data={"data": q}, timeout=60)


def _peak_id_col(conn) -> str:
    """Return the identifier column name for workout_peak_hits."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(workout_peak_hits)").fetchall()}
    if "peak_osm_id" in cols:
        return "peak_osm_id"
    if "peak_id" in cols:
        return "peak_id"
    for cand in ("osm_id", "node_id"):
        if cand in cols:
            return cand
    return "peak_id"


def _peaks_id_col(conn) -> str:
    """Return the identifier column name for the peaks table."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(peaks)").fetchall()}
    if "peak_id" in cols:
        return "peak_id"
    for cand in ("peak_osm_id", "osm_id", "node_id"):
        if cand in cols:
            return cand
    return "peak_id"
    # common legacy fallbacks
    for cand in ("osm_id", "peak_osm_id", "node_id", "peak_node_id"):
        if cand in cols:
            return cand
    return "peak_id"

def get_peak_hits(workout_id: int) -> list[PeakHit]:
    conn = get_connection()
    try:
        hit_col = _peak_id_col(conn)
        hit_cols = {r["name"] for r in conn.execute("PRAGMA table_info(workout_peak_hits)").fetchall()}
        peak_col = _peaks_id_col(conn)
        rows = conn.execute(
            f"""
            SELECT h.{hit_col} AS peak_id, p.name, p.lat, p.lon, p.ele_m, h.distance_m
            FROM workout_peak_hits h
            JOIN peaks p ON p.{peak_col} = h.{hit_col}
            WHERE h.workout_id = ?
            ORDER BY h.distance_m ASC
            """,
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


def compute_and_store_peak_hits(workout_id: int, *, force: bool = False, within_m: float = 50.0) -> list[PeakHit] | None:
    if not force:
        existing = get_peak_hits(workout_id)
        if existing:
            return existing

    pts = _pick_best_route_points(workout_id)
    if pts.empty:
        return None

    data = _overpass_peaks_for_points(pts, within_m=within_m)
    elements = data.get("elements") or []

    peaks = []
    for e in elements:
        if e.get("type") != "node":
            continue
        pid = f"osm_node:{e.get('id')}"
        tags = e.get("tags") or {}
        name = tags.get("name")
        ele = tags.get("ele")
        wikidata = tags.get("wikidata")
        wikipedia = tags.get("wikipedia")
        lat = float(e.get("lat"))
        lon = float(e.get("lon"))
        ele_m = None
        try:
            ele_m = float(str(ele).split()[0]) if ele is not None else None
        except Exception:
            ele_m = None
        peaks.append((pid, name, lat, lon, ele_m, wikidata, wikipedia, tags))

    pts_small = _downsample_points(pts, 200)
    hits: list[PeakHit] = []
    for pid, name, lat, lon, ele_m, wd, wp, tags in peaks:
        best = None
        for _, r in pts_small.iterrows():
            d = _haversine_m(lat, lon, float(r["latitude_deg"]), float(r["longitude_deg"]))
            if best is None or d < best:
                best = d
        if best is None or best > within_m:
            continue
        hits.append(PeakHit(peak_id=pid, name=name, lat=lat, lon=lon, ele_m=ele_m, distance_m=float(best)))

    conn = get_connection()
    try:
        hit_col = _peak_id_col(conn)
        for h in hits:
            tag_match = None
            for pid, name, lat, lon, ele_m, wd, wp, tags in peaks:
                if pid == h.peak_id:
                    tag_match = (wd, wp, tags)
                    break
            wd, wp, tags = tag_match if tag_match else (None, None, {})

            peak_col = _peaks_id_col(conn)
            _upsert_row(
                conn,
                "peaks",
                peak_col,
                {
                    peak_col: h.peak_id,
                    "name": h.name,
                    "lat": h.lat,
                    "lon": h.lon,
                    "ele_m": h.ele_m,
                    "wikidata": wd,
                    "wikipedia": wp,
                    "source": "overpass",
                    "tags_json": json.dumps(tags),
                },
            )
            # Insert hit row (schema-tolerant: some DBs require created_at NOT NULL)
            if "created_at" in hit_cols:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO workout_peak_hits (workout_id, {hit_col}, distance_m, created_at)
                    VALUES (?, ?, ?, datetime('now'))
                    """,
                    (workout_id, h.peak_id, h.distance_m),
                )
            else:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO workout_peak_hits (workout_id, {hit_col}, distance_m)
                    VALUES (?, ?, ?)
                    """,
                    (workout_id, h.peak_id, h.distance_m),
                )
        conn.commit()
    finally:
        conn.close()

    return get_peak_hits(workout_id)



