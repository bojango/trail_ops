from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

_SEMICIRCLE_TO_DEG = 180.0 / 2147483648.0  # 180 / 2^31

def _normalize_lat_lon(lat: float, lon: float) -> tuple[float, float]:
    """Normalize lat/lon to degrees.

    Some HealthFit/FIT exports store coordinates as Garmin FIT 'semicircles' (int32 scaled).
    If values are out of plausible degree bounds, treat them as semicircles and convert.
    """
    try:
        if lat is None or lon is None:
            return lat, lon  # type: ignore[return-value]
        if abs(float(lat)) > 90.0 or abs(float(lon)) > 180.0:
            return float(lat) * _SEMICIRCLE_TO_DEG, float(lon) * _SEMICIRCLE_TO_DEG
        return float(lat), float(lon)
    except Exception:
        return lat, lon  # type: ignore[return-value]

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

def _normalize_latlon(lat: float, lon: float) -> tuple[float, float]:
    """Normalize lat/lon to degrees.

    Some sources store coordinates as FIT 'semicircles' (int32 scaled),
    which look like huge numbers (e.g. 644,000,000). If values are out
    of normal degree bounds, convert them.
    """
    lat_f = float(lat)
    lon_f = float(lon)
    if -90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0:
        return lat_f, lon_f

    # Candidate 1: FIT semicircles (common)
    sc = 180.0 / (2 ** 31)
    lat_sc = lat_f * sc
    lon_sc = lon_f * sc
    if -90.0 <= lat_sc <= 90.0 and -180.0 <= lon_sc <= 180.0:
        return float(lat_sc), float(lon_sc)

    # Candidate 2: E7 fixed-point degrees (less common)
    lat_e7 = lat_f * _SEMICIRCLE_TO_DEG
    lon_e7 = lon_f * _SEMICIRCLE_TO_DEG
    if -90.0 <= lat_e7 <= 90.0 and -180.0 <= lon_e7 <= 180.0:
        return float(lat_e7), float(lon_e7)

    # Give up; return raw floats (caller will likely error, but it's truthful).
    return lat_f, lon_f


def _to_utc(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str).astimezone(timezone.utc)


def _moon_phase_fraction(dt_utc: datetime) -> tuple[float, float]:
    """Return (phase 0..1, illumination 0..1) using a simple approximation."""
    ref = datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc)
    synodic_days = 29.53058867
    days = (dt_utc - ref).total_seconds() / 86400.0
    phase = (days % synodic_days) / synodic_days
    illum = 0.5 * (1 - math.cos(2 * math.pi * phase))
    return float(phase), float(illum)


@dataclass(frozen=True)
class WeatherPoint:
    point_type: str
    lat: float
    lon: float
    ele_m: float | None
    obs_time_utc: str
    temp_c: float | None
    wind_kph: float | None
    precip_mm: float | None
    weather_code: int | None
    precip_sum_mm: float | None
    cloudcover_pct: float | None
    rel_humidity_pct: float | None
    wind_dir_deg: float | None
    dewpoint_c: float | None
    moon_phase: float
    moon_illumination: float
    provider: str
    raw_json: dict[str, Any]


def fetch_weather_for_activity(
    *,
    point_type: str,
    lat: float,
    lon: float,
    ele_m: float | None,
    start_time_iso: str,
    duration_s: float | None,
    timeout_s: int = 30,
) -> WeatherPoint:
    lat, lon = _normalize_latlon(lat, lon)

    dt0 = _to_utc(start_time_iso)
    dt1 = dt0 + timedelta(seconds=float(duration_s or 0.0))

    start_date = dt0.date().isoformat()
    end_date = dt1.date().isoformat()

    lat, lon = _normalize_lat_lon(lat, lon)
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,weather_code,windspeed_10m,winddirection_10m,cloudcover,dewpoint_2m,relativehumidity_2m",
        "timezone": "UTC",
    }

    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    precs = hourly.get("precipitation") or []
    codes = hourly.get("weather_code") or []
    winds = hourly.get("windspeed_10m") or []
    wind_dirs = hourly.get("winddirection_10m") or []
    cloud_pcts = hourly.get("cloudcover") or []
    dewpoints = hourly.get("dewpoint_2m") or []
    humidities = hourly.get("relativehumidity_2m") or []

    best_i = None
    best_dt = None
    for i, t in enumerate(times):
        try:
            dti = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if best_dt is None or abs((dti - dt0).total_seconds()) < abs((best_dt - dt0).total_seconds()):
            best_dt = dti
            best_i = i

    def _safe(arr, i):
        try:
            return arr[i]
        except Exception:
            return None

    temp_c = _safe(temps, best_i) if best_i is not None else None
    precip_mm = _safe(precs, best_i) if best_i is not None else None
    wind_kph = _safe(winds, best_i) if best_i is not None else None
    wcode = _safe(codes, best_i) if best_i is not None else None
    wind_dir = _safe(wind_dirs, best_i) if best_i is not None else None
    cloud_pct = _safe(cloud_pcts, best_i) if best_i is not None else None
    dew_c = _safe(dewpoints, best_i) if best_i is not None else None
    rh_pct = _safe(humidities, best_i) if best_i is not None else None

    precip_sum = 0.0
    have = False
    for i, t in enumerate(times):
        try:
            dti = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if dti >= dt0 - timedelta(hours=1) and dti <= dt1 + timedelta(hours=1):
            v = _safe(precs, i)
            if v is not None:
                precip_sum += float(v)
                have = True

    phase, illum = _moon_phase_fraction(dt0)

    return WeatherPoint(
        point_type=point_type,
        lat=float(lat),
        lon=float(lon),
        ele_m=float(ele_m) if ele_m is not None else None,
        obs_time_utc=(best_dt.isoformat() if best_dt else dt0.isoformat()),
        temp_c=float(temp_c) if temp_c is not None else None,
        wind_kph=float(wind_kph) if wind_kph is not None else None,
        precip_mm=float(precip_mm) if precip_mm is not None else None,
        weather_code=int(wcode) if wcode is not None else None,
        precip_sum_mm=float(precip_sum) if have else None,
        cloudcover_pct=float(cloud_pct) if cloud_pct is not None else None,
        rel_humidity_pct=float(rh_pct) if rh_pct is not None else None,
        wind_dir_deg=float(wind_dir) if wind_dir is not None else None,
        dewpoint_c=float(dew_c) if dew_c is not None else None,
        moon_phase=phase,
        moon_illumination=illum,
        provider="open-meteo-archive",
        raw_json=data,
    )
