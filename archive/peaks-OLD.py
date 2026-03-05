from __future__ import annotations

import datetime
import json
import math
import os
import random
import time
from typing import Any

import requests

from app.db.database import get_connection

DEFAULT_USER_AGENT = "TrailOps/0.1 (local personal dashboard)"
USER_AGENT = os.environ.get("TRAILOPS_USER_AGENT", DEFAULT_USER_AGENT)

# Comma-separated list of Overpass endpoints
OVERPASS_URLS = [u.strip() for u in os.environ.get(
    "TRAILOPS_OVERPASS_URLS",
    "https://overpass-api.de/api/interpreter,https://overpass.private.coffee/api/interpreter,https://overpass.osm.ch/api/interpreter,https://overpass.openstreetmap.ru/api/interpreter",
).split(",") if u.strip()]

OVERPASS_TIMEOUT_S = float(os.environ.get("TRAILOPS_OVERPASS_TIMEOUT_S", "90"))
OVERPASS_MIN_DELAY_S = float(os.environ.get("TRAILOPS_OVERPASS_MIN_DELAY_S", "0.5"))
OVERPASS_MAX_RETRIES = int(os.environ.get("TRAILOPS_OVERPASS_MAX_RETRIES", "4"))

# distance thresholds (meters)
PEAK_WITHIN_M_RUNNING = float(os.environ.get("TRAILOPS_PEAK_WITHIN_M_RUNNING", "100"))
PEAK_WITHIN_M_WALKING = float(os.environ.get("TRAILOPS_PEAK_WITHIN_M_WALKING", "150"))
PEAK_WITHIN_M_HIKING = float(os.environ.get("TRAILOPS_PEAK_WITHIN_M_HIKING", "200"))

# bbox margin in degrees (~1km at UK lat for 0.01)
PEAK_BBOX_MARGIN_DEG = float(os.environ.get("TRAILOPS_PEAK_BBOX_MARGIN_DEG", "0.01"))

MAX_ROUTE_POINTS_FOR_DISTANCE = int(os.environ.get("TRAILOPS_PEAK_MAX_ROUTE_POINTS", "500"))


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _downsample(points: list[tuple[float, float]], max_points: int) -> list[tuple[float, float]]:
    n = len(points)
    if n <= max_points:
        return points
    # evenly spaced indices
    step = (n - 1) / float(max_points - 1)
    idxs = [int(round(i * step)) for i in range(max_points)]
    idxs = sorted(set(min(n - 1, max(0, i)) for i in idxs))
    return [points[i] for i in idxs]


def _pick_within_m(sport_type: str | None) -> float:
    st = (sport_type or "").lower()
    if st.startswith("running"):
        return PEAK_WITHIN_M_RUNNING
    if st.startswith("hiking"):
        return PEAK_WITHIN_M_HIKING
    return PEAK_WITHIN_M_WALKING


def _overpass_post(query: str) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    last_err: Exception | None = None

    # randomize endpoint order each call to spread load
    urls = OVERPASS_URLS[:]
    random.shuffle(urls)

    for attempt in range(1, OVERPASS_MAX_RETRIES + 1):
        for url in urls:
            try:
                time.sleep(max(0.0, OVERPASS_MIN_DELAY_S))
                resp = requests.post(url, data={"data": query}, headers=headers, timeout=OVERPASS_TIMEOUT_S)
                if resp.status_code >= 500:
                    raise RuntimeError(f"Overpass {resp.status_code} from {url}")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_err = e
                # backoff per attempt, then try next endpoint
                time.sleep(min(8.0, 0.6 * (2 ** (attempt - 1))))
                continue
    raise RuntimeError(f"Failed Overpass request after retries. Last error: {last_err}")


def _fetch_peaks_bbox(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> list[dict[str, Any]]:
    """Fetch candidate peaks in a bbox from Overpass.

    We query nodes + ways + relations because some summits are mapped as areas/relations.
    For ways/relations we use the returned `center` coordinates.
    """
    query = f"""[out:json][timeout:180];
(
  node["natural"~"^(peak|hill)$"]({min_lat},{min_lon},{max_lat},{max_lon});
  way["natural"~"^(peak|hill)$"]({min_lat},{min_lon},{max_lat},{max_lon});
  relation["natural"~"^(peak|hill)$"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out body center;
"""
    data = _overpass_post(query)
    elems = data.get("elements", [])
    out: list[dict[str, Any]] = []
    for el in elems:
        tags = el.get("tags", {}) or {}
        name = tags.get("name")
        if not name:
            continue

        etype = el.get("type")
        lat = lon = None
        if etype == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")
        if lat is None or lon is None:
            continue

        ele_m = None
        ele = tags.get("ele")
        if ele is not None:
            try:
                ele_m = float(str(ele).strip().replace("m", ""))
            except Exception:
                ele_m = None

        out.append(
            {
                "peak_osm_id": int(el["id"]),
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "ele_m": ele_m,
                "wikidata": tags.get("wikidata"),
                "wikipedia": tags.get("wikipedia"),
                "source": "overpass",
                "tags": tags,
            }
        )
    return out


def compute_and_store_peak_hits(workout_id: int, *, force: bool = False) -> None:
    """Populate peaks + workout_peak_hits for a workout (FK-safe)."""
    conn = get_connection()
    try:
        # Skip if already enriched unless forcing
        if not force:
            row = conn.execute("SELECT peaks_enriched FROM workouts WHERE id=?", (workout_id,)).fetchone()
            if row and int(row[0] or 0) == 1:
                return

        # Get sport_type for threshold selection
        w = conn.execute("SELECT sport_type FROM workouts WHERE id=?", (workout_id,)).fetchone()
        sport_type = w[0] if w else None
        within_m = _pick_within_m(sport_type)

        # Fetch route points (level 0)
        pts_rows = conn.execute(
            """
            SELECT latitude_deg, longitude_deg
            FROM workout_map_points
            WHERE workout_id=? AND level=0
            ORDER BY seq ASC
            """,
            (workout_id,),
        ).fetchall()
        if not pts_rows:
            return

        route_pts = [(float(r[0]), float(r[1])) for r in pts_rows]
        route_pts = _downsample(route_pts, MAX_ROUTE_POINTS_FOR_DISTANCE)

        lats = [p[0] for p in route_pts]
        lons = [p[1] for p in route_pts]
        # Expand bbox by either configured margin or based on within_m (so we don't miss nearby peaks
        # when a route hugs the bbox edge).
        dyn_margin = max(PEAK_BBOX_MARGIN_DEG, (within_m * 2.0) / 111_000.0)
        min_lat = min(lats) - dyn_margin
        max_lat = max(lats) + dyn_margin
        min_lon = min(lons) - dyn_margin
        max_lon = max(lons) + dyn_margin

        peaks = _fetch_peaks_bbox(min_lat, min_lon, max_lat, max_lon)
        if not peaks:
            return


        now_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        # Upsert peaks first
        with conn:
            for p in peaks:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO peaks(
                        peak_osm_id, name, ele_m, lat, lon, wikidata, wikipedia, updated_at, peak_id, source, tags_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(p["peak_osm_id"]),
                        p.get("name"),
                        p.get("ele_m"),
                        float(p["lat"]),
                        float(p["lon"]),
                        p.get("wikidata"),
                        p.get("wikipedia"),
                        now_utc,
                        str(p["peak_osm_id"]),
                        p.get("source") or "overpass",
                        json.dumps(p.get("tags", {})),
                    ),
                )

        # Now compute distances and insert hits
        hits: list[tuple[int, int, float]] = []
        for p in peaks:
            plat, plon = float(p["lat"]), float(p["lon"])
            dmin = min(_haversine_m(plat, plon, rlat, rlon) for (rlat, rlon) in route_pts)
            if dmin <= within_m:
                hits.append((workout_id, int(p["peak_osm_id"]), float(dmin)))

        with conn:
            # clear prior hits for determinism (this table is keyed by id PK, so delete then insert)
            conn.execute("DELETE FROM workout_peak_hits WHERE workout_id=?", (workout_id,))
            for wid, peak_osm_id, dmin in hits:
                conn.execute(
                    """
                    INSERT INTO workout_peak_hits(
                        workout_id, peak_osm_id, distance_m, hit_type, hit_lat, hit_lon, created_at, peak_id
                    ) VALUES (?, ?, ?, 'near', NULL, NULL, ?, ?)
                    """,
                    (wid, peak_osm_id, dmin, now_utc, str(peak_osm_id)),
                )

            conn.execute("UPDATE workouts SET peaks_enriched=1 WHERE id=?", (workout_id,))
    finally:
        conn.close()


def enrich_peaks_for_workout(workout_id: int, *, force: bool = False) -> int:
    """Public wrapper: returns number of peak hits inserted."""
    compute_and_store_peak_hits(workout_id, force=force)
    conn = get_connection()
    try:
        row = conn.execute("SELECT COUNT(*) FROM workout_peak_hits WHERE workout_id=?", (workout_id,)).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()
