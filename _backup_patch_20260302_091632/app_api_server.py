from fastapi import FastAPI, Depends, Query
from fastapi.responses import RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import math
import sqlite3

from pydantic import BaseModel, Field

from app.api.queries import (
    get_workouts_range,
    get_workout_by_id,
    get_plot_samples,
    get_sparklines_batch,
    get_workout_context,
    get_peaks_dashboard,
)
from app.api.security import api_key_dependency
from app.db.database import get_connection

# Map points helpers (stored in DB by enrichment pipeline)
from app.analysis.map_points import get_map_points as load_map_points, get_markers as load_map_markers


app = FastAPI(
    title="TrailOps API",
    description="Read-only API for TrailOps dashboard and remote access.",
    version="0.1.0",
)

# -----------------------------
# CORS (needed for React dev UI)
# -----------------------------
# React dev server runs on http://127.0.0.1:5173 (or localhost).
# Without CORS, browsers will block fetch() with "Failed to fetch".
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------
# Root + Quality-of-Life Routes
# -----------------------------

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


@app.get("/health")
def health():
    return {"status": "ok"}




# -----------------------------
# Peaks Dashboard (Global)
# -----------------------------

@app.get("/peaks/dashboard", dependencies=[Depends(api_key_dependency)])
def peaks_dashboard(
    range: str = Query("30d"),
    cls: str = Query("wainwrights"),
):
    """Global peaks + POI stats for the Peaks dashboard.

    range: 7d | 30d | 12m | all
    cls: classification key (default: wainwrights)
    """
    return get_peaks_dashboard(range_key=range, class_key=cls)


# -----------------------------
# Workout Endpoints
# -----------------------------

@app.get("/workouts", dependencies=[Depends(api_key_dependency)])
def workouts(
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    sport: Optional[str] = Query("all"),
    limit: int = Query(300, ge=1, le=5000),
):
    return get_workouts_range(start=start, end=end, sport=sport, limit=limit)


@app.get("/workouts/{workout_id}", dependencies=[Depends(api_key_dependency)])
def workout_detail(workout_id: int):
    return get_workout_by_id(workout_id)



@app.get("/workouts/{workout_id}/context", dependencies=[Depends(api_key_dependency)])
def workout_context(workout_id: int):
    return get_workout_context(workout_id)


@app.get("/workouts/{workout_id}/map-points", dependencies=[Depends(api_key_dependency)])
def workout_map_points(
    workout_id: int,
    level: int = Query(0, ge=0, le=3),
    max_points: int = Query(12000, ge=50, le=20000),
    include_markers: bool = Query(True),
):
    """Return simplified route geometry for mapping (OSM renderer in React).

    - level: simplification level stored in workout_map_points (0..3)
    - max_points: safety cap for front-end performance (downsampled by stride)
    """
    df = load_map_points(workout_id=workout_id, level=level)

    # FALLBACK to workout_samples if stored map_points are missing or too sparse.
    # This avoids "corner cutting" when the simplification level/table is under-sampled.
    try:
        if df is None or df.empty or len(df) < 250:
            conn = get_connection()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                  seq,
                  latitude_deg,
                  longitude_deg,
                  seconds_since_start,
                  distance_m,
                  elevation_m
                FROM workout_samples
                WHERE workout_id = ?
                  AND latitude_deg IS NOT NULL
                  AND longitude_deg IS NOT NULL
                ORDER BY seq ASC
                """,
                (workout_id,),
            ).fetchall()
            if rows and len(rows) >= 250:
                # Convert to a lightweight list-of-dicts compatible with the rest of this handler.
                # Downsample by stride if needed, preserving order.
                n = len(rows)
                if n > max_points:
                    stride = int(math.ceil(n / max_points))
                    rows = rows[::stride]
                points = []
                for r in rows:
                    points.append(
                        {
                            "seq": int(r["seq"]) if r["seq"] is not None else None,
                            "lat": float(r["latitude_deg"]),
                            "lon": float(r["longitude_deg"]),
                            "seconds_since_start": float(r["seconds_since_start"]) if r["seconds_since_start"] is not None else None,
                            "distance_m": float(r["distance_m"]) if r["distance_m"] is not None else None,
                            "elevation_m": float(r["elevation_m"]) if r["elevation_m"] is not None else None,
                        }
                    )
                # Build bounds/start/end immediately and return early with these raw points + optional markers.
                bounds = None
                start = None
                end = None
                if points:
                    lats = [p["lat"] for p in points]
                    lons = [p["lon"] for p in points]
                    bounds = {
                        "min_lat": min(lats),
                        "max_lat": max(lats),
                        "min_lon": min(lons),
                        "max_lon": max(lons),
                    }
                    start = {"lat": points[0]["lat"], "lon": points[0]["lon"]}
                    end = {"lat": points[-1]["lat"], "lon": points[-1]["lon"]}

                markers = []
                if include_markers:
                    try:
                        mdf = load_map_markers(workout_id)
                        if mdf is not None and not mdf.empty:
                            for _, mr in mdf.iterrows():
                                try:
                                    markers.append(
                                        {
                                            "id": int(mr["id"]),
                                            "label": str(mr["label"]),
                                            "kind": str(mr["kind"]),
                                            "seq": int(mr["seq"]) if mr["seq"] == mr["seq"] else None,
                                            "lat": float(mr["latitude_deg"]),
                                            "lon": float(mr["longitude_deg"]),
                                        }
                                    )
                                except Exception:
                                    continue
                    except Exception:
                        markers = []

                return {
                    "workout_id": workout_id,
                    "level": level,
                    "points": points,
                    "bounds": bounds,
                    "start": start,
                    "end": end,
                    "markers": markers,
                }
    except Exception:
        # If fallback fails (schema mismatch etc.), continue with stored map_points.
        pass


    points = []
    if not df.empty:
        # Downsample if needed (simple stride, preserves order)
        n = len(df)
        if n > max_points:
            stride = int(math.ceil(n / max_points))
            df = df.iloc[::stride].reset_index(drop=True)

        for _, r in df.iterrows():
            points.append(
                {
                    "seq": int(r["seq"]) if r["seq"] == r["seq"] else None,
                    "lat": float(r["latitude_deg"]),
                    "lon": float(r["longitude_deg"]),
                    "seconds_since_start": float(r["seconds_since_start"]) if r["seconds_since_start"] == r["seconds_since_start"] else None,
                    "distance_m": float(r["distance_m"]) if r["distance_m"] == r["distance_m"] else None,
                    "elevation_m": float(r["elevation_m"]) if r["elevation_m"] == r["elevation_m"] else None,
                }
            )

    bounds = None
    start = None
    end = None
    if points:
        lats = [p["lat"] for p in points]
        lons = [p["lon"] for p in points]
        bounds = {
            "min_lat": min(lats),
            "max_lat": max(lats),
            "min_lon": min(lons),
            "max_lon": max(lons),
        }
        start = {"lat": points[0]["lat"], "lon": points[0]["lon"]}
        end = {"lat": points[-1]["lat"], "lon": points[-1]["lon"]}

    markers = []
    if include_markers:
        try:
            mdf = load_map_markers(workout_id)
            if mdf is not None and not mdf.empty:
                for _, r in mdf.iterrows():
                    try:
                        markers.append(
                            {
                                "id": int(r["id"]),
                                "label": str(r["label"]),
                                "kind": str(r["kind"]),
                                "seq": int(r["seq"]) if r["seq"] == r["seq"] else None,
                                "lat": float(r["latitude_deg"]),
                                "lon": float(r["longitude_deg"]),
                            }
                        )
                    except Exception:
                        continue
        except Exception:
            # Markers are optional. Do not fail map endpoint if marker table is missing.
            markers = []

    return {
        "workout_id": workout_id,
        "level": level,
        "points": points,
        "bounds": bounds,
        "start": start,
        "end": end,
        "markers": markers,
    }


@app.get("/workouts/{workout_id}/plot-samples", dependencies=[Depends(api_key_dependency)])
def workout_plot_samples(
    workout_id: int,
    max_points: int = Query(1000, ge=10, le=10000),
):
    return get_plot_samples(workout_id=workout_id, max_points=max_points)


class SparklinesRequest(BaseModel):
    workout_ids: List[int] = Field(default_factory=list)
    max_points: int = Field(default=60, ge=10, le=300)
    metric_mode: str = Field(default="auto")  # "auto" or explicit metric key


@app.post("/workouts/sparklines", dependencies=[Depends(api_key_dependency)])
def workout_sparklines(req: SparklinesRequest):
    return get_sparklines_batch(
        workout_ids=req.workout_ids,
        max_points=req.max_points,
        metric_mode=req.metric_mode,
    )