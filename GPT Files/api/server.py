from fastapi import FastAPI, Depends, Query
from fastapi.responses import RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from app.api.queries import (
    get_workouts_range,
    get_workout_by_id,
    get_plot_samples,
)
from app.api.security import api_key_dependency


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
    allow_methods=["GET", "OPTIONS"],
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


@app.get("/workouts/{workout_id}/plot-samples", dependencies=[Depends(api_key_dependency)])
def workout_plot_samples(
    workout_id: int,
    max_points: int = Query(1000, ge=10, le=10000),
):
    return get_plot_samples(workout_id=workout_id, max_points=max_points)
