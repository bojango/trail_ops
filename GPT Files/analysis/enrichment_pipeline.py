from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

from app.db.database import get_connection

# Enrichment modules
from app.analysis.map_points import build_map_points_for_workout
from app.analysis.plot_samples import build_and_store_plot_samples
from app.analysis.route_context import (
    compute_and_store_route_context,
    compute_and_store_surface_stats,
    compute_and_store_weather,
    compute_and_store_peak_hits,
)


@dataclass
class EnrichmentResult:
    workout_id: int
    map_points_ok: bool = False
    plot_samples_ok: bool = False
    route_ok: bool = False
    surfaces_ok: bool = False
    weather_ok: bool = False
    peaks_ok: bool = False
    errors: Optional[Dict[str, str]] = None


def enrich_workout(workout_id: int) -> EnrichmentResult:
    """Best-effort enrichment pipeline for a single workout.

    This is intentionally idempotent: safe to re-run. Failures are captured
    but do not raise (so ingestion won't die).
    """
    res = EnrichmentResult(workout_id=int(workout_id), errors={})

    # 1) Map points (sets route_hash/has_gps and provides stable geometry)
    try:
        build_map_points_for_workout(res.workout_id)
        res.map_points_ok = True
    except Exception as e:
        res.errors["map_points"] = repr(e)

    # 2) Plot samples (pace/grade/gap + moving time/pace writes back to workouts)
    try:
        build_and_store_plot_samples(res.workout_id)
        res.plot_samples_ok = True
    except Exception as e:
        res.errors["plot_samples"] = repr(e)

    # 3) Route context (location labels)
    try:
        res.route_ok = bool(compute_and_store_route_context(res.workout_id))
    except Exception as e:
        res.errors["route_context"] = repr(e)
        res.route_ok = False

    # 4) Surface stats (best-effort, doesn't currently return status)
    try:
        compute_and_store_surface_stats(res.workout_id)
        res.surfaces_ok = True
    except Exception as e:
        res.errors["surface_stats"] = repr(e)
        res.surfaces_ok = False

    # 5) Weather
    try:
        res.weather_ok = bool(compute_and_store_weather(res.workout_id))
    except Exception as e:
        res.errors["weather"] = repr(e)
        res.weather_ok = False

    # 6) Peaks
    try:
        hits = compute_and_store_peak_hits(res.workout_id)
        res.peaks_ok = bool(hits) if hits is not None else False
    except Exception as e:
        res.errors["peaks"] = repr(e)
        res.peaks_ok = False

    # Persist existing enrichment flags (only those columns that exist today)
    try:
        conn = get_connection()
        try:
            with conn:
                if res.route_ok:
                    conn.execute("UPDATE workouts SET route_enriched = 1 WHERE id = ?", (res.workout_id,))
                if res.weather_ok:
                    conn.execute("UPDATE workouts SET weather_enriched = 1 WHERE id = ?", (res.workout_id,))
                if res.peaks_ok:
                    conn.execute("UPDATE workouts SET peaks_enriched = 1 WHERE id = ?", (res.workout_id,))
        finally:
            conn.close()
    except Exception as e:
        res.errors["flags"] = repr(e)

    if res.errors and len(res.errors) == 0:
        res.errors = None

    return res
