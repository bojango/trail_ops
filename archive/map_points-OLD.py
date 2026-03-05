from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import math

import numpy as np
import pandas as pd

from db.database import get_connection


# Multiple simplification levels (meters). Lower = more detail.
DP_EPSILON_LEVELS_M: list[float] = [5.0, 10.0, 20.0, 40.0]

# FIT latitude/longitude are often stored as "semicircles" (int32 scaled).
# Some exporters (or earlier ingestor versions) can leave semicircles in the DB but label the columns as *_deg.
# Detect & correct here so downstream mapping uses sane degrees.
_SEMICIRCLES_SCALE = 180.0 / (2**31)

def _semicircles_to_degrees(v: float) -> float:
    return float(v) * _SEMICIRCLES_SCALE

def _normalize_latlon_deg(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    lat = df["latitude_deg"].astype(float)
    lon = df["longitude_deg"].astype(float)

    # Heuristic: if values are way out of WGS84 degree ranges, treat as semicircles and convert.
    if lat.abs().max() > 90.0 or lon.abs().max() > 180.0:
        df = df.copy()
        df["latitude_deg"] = lat.apply(_semicircles_to_degrees)
        df["longitude_deg"] = lon.apply(_semicircles_to_degrees)

    return df



@dataclass(frozen=True)
class MapPoint:
    seq: int
    latitude_deg: float
    longitude_deg: float
    seconds_since_start: float | None
    distance_m: float | None
    elevation_m: float | None


def _project_to_meters(lat_deg: np.ndarray, lon_deg: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Fast local projection to meters (equirectangular around mean latitude).
    Plenty accurate for track simplification at human distances.
    """
    R = 6371000.0
    lat_rad = np.deg2rad(lat_deg)
    lon_rad = np.deg2rad(lon_deg)
    lat0 = float(np.nanmean(lat_rad)) if np.isfinite(lat_rad).any() else 0.0

    # Use local origin to keep numbers stable
    lat_min = float(np.nanmin(lat_rad))
    lon_min = float(np.nanmin(lon_rad))

    x = R * (lon_rad - lon_min) * math.cos(lat0)
    y = R * (lat_rad - lat_min)
    return x, y


def _perp_distance(px: float, py: float, ax: float, ay: float, bx: float, by: float) -> float:
    """Perpendicular distance from P to segment AB."""
    vx, vy = bx - ax, by - ay
    wx, wy = px - ax, py - ay

    c1 = vx * wx + vy * wy
    if c1 <= 0:
        return math.hypot(px - ax, py - ay)

    c2 = vx * vx + vy * vy
    if c2 <= c1:
        return math.hypot(px - bx, py - by)

    t = c1 / c2
    projx = ax + t * vx
    projy = ay + t * vy
    return math.hypot(px - projx, py - projy)


def _douglas_peucker_indices(x: np.ndarray, y: np.ndarray, epsilon: float) -> np.ndarray:
    """
    Returns indices kept by Douglas–Peucker (iterative stack, no recursion).
    """
    n = len(x)
    if n <= 2:
        return np.arange(n, dtype=int)

    keep = np.zeros(n, dtype=bool)
    keep[0] = True
    keep[-1] = True

    stack: list[tuple[int, int]] = [(0, n - 1)]
    while stack:
        start, end = stack.pop()
        ax, ay = float(x[start]), float(y[start])
        bx, by = float(x[end]), float(y[end])

        max_dist = -1.0
        max_idx = -1
        for i in range(start + 1, end):
            d = _perp_distance(float(x[i]), float(y[i]), ax, ay, bx, by)
            if d > max_dist:
                max_dist = d
                max_idx = i

        if max_dist >= epsilon and max_idx != -1:
            keep[max_idx] = True
            stack.append((start, max_idx))
            stack.append((max_idx, end))

    return np.flatnonzero(keep)


def _load_raw_track(workout_id: int) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                seconds_since_start,
                distance_m,
                elevation_m,
                latitude_deg,
                longitude_deg
            FROM workout_samples
            WHERE workout_id = ?
            ORDER BY seconds_since_start ASC
            """,
            conn,
            params=(workout_id,),
        )
    finally:
        conn.close()

    df = df.dropna(subset=["latitude_deg", "longitude_deg"]).copy()
    df = _normalize_latlon_deg(df)
    df = df[(df["latitude_deg"].abs() > 0.00001) & (df["longitude_deg"].abs() > 0.00001)]
    return df.reset_index(drop=True)


def build_map_points_for_workout(workout_id: int, eps_levels_m: Sequence[float] | None = None) -> dict[int, int]:
    """
    Build multiple DP levels for one workout and store them.
    Returns {level: point_count}.
    """
    eps_levels_m = list(eps_levels_m) if eps_levels_m is not None else DP_EPSILON_LEVELS_M
    raw = _load_raw_track(workout_id)
    if raw.empty or len(raw) < 2:
        return {}

    lat = raw["latitude_deg"].to_numpy(dtype=float)
    lon = raw["longitude_deg"].to_numpy(dtype=float)
    x, y = _project_to_meters(lat, lon)

    conn = get_connection()
    try:
        with conn:
            conn.execute("DELETE FROM workout_map_points WHERE workout_id = ?", (workout_id,))
            out_counts: dict[int, int] = {}

            for level, eps in enumerate(eps_levels_m):
                idx = _douglas_peucker_indices(x, y, float(eps))
                idx = np.sort(idx)

                if len(idx) < 2:
                    idx = np.array([0, len(raw) - 1], dtype=int)

                rows = []
                for seq, i in enumerate(idx.tolist()):
                    r = raw.iloc[int(i)]
                    rows.append(
                        (
                            workout_id,
                            int(level),
                            int(seq),
                            float(r["latitude_deg"]),
                            float(r["longitude_deg"]),
                            float(r["seconds_since_start"]) if pd.notna(r["seconds_since_start"]) else None,
                            float(r["distance_m"]) if pd.notna(r["distance_m"]) else None,
                            float(r["elevation_m"]) if pd.notna(r["elevation_m"]) else None,
                        )
                    )

                conn.executemany(
                    """
                    INSERT INTO workout_map_points(
                        workout_id, level, seq,
                        latitude_deg, longitude_deg,
                        seconds_since_start, distance_m, elevation_m
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                out_counts[level] = len(rows)

        return out_counts
    finally:
        conn.close()


def get_available_map_levels(workout_id: int) -> list[int]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT level FROM workout_map_points WHERE workout_id = ? ORDER BY level ASC",
            (workout_id,),
        ).fetchall()
    finally:
        conn.close()
    return [int(r[0]) for r in rows]


def get_map_points(workout_id: int, level: int = 1) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                seq,
                latitude_deg,
                longitude_deg,
                seconds_since_start,
                distance_m,
                elevation_m
            FROM workout_map_points
            WHERE workout_id = ? AND level = ?
            ORDER BY seq ASC
            """,
            conn,
            params=(workout_id, level),
        )
    finally:
        conn.close()
    return df


def get_markers(workout_id: int) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                id,
                label,
                kind,
                seq,
                latitude_deg,
                longitude_deg,
                created_at
            FROM workout_map_markers
            WHERE workout_id = ?
            ORDER BY created_at ASC
            """,
            conn,
            params=(workout_id,),
        )
    finally:
        conn.close()
    return df


def add_marker(
    workout_id: int,
    label: str,
    kind: str,
    latitude_deg: float,
    longitude_deg: float,
    seq: int | None = None,
) -> None:
    conn = get_connection()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO workout_map_markers(workout_id, label, kind, seq, latitude_deg, longitude_deg, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (workout_id, label.strip(), kind.strip(), seq, float(latitude_deg), float(longitude_deg)),
            )
    finally:
        conn.close()


def delete_marker(marker_id: int) -> None:
    conn = get_connection()
    try:
        with conn:
            conn.execute("DELETE FROM workout_map_markers WHERE id = ?", (marker_id,))
    finally:
        conn.close()
