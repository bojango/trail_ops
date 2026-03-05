from __future__ import annotations

import numpy as np
import pandas as pd

from db.database import get_connection
from analysis.gap import compute_grade_pct, gap_from_speed

M_PER_MILE = 1609.344


def _is_walk_hike(sport_type: str | None) -> bool:
    if not sport_type:
        return False
    s = sport_type.lower()
    return s.startswith("walking") or s.startswith("hiking") or ("walk" in s) or ("hike" in s)


# Strava documents a running moving threshold of anything faster than a 30-minute mile pace.
# For walks/hikes, Strava doesn't publish a specific threshold, so we use a more permissive default.
RUN_MOVING_PACE_THRESHOLD_MIN_PER_MILE = 30.0
WALK_HIKE_MOVING_PACE_THRESHOLD_MIN_PER_MILE = 60.0


def _is_running(sport_type: str | None) -> bool:
    if not sport_type:
        return False
    s = sport_type.lower()
    # Works with schemas like "running", "running:trail_running", etc.
    return s.startswith("running") or ("trail" in s and "run" in s) or ("run" in s)


def _safe_numeric(series: pd.Series) -> np.ndarray:
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)


def _ensure_workouts_columns(conn) -> None:
    """Add columns we need if DB is older. Safe to call every time."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(workouts);").fetchall()}
    needed = {
        "moving_time_s": "INTEGER",
        "stationary_time_s": "INTEGER",
        "moving_pace_min_per_mile": "REAL",
    }
    for name, typ in needed.items():
        if name not in cols:
            try:
                conn.execute(f"ALTER TABLE workouts ADD COLUMN {name} {typ};")
            except Exception:
                # If another process added it or SQLite rejects for some reason, ignore.
                pass


def build_and_store_plot_samples(workout_id: int, max_points: int = 900) -> None:
    """
    Build downsampled chart-ready samples for workout_id and store in workout_plot_samples.

    Bulletproof behavior:
    - Coerces bad/missing values to NaN
    - If distance missing, reconstructs distance from speed * dt when possible
    - If seconds_since_start missing, reconstructs from row index (as last resort)
    - If elevation missing, grade/GAP become NaN
    - GAP computed for running workouts only
    - Updates workouts.avg_gap_min_per_mile if column exists and GAP is computable
    """
    conn = get_connection()
    try:
        _ensure_workouts_columns(conn)

        row = conn.execute("SELECT sport_type FROM workouts WHERE id = ?;", (workout_id,)).fetchone()
        sport_type = row[0] if row else None
        is_run = _is_running(sport_type)
        is_walk_hike = _is_walk_hike(sport_type)

        df = pd.read_sql_query(
            """
            SELECT
                id,
                seconds_since_start,
                distance_m,
                elevation_m,
                speed_m_s,
                heart_rate_bpm,
                cadence_spm,
                power_w
            FROM workout_samples
            WHERE workout_id = ?
            ORDER BY
                CASE
                    WHEN seconds_since_start IS NOT NULL THEN seconds_since_start
                    ELSE id
                END;
            """,
            conn,
            params=(workout_id,),
        )

        if df.empty:
            return

        # Coerce numerics
        t_sec = _safe_numeric(df["seconds_since_start"]) if "seconds_since_start" in df else np.full(len(df), np.nan)
        dist = _safe_numeric(df["distance_m"]) if "distance_m" in df else np.full(len(df), np.nan)
        elev = _safe_numeric(df["elevation_m"]) if "elevation_m" in df else np.full(len(df), np.nan)
        speed = _safe_numeric(df["speed_m_s"]) if "speed_m_s" in df else np.full(len(df), np.nan)
        hr = _safe_numeric(df["heart_rate_bpm"]) if "heart_rate_bpm" in df else np.full(len(df), np.nan)
        cad = _safe_numeric(df["cadence_spm"]) if "cadence_spm" in df else np.full(len(df), np.nan)
        pwr = _safe_numeric(df["power_w"]) if "power_w" in df else np.full(len(df), np.nan)

        n = len(df)

        # Build time axis
        if np.isfinite(t_sec).any():
            # forward-fill missing seconds
            t_sec = pd.Series(t_sec).ffill().to_numpy(dtype=float)
            # ensure non-decreasing
            t_sec = np.maximum.accumulate(np.nan_to_num(t_sec, nan=0.0))
        else:
            # last resort fallback: assume 1s spacing
            t_sec = np.arange(n, dtype=float)

        t_min = t_sec / 60.0

        # Rebuild distance if missing/broken
        if not np.isfinite(dist).any() or float(np.nanmax(dist)) <= 0:
            dt = np.diff(t_sec, prepend=t_sec[0])
            dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
            inc = np.where(np.isfinite(speed) & (speed > 0), speed, 0.0) * dt
            dist = np.cumsum(inc)

        # ----------------------------
        # Moving time / stationary time (seconds)
        # ----------------------------
        moving_time_s = None
        stationary_time_s = None
        moving_pace_min_per_mile = None

        pace_thr = None
        if is_run:
            pace_thr = RUN_MOVING_PACE_THRESHOLD_MIN_PER_MILE
        elif is_walk_hike:
            pace_thr = WALK_HIKE_MOVING_PACE_THRESHOLD_MIN_PER_MILE

        if pace_thr is not None:
            # Convert pace threshold to speed threshold (m/s)
            speed_thr = M_PER_MILE / (pace_thr * 60.0)
            dt = np.diff(t_sec, prepend=t_sec[0])
            dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)

            # Also require distance to increase meaningfully to avoid GPS jitter, but use a short window
            # so we don't incorrectly classify slow/stop-start motion as stationary.
            dd = np.diff(dist, prepend=dist[0])
            dd = np.where(np.isfinite(dd) & (dd > 0), dd, 0.0)

            # Derive speed from distance deltas too (more reliable than noisy speed spikes)
            seg_speed = np.zeros_like(dd)
            ok_dt = np.isfinite(dt) & (dt > 0)
            seg_speed[ok_dt] = dd[ok_dt] / dt[ok_dt]

            # Windowed distance delta (meters over last `win` samples)
            win = 3
            d = np.asarray(dist, dtype=float)
            d_prev = np.roll(d, win)
            d_prev[:win] = d[0]
            dd_win = d - d_prev
            dd_win = np.where(np.isfinite(dd_win) & (dd_win > 0), dd_win, 0.0)

            # Threshold over the window.
            # Runs: 2.0m over ~3 samples reduces drift counted as moving during stops.
            # Walk/hike: keep permissive.
            dist_jitter_win_m = 2.0 if is_run else 0.5

            moving_mask = (
                np.isfinite(seg_speed) & (seg_speed >= speed_thr)
                & np.isfinite(dd_win) & (dd_win >= dist_jitter_win_m)
                & ok_dt
            )

            # Debounce: once moving, keep counting as moving for a short grace period.
            # This reduces flicker around junctions/gates and matches Strava-like behavior better.
            DEBOUNCE_S = 2.0
            if DEBOUNCE_S > 0 and len(moving_mask) > 0:
                # Vectorized-ish: expand each moving segment forward until time exceeds DEBOUNCE_S
                moving_idx = np.flatnonzero(moving_mask)
                if moving_idx.size:
                    # For each moving index i, mark subsequent indices j where t_sec[j] <= t_sec[i] + DEBOUNCE_S
                    for i in moving_idx:
                        t_end = t_sec[i] + DEBOUNCE_S
                        j = i + 1
                        while j < len(moving_mask) and t_sec[j] <= t_end:
                            moving_mask[j] = True
                            j += 1

            moving_time_s = float(np.sum(dt[moving_mask]))

            elapsed_s = float(t_sec[-1] - t_sec[0]) if len(t_sec) else 0.0
            stationary_time_s = max(0.0, elapsed_s - moving_time_s)

            if moving_time_s > 0:
                moving_dist_m = float(np.sum(dd[moving_mask]))
                if moving_dist_m > 0:
                    moving_pace_min_per_mile = (moving_time_s / moving_dist_m) * (M_PER_MILE / 60.0)

        # ----------------------------
        # Pace (min/mi) from speed
        # ----------------------------
        pace = np.full(n, np.nan, dtype=float)
        ok_speed = np.isfinite(speed) & (speed > 0.1)
        pace[ok_speed] = (M_PER_MILE / speed[ok_speed]) / 60.0
        pace_s = pd.Series(pace).rolling(30, min_periods=1).mean().to_numpy(dtype=float)

        # Elevation ft
        elev_ft = np.where(np.isfinite(elev), elev * 3.28084, np.nan)

        # ----------------------------
        # Grade + GAP (running only; require some elev data)
        # ----------------------------
        grade_pct = np.full(n, np.nan, dtype=float)
        gap = np.full(n, np.nan, dtype=float)

        if is_run and np.isfinite(elev).any() and np.isfinite(dist).any():
            grade_pct = compute_grade_pct(dist, elev)
            gap = gap_from_speed(speed, grade_pct)

        gap_s = pd.Series(gap).rolling(30, min_periods=1).mean().to_numpy(dtype=float)

        # Downsample indices
        if n > max_points:
            idx = np.linspace(0, n - 1, max_points, dtype=int)
        else:
            idx = np.arange(n, dtype=int)

        out = pd.DataFrame(
            {
                "workout_id": workout_id,
                "t_min": t_min[idx],
                "pace_min_per_mile": pace_s[idx],
                "gap_min_per_mile": gap_s[idx],
                "grade_pct": grade_pct[idx],
                "elevation_ft": elev_ft[idx],
                "heart_rate_bpm": hr[idx],
                "cadence_spm": cad[idx],
                "power_w": pwr[idx],
            }
        )

        with conn:
            conn.execute("DELETE FROM workout_plot_samples WHERE workout_id = ?;", (workout_id,))
            out.to_sql("workout_plot_samples", conn, if_exists="append", index=False)

            # Update avg_gap_min_per_mile (flat-equivalent pace) if computable
            if is_run and np.isfinite(grade_pct).any() and np.isfinite(speed).any() and np.isfinite(dist).any():
                try:
                    grade_frac = grade_pct / 100.0
                    from analysis.gap import minetti_cost_running_j_per_kg_per_m

                    cr = minetti_cost_running_j_per_kg_per_m(grade_frac)
                    cr0 = 3.6

                    d = np.asarray(dist, dtype=float)
                    v = np.asarray(speed, dtype=float)

                    dd = np.diff(d, prepend=d[0])
                    dd = np.where(np.isfinite(dd) & (dd > 0), dd, 0.0)

                    ok = (
                        np.isfinite(dd) & (dd > 0)
                        & np.isfinite(v) & (v > 0.1)
                        & np.isfinite(cr) & (cr > 0.2)
                    )
                    if ok.any():
                        v_eq = v[ok] * (cr[ok] / cr0)
                        ok2 = np.isfinite(v_eq) & (v_eq > 0.1)
                        if ok2.any():
                            dd_ok = dd[ok][ok2]
                            v_eq_ok = v_eq[ok2]
                            time_eq_s = float(np.sum(dd_ok / v_eq_ok))
                            total_dist_m = float(np.sum(dd_ok))
                            if total_dist_m > 0 and time_eq_s > 0:
                                flat_pace_min_per_mile = (time_eq_s / total_dist_m) * (M_PER_MILE / 60.0)
                                conn.execute(
                                    "UPDATE workouts SET avg_gap_min_per_mile = ? WHERE id = ?;",
                                    (float(flat_pace_min_per_mile), workout_id),
                                )
                except Exception:
                    # Never break ingestion/backfill for a metric.
                    pass

            # Store moving metrics for run/walk/hike (if computed)
            if moving_time_s is not None and stationary_time_s is not None:
                try:
                    conn.execute(
                        "UPDATE workouts SET moving_time_s = ?, stationary_time_s = ?, moving_pace_min_per_mile = ? WHERE id = ?;",
                        (
                            int(moving_time_s),
                            int(stationary_time_s),
                            float(moving_pace_min_per_mile) if moving_pace_min_per_mile is not None else None,
                            workout_id,
                        ),
                    )
                except Exception:
                    pass

    finally:
        conn.close()
