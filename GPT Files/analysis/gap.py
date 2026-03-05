from __future__ import annotations

import numpy as np
import pandas as pd

M_PER_MILE = 1609.344


def compute_grade_pct(
    distance_m,
    elevation_m,
    *,
    grid_step_m: float = 5.0,
    elev_smooth_window_m: float = 25.0,
    grade_window_m: float = 30.0,
    clamp_pct: float = 45.0,
) -> np.ndarray:
    """
    Compute grade (%) aligned to original samples, robustly.

    - Filters non-finite distance/elevation
    - Forces distance monotonic
    - Interpolates elevation onto a distance grid (default 5m)
    - Smooths elevation in distance-domain (median)
    - Computes grade as central difference over grade_window_m
    - Interpolates grade back to original sample distance points
    - Clamps grade to avoid spikes from noise
    - Returns NaNs if grade cannot be computed safely
    """
    d = np.asarray(distance_m, dtype=float)
    e = np.asarray(elevation_m, dtype=float)

    n = len(d)
    if n < 10:
        return np.full(n, np.nan, dtype=float)

    ok = np.isfinite(d) & np.isfinite(e)
    if ok.sum() < 10:
        return np.full(n, np.nan, dtype=float)

    d_ok = np.maximum.accumulate(d[ok])
    e_ok = e[ok]

    dmax = float(np.nanmax(d_ok))
    if not np.isfinite(dmax) or dmax <= 0:
        return np.full(n, np.nan, dtype=float)

    # Safety: distance corruption can create insane grids
    if dmax > 250_000:  # 250 km cap for a single activity, still generous
        return np.full(n, np.nan, dtype=float)

    step = float(grid_step_m) if grid_step_m and grid_step_m > 0 else 5.0
    grid = np.arange(0.0, dmax + step, step)
    if len(grid) < 10:
        return np.full(n, np.nan, dtype=float)

    elev_grid = np.interp(grid, d_ok, e_ok)

    # Elevation smoothing window in grid points
    win = max(3, int(round(float(elev_smooth_window_m) / step)))
    if win % 2 == 0:
        win += 1

    elev_smooth = (
        pd.Series(elev_grid)
        .rolling(win, center=True, min_periods=max(3, win // 3))
        .median()
        .to_numpy(dtype=float)
    )

    # Grade central-difference window
    half = max(step, float(grade_window_m) / 2.0)
    k = max(1, int(round(half / step)))

    grade_grid = np.full_like(elev_smooth, np.nan, dtype=float)
    if len(elev_smooth) > 2 * k + 2:
        rise = elev_smooth[2 * k :] - elev_smooth[: -2 * k]
        run = grid[2 * k :] - grid[: -2 * k]
        good = np.isfinite(rise) & np.isfinite(run) & (run > 0)
        gfrac = np.full_like(rise, np.nan, dtype=float)
        gfrac[good] = rise[good] / run[good]
        grade_grid[k:-k] = gfrac * 100.0

    # Interpolate grade back to original distances
    # Use 0 for NaNs in grade_grid during interpolation to avoid propagating NaN holes
    grade_grid_safe = np.nan_to_num(grade_grid, nan=0.0)
    grade_pct = np.interp(np.maximum.accumulate(d), grid, grade_grid_safe).astype(float)

    if clamp_pct is not None and np.isfinite(clamp_pct):
        grade_pct = np.clip(grade_pct, -float(clamp_pct), float(clamp_pct))

    # Preserve NaNs where original inputs were invalid
    grade_pct[~ok] = np.nan
    return grade_pct


def minetti_cost_running_j_per_kg_per_m(grade_frac: np.ndarray) -> np.ndarray:
    """
    Minetti-style polynomial energetic cost of running vs slope.
    grade_frac is rise/run (e.g., 0.10 for 10%).
    Returns Cr in J/kg/m.

    Note: This is a baseline “v1” GAP model, designed to be replaceable later.
    """
    g = np.asarray(grade_frac, dtype=float)
    return 155.4 * g**5 - 30.4 * g**4 - 43.3 * g**3 + 46.3 * g**2 + 19.5 * g + 3.6


def gap_from_speed(speed_m_s, grade_pct) -> np.ndarray:
    """
    Compute GAP pace (min/mi) from speed (m/s) and grade (%).

    Equivalent flat speed:
      v_eq = v * Cr(grade) / Cr(0)

    GAP pace:
      pace_gap = (meters_per_mile / v_eq) / 60

    Returns NaNs where speed/grade are invalid.
    """
    speed = np.asarray(speed_m_s, dtype=float)
    grade_pct = np.asarray(grade_pct, dtype=float)

    n = len(speed)
    if n == 0:
        return np.array([], dtype=float)

    grade_frac = grade_pct / 100.0
    cr = minetti_cost_running_j_per_kg_per_m(grade_frac)
    cr0 = 3.6

    v_eq = np.full(n, np.nan, dtype=float)
    ok = np.isfinite(speed) & np.isfinite(cr) & (speed > 0.1) & (cr > 0.2)
    v_eq[ok] = speed[ok] * (cr[ok] / cr0)

    gap_pace = np.full(n, np.nan, dtype=float)
    ok2 = np.isfinite(v_eq) & (v_eq > 0.1)
    gap_pace[ok2] = (M_PER_MILE / v_eq[ok2]) / 60.0

    return gap_pace