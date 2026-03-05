from __future__ import annotations

import numpy as np
import pandas as pd

from app.db.database import DB_PATH, get_pandas_connection
from app.analysis.plot_samples import _safe_numeric, _is_running, _is_walk_hike, M_PER_MILE
from app.analysis.gap import compute_grade_pct, gap_from_speed


def debug_workout(workout_id: int) -> None:
    print("DB_PATH:", DB_PATH)
    conn = get_pandas_connection()
    try:
        row = conn.execute("SELECT sport_type FROM workouts WHERE id = ?;", (workout_id,)).fetchone()
        sport_type = row[0] if row else None
        is_run = _is_running(sport_type)
        is_wh = _is_walk_hike(sport_type)
        print("workout_id:", workout_id, "sport_type:", sport_type, "is_run:", is_run, "is_walk_hike:", is_wh)

        df = pd.read_sql_query(
            '''
            SELECT seconds_since_start, distance_m, elevation_m, speed_m_s
            FROM workout_samples
            WHERE workout_id = ?
            ORDER BY
                CASE
                    WHEN seconds_since_start IS NOT NULL THEN seconds_since_start
                    ELSE id
                END
            ''',
            conn,
            params=(workout_id,),
        )
        print("samples rows:", len(df))
        if df.empty:
            return

        # Coerce
        for c in ["seconds_since_start","distance_m","elevation_m","speed_m_s"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        t_sec = _safe_numeric(df["seconds_since_start"])
        dist = _safe_numeric(df["distance_m"])
        elev = _safe_numeric(df["elevation_m"])
        speed = _safe_numeric(df["speed_m_s"])

        print("t_sec finite:", int(np.isfinite(t_sec).sum()), "min:", float(np.nanmin(t_sec)), "max:", float(np.nanmax(t_sec)))
        print("dist finite:", int(np.isfinite(dist).sum()), "min:", float(np.nanmin(dist)), "max:", float(np.nanmax(dist)))
        print("elev finite:", int(np.isfinite(elev).sum()), "min:", float(np.nanmin(elev)), "max:", float(np.nanmax(elev)))
        print("speed finite:", int(np.isfinite(speed).sum()), "min:", float(np.nanmin(speed)) if np.isfinite(speed).any() else None, "max:", float(np.nanmax(speed)) if np.isfinite(speed).any() else None)

        # Build time axis mimic plot_samples
        if np.isfinite(t_sec).any():
            t_sec = pd.Series(t_sec).ffill().to_numpy(dtype=float)
            t_sec = np.maximum.accumulate(np.nan_to_num(t_sec, nan=0.0))
        else:
            t_sec = np.arange(len(df), dtype=float)

        # Rebuild distance if missing
        if (not np.isfinite(dist).any()) or float(np.nanmax(dist)) <= 0:
            dt = np.diff(t_sec, prepend=t_sec[0])
            dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
            inc = np.where(np.isfinite(speed) & (speed > 0), speed, 0.0) * dt
            dist = np.cumsum(inc)

        dt_raw = np.diff(t_sec, prepend=t_sec[0])
        dt_raw = np.where(np.isfinite(dt_raw) & (dt_raw > 0), dt_raw, np.nan)
        dd_raw = np.diff(dist, prepend=dist[0])
        dd_raw = np.where(np.isfinite(dd_raw) & (dd_raw >= 0), dd_raw, np.nan)

        speed_eff = np.full(len(df), np.nan, dtype=float)
        ok_seg = np.isfinite(dd_raw) & np.isfinite(dt_raw) & (dt_raw > 0)
        speed_eff[ok_seg] = dd_raw[ok_seg] / dt_raw[ok_seg]
        if np.isfinite(speed).any():
            speed_eff[~np.isfinite(speed_eff)] = speed[~np.isfinite(speed_eff)]

        print("dt_raw finite:", int(np.isfinite(dt_raw).sum()), "dd_raw finite:", int(np.isfinite(dd_raw).sum()))
        print("speed_eff finite:", int(np.isfinite(speed_eff).sum()), "min:", float(np.nanmin(speed_eff)) if np.isfinite(speed_eff).any() else None, "max:", float(np.nanmax(speed_eff)) if np.isfinite(speed_eff).any() else None)

        # Pace
        pace = np.full(len(df), np.nan, dtype=float)
        ok_speed = np.isfinite(speed_eff) & (speed_eff > 0.1)
        pace[ok_speed] = (M_PER_MILE / speed_eff[ok_speed]) / 60.0
        print("pace finite:", int(np.isfinite(pace).sum()), "min:", float(np.nanmin(pace)) if np.isfinite(pace).any() else None, "max:", float(np.nanmax(pace)) if np.isfinite(pace).any() else None)

        # Grade + GAP
        grade_pct = compute_grade_pct(dist, elev) if (is_run and np.isfinite(elev).any() and np.isfinite(dist).any()) else np.full(len(df), np.nan)
        print("grade_pct finite:", int(np.isfinite(grade_pct).sum()), "min:", float(np.nanmin(grade_pct)) if np.isfinite(grade_pct).any() else None, "max:", float(np.nanmax(grade_pct)) if np.isfinite(grade_pct).any() else None)

        grade_for_gap = np.where(np.isfinite(grade_pct), grade_pct, 0.0)
        gap = gap_from_speed(speed_eff, grade_for_gap) if is_run else np.full(len(df), np.nan)
        print("gap finite:", int(np.isfinite(gap).sum()), "min:", float(np.nanmin(gap)) if np.isfinite(gap).any() else None, "max:", float(np.nanmax(gap)) if np.isfinite(gap).any() else None)

    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    wid = int(sys.argv[1]) if len(sys.argv) > 1 else 2234
    debug_workout(wid)
