from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone

DB_PATH = r"C:\trail_ops\data\trailops.db"

def moon_phase_fraction(dt_utc: datetime) -> tuple[float, float]:
    # Same simple approximation used elsewhere in the codebase.
    ref = datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc)
    synodic_days = 29.53058867
    days = (dt_utc - ref).total_seconds() / 86400.0
    phase = (days % synodic_days) / synodic_days
    illum = 0.5 * (1 - math.cos(2 * math.pi * phase))
    return float(phase), float(illum)

def moon_phase_name_from_fraction(phase: float | None) -> str | None:
    if phase is None:
        return None
    try:
        p = float(phase) % 1.0
    except Exception:
        return None

    if p < 0.0625 or p >= 0.9375:
        return "New Moon"
    if p < 0.1875:
        return "Waxing Crescent"
    if p < 0.3125:
        return "First Quarter"
    if p < 0.4375:
        return "Waxing Gibbous"
    if p < 0.5625:
        return "Full Moon"
    if p < 0.6875:
        return "Waning Gibbous"
    if p < 0.8125:
        return "Last Quarter"
    return "Waning Crescent"

def to_utc(dt_iso: str) -> datetime:
    # Handles 'Z' and offsets
    s = dt_iso.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)

def backfill_one(conn: sqlite3.Connection, workout_id: int) -> int:
    row = conn.execute(
        "SELECT start_time_utc FROM workout_weather WHERE workout_id = ? LIMIT 1",
        (workout_id,),
    ).fetchone()
    start_time_utc = row[0] if row else None

    if not start_time_utc:
        # Fallback to workouts table
        row2 = conn.execute(
            "SELECT start_time FROM workouts WHERE id = ? LIMIT 1",
            (workout_id,),
        ).fetchone()
        start_time_utc = row2[0] if row2 else None

    if not start_time_utc:
        return 0

    dt0 = to_utc(start_time_utc)
    phase, illum = moon_phase_fraction(dt0)
    name = moon_phase_name_from_fraction(phase)

    cur = conn.execute(
        "UPDATE workout_weather "
        "SET moon_phase = ?, moon_illumination = ?, moon_phase_name = ? "
        "WHERE workout_id = ?",
        (phase, illum, name, workout_id),
    )
    return int(cur.rowcount or 0)

def backfill_all(conn: sqlite3.Connection) -> int:
    ids = [r[0] for r in conn.execute(
        "SELECT workout_id FROM workout_weather "
        "WHERE moon_phase IS NULL OR moon_illumination IS NULL OR moon_phase_name IS NULL"
    ).fetchall()]
    updated = 0
    for wid in ids:
        updated += backfill_one(conn, int(wid))
    return updated

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--workout-id", type=int, default=None, help="Backfill a single workout_id")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with conn:
            if args.workout_id is not None:
                n = backfill_one(conn, int(args.workout_id))
                print(f"updated_rows={n} (workout_id={args.workout_id})")
            else:
                n = backfill_all(conn)
                print(f"updated_rows={n} (all missing moon fields)")
    finally:
        conn.close()
