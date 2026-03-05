from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Any

# Allow running as a script (but prefer: python -m app.tools.backfill_route_context)
if __package__ is None or __package__ == "":
    here = Path(__file__).resolve()
    sys.path.insert(0, str(here.parents[2]))

from app.db.database import get_connection, init_db  # noqa: E402
from app.analysis.route_context import (  # noqa: E402
    compute_and_store_route_context,
    compute_and_store_surface_stats,
    compute_and_store_weather,
)
from app.analysis.peaks import compute_and_store_peak_hits  # noqa: E402


def _select_from_report(path: Path) -> list[int]:
    ids: list[int] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if any((r.get(k) or "").strip() for k in ("location_error", "surface_error", "peaks_error", "weather_error")):
                try:
                    ids.append(int(r["workout_id"]))
                except Exception:
                    continue
    # preserve order (most recent likely already first)
    seen: set[int] = set()
    out: list[int] = []
    for wid in ids:
        if wid not in seen:
            seen.add(wid)
            out.append(wid)
    return out


def _select_workouts(limit: int, gps_only: bool, prefixes: list[str]) -> list[tuple[int, str, str]]:
    conn = get_connection()
    try:
        where = []
        params: list[Any] = []
        if gps_only:
            where.append("has_gps=1")
        if prefixes:
            # sport_type is like "walking:generic"
            like_clauses = []
            for p in prefixes:
                like_clauses.append("sport_type LIKE ?")
                params.append(f"{p}:%")
            where.append("(" + " OR ".join(like_clauses) + ")")
        wh = (" WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
        SELECT id, start_time, sport_type
        FROM workouts
        {wh}
        ORDER BY start_time DESC
        LIMIT ?
        """
        params.append(limit)
        rows = conn.execute(sql, tuple(params)).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]
    finally:
        conn.close()


def _get_location_label(conn, workout_id: int) -> str | None:
    row = conn.execute(
        "SELECT location_label, center_location_label, start_location_label FROM workout_route_context WHERE workout_id=?",
        (workout_id,),
    ).fetchone()
    if not row:
        return None
    return row[0] or row[1] or row[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill route context + surface + peaks + weather (best-effort).")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--gps-only", action="store_true", default=False)
    parser.add_argument("--prefixes", nargs="*", default=["running", "walking", "hiking"])
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--report", type=str, default="route_context_backfill_report.csv")
    parser.add_argument("--from-report", type=str, default=None, help="Retry only failures from a prior report CSV.")
    args = parser.parse_args()

    init_db()

    if args.from_report:
        workout_ids = _select_from_report(Path(args.from_report))
        if args.limit:
            workout_ids = workout_ids[: args.limit]
        selected = []
        conn = get_connection()
        try:
            for wid in workout_ids:
                r = conn.execute("SELECT start_time, sport_type FROM workouts WHERE id=?", (wid,)).fetchone()
                if r:
                    selected.append((wid, str(r[0]), str(r[1])))
        finally:
            conn.close()
    else:
        selected = _select_workouts(args.limit, bool(args.gps_only), list(args.prefixes))

    print(f"Found {len(selected)} workouts to process (most recent first). Prefixes: {args.prefixes} | gps_only={args.gps_only}")
    if not selected:
        return

    print("\nSelected workouts:")
    for wid, st, sp in selected:
        print(f"  - workout_id={wid} | {st} | {sp}")

    out_path = Path(args.report)
    fieldnames = [
        "workout_id",
        "start_time",
        "sport_type",
        "location_label",
        "location_error",
        "surface_ok",
        "surface_error",
        "peaks_count",
        "peaks_error",
        "weather_ok",
        "weather_error",
    ]

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for wid, start_time, sport_type in selected:
            location_label = None
            location_error = None
            surface_ok = False
            surface_error = None
            peaks_count = 0
            peaks_error = None
            weather_ok = False
            weather_error = None

            # route context
            try:
                compute_and_store_route_context(wid, force=bool(args.force))
                conn = get_connection()
                try:
                    location_label = _get_location_label(conn, wid)
                    # mark flag if row exists
                    if conn.execute("SELECT 1 FROM workout_route_context WHERE workout_id=?", (wid,)).fetchone():
                        conn.execute("UPDATE workouts SET route_enriched=1 WHERE id=?", (wid,))
                        conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                location_error = str(e)

            # surface
            try:
                compute_and_store_surface_stats(wid, force=bool(args.force))
                conn = get_connection()
                try:
                    surface_ok = conn.execute("SELECT 1 FROM workout_surface_stats WHERE workout_id=?", (wid,)).fetchone() is not None
                finally:
                    conn.close()
            except Exception as e:
                surface_error = str(e)

            # peaks
            try:
                compute_and_store_peak_hits(wid, force=bool(args.force))
                conn = get_connection()
                try:
                    peaks_count = int(
                        conn.execute("SELECT COUNT(*) FROM workout_peak_hits WHERE workout_id=?", (wid,)).fetchone()[0]
                    )
                finally:
                    conn.close()
            except Exception as e:
                peaks_error = str(e)

            # weather
            try:
                compute_and_store_weather(wid, force=bool(args.force))
                conn = get_connection()
                try:
                    weather_ok = conn.execute("SELECT 1 FROM workout_weather WHERE workout_id=?", (wid,)).fetchone() is not None
                finally:
                    conn.close()
            except Exception as e:
                weather_error = str(e)

            status = "OK" if not any([location_error, surface_error, peaks_error, weather_error]) else "FAIL"
            if status == "OK":
                print(f"[OK] workout_id={wid} | {start_time} | location={location_label} | surface={'ok' if surface_ok else 'none'} | peaks={peaks_count} | weather={'ok' if weather_ok else 'none'}")
            else:
                print(f"[FAIL] workout_id={wid} | {start_time} | location={location_label}")
                if location_error:
                    print(f"       location_error: {location_error}")
                if surface_error:
                    print(f"       surface_error: {surface_error}")
                if peaks_error:
                    print(f"       peaks_error: {peaks_error}")
                if weather_error:
                    print(f"       weather_error: {weather_error}")

            writer.writerow(
                {
                    "workout_id": wid,
                    "start_time": start_time,
                    "sport_type": sport_type,
                    "location_label": location_label,
                    "location_error": location_error,
                    "surface_ok": int(surface_ok),
                    "surface_error": surface_error,
                    "peaks_count": peaks_count,
                    "peaks_error": peaks_error,
                    "weather_ok": int(weather_ok),
                    "weather_error": weather_error,
                }
            )

            if args.sleep and args.sleep > 0:
                time.sleep(float(args.sleep))

    print(f"\nWrote report: {out_path.resolve()}")


if __name__ == "__main__":
    main()
