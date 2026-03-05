from __future__ import annotations

import argparse
import csv
from pathlib import Path

from app.db.database import init_db, get_connection
from app.analysis.map_points import build_map_points_for_workout


def _select_workouts(limit: int, prefixes: list[str]) -> list[tuple[int, str, str]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id, start_time, sport_type
            FROM workouts
            WHERE ({' OR '.join(['sport_type LIKE ?' for _ in prefixes])})
            ORDER BY start_time DESC
            LIMIT ?
            """,
            tuple([f"{p}%" for p in prefixes] + [limit]),
        ).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]
    finally:
        conn.close()


def _get_route_hash(workout_id: int) -> str | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT route_hash FROM workouts WHERE id = ?", (workout_id,)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill workout_map_points (multi-level simplified routes).")
    parser.add_argument("--limit", type=int, default=50, help="How many workouts to process (most recent first).")
    parser.add_argument(
        "--prefixes",
        nargs="*",
        default=["running", "walking", "hiking"],
        help="sport_type prefixes to include",
    )
    parser.add_argument("--report", type=str, default="map_points_backfill_report.csv", help="CSV report filename.")
    args = parser.parse_args()

    init_db()

    workouts = _select_workouts(args.limit, args.prefixes)
    print(f"Found {len(workouts)} workouts matching prefixes: {args.prefixes}")

    report_path = Path(args.report).resolve()
    with report_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["workout_id", "start_time", "sport_type", "route_hash", "levels_json"])

        for wid, st, sport in workouts:
            levels = build_map_points_for_workout(wid)
            rh = _get_route_hash(wid)
            print(f"[OK] workout_id={wid} levels={levels} route_hash={rh}")
            w.writerow([wid, st, sport, rh, str(levels)])

    print(f"\nWrote report: {report_path}")


if __name__ == "__main__":
    main()
