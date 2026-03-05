from __future__ import annotations

import argparse
from pathlib import Path

from app.db.database import DB_PATH, get_connection, init_db


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workout-id", type=int, default=None, help="Workout id to inspect (optional)")
    args = ap.parse_args()

    print(f"DB_PATH: {DB_PATH}")
    init_db()

    conn = get_connection()
    try:
        def pragma_table(table: str) -> None:
            print(f"\n== PRAGMA table_info({table})")
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            for r in rows:
                print(f"{r['cid']}|{r['name']}|{r['type']}|{r['notnull']}|{r['dflt_value']}|{r['pk']}")

        def pragma_fk(table: str) -> None:
            print(f"\n== PRAGMA foreign_key_list({table})")
            rows = conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
            for r in rows:
                # row columns: id, seq, table, from, to, on_update, on_delete, match
                vals = [str(r[c]) for c in r.keys()]
                print("|".join(vals))

        for t in ["workouts", "workout_map_points", "peaks", "workout_peak_hits", "workout_surface_stats", "workout_route_context"]:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
                pragma_table(t)

        pragma_fk("workout_peak_hits")

        print("\n== Counts")
        for sql, label in [
            ("SELECT COUNT(*) AS n FROM workouts", "workouts"),
            ("SELECT COUNT(*) AS n FROM workout_map_points", "workout_map_points"),
            ("SELECT COUNT(*) AS n FROM peaks", "peaks"),
            ("SELECT COUNT(*) AS n FROM workout_peak_hits", "workout_peak_hits"),
        ]:
            r = conn.execute(sql).fetchone()
            print(f"{label}: {int(r['n'])}")

        if args.workout_id is not None:
            wid = int(args.workout_id)
            print(f"\n== Workout {wid}")
            r = conn.execute("SELECT id, start_time, sport_type, has_gps, peaks_enriched FROM workouts WHERE id=?", (wid,)).fetchone()
            print(dict(r) if r else "not found")

            r = conn.execute("SELECT COUNT(*) AS n FROM workout_map_points WHERE workout_id=? AND level=0", (wid,)).fetchone()
            print(f"map_points(level0): {int(r['n'])}")

            r = conn.execute("SELECT COUNT(*) AS n FROM workout_peak_hits WHERE workout_id=?", (wid,)).fetchone()
            print(f"peak_hits: {int(r['n'])}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
