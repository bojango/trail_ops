import sqlite3
from pathlib import Path

DB_PATH = Path("C:/trail_ops/data/trailops.db")

def run_query(conn, label, query):
    print("\n" + "=" * 60)
    print(label)
    print("=" * 60)
    cur = conn.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    print(f"\nRows returned: {len(rows)}")

def main():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    # 1) Schema check
    run_query(
        conn,
        "PRAGMA table_info(workout_route_context);",
        "PRAGMA table_info(workout_route_context);"
    )

    # 2) GPS workouts vs map points
    run_query(
        conn,
        "GPS workouts vs workouts_with_map_points",
        """
        SELECT
          (SELECT COUNT(*) FROM workouts WHERE has_gps = 1) AS gps_workouts,
          (SELECT COUNT(DISTINCT workout_id) FROM workout_map_points) AS workouts_with_map_points;
        """
    )

    # 3) Workouts with GPS but no map points
    run_query(
        conn,
        "GPS workouts with NO map points (first 20)",
        """
        SELECT w.id, w.start_time, w.sport_type
        FROM workouts w
        LEFT JOIN workout_map_points mp ON mp.workout_id = w.id
        WHERE w.has_gps = 1
          AND mp.workout_id IS NULL
        ORDER BY w.start_time DESC
        LIMIT 20;
        """
    )

    conn.close()

if __name__ == "__main__":
    main()