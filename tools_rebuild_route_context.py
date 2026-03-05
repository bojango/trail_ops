import time
from app.db.database import get_connection, init_db
from app.analysis.route_context import compute_and_store_route_context

SLEEP_S = 1.0

def main():
    init_db()
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT w.id
            FROM workouts w
            WHERE w.has_gps = 1
              AND (
                w.sport_type LIKE 'running:%'
                OR w.sport_type LIKE 'walking:%'
                OR w.sport_type LIKE 'hiking:%'
              )
              AND NOT EXISTS (
                SELECT 1 FROM workout_route_context rc WHERE rc.workout_id = w.id
              )
            ORDER BY w.start_time DESC
        """).fetchall()
        ids = [int(r[0]) for r in rows]

    total = len(ids)
    print(f"Route-context rebuild targets: {total}")
    for i, wid in enumerate(ids, start=1):
        try:
            compute_and_store_route_context(wid, force=True)
            print(f"[OK] {i}/{total} workout_id={wid}")
        except Exception as e:
            print(f"[FAIL] {i}/{total} workout_id={wid} error={e}")
        time.sleep(SLEEP_S)

if __name__ == "__main__":
    main()
