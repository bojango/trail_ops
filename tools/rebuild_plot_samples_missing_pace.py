import sys
sys.path.insert(0, r"C:\trail_ops\app")

from db.database import get_connection
from analysis.plot_samples import build_and_store_plot_samples

conn = get_connection()
cur = conn.cursor()

ids = [r[0] for r in cur.execute("""
SELECT workout_id
FROM workout_plot_samples
GROUP BY workout_id
HAVING SUM(CASE WHEN pace_min_per_mile IS NOT NULL THEN 1 ELSE 0 END) = 0
ORDER BY workout_id DESC
LIMIT 250
""").fetchall()]

print(f"Rebuilding plot samples for {len(ids)} workouts...")

for wid in ids:
    build_and_store_plot_samples(conn, int(wid))
    print(f"  ✓ workout_id={wid}")

conn.close()
print("Done.")
