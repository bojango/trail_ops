import sys
from datetime import datetime
import sqlite3

# Ensure imports work regardless of launch context
sys.path.insert(0, r"C:\trail_ops\app")

from analysis.plot_samples import build_and_store_plot_samples
from analysis.peaks import enrich_peaks_for_workout

DB = r"C:\trail_ops\data\trailops.db"
SINCE = "2026-02-01"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

ids = [r["id"] for r in conn.execute("""
    SELECT id
    FROM workouts
    WHERE start_time >= ?
      AND has_gps = 1
    ORDER BY start_time ASC
""", (SINCE,)).fetchall()]

print(f"Backfilling {len(ids)} workouts since {SINCE}...")

ok_plot = 0
ok_peaks = 0
for i, wid in enumerate(ids, 1):
    try:
        # plot samples (also writes moving_time/moving_pace/avg_gap updates)
        n = build_and_store_plot_samples(wid, force=True)
        ok_plot += 1
    except Exception as e:
        print(f"[{i}/{len(ids)}] plot_samples FAILED wid={wid}: {e}")

    try:
        # peak hits
        hits = enrich_peaks_for_workout(wid, force=True)
        ok_peaks += 1
    except Exception as e:
        print(f"[{i}/{len(ids)}] peaks FAILED wid={wid}: {e}")

    if i % 25 == 0:
        print(f"... {i}/{len(ids)} done")

conn.close()
print(f"Done. plot_samples ok={ok_plot}, peaks ok={ok_peaks}")
