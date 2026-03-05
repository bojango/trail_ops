import sqlite3
from pathlib import Path

paths = [
    Path(r"C:\trail_ops\data\trailops.db"),
    Path(r"C:\trail_ops\trailops.db"),
    Path(r"C:\trail_ops\app\trailops.db"),
]

for p in paths:
    if not p.exists():
        continue
    conn = sqlite3.connect(p)
    try:
        n = conn.execute("SELECT COUNT(*) FROM workout_route_context").fetchone()[0]
        print(f"{p} -> workout_route_context rows: {n}")
    except Exception as e:
        print(f"{p} -> error: {e}")
    finally:
        conn.close()