from __future__ import annotations

import sys
from pathlib import Path
import traceback
import pandas as pd

# Ensure app/ is on sys.path
THIS_FILE = Path(__file__).resolve()
APP_DIR = THIS_FILE.parents[1]  # .../app
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from db.database import init_db, get_connection  # noqa: E402
from analysis.plot_samples import build_and_store_plot_samples  # noqa: E402


def _is_running(sport_type: str | None) -> bool:
    if not sport_type:
        return False
    s = sport_type.lower()
    return s.startswith("running") or ("trail" in s and "run" in s) or ("run" in s)


def main() -> None:
    init_db()

    conn = get_connection()
    try:
        workouts = pd.read_sql_query(
            "SELECT id, sport_type FROM workouts ORDER BY start_time ASC;",
            conn,
        )
    finally:
        conn.close()

    run_df = workouts[workouts["sport_type"].apply(lambda x: _is_running(x or ""))].copy()
    total = len(run_df)

    print(f"Backfilling plot samples + GAP for {total} running workouts (full history).")

    failures = []
    done = 0

    for wid in run_df["id"].astype(int).tolist():
        try:
            build_and_store_plot_samples(wid)
            done += 1
            if done % 25 == 0:
                print(f"Processed {done}/{total}")
        except Exception as e:
            failures.append(
                {
                    "workout_id": wid,
                    "error": repr(e),
                    "traceback": traceback.format_exc(),
                }
            )
            print(f"⚠️  Failed workout_id={wid}: {e!r} (continuing)")

    print(f"Backfill complete: {done}/{total} processed successfully.")
    if failures:
        # Write a failure report to help debug any specific corrupt workouts later
        report_path = Path(__file__).resolve().parent / "backfill_failures.csv"
        pd.DataFrame(failures).to_csv(report_path, index=False)
        print(f"⚠️  {len(failures)} failures. Report saved to: {report_path}")


if __name__ == "__main__":
    main()