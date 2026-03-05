from __future__ import annotations

import traceback
from pathlib import Path

import pandas as pd

from app.db.database import init_db, get_pandas_connection
from app.analysis.plot_samples import build_and_store_plot_samples


def _is_moving_sport(sport_type: str | None) -> bool:
    if not sport_type:
        return False
    s = sport_type.lower()
    return (
        s.startswith("running")
        or s.startswith("walking")
        or s.startswith("hiking")
        or ("run" in s)
        or ("walk" in s)
        or ("hike" in s)
    )


def main() -> None:
    """Backfill plot samples + GAP + moving-time metrics for all run/walk/hike workouts.

    Run this as a module from repo root:
        python -m app.tools.backfill_plot_samples
    """
    init_db()

    conn = get_pandas_connection()
    try:
        workouts = pd.read_sql_query(
            "SELECT id, sport_type FROM workouts ORDER BY start_time ASC;",
            conn,
        )
    finally:
        conn.close()

    sel_df = workouts[workouts["sport_type"].apply(lambda x: _is_moving_sport(x or ""))].copy()
    total = len(sel_df)

    print(f"Backfilling plot samples + GAP + moving-time metrics for {total} run/walk/hike workouts (full history).")

    failures: list[dict[str, str | int]] = []
    done = 0

    for wid in sel_df["id"].astype(int).tolist():
        try:
            build_and_store_plot_samples(wid)
            done += 1
            if done % 25 == 0:
                print(f"Processed {done}/{total}")
        except Exception as e:
            failures.append(
                {
                    "workout_id": int(wid),
                    "error": repr(e),
                    "traceback": traceback.format_exc(),
                }
            )
            print(f"⚠️  Failed workout_id={wid}: {e!r} (continuing)")

    print(f"Backfill complete: {done}/{total} processed successfully.")

    if failures:
        report_path = Path(__file__).resolve().parent / "backfill_failures.csv"
        pd.DataFrame(failures).to_csv(report_path, index=False)
        print(f"⚠️  {len(failures)} failures. Report saved to: {report_path}")


if __name__ == "__main__":
    main()
