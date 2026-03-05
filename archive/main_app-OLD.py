from pathlib import Path
import sqlite3

import pandas as pd
import streamlit as st

from config import get_healthfit_dir, get_db_path
from db.database import init_db, get_db_file
from ingestion.healthfit_scanner import sync_source_files
from ingestion.fit_ingestor import ingest_new_fit_workouts


# -------------------------------------------------------------------
# DB helpers for the ingestion / system page
# -------------------------------------------------------------------
def get_db_table_names() -> list[str]:
    """
    Return a list of table names in the SQLite database.
    If anything goes wrong, return an empty list.
    """
    db_path = get_db_file()
    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    except Exception:
        return []


def get_source_files_overview() -> dict[str, int]:
    """
    Return basic counts from the source_files table.
    If the table does not exist or an error occurs, return zeros.
    """
    db_path = get_db_file()
    overview = {
        "total": 0,
        "fit": 0,
        "gpx": 0,
        "csv": 0,
    }

    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                "SELECT COUNT(*), file_type FROM source_files GROUP BY file_type;"
            )
            for count, ftype in cursor.fetchall():
                overview["total"] += count
                if ftype in overview:
                    overview[ftype] = count
        finally:
            conn.close()
    except Exception:
        # Table might not exist yet or other DB issue; keep defaults.
        pass

    return overview


def get_workouts_overview() -> dict[str, str | int | None]:
    """
    Return basic info about workouts:
    - total count
    - earliest start_time
    - latest start_time
    """
    db_path = get_db_file()
    overview: dict[str, str | int | None] = {
        "total": 0,
        "first_date": None,
        "last_date": None,
    }

    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) AS cnt,
                    MIN(start_time) AS first_start,
                    MAX(start_time) AS last_start
                FROM workouts;
                """
            )
            row = cursor.fetchone()
            if row is None:
                return overview

            cnt, first_start, last_start = row
            overview["total"] = int(cnt or 0)
            overview["first_date"] = first_start
            overview["last_date"] = last_start
        finally:
            conn.close()
    except Exception:
        pass

    return overview


def main() -> None:
    st.set_page_config(
        page_title="TrailOps · System / Ingestion",
        page_icon="⛰️",
        layout="wide",
    )

    st.title("TrailOps · System / Ingestion")
    st.caption("HealthFit wiring, database schema, and FIT ingestion. No charts here, just plumbing.")

    # Initialise the database schema (tables + indexes)
    try:
        init_db()
    except Exception as e:
        st.error(f"Database initialisation error: {e}")
        st.stop()

    # -------------------------------------------------------------------
    # Config summary
    # -------------------------------------------------------------------
    try:
        healthfit_dir = Path(get_healthfit_dir())
        db_path = Path(get_db_path())
    except Exception as e:
        st.error(f"Config error: {e}")
        st.stop()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Configured paths")
        st.write(f"**HealthFit directory:** `{healthfit_dir}`")
        st.write(f"**Database path:** `{db_path}`")

    with col2:
        st.subheader("Filesystem status")
        healthfit_exists = healthfit_dir.exists()
        db_parent_exists = db_path.parent.exists()
        db_exists = db_path.exists()

        st.write(f"HealthFit folder exists: **{healthfit_exists}**")
        st.write(f"DB parent folder exists: **{db_parent_exists}**")
        st.write(f"DB file exists: **{db_exists}**")

    with col3:
        st.subheader("Database schema")
        table_names = get_db_table_names()
        if table_names:
            st.write("Tables:")
            for name in table_names:
                st.write(f"- `{name}`")
        else:
            st.write("No tables found, or unable to read schema.")

    st.markdown("---")

    # -------------------------------------------------------------------
    # Ingestion controls & status for source_files
    # -------------------------------------------------------------------
    st.subheader("Ingestion status · HealthFit → source_files")

    if not healthfit_dir.exists():
        st.error(
            "The HealthFit directory does not exist. "
            "Check the `HEALTHFIT_DIR` value in your `.env` file."
        )
    else:
        col_a, col_b = st.columns([1, 2])

        with col_a:
            if st.button("Scan HealthFit folder & sync to database"):
                with st.spinner("Scanning HealthFit folder and syncing to database..."):
                    summary = sync_source_files(healthfit_dir)
                st.success(
                    f"Sync complete. Discovered {summary['total_discovered']} files, "
                    f"inserted {summary['new_inserted']} new records into source_files."
                )

        with col_b:
            overview = get_source_files_overview()
            st.write("**Current source_files overview:**")
            st.write(
                f"- Total tracked files: **{overview['total']}**  \n"
                f"- FIT files: **{overview['fit']}**  \n"
                f"- GPX files: **{overview['gpx']}**  \n"
                f"- CSV files: **{overview['csv']}**"
            )

    st.markdown("---")

    # -------------------------------------------------------------------
    # FIT → workouts ingestion
    # -------------------------------------------------------------------
    st.subheader("Workout ingestion · FIT → workouts")

    col_w1, col_w2 = st.columns([1, 2])

    with col_w1:
        if st.button("Ingest new FIT workouts"):
            with st.spinner("Parsing FIT files and updating workouts..."):
                result = ingest_new_fit_workouts(max_files=200)
            st.success(
                "FIT ingestion complete. "
                f"Candidates: {result['candidates']}, "
                f"inserted: {result['inserted']}, "
                f"updated: {result.get('updated', 0)}, "
                f"missing files: {result['skipped_missing']}, "
                f"parse errors: {result['skipped_parse_error']}, "
                f"samples written: {result['samples_written']}."
            )

    with col_w2:
        w_overview = get_workouts_overview()
        st.write("**Current workouts overview:**")
        st.write(f"- Total workouts: **{w_overview['total']}**")

        if w_overview["first_date"]:
            st.write(f"- First workout date: `{w_overview['first_date']}`")
        if w_overview["last_date"]:
            st.write(f"- Most recent workout date: `{w_overview['last_date']}`")

    st.markdown("---")

    # -------------------------------------------------------------------
    # Raw HealthFit folder preview
    # -------------------------------------------------------------------
    st.subheader("HealthFit files preview (raw folder listing)")

    if not healthfit_dir.exists():
        st.error(
            "The HealthFit directory does not exist. "
            "Check the `HEALTHFIT_DIR` value in your `.env` file."
        )
        return

    # Collect a small sample of files
    try:
        all_files = sorted(healthfit_dir.iterdir())
    except Exception as e:
        st.error(f"Error reading HealthFit directory: {e}")
        return

    if not all_files:
        st.info("No files found in the HealthFit directory yet.")
        return

    # Filter for interesting extensions
    exts_of_interest = {".fit", ".gpx", ".csv"}
    interesting_files = [f for f in all_files if f.suffix.lower() in exts_of_interest]

    if not interesting_files:
        st.warning(
            "The HealthFit directory exists but no .fit, .gpx or .csv files were found."
        )
        return

    max_files_to_show = 30
    files_to_show = interesting_files[:max_files_to_show]

    st.write(
        f"Showing up to {max_files_to_show} files "
        f"({len(interesting_files)} matching in total)."
    )

    file_rows = []
    for f in files_to_show:
        stat = f.stat()
        file_rows.append(
            {
                "name": f.name,
                "extension": f.suffix.lower(),
                "size_kb": round(stat.st_size / 1024, 1),
                "modified": stat.st_mtime,
            }
        )

    if file_rows:
        folder_df = pd.DataFrame(file_rows)
        folder_df["modified"] = pd.to_datetime(folder_df["modified"], unit="s")
        st.dataframe(folder_df, width="stretch")

    st.markdown("---")
    st.caption(
        "Use this page for wiring & ingestion. For summaries and charts, run the Training Dashboard app."
    )


if __name__ == "__main__":
    main()