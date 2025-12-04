from pathlib import Path
import sqlite3
import datetime

import pandas as pd
import numpy as np
import streamlit as st

from config import get_healthfit_dir, get_db_path
from db.database import init_db, get_db_file
from ingestion.healthfit_scanner import sync_source_files
from ingestion.fit_ingestor import ingest_new_fit_workouts

# -------------------------------------------------------------------
# Helper: map FIT sport_type strings to human-friendly labels
# -------------------------------------------------------------------
SPORT_LABEL_OVERRIDES = {
    "running:generic": "Run (outdoor)",
    "running:indoor_running": "Run (indoor)",
    "cycling:indoor_cycling": "Cycling (indoor)",
    "cycling:road": "Cycling (road)",
    "walking:walking": "Walk",
    "hiking:hiking": "Hike",
    "fitness_equipment:stair_climbing": "Stair climber",
    "generic:generic": "Workout",
    # add more overrides here as you spot recurring patterns
}


def format_sport_label(raw: str) -> str:
    """
    Turn a raw sport_type like 'cycling:indoor_cycling'
    into something vaguely pleasant to read.
    """
    if not raw:
        return "Unknown"

    # Explicit overrides first
    if raw in SPORT_LABEL_OVERRIDES:
        return SPORT_LABEL_OVERRIDES[raw]

    # Fallback: split on ":", replace "_" with " ", capitalise
    parts = raw.split(":")

    def prettify(s: str) -> str:
        s = s.replace("_", " ")
        return s[:1].upper() + s[1:] if s else s

    parts = [prettify(p) for p in parts]

    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} ({parts[1]})"

def classify_sport_for_totals(raw: str | None) -> str:
    """
    Classify sport types into categories for time-on-feet style totals.

    Categories:
    - 'running' : any running / indoor running
    - 'walking' : walking / hiking / indoor walking
    - 'stair'   : stair stepper / stair climbing
    - 'other'   : everything else
    """
    if not raw:
        return "other"

    s = raw.lower()

    if "stair" in s or "step" in s:
        return "stair"

    if "walk" in s or "hike" in s:
        return "walking"

    if "run" in s:
        return "running"

    return "other"

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


def get_recent_workouts(limit: int = 200) -> pd.DataFrame:
    """
    Return a DataFrame of the most recent workouts (up to `limit`).

    Distances and elevation are stored in meters in the database (as per FIT),
    but here we convert to miles and feet for display.
    """
    db_path = get_db_file()

    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                SELECT
                    id,
                    start_time,
                    sport_type,
                    distance_m,
                    duration_s,
                    elevation_gain_m,
                    avg_heart_rate,
                    max_heart_rate
                FROM workouts
                ORDER BY start_time DESC
                LIMIT ?;
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "id",
            "start_time",
            "sport_type",
            "distance_m",
            "duration_s",
            "elevation_gain_m",
            "avg_heart_rate",
            "max_heart_rate",
        ],
    )

    # Basic formatting for display
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")

    # Convert metres to miles (1 mile = 1609.344 m)
    df["distance_mi"] = (df["distance_m"] / 1609.344).round(2)

    # Duration: seconds → minutes
    df["duration_min"] = (df["duration_s"] / 60).round(1)

    # Elevation metres → feet (1 m = 3.28084 ft)
    df["elevation_gain_ft"] = (df["elevation_gain_m"] * 3.28084).round(0)

    # Reorder columns for nicer UI
    df = df[
        [
            "start_time",
            "sport_type",
            "distance_mi",
            "duration_min",
            "elevation_gain_ft",
            "avg_heart_rate",
            "max_heart_rate",
            "id",
        ]
    ]

    return df


def get_workout_detail(workout_id: int) -> dict | None:
    """
    Fetch full details for a single workout from the database.
    Returns a dict with raw metric values and some precomputed display values.
    """
    db_path = get_db_file()
    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                SELECT
                    id,
                    start_time,
                    end_time,
                    sport_type,
                    distance_m,
                    duration_s,
                    elevation_gain_m,
                    avg_heart_rate,
                    max_heart_rate,
                    notes
                FROM workouts
                WHERE id = ?;
                """,
                (workout_id,),
            )
            row = cursor.fetchone()
        finally:
            conn.close()
    except Exception:
        return None

    if row is None:
        return None

    (
        wid,
        start_time,
        end_time,
        sport_type,
        distance_m,
        duration_s,
        elevation_gain_m,
        avg_hr,
        max_hr,
        notes,
    ) = row

    # Conversions
    m_to_mi = 1 / 1609.344
    m_to_ft = 3.28084

    distance_m = float(distance_m or 0.0)
    duration_s = float(duration_s or 0.0)
    elevation_m = float(elevation_gain_m or 0.0)

    distance_mi = distance_m * m_to_mi
    elevation_ft = elevation_m * m_to_ft

    if distance_mi > 0:
        pace_min_per_mile = (duration_s / 60) / distance_mi
    else:
        pace_min_per_mile = None

    return {
        "id": wid,
        "start_time": start_time,
        "end_time": end_time,
        "sport_type": sport_type,
        "distance_m": distance_m,
        "distance_mi": distance_mi,
        "duration_s": duration_s,
        "elevation_m": elevation_m,
        "elevation_ft": elevation_ft,
        "avg_heart_rate": avg_hr,
        "max_heart_rate": max_hr,
        "pace_min_per_mile": pace_min_per_mile,
        "notes": notes,
    }


def main() -> None:
    st.set_page_config(
        page_title="TrailOps · Mission Control",
        page_icon="⛰️",
        layout="wide",
    )

    st.title("TrailOps · Mission Control")
    st.caption("Boot sequence: wiring, database, ingestion, and workouts.")

    # Initialise the database schema
    try:
        init_db()
    except Exception as e:
        st.error(f"Database initialisation error: {e}")
        st.stop()

    # Show config summary
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

    # Ingestion controls & status for source_files
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

    # FIT → workouts ingestion
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
                f"missing files: {result['skipped_missing']}, "
                f"parse errors: {result['skipped_parse_error']}."
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

    # Training summary panel (7 and 14 days + lifetime)
    from analysis.training_summary import get_training_summary

    st.subheader("Training Summary · Short & Long Term")

    summary = get_training_summary()
    last_7 = summary["last_7"]
    last_14 = summary["last_14"]
    lifetime = summary["lifetime"]

    # conversion helpers
    m_to_mi = 1 / 1609.344
    m_to_ft = 3.28084

    def fmt(summary_dict):
        cat = summary_dict["by_category"]
        running = cat["running"]
        walking = cat["walking"]

        total_dist_m = summary_dict["total_distance_m"]
        total_elev_m = summary_dict["total_elevation_m"]

        return {
            "total_distance_mi": round(total_dist_m * m_to_mi, 2),
            "running_distance_mi": round(running["distance_m"] * m_to_mi, 2),
            "walking_distance_mi": round(walking["distance_m"] * m_to_mi, 2),
            "total_elevation_ft": round(total_elev_m * m_to_ft),
            "running_elevation_ft": round(running["elevation_m"] * m_to_ft),
            "walking_elevation_ft": round(walking["elevation_m"] * m_to_ft),
            "duration_hr": round(summary_dict["total_duration_s"] / 3600, 2),
            "count": summary_dict["count"],
        }

    s7 = fmt(last_7)
    s14 = fmt(last_14)
    slife = fmt(lifetime)

    col_t1, col_t2, col_t3 = st.columns(3)

    with col_t1:
        st.markdown("### Last 7 Days")
        st.write(
            f"**Distance:** {s7['total_distance_mi']} mi  \n"
            f"• Running: {s7['running_distance_mi']} mi  \n"
            f"• Walk / Hike / Steps: {s7['walking_distance_mi']} mi"
        )
        st.write(
            f"**Elevation:** {s7['total_elevation_ft']} ft  \n"
            f"• Running: {s7['running_elevation_ft']} ft  \n"
            f"• Walk / Hike / Steps: {s7['walking_elevation_ft']} ft"
        )
        st.write(f"**Time:** {s7['duration_hr']} hr")
        st.write(f"**Workouts:** {s7['count']}")

    with col_t2:
        st.markdown("### Last 14 Days")
        st.write(
            f"**Distance:** {s14['total_distance_mi']} mi  \n"
            f"• Running: {s14['running_distance_mi']} mi  \n"
            f"• Walk / Hike / Steps: {s14['walking_distance_mi']} mi"
        )
        st.write(
            f"**Elevation:** {s14['total_elevation_ft']} ft  \n"
            f"• Running: {s14['running_elevation_ft']} ft  \n"
            f"• Walk / Hike / Steps: {s14['walking_elevation_ft']} ft"
        )
        st.write(f"**Time:** {s14['duration_hr']} hr")
        st.write(f"**Workouts:** {s14['count']}")

    with col_t3:
        st.markdown("### Lifetime")
        st.write(
            f"**Distance:** {slife['total_distance_mi']} mi  \n"
            f"• Running: {slife['running_distance_mi']} mi  \n"
            f"• Walk / Hike / Steps: {slife['walking_distance_mi']} mi"
        )
        st.write(
            f"**Elevation:** {slife['total_elevation_ft']} ft  \n"
            f"• Running: {slife['running_elevation_ft']} ft  \n"
            f"• Walk / Hike / Steps: {slife['walking_elevation_ft']} ft"
        )
        st.write(f"**Time:** {slife['duration_hr']} hr")
        st.write(f"**Workouts:** {slife['count']}")

    st.markdown("---")

    # Raw HealthFit folder preview
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

    # Recent workouts from DB + filter + detail panel
    st.subheader("Recent workouts (from database)")

    recent_df = get_recent_workouts(limit=200)
    if recent_df.empty:
        st.info("No workouts found yet. Try running FIT ingestion above.")
    else:
        # -----------------------------
        # Date range controls
        # -----------------------------
        st.markdown("**Date range**")

        date_mode = st.radio(
            "Preset date range",
            [
                "Last 7 days",
                "Last 14 days",
                "This week",
                "Last week",
                "This month",
                "Last month",
                "Lifetime",
                "Custom",
            ],
            index=0,
            horizontal=True,
        )

        custom_start = custom_end = None
        if date_mode == "Custom":
            today = datetime.date.today()
            col_dr1, col_dr2 = st.columns(2)
            with col_dr1:
                custom_start = st.date_input(
                    "Start date",
                    value=today - datetime.timedelta(days=7),
                    key="recent_start",
                )
            with col_dr2:
                custom_end = st.date_input(
                    "End date",
                    value=today,
                    key="recent_end",
                )

        if date_mode == "Custom":
            start_date = custom_start
            end_date = custom_end
        else:
            start_date, end_date = get_date_range(date_mode)

        # Apply date filter if we have a range
        df_filtered = recent_df.copy()
        if start_date is not None and end_date is not None:
            mask = (df_filtered["start_time"].dt.date >= start_date) & (
                df_filtered["start_time"].dt.date <= end_date
            )
            df_filtered = df_filtered[mask]

        if df_filtered.empty:
            st.info("No workouts found in the selected date range.")
            return

        st.caption(
            f"Showing workouts from {start_date or 'first recorded'} to {end_date or 'latest'} "
            f"(within the last {len(recent_df)} ingested workouts)."
        )

        # -----------------------------
        # Sport-type filter
        # -----------------------------
        sport_options = sorted(df_filtered["sport_type"].dropna().unique().tolist())
        selected_sports = st.multiselect(
            "Filter by sport type",
            options=sport_options,
            default=sport_options,
            format_func=lambda v: format_sport_label(v),
        )

        if selected_sports:
            filtered_df = df_filtered[df_filtered["sport_type"].isin(selected_sports)]
        else:
            filtered_df = df_filtered.iloc[0:0]  # empty

        if filtered_df.empty:
            st.info("No workouts match the selected filters.")
            return

        # -----------------------------
        # Totals for selected filters
        # -----------------------------
        # classify into categories for breakdown
        tmp = filtered_df.copy()
        tmp["category"] = tmp["sport_type"].apply(classify_sport_for_totals)

        running_df = tmp[tmp["category"] == "running"]
        walking_df = tmp[tmp["category"] == "walking"]
        stair_df = tmp[tmp["category"] == "stair"]

        # overall totals (time-on-feet style)
        total_dist_mi = float(filtered_df["distance_mi"].sum())
        total_elev_ft = float(filtered_df["elevation_gain_ft"].sum())
        total_duration_hr = float(filtered_df["duration_min"].sum()) / 60.0
        total_count = len(filtered_df)

        # running totals
        run_dist_mi = float(running_df["distance_mi"].sum()) if not running_df.empty else 0.0
        run_elev_ft = float(running_df["elevation_gain_ft"].sum()) if not running_df.empty else 0.0

        # walking totals
        walk_dist_mi = float(walking_df["distance_mi"].sum()) if not walking_df.empty else 0.0
        walk_elev_ft = float(walking_df["elevation_gain_ft"].sum()) if not walking_df.empty else 0.0

        # stair stepper totals (elevation only is usually meaningful)
        stair_elev_ft = float(stair_df["elevation_gain_ft"].sum()) if not stair_df.empty else 0.0

        st.markdown("#### Totals for selected date range")
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        with col_s1:
            st.metric("Total distance (mi)", f"{total_dist_mi:.1f}")
        with col_s2:
            st.metric("Total elevation (ft)", f"{int(round(total_elev_ft))}")
        with col_s3:
            st.metric("Total time (hr)", f"{total_duration_hr:.2f}")
        with col_s4:
            st.metric("Workouts", total_count)

        # breakdown row
        st.markdown("##### Breakdown by activity")
        col_b1, col_b2, col_b3, col_b4, col_b5 = st.columns(5)
        with col_b1:
            st.metric("Run distance (mi)", f"{run_dist_mi:.1f}")
        with col_b2:
            st.metric("Walk distance (mi)", f"{walk_dist_mi:.1f}")
        with col_b3:
            st.metric("Run elevation (ft)", f"{int(round(run_elev_ft))}")
        with col_b4:
            st.metric("Walk elevation (ft)", f"{int(round(walk_elev_ft))}")
        with col_b5:
            st.metric("Stair elevation (ft)", f"{int(round(stair_elev_ft))}")

        st.dataframe(filtered_df, width="stretch")

        # Build a selection list using the filtered workouts
        records = filtered_df.to_dict(orient="records")

        def option_label(rec: dict) -> str:
            dt = rec["start_time"]
            try:
                dt_str = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                dt_str = str(dt)
            sport_label = format_sport_label(rec["sport_type"])
            return f"{dt_str} · {sport_label} · {rec['distance_mi']:.2f} mi"

        selected_rec = st.selectbox(
            "Select a workout for details",
            options=records,
            format_func=option_label,
        )

        if selected_rec:
            selected_workout_id = int(selected_rec["id"])
            detail = get_workout_detail(selected_workout_id)
            if detail is None:
                st.warning("Could not load details for this workout.")
            else:
                st.markdown("### Workout details")

                col_d1, col_d2, col_d3 = st.columns(3)

                with col_d1:
                    st.write(f"**Date/time:** {detail['start_time']}")
                    st.write(f"**Sport:** {detail['sport_type']}")
                    st.write(f"**Distance:** {detail['distance_mi']:.2f} mi")

                with col_d2:
                    st.write(f"**Duration:** {detail['duration_s'] / 60:.1f} min")
                    if detail["pace_min_per_mile"]:
                        pace = detail["pace_min_per_mile"]
                        pace_min = int(pace)
                        pace_sec = int(round((pace - pace_min) * 60))
                        st.write(f"**Pace:** {pace_min}:{pace_sec:02d} min/mi")
                    else:
                        st.write("**Pace:** n/a")

                with col_d3:
                    st.write(f"**Elevation:** {detail['elevation_ft']:.0f} ft")
                    if detail["avg_heart_rate"]:
                        st.write(f"**Avg HR:** {detail['avg_heart_rate']} bpm")
                    if detail["max_heart_rate"]:
                        st.write(f"**Max HR:** {detail['max_heart_rate']} bpm")

                if detail.get("notes"):
                    st.markdown("**Notes:**")
                    st.write(detail["notes"])

                # ---------------------------------------------
                # Time-series charts for the selected workout
                # ---------------------------------------------
                samples_df = get_workout_samples(selected_workout_id)

                if samples_df.empty:
                    st.info("No per-sample data available for this workout.")
                else:
                    st.markdown("#### Time-series analysis")

                    # Pace (smoothed) & elevation
                    col_ts1, col_ts2 = st.columns(2)

                    with col_ts1:
                        st.caption("Pace (smoothed, min/mile)")
                        pace_df = samples_df[["t_min", "pace_min_per_mile_smooth"]].dropna()
                        if not pace_df.empty:
                            pace_df = pace_df.rename(
                                columns={
                                    "t_min": "Time (min)",
                                    "pace_min_per_mile_smooth": "Pace (min/mi)",
                                }
                            ).set_index("Time (min)")
                            st.line_chart(pace_df)
                        else:
                            st.write("No pace data available.")

                    with col_ts2:
                        st.caption("Elevation (ft)")
                        elev_df = samples_df[["t_min", "elevation_ft"]].dropna()
                        if not elev_df.empty:
                            elev_df = elev_df.rename(
                                columns={
                                    "t_min": "Time (min)",
                                    "elevation_ft": "Elevation (ft)",
                                }
                            ).set_index("Time (min)")
                            st.line_chart(elev_df)
                        else:
                            st.write("No elevation data available.")

                    # HR & cadence
                    col_ts3, col_ts4 = st.columns(2)

                    with col_ts3:
                        st.caption("Heart rate (bpm)")
                        hr_df = samples_df[["t_min", "heart_rate_bpm"]].dropna()
                        if not hr_df.empty:
                            hr_df = hr_df.rename(
                                columns={
                                    "t_min": "Time (min)",
                                    "heart_rate_bpm": "HR (bpm)",
                                }
                            ).set_index("Time (min)")
                            st.line_chart(hr_df)
                        else:
                            st.write("No heart rate data available.")

                    with col_ts4:
                        st.caption("Cadence (spm)")
                        cad_df = samples_df[["t_min", "cadence_spm"]].dropna()
                        if not cad_df.empty:
                            cad_df = cad_df.rename(
                                columns={
                                    "t_min": "Time (min)",
                                    "cadence_spm": "Cadence (spm)",
                                }
                            ).set_index("Time (min)")
                            st.line_chart(cad_df)
                        else:
                            st.write("No cadence data available.")

                    # Power chart, only if present
                    if samples_df["power_w"].notna().any():
                        st.caption("Power (W)")
                        pow_df = samples_df[["t_min", "power_w"]].dropna()
                        if not pow_df.empty:
                            pow_df = pow_df.rename(
                                columns={
                                    "t_min": "Time (min)",
                                    "power_w": "Power (W)",
                                }
                            ).set_index("Time (min)")
                            st.line_chart(pow_df)

# -------------------------------------------------------------------
# Date range helper for recent workouts
# -------------------------------------------------------------------
def get_date_range(mode: str) -> tuple[datetime.date | None, datetime.date | None]:
    """
    Return (start_date, end_date) for a given preset mode.
    If mode == 'Lifetime', returns (None, None).
    """
    today = datetime.date.today()
    this_monday = today - datetime.timedelta(days=today.weekday())
    first_of_this_month = today.replace(day=1)

    if mode == "Last 7 days":
        return today - datetime.timedelta(days=6), today
    if mode == "Last 14 days":
        return today - datetime.timedelta(days=13), today
    if mode == "This week":
        return this_monday, today
    if mode == "Last week":
        last_week_monday = this_monday - datetime.timedelta(days=7)
        last_week_sunday = this_monday - datetime.timedelta(days=1)
        return last_week_monday, last_week_sunday
    if mode == "This month":
        return first_of_this_month, today
    if mode == "Last month":
        last_month_end = first_of_this_month - datetime.timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end
    if mode == "Lifetime":
        return None, None

    # Custom handled separately in the UI
    return None, None

# -------------------------------------------------------------------
# Load per-sample data for a workout (for charts)
# -------------------------------------------------------------------
def get_workout_samples(workout_id: int) -> pd.DataFrame:
    """
    Load per-record samples for a given workout from workout_samples.

    Adds:
    - t_sec: seconds since start
    - t_min: minutes since start
    - distance_mi
    - elevation_ft
    - pace_min_per_mile (instant)
    - pace_min_per_mile_smooth (30-sample moving average, min/mile)
    """
    db_path = get_db_file()
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                id,
                timestamp_utc,
                seconds_since_start,
                distance_m,
                elevation_m,
                speed_m_s,
                heart_rate_bpm,
                cadence_spm,
                power_w,
                stride_length_m
            FROM workout_samples
            WHERE workout_id = ?
            ORDER BY
                CASE
                    WHEN seconds_since_start IS NOT NULL THEN seconds_since_start
                    ELSE id
                END;
            """,
            conn,
            params=(workout_id,),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    # Time axis: prefer seconds_since_start; fallback to timestamp sequence
    if df["seconds_since_start"].notna().any():
        df["t_sec"] = df["seconds_since_start"].ffill()
    elif df["timestamp_utc"].notna().any():
        t = pd.to_datetime(df["timestamp_utc"], utc=True)
        df["t_sec"] = (t - t.iloc[0]).dt.total_seconds()
    else:
        df["t_sec"] = pd.Series(range(len(df)), dtype="float64")

    df["t_min"] = df["t_sec"] / 60.0

    # Distance & elevation in imperial units
    df["distance_mi"] = df["distance_m"] / 1609.344
    df["elevation_ft"] = df["elevation_m"] * 3.28084

    # Pace from speed (m/s) -> sec/mile -> min/mile
    # Ensure we are working with numeric values only
    speed_numeric = pd.to_numeric(df["speed_m_s"], errors="coerce")

    if speed_numeric.notna().any():
        speed_clipped = speed_numeric.clip(lower=0.1)  # avoid division by zero
        df["pace_sec_per_mile"] = 1609.344 / speed_clipped
        df["pace_min_per_mile"] = df["pace_sec_per_mile"] / 60.0
    else:
        df["pace_min_per_mile"] = np.nan

    # Force pace to be float so rolling() can aggregate
    df["pace_min_per_mile"] = pd.to_numeric(
        df["pace_min_per_mile"], errors="coerce"
    )

    # Smoothed pace: 30-sample moving average (≈ 30s at 1 Hz recording)
    df["pace_min_per_mile_smooth"] = (
        df["pace_min_per_mile"]
        .rolling(window=30, min_periods=1)
        .mean()
    )

    return df

if __name__ == "__main__":
    main()