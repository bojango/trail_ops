from pathlib import Path
import sqlite3
from typing import Optional

from config import get_db_path


def get_db_file() -> Path:
    """
    Return the Path object for the SQLite database,
    ensuring the parent directory exists.
    """
    db_path = Path(get_db_path())
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def get_connection() -> sqlite3.Connection:
    """
    Open a connection to the SQLite database with foreign keys enabled.
    """
    db_file = get_db_file()
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """
    Return True if `column` exists in `table`, False otherwise.
    """
    cursor = conn.execute(f"PRAGMA table_info({table});")
    for row in cursor.fetchall():
        # row[1] is the column name
        if row[1].lower() == column.lower():
            return True
    return False


def _ensure_workouts_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the workouts table exists with at least the original columns,
    then add any new columns needed for advanced metrics.
    """
    # Base table definition (original columns)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_fit_file_id INTEGER,
            start_time TEXT,
            end_time TEXT,
            sport_type TEXT,
            distance_m REAL,
            duration_s REAL,
            elevation_gain_m REAL,
            avg_heart_rate REAL,
            max_heart_rate REAL,
            notes TEXT,
            FOREIGN KEY (source_fit_file_id) REFERENCES source_files(id)
                ON UPDATE CASCADE
                ON DELETE SET NULL
        );
        """
    )

    # New columns for session-level metrics
    new_columns_sql: dict[str, str] = {
        "avg_power_w": "REAL",
        "max_power_w": "REAL",
        "total_calories": "REAL",
        "avg_cadence_spm": "REAL",
        "max_cadence_spm": "REAL",
        "total_steps": "REAL",
        "avg_stride_length_m": "REAL",
    }

    for col_name, col_type in new_columns_sql.items():
        if not _column_exists(conn, "workouts", col_name):
            conn.execute(f"ALTER TABLE workouts ADD COLUMN {col_name} {col_type};")


def _ensure_source_files_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the source_files table exists.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            file_type TEXT NOT NULL,             -- e.g. 'fit', 'gpx', 'csv'
            size_bytes INTEGER NOT NULL,
            mtime REAL NOT NULL,                 -- last modified time (timestamp)
            imported_at TEXT NOT NULL            -- ISO timestamp when TrailOps processed it
        );
        """
    )


def _ensure_workout_samples_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the workout_samples table exists.

    This stores per-record / per-trackpoint metrics for each workout and is the
    backbone for time-series charts (pace, cadence, HR, power, elevation, etc.)
    and future GAP calculations.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workout_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL,
            timestamp_utc TEXT,               -- ISO 8601 UTC
            seconds_since_start REAL,         -- seconds from workout start
            distance_m REAL,
            elevation_m REAL,
            speed_m_s REAL,
            heart_rate_bpm REAL,
            power_w REAL,
            cadence_spm REAL,
            stride_length_m REAL,
            vertical_oscillation_m REAL,      -- VO, if available
            ground_contact_time_ms REAL,      -- GCT, if available
            ground_contact_balance_pct REAL,  -- left/right balance, if available
            latitude_deg REAL,
            longitude_deg REAL,
            lap_index INTEGER,
            FOREIGN KEY (workout_id) REFERENCES workouts(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )


def init_db() -> None:
    """
    Create or upgrade the database schema.

    Sets up:
    - source_files: tracks FIT/GPX/CSV files present in the HealthFit folder
    - workouts: per-workout summary, with advanced metrics columns
    - workout_samples: per-record / trackpoint time-series data per workout
    """
    conn = get_connection()
    try:
        with conn:
            _ensure_source_files_table(conn)
            _ensure_workouts_table(conn)
            _ensure_workout_samples_table(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    # Allow manual initialisation by running this file directly, e.g.:
    # python -m db.database
    init_db()
    print("Database initialised / migrated.")