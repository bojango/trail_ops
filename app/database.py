from __future__ import annotations

from pathlib import Path
import sqlite3

from config import get_db_path


def get_db_file() -> Path:
    db_path = Path(get_db_path())
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def get_connection() -> sqlite3.Connection:
    db_file = get_db_file()
    conn = sqlite3.connect(db_file, timeout=30)

    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    for row in cur.fetchall():
        if str(row[1]).lower() == column.lower():
            return True
    return False


def _ensure_source_files_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            file_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            mtime REAL NOT NULL,
            imported_at TEXT NOT NULL
        );
        """
    )


def _ensure_workouts_table(conn: sqlite3.Connection) -> None:
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

    # Migrate in optional columns safely
    optional_cols = {
        "avg_gap_min_per_mile": "REAL",
        "avg_power_w": "REAL",
        "max_power_w": "REAL",
        "avg_cadence_spm": "REAL",
        "max_cadence_spm": "REAL",
        "total_calories": "REAL",
        "total_steps": "REAL",
    }

    for col, typ in optional_cols.items():
        if not _column_exists(conn, "workouts", col):
            conn.execute(f"ALTER TABLE workouts ADD COLUMN {col} {typ};")


def _ensure_workout_samples_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workout_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL,
            timestamp_utc TEXT,
            seconds_since_start REAL,
            distance_m REAL,
            elevation_m REAL,
            speed_m_s REAL,
            heart_rate_bpm REAL,
            power_w REAL,
            cadence_spm REAL,
            stride_length_m REAL,
            vertical_oscillation_m REAL,
            ground_contact_time_ms REAL,
            ground_contact_balance_pct REAL,
            latitude_deg REAL,
            longitude_deg REAL,
            lap_index INTEGER,
            FOREIGN KEY (workout_id) REFERENCES workouts(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )


def _ensure_workout_plot_samples_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workout_plot_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL,
            t_min REAL NOT NULL,
            pace_min_per_mile REAL,
            gap_min_per_mile REAL,
            grade_pct REAL,
            elevation_ft REAL,
            heart_rate_bpm REAL,
            cadence_spm REAL,
            power_w REAL,
            FOREIGN KEY (workout_id) REFERENCES workouts(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )

    # Ensure new columns exist even on older DBs
    for col, typ in {"gap_min_per_mile": "REAL", "grade_pct": "REAL"}.items():
        if not _column_exists(conn, "workout_plot_samples", col):
            conn.execute(f"ALTER TABLE workout_plot_samples ADD COLUMN {col} {typ};")




def _ensure_workout_map_points_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workout_map_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL,
            level INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            latitude_deg REAL NOT NULL,
            longitude_deg REAL NOT NULL,
            seconds_since_start REAL,
            distance_m REAL,
            elevation_m REAL,
            FOREIGN KEY (workout_id) REFERENCES workouts(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )


def _ensure_workout_map_markers_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workout_map_markers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            kind TEXT NOT NULL,
            seq INTEGER,
            latitude_deg REAL NOT NULL,
            longitude_deg REAL NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (workout_id) REFERENCES workouts(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )


def _ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_start_time ON workouts(start_time);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_sport_type ON workouts(sport_type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_samples_workout_id ON workout_samples(workout_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_plot_samples_workout_id ON workout_plot_samples(workout_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_map_points_workout_id_level_seq ON workout_map_points(workout_id, level, seq);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_map_markers_workout_id ON workout_map_markers(workout_id);")


def init_db() -> None:
    conn = get_connection()
    try:
        with conn:
            _ensure_source_files_table(conn)
            _ensure_workouts_table(conn)
            _ensure_workout_samples_table(conn)
            _ensure_workout_plot_samples_table(conn)
            _ensure_workout_map_points_table(conn)
            _ensure_workout_map_markers_table(conn)
            _ensure_indexes(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialised / migrated.")