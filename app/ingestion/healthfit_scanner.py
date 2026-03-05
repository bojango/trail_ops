from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal

import sqlite3

from app.db.database import get_connection
FileType = Literal["fit", "gpx", "csv"]


@dataclass
class SourceFileRecord:
    path: Path
    file_type: FileType
    size_bytes: int
    mtime: float  # POSIX timestamp


EXTENSION_MAP: dict[str, FileType] = {
    ".fit": "fit",
    ".gpx": "gpx",
    ".csv": "csv",
}


def discover_files(root: Path) -> list[SourceFileRecord]:
    """
    Scan the HealthFit directory for .fit, .gpx and .csv files.

    Returns a list of SourceFileRecord objects.
    """
    records: list[SourceFileRecord] = []

    for item in root.iterdir():
        if not item.is_file():
            continue

        ext = item.suffix.lower()
        if ext not in EXTENSION_MAP:
            continue

        file_type: FileType = EXTENSION_MAP[ext]
        stat = item.stat()
        records.append(
            SourceFileRecord(
                path=item.resolve(),
                file_type=file_type,
                size_bytes=stat.st_size,
                mtime=stat.st_mtime,
            )
        )

    return records


def load_existing_paths(conn: sqlite3.Connection) -> set[str]:
    """
    Load the set of file paths already present in source_files.
    Paths are stored as absolute strings.
    """
    cursor = conn.execute("SELECT path FROM source_files;")
    return {row[0] for row in cursor.fetchall()}


def sync_source_files(root: Path) -> dict[str, int]:
    """
    Sync the files in the HealthFit directory with the source_files table.

    - Discovers .fit, .gpx, .csv files in `root`
    - Inserts any new files into source_files
    - Returns a summary dict with counts
    """
    discovered = discover_files(root)
    total_discovered = len(discovered)

    conn = get_connection()
    inserted = 0
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    try:
        existing_paths = load_existing_paths(conn)

        with conn:
            for rec in discovered:
                path_str = str(rec.path)
                if path_str in existing_paths:
                    continue

                conn.execute(
                    """
                    INSERT INTO source_files (path, file_type, size_bytes, mtime, imported_at)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (path_str, rec.file_type, rec.size_bytes, rec.mtime, now_iso),
                )
                inserted += 1
    finally:
        conn.close()

    return {
        "total_discovered": total_discovered,
        "new_inserted": inserted,
    }


if __name__ == "__main__":
    # Small manual test helper:
    from config import get_healthfit_dir

    root = Path(get_healthfit_dir())
    summary = sync_source_files(root)
    print("HealthFit sync summary:", summary)