from __future__ import annotations

"""
TrailOps - Auto ingest (one-shot)

Runs:
1) Sync HealthFit folder -> source_files
2) Ingest new FIT workouts -> workouts + samples
   (fit_ingestor already calls enrichment post-ingest)

Run as:
    python -m app.tools.auto_ingest_once

Designed for Windows Task Scheduler (run every N minutes).
Writes a rolling log to: C:\trail_ops\logs\auto_ingest.log
"""

import datetime as _dt
import os
from pathlib import Path
import traceback

from app.config import get_healthfit_dir, get_db_path
from app.db.database import init_db
from app.ingestion.healthfit_scanner import sync_source_files
from app.ingestion.fit_ingestor import ingest_new_fit_workouts


LOG_REL_PATH = Path("logs") / "auto_ingest.log"
MAX_LOG_BYTES = 2_000_000  # ~2MB


def _ensure_logfile(project_root: Path) -> Path:
    log_path = project_root / LOG_REL_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    # crude rotation
    try:
        if log_path.exists() and log_path.stat().st_size > MAX_LOG_BYTES:
            ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated = log_path.parent / f"auto_ingest_{ts}.log"
            log_path.replace(rotated)
    except Exception:
        pass
    return log_path


def _log(log_path: Path, msg: str) -> None:
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # If file logging fails, stdout is still useful.
        pass


def main() -> int:
    project_root = Path.cwd()
    log_path = _ensure_logfile(project_root)

    _log(log_path, "=== Auto ingest run starting ===")

    try:
        init_db()
    except Exception as e:
        _log(log_path, f"DB init failed: {e!r}")
        _log(log_path, traceback.format_exc())
        return 2

    try:
        healthfit_dir = Path(get_healthfit_dir())
        db_path = Path(get_db_path())
        _log(log_path, f"HealthFit dir: {healthfit_dir}")
        _log(log_path, f"DB path: {db_path}")
    except Exception as e:
        _log(log_path, f"Config error: {e!r}")
        _log(log_path, traceback.format_exc())
        return 3

    if not healthfit_dir.exists():
        _log(log_path, "HealthFit directory does not exist. Aborting.")
        return 4

    # 1) Sync files
    try:
        sync_summary = sync_source_files(healthfit_dir)
        _log(
            log_path,
            "Sync complete: "
            f"discovered={sync_summary.get('total_discovered')} "
            f"new_inserted={sync_summary.get('new_inserted')}",
        )
    except Exception as e:
        _log(log_path, f"Sync failed: {e!r}")
        _log(log_path, traceback.format_exc())
        return 5

    # 2) Ingest FIT
    try:
        result = ingest_new_fit_workouts(max_files=200)
        _log(
            log_path,
            "FIT ingest complete: "
            f"candidates={result.get('candidates')} "
            f"inserted={result.get('inserted')} "
            f"updated={result.get('updated', 0)} "
            f"skipped_missing={result.get('skipped_missing')} "
            f"skipped_parse_error={result.get('skipped_parse_error')} "
            f"samples_written={result.get('samples_written')}"
        )
    except Exception as e:
        _log(log_path, f"FIT ingest failed: {e!r}")
        _log(log_path, traceback.format_exc())
        return 6

    _log(log_path, "=== Auto ingest run finished OK ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
