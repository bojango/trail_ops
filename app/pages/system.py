from __future__ import annotations

import platform
import sqlite3
import traceback
from pathlib import Path
import os

import pandas as pd
import streamlit as st

from app.config import get_healthfit_dir, get_db_path
from app.db.database import get_db_file, get_connection
from app.ingestion.healthfit_scanner import sync_source_files
from app.ingestion.fit_ingestor import ingest_new_fit_workouts


# ----------------------------
# Helpers copied from previous main_app (Phase 0 extraction)
# ----------------------------
def _safe_int(x, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _fmt_bytes(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "n/a"
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n/1024:.1f} KB"
    if n < 1024**3:
        return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


def _get_db_table_names() -> list[str]:
    try:
        conn = get_connection(row_factory="tuple")
        try:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()
    except Exception:
        return []


def _get_source_files_overview() -> dict[str, int]:
    overview = {"total": 0, "fit": 0, "gpx": 0, "csv": 0}
    try:
        conn = get_connection(row_factory="tuple")
        try:
            rows = conn.execute("SELECT COUNT(*), file_type FROM source_files GROUP BY file_type;").fetchall()
        finally:
            conn.close()
        for count, ftype in rows:
            c = _safe_int(count)
            overview["total"] += c
            if ftype in overview:
                overview[str(ftype)] = c
    except Exception:
        pass
    return overview


def _get_workouts_overview() -> dict[str, str | int | None]:
    overview: dict[str, str | int | None] = {"total": 0, "first_date": None, "last_date": None}
    try:
        conn = get_connection(row_factory="tuple")
        try:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS cnt,
                    MIN(start_time) AS first_start,
                    MAX(start_time) AS last_start
                FROM workouts;
                """
            ).fetchone()
        finally:
            conn.close()
        if row:
            cnt, first_start, last_start = row
            overview["total"] = _safe_int(cnt)
            overview["first_date"] = first_start
            overview["last_date"] = last_start
    except Exception:
        pass
    return overview


def _build_directory_tree_text(root: Path) -> str:
    """
    Small, dependency-free directory tree for debugging / sharing with new chats.
    Excludes heavy/noisy folders.
    """
    EXCLUDE = {".venv", "__pycache__", ".git", "archive", "archive_backups"}
    lines: list[str] = []

    def walk(dir_path: Path, prefix: str = "") -> None:
        try:
            entries = sorted(dir_path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except Exception:
            return

        for idx, p in enumerate(entries):
            if p.name in EXCLUDE:
                continue

            connector = "└── " if idx == len(entries) - 1 else "├── "
            lines.append(prefix + connector + p.name)

            if p.is_dir():
                ext_prefix = "    " if idx == len(entries) - 1 else "│   "
                walk(p, prefix + ext_prefix)

    lines.append(str(root))
    walk(root)
    return "\n".join(lines)


def _zip_project(root: Path, out_zip: Path, include_db: bool) -> None:
    """
    Create a zip backup of the project.
    Default excludes the SQLite DB (huge). Optionally include it.
    """
    import zipfile

    EXCLUDE_DIRS = {".venv", "__pycache__", ".git", "archive_backups"}  # keep /archive itself, user might want it
    EXCLUDE_FILES = {"trailops.db"} if not include_db else set()

    def should_skip(path: Path) -> bool:
        parts = {p for p in path.parts}
        if any(d in parts for d in EXCLUDE_DIRS):
            return True
        if path.name in EXCLUDE_FILES:
            return True
        return False

    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in root.rglob("*"):
            if p.is_dir():
                continue
            if should_skip(p):
                continue
            rel = p.relative_to(root)
            zf.write(p, rel.as_posix())


def render_system_page() -> None:
    st.markdown('\n<style>\n/* System page readability surface */\n.to-system-surface {\n  background: #ffffff;\n  color: #111111;\n  padding: 18px 18px;\n  border-radius: 16px;\n  border: 1px solid rgba(0,0,0,0.08);\n  box-shadow: 0 8px 30px rgba(0,0,0,0.10);\n}\n.to-system-surface h1, .to-system-surface h2, .to-system-surface h3,\n.to-system-surface h4, .to-system-surface p, .to-system-surface li {\n  color: #111111 !important;\n}\n.to-system-surface code {\n  color: #111111;\n}\n.to-system-surface .stMarkdown, .to-system-surface .stText, .to-system-surface .stCaption {\n  color: #111111 !important;\n}\n.to-system-surface [data-testid="stExpander"] {\n  background: #ffffff;\n  border: 1px solid rgba(0,0,0,0.08);\n  border-radius: 14px;\n}\n.to-system-surface [data-testid="stExpander"] summary {\n  color: #111111 !important;\n}\n</style>\n', unsafe_allow_html=True)
    st.markdown('<div class="to-system-surface">', unsafe_allow_html=True)

    st.markdown("## System")
    st.caption("Everything operational, diagnostic, and mildly cursed lives here.")

    # Environment snapshot
    with st.expander("Environment snapshot", expanded=True):
        st.write(f"**Python:** {platform.python_version()}")
        st.write(f"**Platform:** {platform.platform()}")
        st.write(f"**Working dir:** `{Path.cwd()}`")
        st.write(f"**DB file:** `{get_db_file()}`")

        try:
            st.write(f"**HealthFit dir:** `{get_healthfit_dir()}`")
        except Exception as e:
            st.warning(f"HealthFit dir not configured: {e}")

        try:
            st.write(f"**DB_PATH in .env:** `{get_db_path()}`")
        except Exception as e:
            st.warning(f"DB_PATH not configured: {e}")

    # DB health checks
    with st.expander("DB health checks", expanded=True):
        tables = _get_db_table_names()
        st.write(f"Tables: **{len(tables)}**")
        st.code("\n".join(tables[:120]) + ("\n..." if len(tables) > 120 else ""))

        wo = _get_workouts_overview()
        st.write(f"Workouts: **{wo.get('total')}**")
        st.write(f"First: `{wo.get('first_date')}`")
        st.write(f"Last: `{wo.get('last_date')}`")

        sf = _get_source_files_overview()
        st.write(f"Source files tracked: **{sf.get('total')}** (fit={sf.get('fit')}, gpx={sf.get('gpx')}, csv={sf.get('csv')})")

    # Auto ingest status (Task Scheduler / manual runs)
    with st.expander("Auto ingest status (sync + ingest)", expanded=True):
        log_path = Path.cwd() / "logs" / "auto_ingest.log"
        colA, colB = st.columns([1, 1])

        def _read_log_tail(p: Path, max_lines: int = 80) -> str:
            if not p.exists():
                return ""
            try:
                with p.open("r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                return "".join(lines[-max_lines:])
            except Exception:
                return ""

        def _parse_state(txt: str) -> tuple[str, float]:
            # crude state machine based on log lines
            if not txt:
                return ("No log found yet.", 0.0)
            if "=== Auto ingest run starting ===" in txt and "=== Auto ingest run finished OK ===" not in txt:
                if "Sync complete:" not in txt:
                    return ("Running: syncing files…", 0.25)
                if "FIT ingest complete:" not in txt:
                    return ("Running: ingesting FIT…", 0.65)
                return ("Running…", 0.5)
            if "=== Auto ingest run finished OK ===" in txt:
                return ("Last run: OK", 1.0)
            if "failed" in txt.lower() or "aborting" in txt.lower():
                return ("Last run: ERROR (see log below)", 1.0)
            return ("Status unknown (see log below)", 0.0)

        tail = _read_log_tail(log_path)
        status_text, prog_val = _parse_state(tail)
        st.write(f"**Log file:** `{log_path}`")
        st.write(f"**Status:** {status_text}")
        st.progress(prog_val)

        with colA:
            if st.button("Run sync + ingest now (manual)", key="btn_manual_auto_ingest"):
                try:
                    hf_dir = get_healthfit_dir()
                    sync_count = sync_source_files(hf_dir)
                    st.success(f"Sync complete: {sync_count} new file(s) registered.")
                except Exception as e:
                    st.error(f"Sync failed: {e}")
                    st.code(traceback.format_exc())

        with colB:
            if st.button("Ingest new FIT workouts now (manual)", key="btn_manual_fit_ingest"):
                try:
                    inserted = ingest_new_fit_workouts()
                    st.success(f"FIT ingest complete: inserted {inserted} workout(s).")
                except Exception as e:
                    st.error(f"Ingest failed: {e}")
                    st.code(traceback.format_exc())

        st.text_area("Auto ingest log (tail)", value=tail, height=220)

    # Directory tree builder
    with st.expander("Project directory tree (for debugging / sharing)", expanded=False):
        root = Path.cwd()
        st.write("Build a trimmed directory tree, useful when you open a new chat.")
        if st.button("Build directory tree"):
            txt = _build_directory_tree_text(root)
            st.code(txt)

    # Backup
    with st.expander("Quick zip backup (no DB by default)", expanded=False):
        root = Path.cwd()
        out_dir = root / "archive_backups"
        out_dir.mkdir(exist_ok=True)
        include_db = st.checkbox("Include the SQLite DB (large)", value=False)

        if st.button("Create zip backup now"):
            ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
            out_zip = out_dir / f"trailops_project_backup_{ts}.zip"
            try:
                with st.spinner("Creating zip backup..."):
                    _zip_project(root, out_zip, include_db=include_db)
                st.success(f"Backup created: {out_zip}")
            except Exception as e:
                st.error(f"Backup failed: {e}")
                st.code(traceback.format_exc())

    # Simple tool runners (safe-ish)
    with st.expander("Tool runners", expanded=False):
        st.write("These run small checks inside the same Python process (no subprocess).")
        col_a, col_b = st.columns(2)

        with col_a:
            if st.button("Run enrichment sanity check", key="btn_sanity_enrichment"):
                try:
                    from app.tools.sanity_check_enrichment import main as sanity_main  # type: ignore
                    sanity_main()
                    st.success("sanity_check_enrichment finished (see console output).")
                except Exception as e:
                    st.error(f"sanity_check_enrichment failed: {e}")
                    st.code(traceback.format_exc())

        with col_b:
            if st.button("Run plot-sample debug for a workout id", key="btn_debug_plot"):
                st.info("Use the CLI tool for this one: python -m app.tools.debug_plot_samples <workout_id>")

    # Guidance
    with st.expander("Common failure modes (translated from Python into English)", expanded=False):
        st.markdown(
            """
- **`ModuleNotFoundError: app`**: Streamlit runs from `app/` by default; main_app adds a sys.path bootstrap.

- **Charts empty / NaNs**: pandas read used a non-tuple row_factory. pandas reads must use
  `get_pandas_connection()`.

- **Map empty**: map_points not backfilled or has_gps=0 for that workout.

- **Ingestion sees files but inserts 0**: source_files already tracked, or HealthFit folder path is wrong.
            """
        )

    st.markdown('</div>', unsafe_allow_html=True)
