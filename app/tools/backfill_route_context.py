"""
TrailOps - Route Context Backfill (with progress/ETA + failure reporting)

Run as a module from the repo root:
    python -m app.tools.backfill_route_context --limit 50 --gps-only --force --sleep 1

This tool is intentionally "dumb but robust":
- Keeps going on per-workout failures (best-effort enrichment).
- Writes a CSV report including all errors so you can retry failures only.
- Prints X/X, percent, elapsed, ETA.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _fmt_td(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    return str(timedelta(seconds=int(seconds))).rjust(8, "0")


def _progress_bar(pct: float, width: int = 20) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(round((pct / 100.0) * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _coerce_peak_count(res: Any) -> Optional[int]:
    """compute_and_store_peak_hits() may return int, list, etc. Normalize to a count."""
    if res is None:
        return None
    if isinstance(res, bool):
        return int(res)
    if isinstance(res, int):
        return res
    try:
        return len(res)  # type: ignore[arg-type]
    except Exception:
        return None


def _get_location_label(conn, workout_id: int) -> Optional[str]:
    """
    Read a human-friendly label from workout_route_context.
    Prefer start > center > generic label.
    """
    row = conn.execute(
        """
        SELECT
          COALESCE(start_location_label, center_location_label, location_label) AS label
        FROM workout_route_context
        WHERE workout_id = ?
        """,
        (workout_id,),
    ).fetchone()
    if not row:
        return None
    # sqlite3.Row doesn't support .get()
    try:
        return row["label"]
    except Exception:
        # fallback if row_factory isn't sqlite3.Row
        return row[0] if len(row) else None


def _select_workouts(conn, limit: int, gps_only: bool, prefixes: Sequence[str]) -> List[Tuple[int, str, str]]:
    """
    Returns list of tuples: (workout_id, start_time, sport_type)
    """
    where = []
    params: List[Any] = []

    if gps_only:
        # GPS-only means: workouts that actually have map points in workout_map_points.
        # This avoids indoor workouts and any mismatches where workouts.has_gps is wrong.
        where.append(
            "EXISTS (SELECT 1 FROM workout_map_points mp WHERE mp.workout_id = workouts.id LIMIT 1)"
        )

    if prefixes:
        # sport_type like "running:..." etc
        likes = []
        for p in prefixes:
            likes.append("sport_type LIKE ?")
            params.append(f"{p}:%")
        where.append("(" + " OR ".join(likes) + ")")

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT id, start_time, sport_type
        FROM workouts
        {where_sql}
        ORDER BY start_time DESC
        LIMIT ?
    """
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    out: List[Tuple[int, str, str]] = []
    for r in rows:
        try:
            out.append((int(r["id"]), str(r["start_time"]), str(r["sport_type"])))
        except Exception:
            out.append((int(r[0]), str(r[1]), str(r[2])))
    return out


def _load_ids_from_report(path: Path, failures_only: bool) -> List[int]:
    ids: List[int] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wid = int(row["workout_id"])
            if not failures_only:
                ids.append(wid)
                continue
            # anything with non-empty *_error or status == FAIL counts as failure
            if (row.get("status") or "").upper() == "FAIL":
                ids.append(wid)
                continue
            for k, v in row.items():
                if k.endswith("_error") and (v or "").strip():
                    ids.append(wid)
                    break
    # keep order but de-dupe
    seen = set()
    ordered: List[int] = []
    for wid in ids:
        if wid not in seen:
            ordered.append(wid)
            seen.add(wid)
    return ordered


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--gps-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--peaks-only", action="store_true", help="Only compute peaks (skip route context, surface, weather)")
    parser.add_argument("--surface-only", action="store_true", help="Only compute surface stats (skip route context, peaks, weather)")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between workouts")
    parser.add_argument("--prefixes", nargs="*", default=["running", "walking", "hiking"])
    parser.add_argument("--report", type=str, default="route_context_backfill_report.csv")

    parser.add_argument("--from-report", type=str, default=None, help="Retry ids from a prior CSV report")
    parser.add_argument("--all-from-report", action="store_true", help="When using --from-report, rerun all rows, not just failures")

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.peaks_only and getattr(args, 'surface_only', False):
        parser.error("--peaks-only and --surface-only are mutually exclusive")

    # Import here so running module resolves app package correctly
    from app.db.database import get_connection, init_db  # noqa
    from app.analysis.route_context import (  # noqa
        compute_and_store_route_context,
        compute_and_store_surface_stats,
        compute_and_store_peak_hits,
        compute_and_store_weather,
    )

    init_db()

    report_path = Path(args.report).resolve()
    # Ensure report isn't locked (e.g., open in Excel)
    try:
        if report_path.exists():
            report_path.unlink()
    except PermissionError:
        # If locked, write to a timestamped alternative in cwd
        ts = time.strftime("%Y%m%d_%H%M%S")
        report_path = Path(f"route_context_backfill_report_{ts}.csv").resolve()

    with get_connection() as conn:
        if args.from_report:
            ids = _load_ids_from_report(Path(args.from_report), failures_only=not bool(args.all_from_report))
            # fetch metadata for display
            qmarks = ",".join(["?"] * len(ids)) if ids else "NULL"
            rows = conn.execute(
                f"SELECT id, start_time, sport_type FROM workouts WHERE id IN ({qmarks}) ORDER BY start_time DESC",
                ids,
            ).fetchall() if ids else []
            selected = []
            for r in rows:
                try:
                    selected.append((int(r["id"]), str(r["start_time"]), str(r["sport_type"])))
                except Exception:
                    selected.append((int(r[0]), str(r[1]), str(r[2])))
        else:
            selected = _select_workouts(conn, args.limit, bool(args.gps_only), args.prefixes)

    if not selected:
        print("No workouts selected.")
        return

    print(f"Selected workouts ({len(selected)}):")
    for wid, st, sp in selected:
        print(f"  - workout_id={wid} | {st} | {sp}")

    fieldnames = [
        "workout_id",
        "start_time",
        "sport_type",
        "status",
        "location",
        "surface",
        "peaks",
        "weather",
        "location_error",
        "surface_error",
        "peaks_error",
        "weather_error",
    ]

    t0 = time.time()
    per_item_times: List[float] = []

    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, (wid, start_time, sport_type) in enumerate(selected, start=1):
            status = "OK"
            location = None
            location_error = ""
            surface_ok = ""
            surface_error = ""
            peaks_val: Any = ""
            peaks_error = ""
            weather_ok = ""
            weather_error = ""

            item_t0 = time.time()

            peaks_only = bool(args.peaks_only)
            surface_only = bool(getattr(args, 'surface_only', False))
            if peaks_only:
                try:
                    with get_connection() as conn:
                        location = _get_location_label(conn, wid)
                except Exception:
                    location = None

            # Route context (location)
            if (not peaks_only) and (not surface_only):
                try:
                    rc = compute_and_store_route_context(wid, force=bool(args.force))
                    if rc is None:
                        status = "FAIL"
                        location_error = "no_route_points (missing workout_map_points or invalid lat/lon)"
                    else:
                        with get_connection() as conn:
                            location = _get_location_label(conn, wid)
                        if not location:
                            status = "FAIL"
                            location_error = "route_context_written_but_label_empty"
                except Exception as e:
                    status = "FAIL"
                    location_error = str(e)

            # Surface stats
            if not peaks_only:
                try:
                    ss = compute_and_store_surface_stats(wid, force=bool(args.force))
                    if ss is None:
                        status = "FAIL"
                        surface_error = "no_route_points (missing workout_map_points or invalid lat/lon)"
                    else:
                        surface_ok = "ok"
                except Exception as e:
                    status = "FAIL"
                    surface_error = str(e)

            # Peaks
            if surface_only:
                peaks_val = "skip"
            else:
                try:
                    res = compute_and_store_peak_hits(wid, force=bool(args.force) or peaks_only)
                    if res is None:
                        status = "FAIL"
                        peaks_error = "no_route_points (missing workout_map_points or invalid lat/lon)"
                        peaks_val = "skip"
                    else:
                        pc = _coerce_peak_count(res)
                        peaks_val = pc if pc is not None else 0
                except Exception as e:
                    status = "FAIL"
                    peaks_error = str(e)
                    peaks_val = "skip"

            # Weather

            if (not peaks_only) and (not surface_only):
                try:
                    w = compute_and_store_weather(wid, force=bool(args.force))
                    if not w:
                        status = "FAIL"
                        weather_error = "no_route_points (missing workout_map_points or invalid lat/lon)"
                    else:
                        weather_ok = "ok"
                except Exception as e:
                    status = "FAIL"
                    weather_error = str(e)

            # In peaks-only mode, we still want a location string for logging (read existing route_context if present)
            if peaks_only and not location:
                try:
                    with get_connection() as conn:
                        location = _get_location_label(conn, wid)
                except Exception:
                    location = location  # keep as-is

            # Write report row (always, including peaks-only)
            writer.writerow(
                {
                    "workout_id": wid,
                    "start_time": start_time,
                    "sport_type": sport_type,
                    "status": status,
                    "location": location or "",
                    "surface": surface_ok or ("skip" if surface_error else ""),
                    "peaks": peaks_val if peaks_val != "" else "",
                    "weather": weather_ok or ("skip" if weather_error else ""),
                    "location_error": location_error,
                    "surface_error": surface_error,
                    "peaks_error": peaks_error,
                    "weather_error": weather_error,
                }
            )
            f.flush()

            # Console output (always, including peaks-only)
            loc_str = location if location else "None"
            surf_str = surface_ok if surface_ok else ("skip" if surface_error else "")
            w_str = weather_ok if weather_ok else ("skip" if weather_error else "")
            p_str = str(peaks_val) if peaks_val != "" else ""
            if status == "OK":
                print(f"[OK] workout_id={wid} | {start_time} | {loc_str} | surface={surf_str} | peaks={p_str} | weather={w_str}")
            else:
                print(f"[FAIL] workout_id={wid} | {start_time} | location={loc_str} | surface={surf_str} | peaks={p_str} | weather={w_str}")
                if location_error:
                    print(f"       location_error: {location_error}")
                if surface_error:
                    print(f"       surface_error: {surface_error}")
                if peaks_error:
                    print(f"       peaks_error: {peaks_error}")
                if weather_error:
                    print(f"       weather_error: {weather_error}")

            # Progress/ETA
            item_dt = time.time() - item_t0
            per_item_times.append(item_dt)
            elapsed = time.time() - t0
            avg = sum(per_item_times) / len(per_item_times)
            remaining = max(0, len(selected) - i)
            eta = remaining * avg
            pct = (i / len(selected)) * 100.0
            print(f"{_progress_bar(pct)} {pct:5.1f}% | {i}/{len(selected)} | elapsed {_fmt_td(elapsed)} | eta {_fmt_td(eta)}")

            if args.sleep and i != len(selected):
                time.sleep(max(0.0, float(args.sleep)))

    print(f"\nWrote report: {report_path}")


if __name__ == "__main__":
    main()
