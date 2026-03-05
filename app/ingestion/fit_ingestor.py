from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import sqlite3
from fitparse import FitFile

from app.db.database import get_connection
from app.analysis.enrichment_pipeline import enrich_workout


@dataclass
class WorkoutSummary:
    """
    Session-level metrics for a workout, derived from the FIT 'session' message.
    """
    start_time_iso: Optional[str]
    end_time_iso: Optional[str]
    sport_type: str
    distance_m: float
    duration_s: float
    elevation_gain_m: float
    avg_heart_rate: Optional[float]
    max_heart_rate: Optional[float]

    # Extended metrics (may be None if the device/file doesn't provide them)
    avg_power_w: Optional[float] = None
    max_power_w: Optional[float] = None
    total_calories: Optional[float] = None
    avg_cadence_spm: Optional[float] = None
    max_cadence_spm: Optional[float] = None
    total_steps: Optional[float] = None
    avg_stride_length_m: Optional[float] = None


@dataclass
class SampleRecord:
    """
    Per-record / per-trackpoint metrics for workout_samples.
    """
    timestamp_utc: Optional[str]
    seconds_since_start: Optional[float]
    distance_m: Optional[float]
    elevation_m: Optional[float]
    speed_m_s: Optional[float]
    heart_rate_bpm: Optional[float]
    power_w: Optional[float]
    cadence_spm: Optional[float]
    stride_length_m: Optional[float]
    vertical_oscillation_m: Optional[float]
    ground_contact_time_ms: Optional[float]
    ground_contact_balance_pct: Optional[float]
    latitude_deg: Optional[float]
    longitude_deg: Optional[float]
    lap_index: Optional[int]


def _to_iso_utc(dt: Optional[datetime]) -> Optional[str]:
    """Convert a datetime to ISO 8601 in UTC, seconds precision."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Ensure a datetime is timezone-aware in UTC.
    Returns None if input is None.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _semicircles_to_degrees(value: Optional[float]) -> Optional[float]:
    """Convert FIT semicircles to degrees.

    FIT position_lat/position_long are typically stored as signed 32-bit 'semicircles'.
    Some exporters may already provide degrees. We detect semicircles by magnitude.
    """
    if value is None:
        return None
    # If it's already in degrees, it should be within [-180, 180]
    if abs(value) <= 180.0:
        return value
    return value * (180.0 / 2147483648.0)


def _parse_session_message(session) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for field in session:
        fields[field.name] = field.value
    return fields


def _extract_summary_from_session(session_fields: Dict[str, Any]) -> WorkoutSummary:
    start_time: Optional[datetime] = session_fields.get("start_time")
    sport = session_fields.get("sport") or "unknown"
    sub_sport = session_fields.get("sub_sport")

    # Total timer time (seconds) and distance (meters) are usually present.
    total_timer_time = (
        session_fields.get("total_timer_time")
        or session_fields.get("total_elapsed_time")
    )
    total_distance = session_fields.get("total_distance")

    # Elevation gain (meters). Different devices may use different field names.
    total_ascent = (
        session_fields.get("total_ascent")
        or session_fields.get("total_climb")
        or 0
    )

    avg_hr = session_fields.get("avg_heart_rate")
    max_hr = session_fields.get("max_heart_rate")

    # Extended metrics
    total_calories = session_fields.get("total_calories")

    # Cadence (running devices may use 'avg_running_cadence')
    avg_cadence = (
        session_fields.get("avg_running_cadence")
        or session_fields.get("avg_cadence")
    )
    max_cadence = (
        session_fields.get("max_running_cadence")
        or session_fields.get("max_cadence")
    )

    avg_power = session_fields.get("avg_power")
    max_power = session_fields.get("max_power")

    total_steps = (
        session_fields.get("total_strides")
        or session_fields.get("total_steps")
    )

    # Stride length is usually in meters at session level (if provided)
    avg_stride_length = session_fields.get("avg_stride_length")

    if start_time and total_timer_time:
        end_time = start_time + timedelta(seconds=float(total_timer_time))
    else:
        end_time = None

    # Build a slightly more descriptive sport_type if we have sub_sport.
    if sub_sport:
        sport_type = f"{sport}:{sub_sport}"
    else:
        sport_type = str(sport)

    distance_m = float(total_distance) if total_distance is not None else 0.0
    duration_s = float(total_timer_time) if total_timer_time is not None else 0.0
    elevation_gain_m = float(total_ascent) if total_ascent is not None else 0.0

    return WorkoutSummary(
        start_time_iso=_to_iso_utc(start_time),
        end_time_iso=_to_iso_utc(end_time),
        sport_type=sport_type,
        distance_m=distance_m,
        duration_s=duration_s,
        elevation_gain_m=elevation_gain_m,
        avg_heart_rate=float(avg_hr) if avg_hr is not None else None,
        max_heart_rate=float(max_hr) if max_hr is not None else None,
        avg_power_w=_safe_float(avg_power),
        max_power_w=_safe_float(max_power),
        total_calories=_safe_float(total_calories),
        avg_cadence_spm=_safe_float(avg_cadence),
        max_cadence_spm=_safe_float(max_cadence),
        total_steps=_safe_float(total_steps),
        avg_stride_length_m=_safe_float(avg_stride_length),
    )


def _extract_samples_from_fit(fit: FitFile, session_start: Optional[datetime]) -> List[SampleRecord]:
    """
    Extract per-record samples from the FIT file.
    """
    samples: List[SampleRecord] = []

    # Normalise session_start to UTC-aware if provided
    reference_start: Optional[datetime] = _ensure_utc(session_start) if session_start else None

    # First pass: if no session_start, find earliest record timestamp
    if reference_start is None:
        for record in fit.get_messages("record"):
            rec_fields = {f.name: f.value for f in record}
            ts = rec_fields.get("timestamp")
            if isinstance(ts, datetime):
                reference_start = _ensure_utc(ts)
                break

    for record in fit.get_messages("record"):
        rec_fields = {f.name: f.value for f in record}

        ts = rec_fields.get("timestamp")
        if isinstance(ts, datetime):
            ts_utc = _ensure_utc(ts)
            ts_iso = _to_iso_utc(ts_utc)
            if reference_start is not None:
                seconds_since_start = (ts_utc - reference_start).total_seconds()
            else:
                seconds_since_start = None
        else:
            ts_iso = None
            seconds_since_start = None

        distance_m = _safe_float(rec_fields.get("distance"))

        # Elevation: enhanced_altitude preferred, fallback to altitude
        elev = rec_fields.get("enhanced_altitude") or rec_fields.get("altitude")
        elevation_m = _safe_float(elev)

        # Speed: enhanced_speed preferred, fallback to speed (m/s)
        spd = rec_fields.get("enhanced_speed") or rec_fields.get("speed")
        speed_m_s = _safe_float(spd)

        hr = _safe_float(rec_fields.get("heart_rate"))

        power = _safe_float(rec_fields.get("power"))

        # Cadence: some files use generic 'cadence' for running as steps/min
        cadence = _safe_float(
            rec_fields.get("cadence") or rec_fields.get("running_cadence")
        )

        # Stride length: device dependent; often in mm or m.
        stride_raw = rec_fields.get("stride_length")
        stride_length_m: Optional[float]
        if stride_raw is None:
            stride_length_m = None
        else:
            try:
                val = float(stride_raw)
                # Heuristic: if > 10 it's probably in mm, convert to meters.
                stride_length_m = val / 1000.0 if val > 10 else val
            except Exception:
                stride_length_m = None

        # Running dynamics, if present
        vo_raw = rec_fields.get("vertical_oscillation")
        if vo_raw is not None:
            # vertical_oscillation is usually in mm; convert to meters
            vo_val = _safe_float(vo_raw)
            vertical_oscillation_m = vo_val / 1000.0 if vo_val and vo_val > 10 else vo_val
        else:
            vertical_oscillation_m = None

        gct_raw = rec_fields.get("stance_time") or rec_fields.get("stance_time_ms")
        ground_contact_time_ms = _safe_float(gct_raw)

        gcb_raw = rec_fields.get("stance_time_balance") or rec_fields.get(
            "stance_time_percent"
        )
        ground_contact_balance_pct = _safe_float(gcb_raw)

        # GPS position
        lat = _semicircles_to_degrees(_safe_float(rec_fields.get("position_lat")))
        lon = _semicircles_to_degrees(_safe_float(rec_fields.get("position_long")))

        # Lap index if provided
        lap_index_val = rec_fields.get("lap_index") or rec_fields.get("lap")
        try:
            lap_index = int(lap_index_val) if lap_index_val is not None else None
        except Exception:
            lap_index = None

        samples.append(
            SampleRecord(
                timestamp_utc=ts_iso,
                seconds_since_start=seconds_since_start,
                distance_m=distance_m,
                elevation_m=elevation_m,
                speed_m_s=_safe_float(speed_m_s),
                heart_rate_bpm=hr,
                power_w=power,
                cadence_spm=cadence,
                stride_length_m=stride_length_m,
                vertical_oscillation_m=vertical_oscillation_m,
                ground_contact_time_ms=ground_contact_time_ms,
                ground_contact_balance_pct=ground_contact_balance_pct,
                latitude_deg=lat,
                longitude_deg=lon,
                lap_index=lap_index,
            )
        )

    return samples


def extract_workout_from_fit(path: Path) -> Optional[Tuple[WorkoutSummary, List[SampleRecord]]]:
    """
    Parse a FIT file and extract:
      - a WorkoutSummary using the 'session' message
      - a list of SampleRecord from 'record' messages

    If parsing fails or no suitable session is found, return None.
    """
    try:
        # Pass a plain string path; let fitparse handle opening.
        fit = FitFile(str(path))

        sessions = list(fit.get_messages("session"))
        if not sessions:
            return None

        # Take the first session; HealthFit exports should generally have one main session.
        session = sessions[0]
        session_fields = _parse_session_message(session)
        summary = _extract_summary_from_session(session_fields)

        # Convert start_time back from ISO for reference if present
        if summary.start_time_iso:
            session_start = datetime.fromisoformat(summary.start_time_iso)
            session_start = _ensure_utc(session_start)
        else:
            session_start = None

        samples = _extract_samples_from_fit(fit, session_start)

        return summary, samples

    except Exception as e:
        # In a real setup we'd log this; for now we just print.
        print(f"[FIT PARSE ERROR] {path}: {e}")
        return None


def get_fit_files_needing_ingestion(
    conn: sqlite3.Connection, limit: int = 200
) -> list[tuple[int, str, Optional[int]]]:
    """
    Return a list of FIT files from source_files that either:

    - have no corresponding workouts row, OR
    - have a workouts row but zero samples in workout_samples.

    This supports backfilling old workouts (which previously had no samples)
    as well as ingesting new FIT files.

    Returns: list of (source_file_id, path, existing_workout_id_or_None)
    """
    query = """
        SELECT
            sf.id AS source_id,
            sf.path AS path,
            MIN(w.id) AS workout_id,
            COUNT(ws.id) AS sample_count
        FROM source_files AS sf
        LEFT JOIN workouts AS w
            ON w.source_fit_file_id = sf.id
        LEFT JOIN workout_samples AS ws
            ON ws.workout_id = w.id
        WHERE sf.file_type = 'fit'
        GROUP BY sf.id
        HAVING workout_id IS NULL OR sample_count = 0
        ORDER BY sf.id
        LIMIT ?;
    """
    cursor = conn.execute(query, (limit,))
    results: list[tuple[int, str, Optional[int]]] = []
    for row in cursor.fetchall():
        source_id = row[0]
        path_str = row[1]
        workout_id = row[2]
        results.append((source_id, path_str, workout_id))
    return results


def ingest_new_fit_workouts(max_files: int = 200) -> dict[str, int]:
    """
    Ingest FIT workouts into the database, including:

    - Session-level metrics into workouts
    - Per-record time-series metrics into workout_samples

    This function will:
    - Find FIT files that either have no workouts row, or have workouts but
      zero samples (legacy ingestions).
    - For each such file, parse the FIT, write/update the workouts row, and
      (re)create all associated workout_samples rows.

    Returns:
        {
            "candidates": <int>,           # FIT files considered for ingestion
            "inserted": <int>,             # workouts created
            "updated": <int>,              # workouts updated (had summary but no samples)
            "skipped_missing": <int>,      # files whose paths no longer exist
            "skipped_parse_error": <int>,  # files that failed to parse
            "samples_written": <int>,      # total sample rows written
        }
    """
    conn = get_connection()

    summary: dict[str, int] = {
        "candidates": 0,
        "inserted": 0,
        "updated": 0,
        "skipped_missing": 0,
        "skipped_parse_error": 0,
        "samples_written": 0,
    }

    try:
        candidates = get_fit_files_needing_ingestion(conn, limit=max_files)
        summary["candidates"] = len(candidates)

        if not candidates:
            return summary

        for source_id, path_str, existing_workout_id in candidates:
            path = Path(path_str)
            if not path.exists():
                summary["skipped_missing"] += 1
                continue

            result = extract_workout_from_fit(path)
            if result is None:
                summary["skipped_parse_error"] += 1
                continue

            ws, samples = result

            with conn:
                # Insert or update the workouts row
                if existing_workout_id is None:
                    # New workout
                    cursor = conn.execute(
                        """
                        INSERT INTO workouts (
                            source_fit_file_id,
                            start_time,
                            end_time,
                            sport_type,
                            distance_m,
                            duration_s,
                            elevation_gain_m,
                            avg_heart_rate,
                            max_heart_rate,
                            avg_power_w,
                            max_power_w,
                            total_calories,
                            avg_cadence_spm,
                            max_cadence_spm,
                            total_steps,
                            avg_stride_length_m
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            source_id,
                            ws.start_time_iso,
                            ws.end_time_iso,
                            ws.sport_type,
                            ws.distance_m,
                            ws.duration_s,
                            ws.elevation_gain_m,
                            ws.avg_heart_rate,
                            ws.max_heart_rate,
                            ws.avg_power_w,
                            ws.max_power_w,
                            ws.total_calories,
                            ws.avg_cadence_spm,
                            ws.max_cadence_spm,
                            ws.total_steps,
                            ws.avg_stride_length_m,
                        ),
                    )
                    workout_id = cursor.lastrowid
                    summary["inserted"] += 1
                else:
                    # Update existing workout (which previously had no samples)
                    workout_id = existing_workout_id
                    conn.execute(
                        """
                        UPDATE workouts
                        SET
                            start_time = ?,
                            end_time = ?,
                            sport_type = ?,
                            distance_m = ?,
                            duration_s = ?,
                            elevation_gain_m = ?,
                            avg_heart_rate = ?,
                            max_heart_rate = ?,
                            avg_power_w = ?,
                            max_power_w = ?,
                            total_calories = ?,
                            avg_cadence_spm = ?,
                            max_cadence_spm = ?,
                            total_steps = ?,
                            avg_stride_length_m = ?
                        WHERE id = ?;
                        """,
                        (
                            ws.start_time_iso,
                            ws.end_time_iso,
                            ws.sport_type,
                            ws.distance_m,
                            ws.duration_s,
                            ws.elevation_gain_m,
                            ws.avg_heart_rate,
                            ws.max_heart_rate,
                            ws.avg_power_w,
                            ws.max_power_w,
                            ws.total_calories,
                            ws.avg_cadence_spm,
                            ws.max_cadence_spm,
                            ws.total_steps,
                            ws.avg_stride_length_m,
                            workout_id,
                        ),
                    )
                    summary["updated"] += 1

                # (Re)write samples: clear any existing and insert fresh
                conn.execute(
                    "DELETE FROM workout_samples WHERE workout_id = ?;",
                    (workout_id,),
                )

                sample_rows = [
                    (
                        workout_id,
                        s.timestamp_utc,
                        s.seconds_since_start,
                        s.distance_m,
                        s.elevation_m,
                        s.speed_m_s,
                        s.heart_rate_bpm,
                        s.power_w,
                        s.cadence_spm,
                        s.stride_length_m,
                        s.vertical_oscillation_m,
                        s.ground_contact_time_ms,
                        s.ground_contact_balance_pct,
                        s.latitude_deg,
                        s.longitude_deg,
                        s.lap_index,
                    )
                    for s in samples
                ]

                if sample_rows:
                    conn.executemany(
                        """
                        INSERT INTO workout_samples (
                            workout_id,
                            timestamp_utc,
                            seconds_since_start,
                            distance_m,
                            elevation_m,
                            speed_m_s,
                            heart_rate_bpm,
                            power_w,
                            cadence_spm,
                            stride_length_m,
                            vertical_oscillation_m,
                            ground_contact_time_ms,
                            ground_contact_balance_pct,
                            latitude_deg,
                            longitude_deg,
                            lap_index
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        sample_rows,
                    )
                    summary["samples_written"] += len(sample_rows)


                # Enrich newly ingested GPS workouts with:
                # - simplified map points + route hash
                # - downsampled plot samples
                # - hierarchical route context (location)
                # - surface stats (v2)
                # - weather (start + high point)
                # - peak hits
                #
                # NOTE: The enrichment helpers open their own SQLite connections.
                # We therefore run them *after* the ingestion transaction has committed.
            # end with conn

            # Run enrichments (best-effort per workout; failures should not kill ingestion).
            # This is intentionally idempotent and safe to re-run.
            try:
                enrich_workout(int(workout_id))
            except Exception:
                # Defensive: pipeline is already best-effort, but ingestion must never die here.
                pass
    finally:
        conn.close()

    return summary


if __name__ == "__main__":
    # Manual test helper: ingest a batch of FIT files that either:
    # - have no workouts row, or
    # - have workouts but zero samples (legacy runs).
    result = ingest_new_fit_workouts(max_files=500)
    print("FIT ingestion summary:", result)
