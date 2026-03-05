from __future__ import annotations

import argparse
import csv
import math
import os
import sqlite3
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple


# Map DoBIH classification codes -> TrailOps class keys (UI dropdown keys)
CODE_TO_CLASS_KEY = {
    "W": "wainwrights",
    "D": "dodds",
    "B": "birketts",
    "Hew": "hewitts",
    "N": "nuttalls",
    "Nu": "nuttalls",
    "M": "munros",
    "C": "corbetts",
    "G": "grahams",
    "Ma": "marilyns",
}


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Earth radius (m)
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


def parse_codes(class_str: str) -> List[str]:
    if not class_str:
        return []
    parts = [p.strip() for p in class_str.replace("|", ",").replace(";", ",").split(",")]
    return [p for p in parts if p]


def pick_latlon_cols(conn: sqlite3.Connection) -> Tuple[str, str]:
    cols = [r[1].lower() for r in conn.execute("PRAGMA table_info(peaks)").fetchall()]
    # Common possibilities
    lat_candidates = ["lat", "latitude"]
    lon_candidates = ["lon", "lng", "longitude"]
    lat_col = next((c for c in lat_candidates if c in cols), None)
    lon_col = next((c for c in lon_candidates if c in cols), None)
    if not lat_col or not lon_col:
        raise RuntimeError(f"Could not find lat/lon columns on peaks table. Found: {cols}")
    return lat_col, lon_col


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        '''
        DROP TABLE IF EXISTS dobih_hills;
        DROP TABLE IF EXISTS peak_dobih_match;
        DROP TABLE IF EXISTS peak_classifications;

        CREATE TABLE dobih_hills (
            number INTEGER PRIMARY KEY,
            name TEXT,
            latitude REAL,
            longitude REAL,
            classification TEXT
        );

        CREATE TABLE peak_dobih_match (
            peak_osm_id INTEGER PRIMARY KEY,
            dobih_number INTEGER,
            distance_m REAL,
            FOREIGN KEY(dobih_number) REFERENCES dobih_hills(number)
        );

        CREATE TABLE peak_classifications (
            peak_osm_id INTEGER NOT NULL,
            class_key TEXT NOT NULL,
            dobih_number INTEGER,
            PRIMARY KEY (peak_osm_id, class_key),
            FOREIGN KEY(dobih_number) REFERENCES dobih_hills(number)
        );

        CREATE INDEX IF NOT EXISTS idx_peak_classifications_class ON peak_classifications(class_key);
        CREATE INDEX IF NOT EXISTS idx_peak_classifications_peak ON peak_classifications(peak_osm_id);
        '''
    )
    conn.commit()


def import_dobih_csv(conn: sqlite3.Connection, csv_path: str) -> int:
    inserted = 0
    cur = conn.cursor()
    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                num = int(row.get("Number") or 0)
            except Exception:
                continue
            name = (row.get("Name") or "").strip()
            cls = (row.get("Classification") or "").strip()
            try:
                lat = float(row.get("Latitude") or "")
                lon = float(row.get("Longitude") or "")
            except Exception:
                continue
            if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                continue
            cur.execute(
                "INSERT INTO dobih_hills(number, name, latitude, longitude, classification) VALUES(?,?,?,?,?)",
                (num, name, lat, lon, cls),
            )
            inserted += 1
    conn.commit()
    return inserted


def build_bucket_index(hills: List[Tuple[int, float, float]]) -> Dict[Tuple[int, int], List[Tuple[int, float, float]]]:
    # Bucket size 0.02 degrees (~2.2km N/S). We'll search neighboring buckets.
    scale = 50.0  # 1/0.02
    buckets: Dict[Tuple[int, int], List[Tuple[int, float, float]]] = defaultdict(list)
    for num, lat, lon in hills:
        bx = int(lat * scale)
        by = int(lon * scale)
        buckets[(bx, by)].append((num, lat, lon))
    return buckets


def iter_neighbor_buckets(bx: int, by: int) -> Iterable[Tuple[int, int]]:
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            yield (bx + dx, by + dy)


def match_peaks(
    conn: sqlite3.Connection,
    threshold_m: float = 250.0,
) -> int:
    lat_col, lon_col = pick_latlon_cols(conn)

    hills = conn.execute("SELECT number, latitude, longitude FROM dobih_hills").fetchall()
    hills_list = [(int(r[0]), float(r[1]), float(r[2])) for r in hills]
    buckets = build_bucket_index(hills_list)
    scale = 50.0

    peaks = conn.execute(f"SELECT peak_osm_id, {lat_col}, {lon_col} FROM peaks").fetchall()

    cur = conn.cursor()
    matched = 0
    for pid, plat, plon in peaks:
        try:
            pid_i = int(pid)
            plat_f = float(plat)
            plon_f = float(plon)
        except Exception:
            continue

        bx = int(plat_f * scale)
        by = int(plon_f * scale)

        best = None  # (dist, num)
        for nb in iter_neighbor_buckets(bx, by):
            for num, hlat, hlon in buckets.get(nb, []):
                d = haversine_m(plat_f, plon_f, hlat, hlon)
                if best is None or d < best[0]:
                    best = (d, num)

        if best and best[0] <= threshold_m:
            cur.execute(
                "INSERT OR REPLACE INTO peak_dobih_match(peak_osm_id, dobih_number, distance_m) VALUES(?,?,?)",
                (pid_i, int(best[1]), float(best[0])),
            )
            matched += 1

    conn.commit()
    return matched


def build_peak_classifications(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    rows = conn.execute(
        '''
        SELECT m.peak_osm_id, h.number, h.classification
        FROM peak_dobih_match m
        JOIN dobih_hills h ON h.number = m.dobih_number
        '''
    ).fetchall()

    inserted = 0
    for pid, num, cls in rows:
        codes = parse_codes(cls or "")
        class_keys = []
        for c in codes:
            if c in CODE_TO_CLASS_KEY:
                class_keys.append(CODE_TO_CLASS_KEY[c])
        # dedupe
        class_keys = sorted(set(class_keys))
        for k in class_keys:
            cur.execute(
                "INSERT OR IGNORE INTO peak_classifications(peak_osm_id, class_key, dobih_number) VALUES(?,?,?)",
                (int(pid), k, int(num)),
            )
            inserted += 1
    conn.commit()
    return inserted


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to trailops.db")
    ap.add_argument("--csv", required=True, help="Path to DoBIH CSV (e.g., DoBIH_v18_4.csv)")
    ap.add_argument("--threshold", type=float, default=250.0, help="Match threshold in meters (default 250)")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    if not os.path.exists(args.csv):
        raise SystemExit(f"CSV not found: {args.csv}")

    conn = sqlite3.connect(args.db)
    try:
        create_tables(conn)
        n = import_dobih_csv(conn, args.csv)
        print(f"Imported DoBIH hills: {n}")
        m = match_peaks(conn, threshold_m=float(args.threshold))
        print(f"Matched peaks -> DoBIH hills: {m}")
        c = build_peak_classifications(conn)
        print(f"Inserted peak_classifications rows: {c}")
        # Quick summary
        summary = conn.execute(
            "SELECT class_key, COUNT(DISTINCT peak_osm_id) FROM peak_classifications GROUP BY class_key ORDER BY 2 DESC"
        ).fetchall()
        print("Class counts (matched peaks):")
        for k, cnt in summary:
            print(f"  {k}: {cnt}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
