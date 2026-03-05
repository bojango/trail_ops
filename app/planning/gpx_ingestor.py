
import gpxpy
from .route_stats import compute_route_stats

def parse_gpx(file_bytes):
    gpx = gpxpy.parse(file_bytes)
    points = []
    for track in gpx.tracks:
        for segment in track.segments:
            for p in segment.points:
                points.append({
                    "lat": p.latitude,
                    "lon": p.longitude,
                    "elev": p.elevation
                })
    stats = compute_route_stats(points)
    return {"points": points, "stats": stats}
