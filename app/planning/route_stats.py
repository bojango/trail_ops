
import math

def haversine(a,b,c,d):
    R = 6371000
    phi1 = math.radians(a)
    phi2 = math.radians(c)
    dphi = math.radians(c-a)
    dlambda = math.radians(d-b)
    h = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.atan2(math.sqrt(h), math.sqrt(1-h))

def compute_route_stats(points):
    if len(points) < 2:
        return {}
    dist=0
    ascent=0
    descent=0
    min_e=min(p["elev"] for p in points if p["elev"] is not None)
    max_e=max(p["elev"] for p in points if p["elev"] is not None)
    for i in range(1,len(points)):
        p1=points[i-1]
        p2=points[i]
        dist += haversine(p1["lat"],p1["lon"],p2["lat"],p2["lon"])
        if p1["elev"] and p2["elev"]:
            d=p2["elev"]-p1["elev"]
            if d>0: ascent+=d
            else: descent+=abs(d)
    return {
        "distance_m": dist,
        "ascent_m": ascent,
        "descent_m": descent,
        "min_elev": min_e,
        "max_elev": max_e
    }
