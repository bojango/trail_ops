
from app.db.database import get_connection

def list_routes():
    conn=get_connection()
    cur=conn.cursor()

    rows=cur.execute("""
    SELECT route_id,name,distance_m,ascent_m
    FROM routes
    ORDER BY created_at DESC
    """).fetchall()

    conn.close()

    return [dict(r) for r in rows]


def get_route(route_id):
    conn=get_connection()
    cur=conn.cursor()

    route=cur.execute("""
    SELECT *
    FROM routes
    WHERE route_id=?
    """,(route_id,)).fetchone()

    points=cur.execute("""
    SELECT lat,lon,elev,seq
    FROM route_points
    WHERE route_id=?
    ORDER BY seq
    """,(route_id,)).fetchall()

    conn.close()

    return {
        "route":dict(route),
        "points":[dict(p) for p in points]
    }
