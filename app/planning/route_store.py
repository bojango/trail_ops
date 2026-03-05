
from app.db.database import get_connection

def init_route_tables():
    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS routes (
        route_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        distance_m REAL,
        ascent_m REAL,
        descent_m REAL,
        min_elev REAL,
        max_elev REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS route_points (
        point_id INTEGER PRIMARY KEY AUTOINCREMENT,
        route_id INTEGER,
        lat REAL,
        lon REAL,
        elev REAL,
        seq INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS route_plans (
        plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
        route_id INTEGER,
        planned_date TEXT,
        tags TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()


def store_route(name,stats,points):
    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
    INSERT INTO routes (name,distance_m,ascent_m,descent_m,min_elev,max_elev)
    VALUES (?,?,?,?,?,?)
    """,(
        name,
        stats.get("distance_m"),
        stats.get("ascent_m"),
        stats.get("descent_m"),
        stats.get("min_elev"),
        stats.get("max_elev")
    ))

    route_id=cur.lastrowid

    for i,p in enumerate(points):
        cur.execute("""
        INSERT INTO route_points (route_id,lat,lon,elev,seq)
        VALUES (?,?,?,?,?)
        """,(route_id,p["lat"],p["lon"],p["elev"],i))

    conn.commit()
    conn.close()

    return route_id
