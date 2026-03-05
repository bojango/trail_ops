
from fastapi import APIRouter
from app.planning.route_queries import list_routes,get_route

router=APIRouter()

@router.get("/planning/routes")
def routes():
    return list_routes()

@router.get("/planning/route/{route_id}")
def route(route_id:int):
    return get_route(route_id)
