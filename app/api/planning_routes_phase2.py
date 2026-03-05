
from fastapi import APIRouter,UploadFile,File
from app.planning.gpx_ingestor import parse_gpx
from app.planning.route_store import init_route_tables,store_route

router=APIRouter()

@router.on_event("startup")
def init_tables():
    init_route_tables()

@router.post("/planning/upload_and_store_gpx")
async def upload_and_store_gpx(file:UploadFile=File(...)):
    content=await file.read()
    data=parse_gpx(content)

    route_id=store_route(
        file.filename,
        data["stats"],
        data["points"]
    )

    return {
        "route_id":route_id,
        "stats":data["stats"]
    }
