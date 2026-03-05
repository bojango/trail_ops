
from fastapi import APIRouter, UploadFile, File
from app.planning.gpx_ingestor import parse_gpx

router = APIRouter()

@router.post("/planning/upload_gpx")
async def upload_gpx(file: UploadFile = File(...)):
    content = await file.read()
    data = parse_gpx(content)
    return data
