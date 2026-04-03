from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

router = APIRouter()

FRONTEND_DIST_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

def mount_frontend_assets(app: FastAPI) -> None:
    if (FRONTEND_DIST_DIR / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=FRONTEND_DIST_DIR / "assets"), name="frontend-assets")


def _frontend_file_response(relative_path: str) -> FileResponse:
    return FileResponse(FRONTEND_DIST_DIR / relative_path, media_type="text/html; charset=utf-8")


@router.get("/")
def serve_frontend_index():
    if not (FRONTEND_DIST_DIR / "index.html").is_file():
        raise HTTPException(status_code=404, detail="Frontend dist not found")
    return _frontend_file_response("index.html")


@router.get("/{full_path:path}")
def serve_frontend_spa(full_path: str):
    normalized_path = (full_path or "").strip().lstrip("/")
    if not normalized_path:
        return serve_frontend_index()
    if normalized_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")

    candidate = (FRONTEND_DIST_DIR / normalized_path).resolve()
    if FRONTEND_DIST_DIR.exists() and candidate.is_file() and FRONTEND_DIST_DIR in candidate.parents:
        return FileResponse(candidate)

    if not (FRONTEND_DIST_DIR / "index.html").is_file():
        raise HTTPException(status_code=404, detail="Frontend dist not found")
    return _frontend_file_response("index.html")
