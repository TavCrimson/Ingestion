"""POST /v1/ingest — trigger the ingestion pipeline for an uploaded file."""
from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, UploadFile
from sqlalchemy.orm import Session

from ingestion.api.auth import verify_api_key
from ingestion.api.rate_limit import rate_limit
from ingestion.db.engine import get_db
from ingestion.pipeline.runner import ingest_file

router = APIRouter()


def _run_ingest(file_path: Path, db: Session) -> None:
    try:
        ingest_file(file_path, db)
    finally:
        try:
            file_path.unlink(missing_ok=True)
        except Exception:
            pass
        db.close()


@router.post("/ingest")
async def ingest(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    content = await file.read()

    # Write to a temp file so the pipeline can use path-based extractors
    suffix = Path(file.filename or "upload").suffix or ".bin"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(content)
    tmp.flush()
    tmp.close()

    tmp_path = Path(tmp.name)
    # Rename to original filename for better classification signals
    renamed = tmp_path.parent / (file.filename or tmp_path.name)
    tmp_path.rename(renamed)

    from ingestion.db.engine import SessionLocal
    bg_db = SessionLocal()

    background_tasks.add_task(_run_ingest, renamed, bg_db)

    return {
        "status": "queued",
        "filename": file.filename,
        "message": "Ingestion started in background",
    }
