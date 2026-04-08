"""Stage 2: Format detection — determine MIME type."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from ingestion.db import crud


_EXT_MIME: dict[str, str] = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".html": "text/html",
    ".htm": "text/html",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".json": "application/json",
    ".txt": "text/plain",
    ".csv": "text/csv",
}


def run(raw_doc_id: str, db: Session) -> str:
    """Detect MIME type and update the RawDocument. Returns the detected MIME type."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    path = Path(raw_doc.stored_path)

    mime_type = _detect(path)
    raw_doc.mime_type = mime_type
    db.commit()
    return mime_type


def _detect(path: Path) -> str:
    # Try python-magic first (most reliable)
    try:
        import magic
        return magic.from_file(str(path), mime=True)
    except Exception:
        pass
    # Fall back to extension mapping
    ext = path.suffix.lower()
    return _EXT_MIME.get(ext, "application/octet-stream")
