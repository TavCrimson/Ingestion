"""Stage 1: Acquisition — validate the file and write a RawDocument record."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.storage.raw_store import raw_store
from ingestion.storage.file_hash import sha256_file


def run(path: Path, db: Session) -> str:
    """
    Saves file to raw store and creates a RawDocument row.
    Returns the raw_doc_id. Raises if the file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    stored_path, file_hash = raw_store.save(path)

    existing = crud.get_raw_doc_by_hash(db, file_hash)
    if existing:
        return existing.id

    doc = crud.create_raw_doc(
        db,
        id=None,
        filename=path.name,
        original_path=str(path),
        stored_path=str(stored_path),
        file_hash=file_hash,
        file_size_bytes=path.stat().st_size,
    )
    db.commit()
    return doc.id
