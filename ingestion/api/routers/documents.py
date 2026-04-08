"""GET /v1/documents — browse canonical documents and their chunks."""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ingestion.api.rate_limit import rate_limit
from ingestion.db import crud
from ingestion.db.engine import get_db
from ingestion.db.models import CanonicalDocument, Chunk

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/documents")
def list_documents(
    status: str | None = None,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    q = db.query(CanonicalDocument)
    if status:
        q = q.filter(CanonicalDocument.status == status)
    docs = q.order_by(CanonicalDocument.created_at.desc()).all()

    result = []
    for doc in docs:
        chunks = db.query(Chunk).filter(Chunk.canonical_doc_id == doc.id).all()
        published_chunks = sum(1 for c in chunks if c.passed_review)
        result.append({
            "id": doc.id,
            "title": doc.title,
            "content_type": doc.content_type,
            "authority_level": doc.authority_level,
            "status": doc.status,
            "version": doc.version,
            "total_chunks": len(chunks),
            "published_chunks": published_chunks,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "published_at": doc.published_at.isoformat() if doc.published_at else None,
        })
    return result


@router.get("/documents/{doc_id}")
def get_document(
    doc_id: str,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    doc = crud.get_canonical(db, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    chunks = crud.get_chunks_for_doc(db, doc_id)
    raw = crud.get_raw_doc(db, doc.raw_doc_id) if doc.raw_doc_id else None
    entities = crud.get_entities_for_doc(db, doc_id)

    return {
        "id": doc.id,
        "title": doc.title,
        "content_type": doc.content_type,
        "authority_level": doc.authority_level,
        "status": doc.status,
        "version": doc.version,
        "source_url": doc.source_url,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
        "published_at": doc.published_at.isoformat() if doc.published_at else None,
        "original_filename": raw.filename if raw else None,
        "entities": [
            {"id": e.id, "name": e.name, "entity_type": e.entity_type}
            for e in entities
        ],
        "chunks": [
            {
                "id": c.id,
                "chunk_index": c.chunk_index,
                "heading_context": c.heading_context,
                "text": c.text,
                "confidence_score": c.confidence_score,
                "passed_review": c.passed_review,
            }
            for c in chunks
        ],
    }


@router.get("/documents/{doc_id}/file")
def download_document(
    doc_id: str,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    """Stream the original uploaded file back to the browser."""
    doc = crud.get_canonical(db, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    raw = crud.get_raw_doc(db, doc.raw_doc_id) if doc.raw_doc_id else None
    if not raw or not raw.stored_path:
        raise HTTPException(status_code=404, detail="Original file not available")

    file_path = Path(raw.stored_path)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(
        path=str(file_path),
        filename=raw.filename,
        media_type=raw.mime_type or "application/octet-stream",
    )


@router.delete("/documents/{doc_id}")
def delete_document(
    doc_id: str,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    doc = crud.get_canonical(db, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    title = doc.title  # save before deletion

    # Remove embeddings from ChromaDB first (while chunk IDs are still queryable)
    try:
        from ingestion.storage.vector_store import VectorStore
        vs = VectorStore()
        for chunk in crud.get_chunks_for_doc(db, doc_id):
            try:
                vs.delete(chunk.id)
            except Exception:
                pass  # chunk may not have been indexed yet
    except Exception as exc:
        logger.warning("Could not clean up vector store during deletion: %s", exc)

    # Delete all DB records (handles FTS cleanup via trigger)
    meta = crud.delete_document(db, doc_id)
    db.commit()

    # Remove the original uploaded file from disk if it was stored
    if meta and meta.get("stored_path"):
        try:
            Path(meta["stored_path"]).unlink(missing_ok=True)
            logger.info("Deleted raw file: %s", meta["stored_path"])
        except Exception as exc:
            logger.warning("Could not delete raw file %s: %s", meta["stored_path"], exc)

    return {"deleted": doc_id, "title": title}
