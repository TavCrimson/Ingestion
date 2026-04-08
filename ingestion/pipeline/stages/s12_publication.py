"""Stage 12: Publication — index approved chunks into vector store."""
from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.embeddings.encoder import Encoder
from ingestion.storage.vector_store import vector_store

logger = logging.getLogger(__name__)


def publish_chunk(chunk_id: str, db: Session) -> None:
    """Embed and index a single approved chunk."""
    chunk = crud.get_chunk(db, chunk_id)
    if chunk is None or not chunk.passed_review:
        return

    canonical = crud.get_canonical(db, chunk.canonical_doc_id)
    embedding = Encoder.get().encode_one(chunk.text)

    metadata = {
        "canonical_doc_id": chunk.canonical_doc_id,
        "content_type": canonical.content_type or "general",
        "authority_level": chunk.authority_level,
        "confidence_score": chunk.confidence_score,
        "heading_context": chunk.heading_context or "",
        "chunk_index": chunk.chunk_index,
    }

    vector_store.upsert(
        chunk_id=chunk.id,
        text=chunk.text,
        embedding=embedding,
        metadata=metadata,
    )

    # Record in index layer
    from ingestion.db.models import IndexEntry
    existing = db.query(IndexEntry).filter(IndexEntry.chunk_id == chunk.id).first()
    if existing is None:
        from ingestion.config import settings
        crud.create_index_entry(
            db,
            chunk_id=chunk.id,
            chroma_id=chunk.id,
            embedding_model=settings.embedding_model,
        )
    db.flush()


def run(raw_doc_id: str, db: Session) -> int:
    """Publish all approved chunks for a document. Returns count published."""
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    chunks = crud.get_chunks_for_doc(db, canonical.id)

    published = 0
    for chunk in chunks:
        if chunk.passed_review:
            try:
                publish_chunk(chunk.id, db)
                published += 1
            except Exception as e:
                logger.error(f"Failed to publish chunk {chunk.id}: {e}")

    if published > 0:
        crud.publish_canonical(db, canonical.id)

    db.commit()
    return published
