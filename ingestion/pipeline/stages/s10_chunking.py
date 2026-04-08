"""Stage 10: Chunking — split text into semantic chunks and write to Chunk layer."""
from __future__ import annotations

from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.pipeline.chunker import chunk_text
from ingestion.pipeline.stages.s03_extraction import load_extracted


def run(raw_doc_id: str, db: Session) -> list[str]:
    """Chunk extracted text and write Chunk rows. Returns list of chunk IDs."""
    data = load_extracted(raw_doc_id, db)
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)

    # Remove any existing chunks for this doc (idempotent re-run)
    from ingestion.db.models import Chunk
    db.query(Chunk).filter(Chunk.canonical_doc_id == canonical.id).delete()
    db.flush()

    chunks = chunk_text(data["text"])
    chunk_ids = []
    for c in chunks:
        if not c.text.strip():
            continue
        chunk = crud.create_chunk(
            db,
            canonical_doc_id=canonical.id,
            chunk_index=c.chunk_index,
            text=c.text,
            char_start=c.char_start,
            char_end=c.char_end,
            heading_context=c.heading_context,
            authority_level=canonical.authority_level,
        )
        chunk_ids.append(chunk.id)

    db.commit()
    return chunk_ids
