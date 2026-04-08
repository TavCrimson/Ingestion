"""Stage 8: Deduplication — hash exact-match then embedding similarity."""
from __future__ import annotations

import logging
from sqlalchemy.orm import Session

from ingestion.config import settings
from ingestion.db import crud
from ingestion.pipeline.stages.s03_extraction import load_extracted

logger = logging.getLogger(__name__)


def run(raw_doc_id: str, db: Session) -> dict:
    """
    Check for duplicates. Returns:
      {"is_duplicate": bool, "duplicate_of": str|None, "similar": list[str], "needs_review": bool}
    """
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    result = {
        "is_duplicate": False,
        "duplicate_of": None,
        "similar": [],
        "needs_review": False,
    }

    # Pass 1: Exact hash match
    existing = (
        db.query(crud.RawDocument)
        .filter(
            crud.RawDocument.file_hash == raw_doc.file_hash,
            crud.RawDocument.id != raw_doc_id,
        )
        .first()
    )
    if existing:
        result["is_duplicate"] = True
        result["duplicate_of"] = existing.id
        result["needs_review"] = True
        _flag_for_review(raw_doc_id, existing.id, "exact_hash_duplicate", db)
        db.commit()
        return result

    # Pass 2: Embedding similarity
    data = load_extracted(raw_doc_id, db)
    sample_text = data["text"][:512]
    if not sample_text.strip():
        return result

    try:
        from ingestion.embeddings.encoder import Encoder
        from ingestion.storage.vector_store import vector_store

        embedding = Encoder.get().encode_one(sample_text)
        hits = vector_store.query(embedding, top_k=3)

        for hit in hits:
            score = hit["score"]
            if score >= settings.dedup_near_duplicate_threshold:
                result["similar"].append(hit["chunk_id"])
                result["needs_review"] = True
                _flag_for_review(raw_doc_id, hit["chunk_id"], "embedding_near_duplicate", db)
            elif score >= settings.dedup_similar_lower_bound:
                result["similar"].append(hit["chunk_id"])
                # Flag as similar for human review but don't block
                result["needs_review"] = True
    except Exception as exc:
        logger.warning(
            "Embedding similarity check failed for %s — skipping: %s", raw_doc_id, exc
        )

    if result["needs_review"]:
        db.commit()
    return result


def _flag_for_review(raw_doc_id: str, duplicate_id: str, reason: str, db: Session) -> None:
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    if canonical:
        from ingestion.review.queue import ReviewQueue
        ReviewQueue.enqueue(
            db=db,
            canonical_doc_id=canonical.id,
            reason=f"{reason}: potential duplicate of {duplicate_id}",
        )
