"""Tests for review queue, including cascading delete on rejection."""
from unittest.mock import patch, MagicMock
import pytest


def _seed_doc_with_chunks(db):
    """Helper: create a canonical doc with 2 chunks and a doc-level review item."""
    from ingestion.db import crud
    from ingestion.db.models import Chunk

    raw = crud.create_raw_doc(
        db,
        filename="doc.pdf",
        original_path="/tmp/doc.pdf",
        stored_path="/tmp/doc.pdf",
        file_hash="aabbcc",
        file_size_bytes=100,
    )
    db.flush()
    canonical = crud.create_canonical(
        db,
        raw_doc_id=raw.id,
        content_type="prd",
        authority_level="observed",
        status="review",
    )
    db.flush()
    for i in range(2):
        crud.create_chunk(
            db,
            canonical_doc_id=canonical.id,
            chunk_index=i,
            text=f"Chunk {i} text",
            confidence_score=0.9,
            authority_level="observed",
        )
    db.flush()
    review_item = crud.create_review_item(
        db,
        canonical_doc_id=canonical.id,
        assigned_role="reviewer",
        reason="needs review",
    )
    db.commit()
    return canonical.id, review_item.id


def test_reject_doc_removes_chunks_from_db(db):
    """Rejecting a doc-level review item must delete all its chunks from the DB."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db.models import Chunk

    canonical_id, item_id = _seed_doc_with_chunks(db)

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, item_id, "tester", "not relevant")

    remaining = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).count()
    assert remaining == 0


def test_reject_doc_removes_chunks_from_vector_store(db):
    """Rejecting a doc must call vector_store.delete for each chunk."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db.models import Chunk

    canonical_id, item_id = _seed_doc_with_chunks(db)

    # Get chunk IDs before deletion
    chunk_ids = {c.id for c in db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).all()}
    assert len(chunk_ids) == 2

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, item_id, "tester", "not relevant")

    deleted_ids = {call.args[0] for call in mock_vs.delete.call_args_list}
    assert deleted_ids == chunk_ids


def test_reject_chunk_level_does_not_cascade(db):
    """Rejecting a chunk-level item must NOT delete sibling chunks from DB."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db import crud
    from ingestion.db.models import Chunk

    canonical_id, _ = _seed_doc_with_chunks(db)
    chunks = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).all()

    # Create a chunk-level review item for the first chunk only
    chunk_item = crud.create_review_item(
        db,
        canonical_doc_id=canonical_id,
        chunk_id=chunks[0].id,
        assigned_role="reviewer",
        reason="bad chunk",
    )
    db.commit()

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, chunk_item.id, "tester", "bad content")

    # No chunks deleted from DB at chunk-level rejection
    remaining = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).count()
    assert remaining == 2
