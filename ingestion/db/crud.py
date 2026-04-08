"""Reusable database query functions."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from ingestion.db.models import (
    ApiKey, CanonicalDocument, Chunk, Entity,
    IndexEntry, PipelineRun, RawDocument, Relationship, ReviewQueueItem,
)


# ---------------------------------------------------------------------------
# RawDocument
# ---------------------------------------------------------------------------

def get_raw_doc_by_hash(db: Session, file_hash: str) -> Optional[RawDocument]:
    return db.query(RawDocument).filter(RawDocument.file_hash == file_hash).first()


def get_raw_doc(db: Session, doc_id: str) -> Optional[RawDocument]:
    return db.query(RawDocument).filter(RawDocument.id == doc_id).first()


def create_raw_doc(db: Session, **kwargs) -> RawDocument:
    doc = RawDocument(**kwargs)
    db.add(doc)
    db.flush()
    return doc


# ---------------------------------------------------------------------------
# CanonicalDocument
# ---------------------------------------------------------------------------

def get_canonical_by_raw(db: Session, raw_doc_id: str) -> Optional[CanonicalDocument]:
    return db.query(CanonicalDocument).filter(CanonicalDocument.raw_doc_id == raw_doc_id).first()


def get_canonical(db: Session, doc_id: str) -> Optional[CanonicalDocument]:
    return db.query(CanonicalDocument).filter(CanonicalDocument.id == doc_id).first()


def create_canonical(db: Session, **kwargs) -> CanonicalDocument:
    doc = CanonicalDocument(**kwargs)
    db.add(doc)
    db.flush()
    return doc


def publish_canonical(db: Session, doc_id: str) -> None:
    db.query(CanonicalDocument).filter(CanonicalDocument.id == doc_id).update({
        "status": "published",
        "published_at": datetime.now(timezone.utc),
    })


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------

def get_entity(db: Session, entity_id: str) -> Optional[Entity]:
    return db.query(Entity).filter(Entity.id == entity_id).first()


def find_entity_by_name(db: Session, normalized_name: str) -> Optional[Entity]:
    return db.query(Entity).filter(Entity.normalized_name == normalized_name).first()


def create_entity(db: Session, **kwargs) -> Entity:
    entity = Entity(**kwargs)
    db.add(entity)
    db.flush()
    return entity


def get_entities_for_doc(db: Session, canonical_doc_id: str) -> list[Entity]:
    return db.query(Entity).filter(Entity.canonical_doc_id == canonical_doc_id).all()


# ---------------------------------------------------------------------------
# Relationship
# ---------------------------------------------------------------------------

def create_relationship(db: Session, **kwargs) -> Relationship:
    rel = Relationship(**kwargs)
    db.add(rel)
    db.flush()
    return rel


def get_relationships_for_entity(db: Session, entity_id: str) -> list[Relationship]:
    return db.query(Relationship).filter(
        (Relationship.source_entity_id == entity_id) |
        (Relationship.target_entity_id == entity_id)
    ).all()


# ---------------------------------------------------------------------------
# Chunk
# ---------------------------------------------------------------------------

def create_chunk(db: Session, **kwargs) -> Chunk:
    chunk = Chunk(**kwargs)
    db.add(chunk)
    db.flush()
    return chunk


def get_chunk(db: Session, chunk_id: str) -> Optional[Chunk]:
    return db.query(Chunk).filter(Chunk.id == chunk_id).first()


def get_chunks_for_doc(db: Session, canonical_doc_id: str) -> list[Chunk]:
    return db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).order_by(Chunk.chunk_index).all()


def mark_chunk_passed_review(db: Session, chunk_id: str) -> None:
    db.query(Chunk).filter(Chunk.id == chunk_id).update({"passed_review": True})


# ---------------------------------------------------------------------------
# IndexEntry
# ---------------------------------------------------------------------------

def create_index_entry(db: Session, **kwargs) -> IndexEntry:
    entry = IndexEntry(**kwargs)
    db.add(entry)
    db.flush()
    return entry


# ---------------------------------------------------------------------------
# PipelineRun
# ---------------------------------------------------------------------------

def get_pipeline_run(db: Session, raw_doc_id: str, stage: str) -> Optional[PipelineRun]:
    return db.query(PipelineRun).filter(
        PipelineRun.raw_doc_id == raw_doc_id,
        PipelineRun.stage == stage,
    ).first()


def upsert_pipeline_run(db: Session, raw_doc_id: str, stage: str, status: str, error_msg: str = None) -> PipelineRun:
    run = get_pipeline_run(db, raw_doc_id, stage)
    now = datetime.now(timezone.utc)
    if run is None:
        run = PipelineRun(raw_doc_id=raw_doc_id, stage=stage)
        db.add(run)
    run.status = status
    if status == "running":
        run.started_at = now
    elif status in ("completed", "failed"):
        run.completed_at = now
    if error_msg is not None:
        run.error_msg = error_msg
    db.flush()
    return run


def get_completed_stages(db: Session, raw_doc_id: str) -> set[str]:
    runs = db.query(PipelineRun).filter(
        PipelineRun.raw_doc_id == raw_doc_id,
        PipelineRun.status == "completed",
    ).all()
    return {r.stage for r in runs}


# ---------------------------------------------------------------------------
# ReviewQueueItem
# ---------------------------------------------------------------------------

def create_review_item(db: Session, **kwargs) -> ReviewQueueItem:
    item = ReviewQueueItem(**kwargs)
    db.add(item)
    db.flush()
    return item


def get_pending_review_items(db: Session, role: str = None) -> list[ReviewQueueItem]:
    q = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "pending")
    if role:
        q = q.filter(ReviewQueueItem.assigned_role == role)
    return q.all()


def get_pending_review_items_paginated(
    db: Session,
    role: str = None,
    limit: int = 100,
    offset: int = 0,
) -> list[ReviewQueueItem]:
    q = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "pending")
    if role:
        q = q.filter(ReviewQueueItem.assigned_role == role)
    return q.order_by(ReviewQueueItem.created_at).offset(offset).limit(limit).all()


def get_review_item(db: Session, item_id: str) -> Optional[ReviewQueueItem]:
    return db.query(ReviewQueueItem).filter(ReviewQueueItem.id == item_id).first()


# ---------------------------------------------------------------------------
# Document deletion
# ---------------------------------------------------------------------------

def delete_document(db: Session, canonical_doc_id: str) -> dict | None:
    """
    Fully remove a document and all associated data from the database.
    Returns a metadata dict (chunk_ids, raw stored_path) for the caller
    to finish cleaning up ChromaDB and the raw file on disk.
    """
    canonical = db.query(CanonicalDocument).filter(CanonicalDocument.id == canonical_doc_id).first()
    if not canonical:
        return None

    raw_doc_id = canonical.raw_doc_id

    # Collect chunk IDs so we can clean up index entries and review items
    chunk_ids = [
        c.id for c in db.query(Chunk.id).filter(Chunk.canonical_doc_id == canonical_doc_id).all()
    ]

    # 1. Remove IndexEntries (FK → Chunk)
    if chunk_ids:
        db.query(IndexEntry).filter(IndexEntry.chunk_id.in_(chunk_ids)).delete(
            synchronize_session=False
        )

    # 2. Remove ReviewQueueItems linked to chunks or to the document itself
    if chunk_ids:
        db.query(ReviewQueueItem).filter(ReviewQueueItem.chunk_id.in_(chunk_ids)).delete(
            synchronize_session=False
        )
    db.query(ReviewQueueItem).filter(
        ReviewQueueItem.canonical_doc_id == canonical_doc_id
    ).delete(synchronize_session=False)

    # 3. Remove Chunks — FTS5 delete trigger fires automatically
    db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).delete(
        synchronize_session=False
    )

    # 4. Remove Relationships whose source or target entity belongs to this doc
    entity_ids = [
        e.id for e in db.query(Entity.id).filter(Entity.canonical_doc_id == canonical_doc_id).all()
    ]
    if entity_ids:
        db.query(Relationship).filter(
            (Relationship.source_entity_id.in_(entity_ids))
            | (Relationship.target_entity_id.in_(entity_ids))
        ).delete(synchronize_session=False)

    # 5. Remove Entities (FK → CanonicalDocument)
    db.query(Entity).filter(Entity.canonical_doc_id == canonical_doc_id).delete(
        synchronize_session=False
    )

    # 6. Remove PipelineRuns (FK → RawDocument)
    if raw_doc_id:
        db.query(PipelineRun).filter(PipelineRun.raw_doc_id == raw_doc_id).delete(
            synchronize_session=False
        )

    # 7. Remove CanonicalDocument
    db.delete(canonical)
    db.flush()

    # 8. Remove RawDocument (now that canonical is gone)
    stored_path = None
    if raw_doc_id:
        raw = db.query(RawDocument).filter(RawDocument.id == raw_doc_id).first()
        if raw:
            stored_path = raw.stored_path
            db.delete(raw)
            db.flush()

    return {
        "canonical_doc_id": canonical_doc_id,
        "raw_doc_id": raw_doc_id,
        "chunk_ids": chunk_ids,
        "stored_path": stored_path,
    }


def delete_chunks_for_canonical(db: Session, canonical_doc_id: str) -> list[str]:
    """
    Delete all Chunk rows (and their IndexEntry rows) for a canonical doc.
    Returns the list of deleted chunk IDs so the caller can clean up the vector store.
    """
    chunk_ids = [
        c.id for c in db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).all()
    ]
    if chunk_ids:
        db.query(IndexEntry).filter(IndexEntry.chunk_id.in_(chunk_ids)).delete(
            synchronize_session=False
        )
        db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).delete(
            synchronize_session=False
        )
    return chunk_ids


# ---------------------------------------------------------------------------
# ApiKey
# ---------------------------------------------------------------------------

def get_api_key_by_hash(db: Session, key_hash: str) -> Optional[ApiKey]:
    return db.query(ApiKey).filter(ApiKey.key_hash == key_hash, ApiKey.active == True).first()
