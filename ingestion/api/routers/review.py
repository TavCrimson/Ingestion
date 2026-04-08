"""Review queue endpoints for the UI."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ingestion.api.rate_limit import rate_limit
from ingestion.api.schemas.review import ApproveRequest, RejectRequest, ReviewItemOut
from ingestion.db import crud
from ingestion.db.engine import get_db
from ingestion.db.models import ReviewQueueItem
from ingestion.review.queue import ReviewQueue

router = APIRouter()


def _serialise(item: ReviewQueueItem, db: Session) -> ReviewItemOut:
    chunk_preview = None
    heading_context = None
    chunk_index = None
    confidence_score = None

    if item.chunk_id:
        chunk = crud.get_chunk(db, item.chunk_id)
        if chunk:
            chunk_preview = chunk.text[:400]
            heading_context = chunk.heading_context or None
            chunk_index = chunk.chunk_index
            confidence_score = chunk.confidence_score

    doc_title = None
    content_type = None
    authority_level = None
    if item.canonical_doc_id:
        canonical = crud.get_canonical(db, item.canonical_doc_id)
        if canonical:
            doc_title = canonical.title
            content_type = canonical.content_type
            authority_level = canonical.authority_level

    return ReviewItemOut(
        id=item.id,
        canonical_doc_id=item.canonical_doc_id,
        chunk_id=item.chunk_id,
        assigned_role=item.assigned_role,
        reason=item.reason,
        status=item.status,
        created_at=item.created_at.isoformat() if item.created_at else None,
        due_at=item.due_at.isoformat() if item.due_at else None,
        chunk_preview=chunk_preview,
        doc_title=doc_title,
        content_type=content_type,
        authority_level=authority_level,
        heading_context=heading_context,
        chunk_index=chunk_index,
        confidence_score=confidence_score,
    )


@router.get("/queue", response_model=list[ReviewItemOut])
def get_queue(
    role: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    items = crud.get_pending_review_items_paginated(db, role=role, limit=limit, offset=offset)
    return [_serialise(i, db) for i in items]


@router.get("/queue/stats")
def queue_stats(
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    from ingestion.db.models import ReviewQueueItem, CanonicalDocument
    pending = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "pending").count()
    approved = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "approved").count()
    rejected = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "rejected").count()
    published = db.query(CanonicalDocument).filter(CanonicalDocument.status == "published").count()
    total_docs = db.query(CanonicalDocument).count()
    return {
        "pending": pending,
        "approved": approved,
        "rejected": rejected,
        "published_docs": published,
        "total_docs": total_docs,
    }


@router.post("/queue/{item_id}/approve", response_model=ReviewItemOut)
def approve_item(
    item_id: str,
    body: ApproveRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    try:
        item = ReviewQueue.approve(db, item_id, body.reviewer)
        return _serialise(item, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/queue/{item_id}/reject", response_model=ReviewItemOut)
def reject_item(
    item_id: str,
    body: RejectRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    try:
        item = ReviewQueue.reject(db, item_id, body.reviewer, body.reason)
        return _serialise(item, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
