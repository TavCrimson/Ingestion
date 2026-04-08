"""Review queue management."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session

from ingestion.config import settings
from ingestion.db import crud
from ingestion.db.models import ReviewQueueItem


class ReviewQueue:
    @staticmethod
    def enqueue(
        db: Session,
        canonical_doc_id: str | None = None,
        chunk_id: str | None = None,
        reason: str | None = None,
        role: str = "reviewer",
    ) -> ReviewQueueItem:
        due_at = datetime.now(timezone.utc) + timedelta(hours=settings.escalation_timeout_hours)
        item = crud.create_review_item(
            db,
            chunk_id=chunk_id,
            canonical_doc_id=canonical_doc_id,
            assigned_role=role,
            reason=reason,
            due_at=due_at,
        )
        return item

    @staticmethod
    def get_pending(db: Session, role: str | None = None) -> list[ReviewQueueItem]:
        return crud.get_pending_review_items(db, role=role)

    @staticmethod
    def approve(db: Session, item_id: str, reviewer: str) -> ReviewQueueItem:
        item = crud.get_review_item(db, item_id)
        if item is None:
            raise ValueError(f"Review item {item_id} not found")

        item.status = "approved"
        item.resolved_at = datetime.now(timezone.utc)
        item.resolved_by = reviewer

        # Approve the chunk if present
        if item.chunk_id:
            crud.mark_chunk_passed_review(db, item.chunk_id)
            # Publish the chunk immediately
            from ingestion.pipeline.stages.s12_publication import publish_chunk
            publish_chunk(item.chunk_id, db)

        # Update canonical status
        if item.canonical_doc_id:
            canonical = crud.get_canonical(db, item.canonical_doc_id)
            if canonical and canonical.status not in ("published",):
                from ingestion.db.crud import publish_canonical
                # Only publish canonical if all its review items are resolved
                pending = db.query(ReviewQueueItem).filter(
                    ReviewQueueItem.canonical_doc_id == item.canonical_doc_id,
                    ReviewQueueItem.status == "pending",
                    ReviewQueueItem.id != item_id,
                ).count()
                if pending == 0:
                    publish_canonical(db, item.canonical_doc_id)

        db.commit()
        return item

    @staticmethod
    def reject(db: Session, item_id: str, reviewer: str, reason: str) -> ReviewQueueItem:
        item = crud.get_review_item(db, item_id)
        if item is None:
            raise ValueError(f"Review item {item_id} not found")

        item.status = "rejected"
        item.resolved_at = datetime.now(timezone.utc)
        item.resolved_by = reviewer
        item.rejection_reason = reason

        # Mark canonical doc as rejected if rejecting at doc level
        if item.canonical_doc_id and item.chunk_id is None:
            canonical = crud.get_canonical(db, item.canonical_doc_id)
            if canonical:
                canonical.status = "rejected"

        db.commit()
        return item

    @staticmethod
    def edit_and_approve(db: Session, item_id: str, reviewer: str, new_text: str) -> ReviewQueueItem:
        """Update chunk text then approve."""
        item = crud.get_review_item(db, item_id)
        if item is None:
            raise ValueError(f"Review item {item_id} not found")

        if item.chunk_id:
            chunk = crud.get_chunk(db, item.chunk_id)
            if chunk:
                chunk.text = new_text

        return ReviewQueue.approve(db, item_id, reviewer)
