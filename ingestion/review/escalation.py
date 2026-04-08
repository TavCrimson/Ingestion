"""Escalation background task — promotes overdue review items."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from ingestion.config import settings
from ingestion.db.engine import SessionLocal
from ingestion.db.models import ReviewQueueItem
from ingestion.review.roles import next_role

logger = logging.getLogger(__name__)


def run_escalation() -> int:
    """
    Find review items past their due_at time and escalate them.
    Returns the number of items escalated.
    """
    db = SessionLocal()
    count = 0
    try:
        now = datetime.now(timezone.utc)
        overdue = db.query(ReviewQueueItem).filter(
            ReviewQueueItem.status == "pending",
            ReviewQueueItem.due_at < now,
        ).all()

        for item in overdue:
            new_role = next_role(item.assigned_role)
            if new_role != item.assigned_role:
                logger.info(
                    f"Escalating review item {item.id} from {item.assigned_role} to {new_role}"
                )
                item.assigned_role = new_role
                item.status = "escalated"
                # Reset due_at for the new role
                from datetime import timedelta
                item.due_at = now + timedelta(hours=settings.escalation_timeout_hours)
                item.status = "pending"
                count += 1

        db.commit()
    except Exception as e:
        logger.error(f"Escalation error: {e}")
        db.rollback()
    finally:
        db.close()
    return count


async def escalation_loop() -> None:
    """Async loop for use as a FastAPI background task."""
    interval = settings.escalation_check_interval_minutes * 60
    while True:
        await asyncio.sleep(interval)
        try:
            count = run_escalation()
            if count:
                logger.info(f"Escalated {count} review items")
        except Exception as e:
            logger.error(f"Escalation loop error: {e}")
