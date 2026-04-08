"""Sliding-window rate limiter backed by SQLite for restart-safety."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from ingestion.api.auth import verify_api_key
from ingestion.db.engine import get_db
from ingestion.db.models import ApiKey, RateLimitEntry


def db_rate_limit(api_key: ApiKey, db: Session) -> None:
    """
    Enforce rate limit using DB-persisted entries.
    Raises HTTP 429 if the key has exceeded its per-minute limit.
    Cleans expired entries on every call.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=60)

    # Evict expired entries for this key
    db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == api_key.key_hash,
        RateLimitEntry.requested_at < window_start,
    ).delete(synchronize_session=False)

    # Count entries in current window
    count = db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == api_key.key_hash,
        RateLimitEntry.requested_at >= window_start,
    ).count()

    if count >= api_key.rate_limit_per_minute:
        db.commit()  # persist eviction even on rejection
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    db.add(RateLimitEntry(key_hash=api_key.key_hash, requested_at=now))
    db.commit()


def rate_limit(
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db),
) -> ApiKey:
    db_rate_limit(api_key, db)
    return api_key
