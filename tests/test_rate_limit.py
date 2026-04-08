"""Tests for DB-backed rate limiter."""
from datetime import datetime, timezone, timedelta
import pytest


def test_rate_limit_enforced_from_db(db):
    """Rate limit must be enforced using DB-persisted entries."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey

    key = ApiKey(key_hash="ratelimitkey", label="test", rate_limit_per_minute=3)
    db.add(key)
    db.commit()

    # First 3 calls should succeed
    for _ in range(3):
        db_rate_limit(key, db)  # must not raise

    # 4th call should raise HTTPException 429
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        db_rate_limit(key, db)
    assert exc_info.value.status_code == 429


def test_rate_limit_window_expires(db):
    """Entries older than 60s must not count toward the limit."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey, RateLimitEntry

    key = ApiKey(key_hash="expirekey", label="expire", rate_limit_per_minute=2)
    db.add(key)
    db.commit()

    # Insert 2 old entries (>60s ago) — these are expired
    old_time = datetime.now(timezone.utc) - timedelta(seconds=90)
    for _ in range(2):
        db.add(RateLimitEntry(key_hash="expirekey", requested_at=old_time))
    db.commit()

    # Should succeed because old entries are beyond the 60s window
    db_rate_limit(key, db)  # must not raise


def test_rate_limit_cleans_old_entries(db):
    """After a request, entries older than 60s for that key should be removed."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey, RateLimitEntry

    key = ApiKey(key_hash="cleankey", label="clean", rate_limit_per_minute=10)
    db.add(key)
    db.commit()

    # Insert one expired entry
    old_time = datetime.now(timezone.utc) - timedelta(seconds=120)
    db.add(RateLimitEntry(key_hash="cleankey", requested_at=old_time))
    db.commit()

    db_rate_limit(key, db)

    # Expired entry must be gone
    remaining_old = db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == "cleankey",
        RateLimitEntry.requested_at < datetime.now(timezone.utc) - timedelta(seconds=60),
    ).count()
    assert remaining_old == 0
