"""Sliding-window in-memory rate limiter."""
from __future__ import annotations

import time
from collections import defaultdict, deque

from fastapi import Depends, HTTPException

from ingestion.api.auth import verify_api_key
from ingestion.db.models import ApiKey

_windows: dict[str, deque] = defaultdict(deque)


def rate_limit(api_key: ApiKey = Depends(verify_api_key)) -> ApiKey:
    now = time.monotonic()
    window = _windows[api_key.key_hash]
    cutoff = now - 60.0

    # Evict expired entries
    while window and window[0] < cutoff:
        window.popleft()

    if len(window) >= api_key.rate_limit_per_minute:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    window.append(now)
    return api_key
