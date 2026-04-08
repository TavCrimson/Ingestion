"""Authority level definitions and assignment rules."""
from __future__ import annotations

from enum import Enum


class AuthorityLevel(str, Enum):
    AUTHORITATIVE = "authoritative"   # Approved internal source of truth
    OBSERVED = "observed"             # External captured data
    DERIVED = "derived"               # AI-extracted or summarised
    PROPOSED = "proposed"             # Unapproved or low-confidence data


# Content types that are always treated as authoritative
_AUTHORITATIVE_CONTENT_TYPES = {"prd", "decision"}

# Content types from external/crawled sources
_OBSERVED_CONTENT_TYPES = {"competitor", "integration"}

# Content types that are AI-generated or derived
_DERIVED_CONTENT_TYPES = {"insight"}


def assign_authority(content_type: str | None, source_is_internal: bool = True) -> AuthorityLevel:
    """Assign an authority level based on content type and source."""
    if content_type in _AUTHORITATIVE_CONTENT_TYPES and source_is_internal:
        return AuthorityLevel.AUTHORITATIVE
    if content_type in _OBSERVED_CONTENT_TYPES or not source_is_internal:
        return AuthorityLevel.OBSERVED
    if content_type in _DERIVED_CONTENT_TYPES:
        return AuthorityLevel.DERIVED
    return AuthorityLevel.PROPOSED


# Confidence thresholds — chunks below these go to review queue
CONFIDENCE_THRESHOLDS: dict[str, float] = {
    "prd": 0.85,
    "competitor": 0.80,
    "insight": 0.75,
    "decision": 0.85,
    "metric": 0.80,
    "integration": 0.80,
    "general": 0.80,
}

# All proposed content and authoritative content must go through review regardless
ALWAYS_REVIEW_AUTHORITIES = {AuthorityLevel.PROPOSED, AuthorityLevel.AUTHORITATIVE}


def requires_review(
    content_type: str | None,
    authority_level: str | AuthorityLevel,
    confidence_score: float,
) -> bool:
    """Return True if this item must enter the review queue."""
    level = AuthorityLevel(authority_level) if isinstance(authority_level, str) else authority_level
    if level in ALWAYS_REVIEW_AUTHORITIES:
        return True
    threshold = CONFIDENCE_THRESHOLDS.get(content_type or "general", 0.80)
    return confidence_score < threshold
