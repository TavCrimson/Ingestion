"""Stage 11: Confidence scoring — assign scores and route to review if needed."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ingestion.authority.model import requires_review
from ingestion.db import crud


def score_chunk(text: str, content_type: str | None) -> float:
    """Heuristic confidence score for a chunk."""
    score = 0.9  # Start high

    # Penalise very short chunks
    if len(text.split()) < 10:
        score -= 0.15

    # Penalise if text looks garbled (high ratio of non-ASCII)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    if len(text) > 0 and non_ascii / len(text) > 0.3:
        score -= 0.2

    # Penalise excessive whitespace / likely OCR noise
    if text.count("  ") > len(text) // 20:
        score -= 0.1

    return max(round(score, 2), 0.0)


def _review_reason(score: float, content_type: str | None, authority: str) -> str:
    """Generate a human-readable explanation for why this chunk needs review."""
    from ingestion.authority.model import CONFIDENCE_THRESHOLDS
    reasons = []

    if authority == "authoritative":
        reasons.append("Authoritative document — all content requires approval before publishing")
    elif authority == "proposed":
        reasons.append("Proposed content — not yet approved as a trusted source")

    if authority not in ("authoritative", "proposed"):
        threshold = CONFIDENCE_THRESHOLDS.get(content_type or "general", 0.80)
        if score < threshold:
            reasons.append(
                f"Low confidence score ({score:.0%}) — below the {threshold:.0%} threshold for {content_type or 'general'} content"
            )

    if not reasons:
        reasons.append(f"Flagged for review (confidence {score:.0%}, {authority} {content_type or 'content'})")

    return " · ".join(reasons)


def run(raw_doc_id: str, db: Session) -> None:
    """Score each chunk and enqueue low-confidence ones for review."""
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    chunks = crud.get_chunks_for_doc(db, canonical.id)

    from ingestion.review.queue import ReviewQueue

    for chunk in chunks:
        score = score_chunk(chunk.text, canonical.content_type)
        chunk.confidence_score = score

        if requires_review(canonical.content_type, canonical.authority_level, score):
            ReviewQueue.enqueue(
                db=db,
                chunk_id=chunk.id,
                canonical_doc_id=canonical.id,
                reason=_review_reason(score, canonical.content_type, canonical.authority_level),
            )
        else:
            chunk.passed_review = True

    db.commit()
