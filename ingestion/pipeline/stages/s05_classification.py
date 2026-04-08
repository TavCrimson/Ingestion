"""Stage 5: Classification — identify document type using rules."""
from __future__ import annotations

import re
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.pipeline.stages.s03_extraction import load_extracted

_PATTERNS_PATH = Path(__file__).parents[3] / "patterns.yaml"
_patterns: dict | None = None


def _load_patterns() -> dict:
    global _patterns
    if _patterns is None:
        with open(_PATTERNS_PATH, encoding="utf-8") as f:
            _patterns = yaml.safe_load(f)
    return _patterns


def classify(filename: str, text: str) -> tuple[str, float]:
    """
    Returns (content_type, confidence).
    content_type is one of: prd, competitor, insight, decision, metric, integration, general
    """
    patterns = _load_patterns()
    classification_rules: dict[str, list[str]] = patterns.get("classification_rules", {})

    sample = (filename.lower() + " " + text[:1000].lower())
    scores: dict[str, int] = {}

    for content_type, keywords in classification_rules.items():
        count = sum(1 for kw in keywords if kw.lower() in sample)
        if count:
            scores[content_type] = count

    if not scores:
        return "general", 0.5

    best_type = max(scores, key=scores.__getitem__)
    total = sum(scores.values())
    confidence = min(scores[best_type] / max(total, 1) + 0.4, 0.95)
    return best_type, confidence


def run(raw_doc_id: str, db: Session) -> str:
    """Classify the document and create/update CanonicalDocument. Returns content_type."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    data = load_extracted(raw_doc_id, db)
    content_type, confidence = classify(raw_doc.filename, data["text"])

    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    if canonical is None:
        from ingestion.authority.model import assign_authority
        authority = assign_authority(content_type, source_is_internal=True)
        canonical = crud.create_canonical(
            db,
            raw_doc_id=raw_doc_id,
            content_type=content_type,
            authority_level=authority.value,
            status="draft",
        )
    else:
        canonical.content_type = content_type

    db.commit()
    return content_type
