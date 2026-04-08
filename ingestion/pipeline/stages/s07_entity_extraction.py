"""Stage 7: Entity extraction — regex + pattern-based."""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.pipeline.stages.s03_extraction import load_extracted

_PATTERNS_PATH = Path(__file__).parents[3] / "patterns.yaml"
_patterns_cache: dict | None = None


def _load_patterns() -> dict:
    global _patterns_cache
    if _patterns_cache is None:
        with open(_PATTERNS_PATH, encoding="utf-8") as f:
            _patterns_cache = yaml.safe_load(f)
    return _patterns_cache


def extract_entities(text: str) -> list[dict]:
    """Returns list of {name, entity_type, normalized_name, confidence_score}."""
    patterns = _load_patterns()
    found: list[dict] = []
    seen: set[str] = set()

    def _add(name: str, entity_type: str, confidence: float = 0.9):
        norm = name.strip().lower()
        if norm not in seen:
            seen.add(norm)
            found.append({
                "name": name.strip(),
                "entity_type": entity_type,
                "normalized_name": norm,
                "confidence_score": confidence,
            })

    # Products
    for product in patterns.get("products", []):
        if re.search(re.escape(product), text, re.IGNORECASE):
            _add(product, "Product")

    # Competitors
    for competitor in patterns.get("competitors", []):
        if re.search(re.escape(competitor), text, re.IGNORECASE):
            _add(competitor, "Competitor")

    # Version strings
    for pattern in patterns.get("version_patterns", []):
        for m in re.finditer(pattern, text):
            _add(m.group(0).strip(), "Version", confidence=0.7)

    return found


def run(raw_doc_id: str, db: Session) -> list[str]:
    """Extract entities and write to Entity layer. Returns list of entity IDs."""
    data = load_extracted(raw_doc_id, db)
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    entities = extract_entities(data["text"])

    entity_ids = []
    for e in entities:
        existing = crud.find_entity_by_name(db, e["normalized_name"])
        if existing:
            entity_ids.append(existing.id)
            continue
        entity = crud.create_entity(
            db,
            canonical_doc_id=canonical.id,
            name=e["name"],
            entity_type=e["entity_type"],
            normalized_name=e["normalized_name"],
            confidence_score=e["confidence_score"],
            authority_level=canonical.authority_level,
            source_references=json.dumps([raw_doc_id]),
        )
        entity_ids.append(entity.id)

    db.commit()
    return entity_ids
