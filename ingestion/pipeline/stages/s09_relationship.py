"""Stage 9: Relationship mapping — link entities to each other."""
from __future__ import annotations

from sqlalchemy.orm import Session

from ingestion.db import crud


def run(raw_doc_id: str, db: Session) -> list[str]:
    """
    Infer relationships between entities in this document.
    Returns list of created relationship IDs.
    """
    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    entities = crud.get_entities_for_doc(db, canonical.id)

    rel_ids = []
    products = [e for e in entities if e.entity_type == "Product"]
    features = [e for e in entities if e.entity_type == "Feature"]
    competitors = [e for e in entities if e.entity_type == "Competitor"]
    versions = [e for e in entities if e.entity_type == "Version"]

    # Features belong to products
    for product in products:
        for feature in features:
            rel = _create_rel(db, feature.id, product.id, "belongs_to", canonical.id)
            if rel:
                rel_ids.append(rel)

    # Products compete with competitors
    for product in products:
        for competitor in competitors:
            rel = _create_rel(db, product.id, competitor.id, "competes_with", canonical.id)
            if rel:
                rel_ids.append(rel)

    # Products have versions
    for product in products:
        for version in versions:
            rel = _create_rel(db, product.id, version.id, "has_version", canonical.id)
            if rel:
                rel_ids.append(rel)

    db.commit()
    return rel_ids


def _create_rel(db: Session, source_id: str, target_id: str, rel_type: str, doc_id: str) -> str | None:
    # Avoid duplicate relationships
    from ingestion.db.models import Relationship
    existing = db.query(Relationship).filter(
        Relationship.source_entity_id == source_id,
        Relationship.target_entity_id == target_id,
        Relationship.relationship_type == rel_type,
    ).first()
    if existing:
        return None
    rel = crud.create_relationship(
        db,
        source_entity_id=source_id,
        target_entity_id=target_id,
        relationship_type=rel_type,
        source_doc_id=doc_id,
    )
    return rel.id
