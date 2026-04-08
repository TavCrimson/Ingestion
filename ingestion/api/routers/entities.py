"""GET /v1/entities/{id} — retrieve an entity with its relationships."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ingestion.api.auth import verify_api_key
from ingestion.api.rate_limit import rate_limit
from ingestion.api.schemas.entities import EntityOut, RelationshipOut
from ingestion.db.engine import get_db
from ingestion.db import crud

router = APIRouter()


@router.get("/entities/{entity_id}", response_model=EntityOut)
def get_entity(
    entity_id: str,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    entity = crud.get_entity(db, entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")

    relationships = crud.get_relationships_for_entity(db, entity_id)
    rel_out = [
        RelationshipOut(
            id=r.id,
            relationship_type=r.relationship_type,
            source_entity_id=r.source_entity_id,
            target_entity_id=r.target_entity_id,
            confidence_score=r.confidence_score,
        )
        for r in relationships
    ]

    aliases = []
    if entity.aliases:
        try:
            aliases = json.loads(entity.aliases)
        except Exception:
            aliases = [entity.aliases]

    source_refs = []
    if entity.source_references:
        try:
            source_refs = json.loads(entity.source_references)
        except Exception:
            source_refs = [entity.source_references]

    return EntityOut(
        id=entity.id,
        name=entity.name,
        entity_type=entity.entity_type,
        normalized_name=entity.normalized_name,
        aliases=aliases,
        confidence_score=entity.confidence_score,
        authority_level=entity.authority_level,
        source_references=source_refs,
        relationships=rel_out,
    )
