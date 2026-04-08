from __future__ import annotations

from pydantic import BaseModel


class RelationshipOut(BaseModel):
    id: str
    relationship_type: str
    source_entity_id: str
    target_entity_id: str
    confidence_score: float


class EntityOut(BaseModel):
    id: str
    name: str
    entity_type: str
    normalized_name: str
    aliases: list[str]
    confidence_score: float
    authority_level: str
    source_references: list[str]
    relationships: list[RelationshipOut]
