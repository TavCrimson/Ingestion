from __future__ import annotations

from pydantic import BaseModel, Field


class LinkedContextRequest(BaseModel):
    entity_ids: list[str] = Field(..., min_length=1)
    hops: int = Field(default=1, ge=1, le=2)
    top_k_chunks: int = Field(default=5, ge=1, le=20)


class LinkedContextResponse(BaseModel):
    entity_ids: list[str]
    connected_entities: list[dict]
    chunks: list[dict]
    documents: list[dict]
