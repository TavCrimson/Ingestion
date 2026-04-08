from __future__ import annotations

from pydantic import BaseModel, Field


class ContextRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    content_types: list[str] | None = None
    authority_levels: list[str] | None = None
    top_k: int = Field(default=5, ge=1, le=50)


class ContextChunk(BaseModel):
    chunk_id: str
    text: str
    score: float
    heading_context: str | None
    canonical_doc_id: str
    content_type: str | None
    authority_level: str


class ContextResponse(BaseModel):
    query: str
    results: list[ContextChunk]
    total: int
