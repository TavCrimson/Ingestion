from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    mode: Literal["keyword", "semantic", "hybrid"] = "hybrid"
    content_types: list[str] | None = None
    authority_levels: list[str] | None = None
    top_k: int = Field(default=10, ge=1, le=100)


class SearchResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    match_type: str  # "keyword", "semantic", or "hybrid"
    heading_context: str | None
    canonical_doc_id: str
    content_type: str | None
    authority_level: str


class SearchResponse(BaseModel):
    query: str
    mode: str
    results: list[SearchResult]
    total: int
