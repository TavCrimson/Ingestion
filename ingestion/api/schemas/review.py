from __future__ import annotations
from pydantic import BaseModel


class ReviewItemOut(BaseModel):
    id: str
    canonical_doc_id: str | None
    chunk_id: str | None
    assigned_role: str
    reason: str | None
    status: str
    created_at: str
    due_at: str | None
    chunk_preview: str | None
    doc_title: str | None
    content_type: str | None
    authority_level: str | None
    heading_context: str | None
    chunk_index: int | None
    confidence_score: float | None


class ApproveRequest(BaseModel):
    reviewer: str = "ui-user"


class RejectRequest(BaseModel):
    reviewer: str = "ui-user"
    reason: str
