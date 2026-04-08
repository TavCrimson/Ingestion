"""POST /v1/context — retrieve relevant chunks for a query."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ingestion.api.auth import verify_api_key
from ingestion.api.rate_limit import rate_limit
from ingestion.api.schemas.context import ContextChunk, ContextRequest, ContextResponse
from ingestion.db.engine import get_db
from ingestion.db import crud
from ingestion.embeddings.encoder import Encoder
from ingestion.storage.vector_store import vector_store

router = APIRouter()


@router.post("/context", response_model=ContextResponse)
def get_context(
    req: ContextRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    embedding = Encoder.get().encode_one(req.query)

    where: dict | None = None
    filters = {}
    if req.content_types:
        filters["content_type"] = {"$in": req.content_types}
    if req.authority_levels:
        filters["authority_level"] = {"$in": req.authority_levels}
    if filters:
        where = filters

    hits = vector_store.query(embedding, top_k=req.top_k, where=where or None)

    results = []
    for hit in hits:
        chunk = crud.get_chunk(db, hit["chunk_id"])
        if chunk is None:
            continue
        canonical = crud.get_canonical(db, chunk.canonical_doc_id)
        results.append(ContextChunk(
            chunk_id=hit["chunk_id"],
            text=hit["text"],
            score=round(hit["score"], 4),
            heading_context=chunk.heading_context,
            canonical_doc_id=chunk.canonical_doc_id,
            content_type=canonical.content_type if canonical else None,
            authority_level=chunk.authority_level,
        ))

    return ContextResponse(query=req.query, results=results, total=len(results))
