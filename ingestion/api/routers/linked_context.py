"""POST /v1/linked-context — entity graph traversal + associated chunks."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ingestion.api.rate_limit import rate_limit
from ingestion.api.schemas.linked_context import LinkedContextRequest, LinkedContextResponse
from ingestion.db.engine import get_db
from ingestion.db import crud
from ingestion.db.models import Relationship

router = APIRouter()


def _traverse(db: Session, entity_ids: list[str], hops: int) -> set[str]:
    """Return all entity IDs reachable within `hops` from the seed set."""
    visited = set(entity_ids)
    frontier = set(entity_ids)
    for _ in range(hops):
        next_frontier: set[str] = set()
        for eid in frontier:
            rels = crud.get_relationships_for_entity(db, eid)
            for r in rels:
                for nid in (r.source_entity_id, r.target_entity_id):
                    if nid not in visited:
                        next_frontier.add(nid)
        visited |= next_frontier
        frontier = next_frontier
    return visited


@router.post("/linked-context", response_model=LinkedContextResponse)
def linked_context(
    req: LinkedContextRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    all_entity_ids = _traverse(db, req.entity_ids, req.hops)

    # Collect entities
    connected_entities = []
    doc_ids: set[str] = set()
    for eid in all_entity_ids:
        entity = crud.get_entity(db, eid)
        if entity:
            connected_entities.append({"id": entity.id, "name": entity.name, "entity_type": entity.entity_type})
            doc_ids.add(entity.canonical_doc_id)

    # Collect chunks from associated docs
    chunks_out = []
    for doc_id in list(doc_ids)[:10]:  # cap at 10 docs
        doc_chunks = crud.get_chunks_for_doc(db, doc_id)
        for chunk in doc_chunks[:req.top_k_chunks]:
            if chunk.passed_review:
                chunks_out.append({
                    "chunk_id": chunk.id,
                    "text": chunk.text,
                    "heading_context": chunk.heading_context,
                    "canonical_doc_id": chunk.canonical_doc_id,
                })

    # Collect documents
    docs_out = []
    for doc_id in doc_ids:
        canonical = crud.get_canonical(db, doc_id)
        if canonical:
            docs_out.append({
                "id": canonical.id,
                "title": canonical.title,
                "content_type": canonical.content_type,
                "authority_level": canonical.authority_level,
                "status": canonical.status,
            })

    return LinkedContextResponse(
        entity_ids=req.entity_ids,
        connected_entities=connected_entities,
        chunks=chunks_out,
        documents=docs_out,
    )
