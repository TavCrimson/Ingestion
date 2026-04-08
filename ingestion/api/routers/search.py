"""POST /v1/search — keyword, semantic, or hybrid search."""
from __future__ import annotations

import re
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from ingestion.api.rate_limit import rate_limit
from ingestion.api.schemas.search import SearchRequest, SearchResponse, SearchResult
from ingestion.config import settings
from ingestion.db.engine import get_db
from ingestion.db import crud
from ingestion.embeddings.encoder import Encoder
from ingestion.storage.vector_store import vector_store

router = APIRouter()


def _sanitize_fts(query: str) -> str:
    """
    Convert a natural-language query into an FTS5-safe expression.
    Uses OR between terms so any meaningful word is a hit, letting FTS5
    rank chunks that match more terms higher.  Strips punctuation that
    would be interpreted as FTS5 operators.
    """
    # Remove FTS5 special characters
    cleaned = re.sub(r"[^\w\s]", " ", query)
    # Keep only tokens longer than 2 characters (skip "is", "of", "a", etc.)
    terms = [t for t in cleaned.split() if len(t) > 2]
    if not terms:
        return re.sub(r"[^\w\s]", " ", query).strip()
    return " OR ".join(terms)


def _keyword_search(db: Session, query: str, top_k: int, content_types: list | None, authority_levels: list | None) -> list[dict]:
    """SQLite FTS5 keyword search."""
    try:
        fts_query = _sanitize_fts(query)
        # Select chunk_id and rank only; full text is fetched from the chunk record
        # so the LLM/UI always receives complete, untruncated content without HTML markup
        sql = "SELECT chunk_id, rank FROM chunks_fts WHERE chunks_fts MATCH :q ORDER BY rank LIMIT :k"
        rows = db.execute(text(sql), {"q": fts_query, "k": top_k}).fetchall()
        results = []
        for row in rows:
            chunk = crud.get_chunk(db, row.chunk_id)
            if chunk is None or not chunk.passed_review:
                continue
            canonical = crud.get_canonical(db, chunk.canonical_doc_id)
            if content_types and (canonical is None or canonical.content_type not in content_types):
                continue
            if authority_levels and chunk.authority_level not in authority_levels:
                continue
            results.append({
                "chunk_id": chunk.id,
                "text": chunk.text,
                "score": min(1.0, 1.0 / (1.0 + abs(row.rank))),
                "match_type": "keyword",
                "heading_context": chunk.heading_context,
                "canonical_doc_id": chunk.canonical_doc_id,
                "content_type": canonical.content_type if canonical else None,
                "authority_level": chunk.authority_level,
            })
        return results
    except Exception:
        return []


def _semantic_search(query: str, top_k: int, content_types: list | None, authority_levels: list | None, db: Session) -> list[dict]:
    embedding = Encoder.get().encode_one(query)
    where: dict | None = None
    filters = {}
    if content_types:
        filters["content_type"] = {"$in": content_types}
    if authority_levels:
        filters["authority_level"] = {"$in": authority_levels}
    if filters:
        where = filters

    hits = vector_store.query(embedding, top_k=top_k, where=where)
    results = []
    for hit in hits:
        chunk = crud.get_chunk(db, hit["chunk_id"])
        if chunk is None:
            continue
        canonical = crud.get_canonical(db, chunk.canonical_doc_id)
        results.append({
            "chunk_id": hit["chunk_id"],
            "text": hit["text"],
            "score": round(hit["score"], 4),
            "match_type": "semantic",
            "heading_context": chunk.heading_context,
            "canonical_doc_id": chunk.canonical_doc_id,
            "content_type": canonical.content_type if canonical else None,
            "authority_level": chunk.authority_level,
        })
    return results


def _merge_results(keyword: list[dict], semantic: list[dict], top_k: int) -> list[dict]:
    """Simple reciprocal rank fusion."""
    scores: dict[str, float] = {}
    by_id: dict[str, dict] = {}

    for rank, r in enumerate(keyword):
        cid = r["chunk_id"]
        scores[cid] = scores.get(cid, 0.0) + 1.0 / (rank + settings.rrf_rank_offset)
        by_id[cid] = {**r, "match_type": "hybrid"}

    for rank, r in enumerate(semantic):
        cid = r["chunk_id"]
        scores[cid] = scores.get(cid, 0.0) + 1.0 / (rank + settings.rrf_rank_offset)
        if cid not in by_id:
            by_id[cid] = {**r, "match_type": "hybrid"}

    merged = sorted(by_id.values(), key=lambda x: scores[x["chunk_id"]], reverse=True)
    for item in merged:
        item["score"] = round(scores[item["chunk_id"]], 6)
    return merged[:top_k]


@router.post("/search", response_model=SearchResponse)
def search(
    req: SearchRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    if req.mode == "keyword":
        results = _keyword_search(db, req.query, req.top_k, req.content_types, req.authority_levels)
    elif req.mode == "semantic":
        results = _semantic_search(req.query, req.top_k, req.content_types, req.authority_levels, db)
    else:
        kw = _keyword_search(db, req.query, req.top_k, req.content_types, req.authority_levels)
        sem = _semantic_search(req.query, req.top_k, req.content_types, req.authority_levels, db)
        results = _merge_results(kw, sem, req.top_k)

    return SearchResponse(
        query=req.query,
        mode=req.mode,
        results=[SearchResult(**r) for r in results],
        total=len(results),
    )
