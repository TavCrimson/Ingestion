"""POST /v1/chat — RAG-grounded chat agent over the knowledge repository."""
from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ingestion.api.rate_limit import rate_limit
from ingestion.config import settings
from ingestion.db.engine import get_db

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Schemas ──────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    mode: Literal["standard", "deep"] = "standard"


class ChatResponse(BaseModel):
    reply: str
    sources: list[dict]   # [{title, doc_id, score}]
    hops: int = 1         # number of search calls made (>1 means deep mode was used)


# ── Tool definition for deep/agentic mode ────────────────────────────────────

SEARCH_TOOL = {
    "name": "search_knowledge_base",
    "description": (
        "Search the knowledge base using hybrid semantic and keyword search. "
        "Call this tool with a focused query to retrieve relevant excerpts. "
        "Call it multiple times with different queries to gather comprehensive information "
        "before writing your final answer."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A focused search query to find relevant information."
            }
        },
        "required": ["query"]
    }
}


# ── Helper: single-pass retrieval ────────────────────────────────────────────

def _retrieve_context(query: str, db: Session) -> tuple[str, list[dict]]:
    """
    Hybrid search (semantic + FTS keyword → Reciprocal Rank Fusion) over
    published chunks, returning (formatted_context_string, source_list).
    """
    from ingestion.api.routers.search import (
        _keyword_search,
        _semantic_search,
        _merge_results,
    )
    from ingestion.db.models import CanonicalDocument

    k = settings.chat_context_chunks

    kw_hits  = _keyword_search(db, query, k, None, None)
    sem_hits = _semantic_search(query, k, None, None, db)
    hits     = _merge_results(kw_hits, sem_hits, k)

    if not hits:
        return "", []

    context_parts = []
    sources = []

    for hit in hits:
        doc = db.query(CanonicalDocument).filter(
            CanonicalDocument.id == hit["canonical_doc_id"]
        ).first()

        doc_title = doc.title if doc else "Unknown document"
        heading = f" › {hit['heading_context']}" if hit.get("heading_context") else ""

        context_parts.append(
            f"[Source: {doc_title}{heading}]\n{hit['text']}"
        )
        sources.append({
            "title": doc_title,
            "doc_id": hit.get("canonical_doc_id", ""),
            "score": round(hit.get("score", 0), 4),
        })

    return "\n\n---\n\n".join(context_parts), sources


# ── Helper: agentic multi-hop retrieval ──────────────────────────────────────

def _agentic_chat(
    body_messages: list[ChatMessage],
    db: Session,
) -> tuple[str, list[dict], int]:
    """
    Give Claude a search tool and let it call the knowledge base as many times
    as it needs (up to MAX_HOPS) before writing its final answer.
    Returns (reply, deduplicated_sources, hop_count).
    """
    import anthropic

    MAX_HOPS = 6

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    system = (
        "You are a knowledgeable assistant for a product knowledge repository. "
        "Use the search_knowledge_base tool to find relevant information before answering. "
        "Search multiple times with different, focused queries if the first results are "
        "incomplete or if the question has multiple parts. "
        "Only answer based on what you find — do not invent information. "
        "When referencing information, mention the source document name."
    )

    messages = [{"role": m.role, "content": m.content} for m in body_messages]

    all_sources: list[dict] = []
    seen_chunk_ids: set[str] = set()
    hops = 0

    for _ in range(MAX_HOPS):
        response = client.messages.create(
            model=settings.chat_model,
            max_tokens=1024,
            system=system,
            tools=[SEARCH_TOOL],
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            reply = next(
                (b.text for b in response.content if hasattr(b, "text")), ""
            )
            return reply, all_sources, hops

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []

            for block in response.content:
                if block.type != "tool_use" or block.name != "search_knowledge_base":
                    continue

                query = block.input.get("query", "")
                hops += 1
                context, sources = _retrieve_context(query, db)

                # Deduplicate sources across hops
                for s in sources:
                    cid = s.get("chunk_index", "")
                    if cid not in seen_chunk_ids:
                        seen_chunk_ids.add(cid)
                        all_sources.append(s)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": context if context else "No results found for this query.",
                })

            messages.append({"role": "user", "content": tool_results})

        else:
            # Unexpected stop reason — extract any text and return
            break

    # Fallback: extract text from last response if available
    try:
        reply = next((b.text for b in response.content if hasattr(b, "text")), "")
    except Exception:
        reply = "Could not generate a response after searching the knowledge base."

    return reply, all_sources, hops


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/chat", response_model=ChatResponse)
def chat(
    body: ChatRequest,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="Chat is not configured. Add ANTHROPIC_API_KEY to your .env file.",
        )

    if not body.messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    try:
        if body.mode == "deep":
            reply, sources, hops = _agentic_chat(body.messages, db)
        else:
            # Standard mode: single-pass retrieval
            user_query = next(
                (m.content for m in reversed(body.messages) if m.role == "user"), ""
            )
            context, sources = _retrieve_context(user_query, db)

            if context:
                system_prompt = (
                    "You are a knowledgeable assistant for a product knowledge repository. "
                    "Answer questions using ONLY the context excerpts provided below. "
                    "If the answer isn't in the context, say so clearly — do not invent information. "
                    "When referencing information, mention the source document name.\n\n"
                    "CONTEXT FROM KNOWLEDGE REPOSITORY:\n\n"
                    + context
                )
            else:
                system_prompt = (
                    "You are a knowledgeable assistant for a product knowledge repository. "
                    "No relevant documents were found for this query. "
                    "Let the user know their question couldn't be matched to any published content, "
                    "and suggest they check that relevant documents have been ingested and approved."
                )

            import anthropic
            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
            response = client.messages.create(
                model=settings.chat_model,
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": m.role, "content": m.content} for m in body.messages],
            )
            reply = response.content[0].text
            hops = 1

    except Exception as exc:
        logger.exception("Chat failed")
        raise HTTPException(status_code=502, detail=f"LLM error: {exc}") from exc

    return ChatResponse(reply=reply, sources=sources, hops=hops)
