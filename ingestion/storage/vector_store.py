"""ChromaDB wrapper for semantic chunk storage and retrieval."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import chromadb
from chromadb.config import Settings as ChromaSettings

from ingestion.config import settings


class VectorStore:
    COLLECTION_NAME = "chunks"

    def __init__(self, persist_dir: Path | None = None):
        path = str(persist_dir or settings.chroma_dir)
        self._client = chromadb.PersistentClient(
            path=path,
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    def upsert(
        self,
        chunk_id: str,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> None:
        self._collection.upsert(
            ids=[chunk_id],
            documents=[text],
            embeddings=[embedding],
            metadatas=[metadata],
        )

    def query(
        self,
        embedding: list[float],
        top_k: int = 5,
        where: dict | None = None,
    ) -> list[dict]:
        kwargs: dict[str, Any] = {
            "query_embeddings": [embedding],
            "n_results": top_k,
            "include": ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where

        result = self._collection.query(**kwargs)

        hits = []
        ids = result["ids"][0]
        docs = result["documents"][0]
        metas = result["metadatas"][0]
        dists = result["distances"][0]
        for chunk_id, doc, meta, dist in zip(ids, docs, metas, dists):
            hits.append({
                "chunk_id": chunk_id,
                "text": doc,
                "metadata": meta,
                "distance": dist,
                "score": 1.0 - dist,  # cosine similarity
            })
        return hits

    def delete(self, chunk_id: str) -> None:
        self._collection.delete(ids=[chunk_id])

    def count(self) -> int:
        return self._collection.count()


vector_store = VectorStore()
