"""Singleton sentence-transformer encoder."""
from __future__ import annotations

import os
from pathlib import Path

from ingestion.config import settings


class Encoder:
    _instance: "Encoder | None" = None
    _model = None

    def __init__(self):
        os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", str(settings.models_dir))
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(settings.embedding_model)

    @classmethod
    def get(cls) -> "Encoder":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def encode(self, texts: list[str]) -> list[list[float]]:
        """Encode a list of texts and return a list of float vectors."""
        embeddings = self._model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
        return embeddings.tolist()

    def encode_one(self, text: str) -> list[float]:
        return self.encode([text])[0]
