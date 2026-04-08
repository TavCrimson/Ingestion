from __future__ import annotations

import json
from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase


def _flatten(obj, parts: list, depth: int = 0) -> None:
    if depth > 10:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            parts.append(str(k))
            _flatten(v, parts, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _flatten(item, parts, depth + 1)
    elif obj is not None:
        parts.append(str(obj))


class JsonExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        raw = path.read_text(encoding="utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return ExtractedText(text=raw, warnings=["Invalid JSON, using raw text"])
        parts: list[str] = []
        _flatten(data, parts)
        return ExtractedText(text="\n".join(parts), metadata={})
