from __future__ import annotations

from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase


class TxtExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        text = path.read_text(encoding="utf-8", errors="replace")
        return ExtractedText(text=text)
