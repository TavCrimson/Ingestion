from __future__ import annotations

from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase


class DocxExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        from docx import Document
        doc = Document(str(path))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n\n".join(paragraphs)
        metadata = {}
        try:
            core = doc.core_properties
            metadata = {
                "title": core.title,
                "author": core.author,
                "created": str(core.created) if core.created else None,
                "modified": str(core.modified) if core.modified else None,
            }
        except Exception:
            pass
        return ExtractedText(text=text, metadata=metadata)
