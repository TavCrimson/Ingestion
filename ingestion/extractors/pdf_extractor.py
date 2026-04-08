from __future__ import annotations

from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase


class PdfExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        warnings = []
        text = self._pdfminer(path, warnings)
        if not text.strip():
            text = self._pypdf(path, warnings)
        return ExtractedText(text=text, metadata={}, warnings=warnings)

    def _pdfminer(self, path: Path, warnings: list) -> str:
        try:
            from pdfminer.high_level import extract_text
            return extract_text(str(path))
        except Exception as e:
            warnings.append(f"pdfminer failed: {e}")
            return ""

    def _pypdf(self, path: Path, warnings: list) -> str:
        try:
            from pypdf import PdfReader
            reader = PdfReader(str(path))
            pages = [page.extract_text() or "" for page in reader.pages]
            return "\n".join(pages)
        except Exception as e:
            warnings.append(f"pypdf failed: {e}")
            return ""
