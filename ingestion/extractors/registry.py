"""Maps file extensions and MIME types to extractor classes."""
from __future__ import annotations

from pathlib import Path

from ingestion.extractors.base import ExtractorBase
from ingestion.extractors.pdf_extractor import PdfExtractor
from ingestion.extractors.docx_extractor import DocxExtractor
from ingestion.extractors.pptx_extractor import PptxExtractor
from ingestion.extractors.html_extractor import HtmlExtractor
from ingestion.extractors.markdown_extractor import MarkdownExtractor
from ingestion.extractors.json_extractor import JsonExtractor
from ingestion.extractors.txt_extractor import TxtExtractor

_EXT_MAP: dict[str, type[ExtractorBase]] = {
    ".pdf": PdfExtractor,
    ".docx": DocxExtractor,
    ".doc": DocxExtractor,
    ".pptx": PptxExtractor,
    ".ppt": PptxExtractor,
    ".html": HtmlExtractor,
    ".htm": HtmlExtractor,
    ".md": MarkdownExtractor,
    ".markdown": MarkdownExtractor,
    ".json": JsonExtractor,
    ".txt": TxtExtractor,
    ".text": TxtExtractor,
    ".csv": TxtExtractor,
}

_MIME_MAP: dict[str, type[ExtractorBase]] = {
    "application/pdf": PdfExtractor,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DocxExtractor,
    "application/msword": DocxExtractor,
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": PptxExtractor,
    "application/vnd.ms-powerpoint": PptxExtractor,
    "text/html": HtmlExtractor,
    "text/markdown": MarkdownExtractor,
    "application/json": JsonExtractor,
    "text/plain": TxtExtractor,
    "text/csv": TxtExtractor,
}


def get_extractor(path: Path, mime_type: str | None = None) -> ExtractorBase:
    if mime_type:
        cls = _MIME_MAP.get(mime_type)
        if cls:
            return cls()
    ext = path.suffix.lower()
    cls = _EXT_MAP.get(ext, TxtExtractor)
    return cls()


def supported_extensions() -> list[str]:
    return list(_EXT_MAP.keys())
