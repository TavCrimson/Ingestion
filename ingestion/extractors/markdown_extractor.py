from __future__ import annotations

import re
from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase

_FRONTMATTER_RE = re.compile(r"^---\s*\n.*?\n---\s*\n", re.DOTALL)


class MarkdownExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        text = path.read_text(encoding="utf-8", errors="replace")
        frontmatter_meta = {}
        match = _FRONTMATTER_RE.match(text)
        if match:
            fm_block = match.group(0)
            text = text[len(fm_block):]
            try:
                import yaml
                frontmatter_meta = yaml.safe_load(fm_block.strip("---\n")) or {}
            except Exception:
                pass
        return ExtractedText(text=text.strip(), metadata=frontmatter_meta)
