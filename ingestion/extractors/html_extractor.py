from __future__ import annotations

from pathlib import Path

from ingestion.extractors.base import ExtractedText, ExtractorBase


class HtmlExtractor(ExtractorBase):
    def extract(self, path: Path) -> ExtractedText:
        from bs4 import BeautifulSoup
        html = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
        return ExtractedText(
            text="\n".join(lines),
            metadata={"title": title},
        )
