from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ExtractedText:
    text: str
    metadata: dict = field(default_factory=dict)
    pages: int | None = None
    warnings: list[str] = field(default_factory=list)


class ExtractorBase:
    def extract(self, path: Path) -> ExtractedText:
        raise NotImplementedError
