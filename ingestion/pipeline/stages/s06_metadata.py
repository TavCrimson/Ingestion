"""Stage 6: Metadata generation — extract title, version, author."""
from __future__ import annotations

import re
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.pipeline.stages.s03_extraction import load_extracted

_PATTERNS_PATH = Path(__file__).parents[3] / "patterns.yaml"

_VERSION_FALLBACK_RE = re.compile(r"[Vv]ersion\s+(\d+\.\d+[\w.]*)")
_TITLE_RE = re.compile(r"^#\s+(.+)$", re.MULTILINE)


def _load_version_patterns() -> list[str]:
    with open(_PATTERNS_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("version_patterns", [])


def extract_metadata(filename: str, text: str, extracted_meta: dict) -> dict:
    meta = dict(extracted_meta)

    # Title: prefer extracted, then first H1, then filename stem
    if not meta.get("title"):
        m = _TITLE_RE.search(text)
        if m:
            meta["title"] = m.group(1).strip()
        else:
            meta["title"] = Path(filename).stem.replace("_", " ").replace("-", " ").title()

    # Version
    if not meta.get("version"):
        patterns = _load_version_patterns()
        for pattern in patterns:
            m = re.search(pattern, text)
            if m:
                meta["version"] = m.group(0).strip()
                break
        if not meta.get("version"):
            m = _VERSION_FALLBACK_RE.search(text)
            if m:
                meta["version"] = m.group(1)

    return meta


def run(raw_doc_id: str, db: Session) -> dict:
    """Extract metadata and update CanonicalDocument. Returns metadata dict."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    data = load_extracted(raw_doc_id, db)
    meta = extract_metadata(raw_doc.filename, data["text"], data.get("metadata", {}))

    canonical = crud.get_canonical_by_raw(db, raw_doc_id)
    if canonical:
        canonical.title = meta.get("title")
        canonical.version = meta.get("version")
        db.commit()

    return meta
