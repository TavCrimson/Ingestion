"""Stage 3: Extraction — extract text from the raw document."""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.extractors.registry import get_extractor


def run(raw_doc_id: str, db: Session) -> str:
    """Extract text, write sidecar, and persist to RawDocument.extracted_text. Returns text."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    path = Path(raw_doc.stored_path)
    extractor = get_extractor(path, raw_doc.mime_type)
    result = extractor.extract(path)

    payload = json.dumps({"text": result.text, "metadata": result.metadata, "warnings": result.warnings})

    # Primary: persist to DB
    raw_doc.extracted_text = payload
    db.flush()

    # Secondary: write sidecar for backwards compatibility
    sidecar_path = path.with_suffix(path.suffix + ".extracted.json")
    try:
        sidecar_path.write_text(payload, encoding="utf-8")
    except OSError:
        pass  # sidecar is a best-effort cache; DB is the source of truth

    return result.text


def load_extracted(raw_doc_id: str, db: Session) -> dict:
    """Load previously extracted text. Prefers DB; falls back to sidecar."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)

    # Primary: DB field
    if raw_doc.extracted_text:
        try:
            return json.loads(raw_doc.extracted_text)
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: sidecar file
    path = Path(raw_doc.stored_path)
    sidecar_path = path.with_suffix(path.suffix + ".extracted.json")
    if sidecar_path.exists():
        return json.loads(sidecar_path.read_text(encoding="utf-8"))

    return {"text": "", "metadata": {}, "warnings": []}
