"""Stage 4: Cleaning — remove noise, normalise whitespace."""
from __future__ import annotations

import re

from sqlalchemy.orm import Session

from ingestion.pipeline.stages.s03_extraction import load_extracted, run as s03_run
from ingestion.db import crud


# Patterns for common boilerplate
_PAGE_NUM_RE = re.compile(r"\bPage\s+\d+\s+of\s+\d+\b", re.IGNORECASE)
_MULTIPLE_BLANK_RE = re.compile(r"\n{3,}")
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def clean(text: str) -> str:
    text = _CONTROL_CHARS_RE.sub("", text)
    text = _PAGE_NUM_RE.sub("", text)
    # Normalise line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Collapse excessive blank lines
    text = _MULTIPLE_BLANK_RE.sub("\n\n", text)
    # Strip trailing whitespace per line
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines).strip()


def run(raw_doc_id: str, db: Session) -> str:
    """Clean extracted text, update DB field and sidecar. Returns cleaned text."""
    import json
    from pathlib import Path

    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    path = Path(raw_doc.stored_path)
    sidecar_path = path.with_suffix(path.suffix + ".extracted.json")

    data = load_extracted(raw_doc_id, db)
    cleaned = clean(data["text"])
    data["text"] = cleaned

    payload = json.dumps(data)

    # Update DB field (primary)
    raw_doc.extracted_text = payload
    db.flush()

    # Update sidecar (best-effort)
    try:
        sidecar_path.write_text(payload, encoding="utf-8")
    except OSError:
        pass

    return cleaned
