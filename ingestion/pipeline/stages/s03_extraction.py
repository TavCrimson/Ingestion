"""Stage 3: Extraction — extract text from the raw document."""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from ingestion.db import crud
from ingestion.extractors.registry import get_extractor


# Store extracted text in the canonical layer's title field temporarily,
# and in a side-car text file for use by later stages.

def run(raw_doc_id: str, db: Session) -> str:
    """Extract text and write sidecar. Returns extracted text."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    path = Path(raw_doc.stored_path)
    extractor = get_extractor(path, raw_doc.mime_type)
    result = extractor.extract(path)

    # Write sidecar JSON so later stages can read without re-extracting
    sidecar_path = path.with_suffix(path.suffix + ".extracted.json")
    sidecar_path.write_text(
        json.dumps({"text": result.text, "metadata": result.metadata, "warnings": result.warnings}),
        encoding="utf-8",
    )

    return result.text


def load_extracted(raw_doc_id: str, db: Session) -> dict:
    """Load previously extracted text from the sidecar file."""
    raw_doc = crud.get_raw_doc(db, raw_doc_id)
    path = Path(raw_doc.stored_path)
    sidecar_path = path.with_suffix(path.suffix + ".extracted.json")
    if sidecar_path.exists():
        return json.loads(sidecar_path.read_text(encoding="utf-8"))
    return {"text": "", "metadata": {}, "warnings": []}
