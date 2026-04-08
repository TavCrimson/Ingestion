"""Tests for DB-backed extracted text storage."""
import json
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_s03_writes_extracted_text_to_raw_doc(db, tmp_raw_dir):
    """Stage 3 must persist extracted text on RawDocument.extracted_text."""
    from ingestion.db import crud
    from ingestion.pipeline.stages import s03_extraction

    # Create a real text file to extract from
    doc_file = tmp_raw_dir / "sample.txt"
    doc_file.write_text("Hello world content", encoding="utf-8")

    raw = crud.create_raw_doc(
        db,
        filename="sample.txt",
        original_path=str(doc_file),
        stored_path=str(doc_file),
        file_hash="112233",
        file_size_bytes=19,
    )
    db.commit()

    s03_extraction.run(raw.id, db)

    db.refresh(raw)
    assert raw.extracted_text is not None
    assert "Hello world" in json.loads(raw.extracted_text)["text"]


def test_load_extracted_reads_from_db_first(db, tmp_raw_dir):
    """load_extracted must return DB text when available, even if sidecar is stale."""
    from ingestion.db import crud
    from ingestion.pipeline.stages.s03_extraction import load_extracted

    doc_file = tmp_raw_dir / "db_test.txt"
    doc_file.write_text("original content", encoding="utf-8")

    raw = crud.create_raw_doc(
        db,
        filename="db_test.txt",
        original_path=str(doc_file),
        stored_path=str(doc_file),
        file_hash="445566",
        file_size_bytes=16,
    )
    # Write DB text directly
    raw.extracted_text = json.dumps({"text": "DB text wins", "metadata": {}, "warnings": []})
    db.commit()

    # Write stale sidecar
    sidecar = Path(str(doc_file) + ".extracted.json")
    sidecar.write_text(
        json.dumps({"text": "stale sidecar", "metadata": {}, "warnings": []}),
        encoding="utf-8"
    )

    result = load_extracted(raw.id, db)
    assert result["text"] == "DB text wins"


def test_load_extracted_falls_back_to_sidecar(db, tmp_raw_dir):
    """load_extracted falls back to sidecar if DB field is empty."""
    from ingestion.db import crud
    from ingestion.pipeline.stages.s03_extraction import load_extracted

    doc_file = tmp_raw_dir / "sidecar_test.txt"
    doc_file.write_text("text", encoding="utf-8")

    raw = crud.create_raw_doc(
        db,
        filename="sidecar_test.txt",
        original_path=str(doc_file),
        stored_path=str(doc_file),
        file_hash="778899",
        file_size_bytes=4,
    )
    db.commit()
    # extracted_text is NULL

    sidecar = Path(str(doc_file) + ".extracted.json")
    sidecar.write_text(
        json.dumps({"text": "sidecar content", "metadata": {}, "warnings": []}),
        encoding="utf-8"
    )

    result = load_extracted(raw.id, db)
    assert result["text"] == "sidecar content"
