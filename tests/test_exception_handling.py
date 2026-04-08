"""Verify that swallowed exceptions are now logged, not silently dropped."""
import logging
from unittest.mock import MagicMock


def test_keyword_search_logs_fts_error(caplog):
    """When FTS5 raises, _keyword_search must log the error and return []."""
    from ingestion.api.routers.search import _keyword_search

    bad_db = MagicMock()
    bad_db.execute.side_effect = Exception("FTS5 table missing")

    with caplog.at_level(logging.ERROR, logger="ingestion.api.routers.search"):
        result = _keyword_search(bad_db, "query", 5, None, None)

    assert result == []
    assert any(
        "FTS5" in r.message or "fts" in r.message.lower() or "table missing" in r.message.lower()
        for r in caplog.records
    )


def test_dedup_vector_error_is_logged(caplog):
    """When encoder/vector store raises during similarity check, error must be logged."""
    from unittest.mock import patch, MagicMock
    import ingestion.pipeline.stages.s08_deduplication as dedup_mod

    mock_db = MagicMock()
    mock_raw_doc = MagicMock()
    mock_raw_doc.file_hash = "abc123"
    mock_raw_doc.id = "raw1"

    with patch.object(dedup_mod, "crud") as mock_crud, \
         patch.object(dedup_mod, "load_extracted") as mock_load, \
         caplog.at_level(logging.WARNING, logger="ingestion.pipeline.stages.s08_deduplication"):

        mock_crud.get_raw_doc.return_value = mock_raw_doc
        # Simulate no exact-hash duplicate
        mock_crud.RawDocument = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        mock_load.return_value = {"text": "some text content here", "metadata": {}, "warnings": []}

        # Patch Encoder to raise so the except block is hit
        with patch("ingestion.embeddings.encoder.Encoder") as mock_enc:
            mock_enc.get.side_effect = RuntimeError("model not loaded")
            result = dedup_mod.run("raw1", mock_db)

    # Should return a result dict (not crash)
    assert "is_duplicate" in result
    # Must have logged something
    assert len(caplog.records) > 0
