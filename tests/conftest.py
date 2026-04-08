"""Shared pytest fixtures for the ingestion test suite."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from ingestion.db.engine import Base
from ingestion.db.models import ApiKey
from ingestion.storage.file_hash import sha256_string


@pytest.fixture()
def db():
    """In-memory SQLite session with all tables and FTS5 triggers created."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine, "connect")
    def set_pragmas(conn, _):
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(bind=engine)

    with engine.connect() as conn:
        conn.execute(text(
            "CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts "
            "USING fts5(chunk_id UNINDEXED, text)"
        ))
        conn.execute(text(
            "CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN "
            "INSERT INTO chunks_fts(rowid, chunk_id, text) VALUES (new.rowid, new.id, new.text); "
            "END"
        ))
        conn.execute(text(
            "CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN "
            "INSERT INTO chunks_fts(chunks_fts, rowid, chunk_id, text) "
            "VALUES ('delete', old.rowid, old.id, old.text); "
            "END"
        ))
        conn.commit()

    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture()
def tmp_raw_dir(tmp_path):
    """Temporary directory for raw document storage."""
    d = tmp_path / "raw"
    d.mkdir()
    return d


@pytest.fixture()
def mock_vector_store():
    """Mock ChromaDB vector store — returns empty results, records upserts."""
    store = MagicMock()
    store.query.return_value = []
    store.upsert.return_value = None
    store.delete.return_value = None
    return store


@pytest.fixture()
def api_key(db):
    """Seed one active API key and return the raw string."""
    raw = "test-key-abc123"
    key = ApiKey(
        key_hash=sha256_string(raw),
        label="test",
        rate_limit_per_minute=10,
    )
    db.add(key)
    db.commit()
    return raw
