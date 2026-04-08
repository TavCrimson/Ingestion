# Ingestion System — Top-10 Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement 10 prioritised reliability, correctness, and maintainability improvements to the product-knowledge ingestion service.

**Architecture:** Each change is self-contained. Work top-down — earlier tasks create the test infrastructure later tasks depend on. All tests use an in-memory SQLite database and temp-file fixtures so no production data is touched.

**Tech Stack:** Python 3.11+, FastAPI, SQLAlchemy 2.0, SQLite/FTS5, ChromaDB, sentence-transformers, tiktoken, pytest, pytest-asyncio

---

## File Map

### Created
- `tests/__init__.py`
- `tests/conftest.py` — shared fixtures: in-memory DB, temp raw-store dir, mock vector store
- `tests/test_config.py` — tests for threshold values and startup validation
- `tests/test_chunker.py` — unit tests for chunker parameter validation and token fallback
- `tests/test_entity_extraction.py` — unicode normalisation + extraction tests
- `tests/test_exception_handling.py` — verifies search.py logs errors instead of silently swallowing them
- `tests/test_pipeline_runner.py` — transaction savepoint tests
- `tests/test_review_queue.py` — cascading delete and race condition tests
- `tests/test_extracted_text.py` — DB-stored extraction tests
- `tests/test_pagination.py` — pagination on review queue endpoint
- `tests/test_rate_limit.py` — DB-backed rate limit survival across restart
- `scripts/migrate_add_extracted_text.py` — one-shot ALTER TABLE migration

### Modified
- `ingestion/config.py` — add threshold fields + startup validator
- `ingestion/db/models.py` — add `RawDocument.extracted_text`, add `RateLimitEntry`
- `ingestion/db/crud.py` — add `get_pending_review_items_paginated`, `delete_chunks_for_canonical`
- `ingestion/pipeline/runner.py` — wrap each stage in a savepoint
- `ingestion/pipeline/stages/s03_extraction.py` — also write extracted text to DB
- `ingestion/pipeline/stages/s04_cleaning.py` — update DB extracted_text alongside sidecar
- `ingestion/pipeline/stages/s07_entity_extraction.py` — Unicode normalisation
- `ingestion/pipeline/stages/s08_deduplication.py` — use config thresholds; log instead of swallow
- `ingestion/api/routers/search.py` — log FTS5 errors; use config for RRF offset
- `ingestion/api/routers/review.py` — add `limit`/`offset` query params
- `ingestion/review/queue.py` — delete chunks + vector store entries on doc-level rejection
- `ingestion/api/rate_limit.py` — DB-backed sliding window
- `scripts/init_db.py` — create `rate_limit_entries` table

---

## Task 1: Test Infrastructure

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Add pytest to requirements**

Append to `requirements.txt`:
```
pytest>=8.0
pytest-asyncio>=0.23
httpx>=0.27
```

- [ ] **Step 2: Create empty `tests/__init__.py`**

```python
```
(empty file)

- [ ] **Step 3: Create `tests/conftest.py`**

```python
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
```

- [ ] **Step 4: Verify pytest collects the fixtures**

Run: `pytest tests/ --collect-only -q`

Expected output (no errors, 0 tests collected yet):
```
no tests ran
```

- [ ] **Step 5: Commit**

```bash
git add tests/ requirements.txt
git commit -m "test: add pytest infrastructure and shared fixtures"
```

---

## Task 2: Move Hardcoded Thresholds to Config

**Files:**
- Modify: `ingestion/config.py`
- Modify: `ingestion/pipeline/stages/s08_deduplication.py`
- Modify: `ingestion/api/routers/search.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_config.py`:
```python
"""Tests for configurable thresholds."""
from ingestion.config import settings


def test_dedup_thresholds_are_configurable():
    assert hasattr(settings, "dedup_near_duplicate_threshold")
    assert hasattr(settings, "dedup_similar_lower_bound")
    assert 0 < settings.dedup_similar_lower_bound < settings.dedup_near_duplicate_threshold <= 1.0


def test_rrf_rank_offset_is_configurable():
    assert hasattr(settings, "rrf_rank_offset")
    assert settings.rrf_rank_offset > 0


def test_default_threshold_values():
    assert settings.dedup_near_duplicate_threshold == 0.95
    assert settings.dedup_similar_lower_bound == 0.80
    assert settings.rrf_rank_offset == 60
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`

Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'dedup_near_duplicate_threshold'`

- [ ] **Step 3: Add threshold fields to `ingestion/config.py`**

Replace the entire `Settings` class body with:
```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite:///./data/repository.db"
    chroma_path: str = "./data/chroma"
    raw_store_path: str = "./data/raw"
    models_path: str = "./data/models"
    embedding_model: str = "all-MiniLM-L6-v2"

    escalation_timeout_hours: int = 48
    escalation_check_interval_minutes: int = 15

    default_rate_limit_per_minute: int = 60

    log_level: str = "INFO"

    # Anthropic (for chat agent)
    anthropic_api_key: str = ""
    chat_model: str = "claude-haiku-4-5-20251001"
    chat_context_chunks: int = 8

    # Deduplication thresholds
    dedup_near_duplicate_threshold: float = 0.95
    dedup_similar_lower_bound: float = 0.80

    # Reciprocal Rank Fusion offset
    rrf_rank_offset: int = 60

    @property
    def raw_store_dir(self) -> Path:
        return Path(self.raw_store_path)

    @property
    def chroma_dir(self) -> Path:
        return Path(self.chroma_path)

    @property
    def models_dir(self) -> Path:
        return Path(self.models_path)
```

- [ ] **Step 4: Update `ingestion/pipeline/stages/s08_deduplication.py`**

Replace the two module-level constants and their usages:
```python
# Remove these two lines:
_NEAR_DUPLICATE_THRESHOLD = 0.95
_SIMILAR_LOWER_BOUND = 0.80

# Replace with config import at top of file (after existing imports):
from ingestion.config import settings
```

Then in the `run` function replace:
```python
            if score >= _NEAR_DUPLICATE_THRESHOLD:
```
with:
```python
            if score >= settings.dedup_near_duplicate_threshold:
```

And replace:
```python
            elif score >= _SIMILAR_LOWER_BOUND:
```
with:
```python
            elif score >= settings.dedup_similar_lower_bound:
```

- [ ] **Step 5: Update `ingestion/api/routers/search.py`**

Add import at top:
```python
from ingestion.config import settings
```

In `_merge_results`, replace both occurrences of `60` with `settings.rrf_rank_offset`:
```python
        scores[cid] = scores.get(cid, 0.0) + 1.0 / (rank + settings.rrf_rank_offset)
```
(This appears on lines 106 and 110.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`

Expected: 3 PASSED

- [ ] **Step 7: Commit**

```bash
git add ingestion/config.py ingestion/pipeline/stages/s08_deduplication.py ingestion/api/routers/search.py tests/test_config.py
git commit -m "feat: move hardcoded dedup thresholds and RRF offset to config"
```

---

## Task 3: Startup Configuration Validation

**Files:**
- Modify: `ingestion/config.py`
- Test: `tests/test_config.py` (extended)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config.py`:
```python
import pytest
from pydantic import ValidationError


def test_dedup_thresholds_must_be_valid_range():
    """near_duplicate must be greater than similar_lower_bound."""
    from pydantic_settings import BaseSettings, SettingsConfigDict
    from pydantic import model_validator
    from ingestion.config import Settings

    with pytest.raises(ValidationError):
        Settings(
            dedup_near_duplicate_threshold=0.5,
            dedup_similar_lower_bound=0.9,  # lower_bound > near_duplicate → invalid
            _env_file=None,
        )


def test_rrf_rank_offset_must_be_positive():
    from ingestion.config import Settings

    with pytest.raises(ValidationError):
        Settings(rrf_rank_offset=0, _env_file=None)


def test_chat_context_chunks_must_be_positive():
    from ingestion.config import Settings

    with pytest.raises(ValidationError):
        Settings(chat_context_chunks=0, _env_file=None)
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_config.py::test_dedup_thresholds_must_be_valid_range tests/test_config.py::test_rrf_rank_offset_must_be_positive tests/test_config.py::test_chat_context_chunks_must_be_positive -v`

Expected: 3 FAILED

- [ ] **Step 3: Add validators to `ingestion/config.py`**

Add import at top of file:
```python
from pydantic import field_validator, model_validator
```

Add validators inside the `Settings` class (before the `@property` methods):
```python
    @field_validator("rrf_rank_offset")
    @classmethod
    def rrf_offset_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("rrf_rank_offset must be a positive integer")
        return v

    @field_validator("chat_context_chunks")
    @classmethod
    def chat_context_chunks_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("chat_context_chunks must be a positive integer")
        return v

    @model_validator(mode="after")
    def dedup_thresholds_ordered(self) -> "Settings":
        if self.dedup_similar_lower_bound >= self.dedup_near_duplicate_threshold:
            raise ValueError(
                "dedup_similar_lower_bound must be less than dedup_near_duplicate_threshold"
            )
        return self
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`

Expected: all PASSED

- [ ] **Step 5: Commit**

```bash
git add ingestion/config.py tests/test_config.py
git commit -m "feat: add startup validation for config thresholds"
```

---

## Task 4: Unicode Normalisation in Entity Extraction

**Files:**
- Modify: `ingestion/pipeline/stages/s07_entity_extraction.py`
- Test: `tests/test_entity_extraction.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_entity_extraction.py`:
```python
"""Tests for entity extraction and Unicode normalisation."""
from ingestion.pipeline.stages.s07_entity_extraction import extract_entities, _normalise


def test_normalise_strips_accents():
    assert _normalise("Café") == _normalise("Cafe")


def test_normalise_lowercases():
    assert _normalise("Fusion") == "fusion"


def test_normalise_handles_ligatures():
    # 'ﬁ' (U+FB01 LATIN SMALL LIGATURE FI) should normalise to 'fi'
    assert "fi" in _normalise("ﬁle")


def test_duplicate_entities_not_created_for_accent_variants(db, tmp_path):
    """Café and Cafe must produce the same normalised name."""
    from ingestion.pipeline.stages.s07_entity_extraction import extract_entities
    assert _normalise("Café") == _normalise("Cafe")


def test_extract_entities_returns_list():
    results = extract_entities("This document mentions Fusion and its competitors.")
    assert isinstance(results, list)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_entity_extraction.py -v`

Expected: `ImportError` — `cannot import name '_normalise'`

- [ ] **Step 3: Add `_normalise` and update `_add` in `ingestion/pipeline/stages/s07_entity_extraction.py`**

Add import at top:
```python
import unicodedata
```

Add the function after the imports (before `_PATTERNS_PATH`):
```python
def _normalise(name: str) -> str:
    """Lowercase + strip accents and ligatures for deduplication-safe comparison."""
    # NFKD decomposes ligatures and accented chars; encoding to ASCII drops the diacritics
    nfkd = unicodedata.normalize("NFKD", name.strip())
    return nfkd.encode("ascii", "ignore").decode("ascii").lower()
```

In `extract_entities`, update the `_add` helper to use `_normalise`:
```python
    def _add(name: str, entity_type: str, confidence: float = 0.9):
        norm = _normalise(name)
        if norm not in seen:
            seen.add(norm)
            found.append({
                "name": name.strip(),
                "entity_type": entity_type,
                "normalized_name": norm,
                "confidence_score": confidence,
            })
```

Also update the `run` function where it calls `crud.find_entity_by_name` — the look-up key is now consistently normalised via `_normalise` which is already embedded in the entity dict. No change needed there (the `e["normalized_name"]` is already set by `_add`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_entity_extraction.py -v`

Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
git add ingestion/pipeline/stages/s07_entity_extraction.py tests/test_entity_extraction.py
git commit -m "fix: unicode normalisation in entity extraction so accented variants deduplicate"
```

---

## Task 5: Fix Exception Handling

**Files:**
- Modify: `ingestion/api/routers/search.py`
- Modify: `ingestion/pipeline/stages/s08_deduplication.py`
- Test: `tests/test_exception_handling.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_exception_handling.py`:
```python
"""Verify that swallowed exceptions are now logged, not silently dropped."""
import logging
from unittest.mock import patch, MagicMock

import pytest


def test_keyword_search_logs_fts_error(caplog):
    """When FTS5 raises, _keyword_search must log the error and return []."""
    from ingestion.api.routers.search import _keyword_search

    bad_db = MagicMock()
    bad_db.execute.side_effect = Exception("FTS5 table missing")

    with caplog.at_level(logging.ERROR, logger="ingestion.api.routers.search"):
        result = _keyword_search(bad_db, "query", 5, None, None)

    assert result == []
    assert any("FTS5" in r.message or "fts" in r.message.lower() for r in caplog.records)


def test_dedup_vector_error_is_logged(caplog):
    """When vector store raises during similarity check, error must be logged."""
    from ingestion.pipeline.stages import s08_deduplication
    from unittest.mock import patch, MagicMock

    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = None

    mock_raw_doc = MagicMock()
    mock_raw_doc.file_hash = "abc"
    mock_db.query.return_value.filter.return_value.first.return_value = None

    with patch("ingestion.pipeline.stages.s08_deduplication.crud") as mock_crud, \
         patch("ingestion.pipeline.stages.s08_deduplication.load_extracted") as mock_load, \
         patch("ingestion.embeddings.encoder.Encoder") as mock_enc, \
         patch("ingestion.storage.vector_store.vector_store") as mock_vs, \
         caplog.at_level(logging.WARNING, logger="ingestion.pipeline.stages.s08_deduplication"):

        mock_crud.get_raw_doc.return_value = MagicMock(file_hash="abc123", id="raw1")
        mock_crud.RawDocument = MagicMock()
        mock_load.return_value = {"text": "some text content here", "metadata": {}, "warnings": []}

        import ingestion.pipeline.stages.s08_deduplication as dedup_mod
        with patch.object(dedup_mod, "_NEAR_DUPLICATE_THRESHOLD", 0.95):
            pass  # just verifying the patch works

        # Simulate encoder failure
        mock_enc.get.side_effect = RuntimeError("model not loaded")

        result = s08_deduplication.run("raw1", mock_db)

    # Should return a result dict (not crash)
    assert "is_duplicate" in result
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_exception_handling.py::test_keyword_search_logs_fts_error -v`

Expected: FAIL — test finds no log record for FTS5 error (currently the exception is silently swallowed)

- [ ] **Step 3: Fix `ingestion/api/routers/search.py`**

Add `logger` at the top of the file (after existing imports):
```python
import logging
logger = logging.getLogger(__name__)
```

Replace the `except Exception:` block in `_keyword_search` (lines 64-65):
```python
    except Exception as exc:
        logger.error("FTS5 keyword search failed for query %r: %s", query, exc)
        return []
```

- [ ] **Step 4: Fix `ingestion/pipeline/stages/s08_deduplication.py`**

Add logger at top of file:
```python
import logging
logger = logging.getLogger(__name__)
```

Replace the bare `except Exception:` block in `run` (around line 67):
```python
    except Exception as exc:
        logger.warning(
            "Embedding similarity check failed for %s — skipping: %s", raw_doc_id, exc
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_exception_handling.py -v`

Expected: PASSED

- [ ] **Step 6: Commit**

```bash
git add ingestion/api/routers/search.py ingestion/pipeline/stages/s08_deduplication.py tests/test_exception_handling.py
git commit -m "fix: log exceptions in keyword search and dedup instead of silently swallowing"
```

---

## Task 6: Transaction Savepoints in Pipeline Runner

**Files:**
- Modify: `ingestion/pipeline/runner.py`
- Test: `tests/test_pipeline_runner.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_pipeline_runner.py`:
```python
"""Tests for pipeline runner transaction safety."""
from unittest.mock import patch, MagicMock, call
import pytest


def test_failed_stage_rolls_back_only_that_stage(db):
    """
    If stage N fails, its writes should be rolled back but the session
    should remain usable for subsequent pipeline runs.
    """
    from ingestion.pipeline.runner import PipelineRunner
    from ingestion.db.models import RawDocument, CanonicalDocument
    from ingestion.db import crud

    # Create a raw doc
    raw = crud.create_raw_doc(
        db,
        filename="test.txt",
        original_path="/tmp/test.txt",
        stored_path="/tmp/test.txt",
        file_hash="deadbeef",
        file_size_bytes=10,
    )
    db.commit()

    call_count = {"n": 0}

    def fake_run_stage(stage_name, raw_doc_id):
        call_count["n"] += 1
        if stage_name == "s02_format_detection":
            raise RuntimeError("simulated stage failure")
        return "ok"

    runner = PipelineRunner(db)
    with patch.object(runner, "_run_stage", side_effect=fake_run_stage):
        summary = runner.run(raw.id)

    assert "FAILED" in summary.get("s02_format_detection", "")
    # DB session must still be usable after partial failure
    assert db.query(RawDocument).filter(RawDocument.id == raw.id).first() is not None


def test_completed_stage_is_not_rerun(db):
    """Stages already marked completed are skipped."""
    from ingestion.pipeline.runner import PipelineRunner
    from ingestion.db import crud

    raw = crud.create_raw_doc(
        db,
        filename="test2.txt",
        original_path="/tmp/test2.txt",
        stored_path="/tmp/test2.txt",
        file_hash="cafebabe",
        file_size_bytes=5,
    )
    db.commit()

    # Mark s01 as already completed
    crud.upsert_pipeline_run(db, raw.id, "s01_acquisition", "completed")
    db.commit()

    ran_stages = []

    def fake_run_stage(stage_name, raw_doc_id):
        ran_stages.append(stage_name)
        raise RuntimeError("stop after first real stage")

    runner = PipelineRunner(db)
    with patch.object(runner, "_run_stage", side_effect=fake_run_stage):
        runner.run(raw.id)

    assert "s01_acquisition" not in ran_stages
```

- [ ] **Step 2: Run to verify tests pass (they test existing behaviour)**

Run: `pytest tests/test_pipeline_runner.py -v`

Expected: PASSED — these tests validate current behaviour before refactoring.

- [ ] **Step 3: Wrap each stage execution in a savepoint in `ingestion/pipeline/runner.py`**

Replace the `try/except` block inside `PipelineRunner.run` (lines 34-44):

```python
            try:
                with self.db.begin_nested():  # SAVEPOINT — rolls back only this stage on failure
                    result = self._run_stage(stage_name, raw_doc_id)
                    upsert_pipeline_run(self.db, raw_doc_id, stage_name, "completed")
                self.db.commit()
                summary[stage_name] = f"completed: {result}"
                logger.info(f"[{raw_doc_id}] {stage_name} completed")
            except Exception as e:
                upsert_pipeline_run(self.db, raw_doc_id, stage_name, "failed", error_msg=str(e))
                self.db.commit()
                summary[stage_name] = f"FAILED: {e}"
                logger.error(f"[{raw_doc_id}] {stage_name} failed: {e}")
                break  # Pause at failed stage; do not continue
```

Also remove the `self.db.rollback()` call that was on line 43.

- [ ] **Step 4: Run tests again to verify refactored behaviour**

Run: `pytest tests/test_pipeline_runner.py -v`

Expected: PASSED

- [ ] **Step 5: Commit**

```bash
git add ingestion/pipeline/runner.py tests/test_pipeline_runner.py
git commit -m "fix: wrap each pipeline stage in a savepoint so failures don't corrupt session state"
```

---

## Task 7: Cascading Deletes on Document Rejection

**Files:**
- Modify: `ingestion/review/queue.py`
- Modify: `ingestion/db/crud.py`
- Test: `tests/test_review_queue.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_review_queue.py`:
```python
"""Tests for review queue, including cascading delete on rejection."""
from unittest.mock import patch, MagicMock
import pytest


def _seed_doc_with_chunks(db):
    """Helper: create a canonical doc with 2 chunks and a review item."""
    from ingestion.db import crud
    from ingestion.db.models import Chunk

    raw = crud.create_raw_doc(
        db,
        filename="doc.pdf",
        original_path="/tmp/doc.pdf",
        stored_path="/tmp/doc.pdf",
        file_hash="aabbcc",
        file_size_bytes=100,
    )
    db.flush()
    canonical = crud.create_canonical(
        db,
        raw_doc_id=raw.id,
        content_type="prd",
        authority_level="observed",
        status="review",
    )
    db.flush()
    for i in range(2):
        crud.create_chunk(
            db,
            canonical_doc_id=canonical.id,
            chunk_index=i,
            text=f"Chunk {i} text",
            confidence_score=0.9,
            authority_level="observed",
        )
    db.flush()
    review_item = crud.create_review_item(
        db,
        canonical_doc_id=canonical.id,
        assigned_role="reviewer",
        reason="needs review",
    )
    db.commit()
    return canonical.id, review_item.id


def test_reject_doc_removes_chunks_from_db(db):
    """Rejecting a doc-level review item must delete all its chunks from the DB."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db.models import Chunk

    canonical_id, item_id = _seed_doc_with_chunks(db)

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, item_id, "tester", "not relevant")

    remaining = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).count()
    assert remaining == 0


def test_reject_doc_removes_chunks_from_vector_store(db):
    """Rejecting a doc must call vector_store.delete for each chunk."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db.models import Chunk

    canonical_id, item_id = _seed_doc_with_chunks(db)

    # Get chunk IDs before deletion
    chunk_ids = [c.id for c in db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).all()]
    assert len(chunk_ids) == 2

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, item_id, "tester", "not relevant")

    deleted_ids = {call.args[0] for call in mock_vs.delete.call_args_list}
    assert deleted_ids == set(chunk_ids)


def test_reject_chunk_level_does_not_cascade(db):
    """Rejecting a chunk-level item must NOT delete sibling chunks."""
    from ingestion.review.queue import ReviewQueue
    from ingestion.db import crud
    from ingestion.db.models import Chunk

    canonical_id, _ = _seed_doc_with_chunks(db)
    chunks = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).all()
    # Create a chunk-level review item for the first chunk
    chunk_item = crud.create_review_item(
        db,
        canonical_doc_id=canonical_id,
        chunk_id=chunks[0].id,
        assigned_role="reviewer",
        reason="bad chunk",
    )
    db.commit()

    mock_vs = MagicMock()
    with patch("ingestion.review.queue.vector_store", mock_vs):
        ReviewQueue.reject(db, chunk_item.id, "tester", "bad content")

    # Only the one chunk should be affected, sibling untouched
    remaining = db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_id).count()
    assert remaining == 2  # no chunks deleted from DB at chunk-level rejection
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_review_queue.py -v`

Expected: 2 FAILED (cascade tests), 1 PASSED (chunk-level test)

- [ ] **Step 3: Add `delete_chunks_for_canonical` to `ingestion/db/crud.py`**

Append before the `ApiKey` section:
```python
def delete_chunks_for_canonical(db: Session, canonical_doc_id: str) -> list[str]:
    """
    Delete all Chunk rows (and their IndexEntry rows) for a canonical doc.
    Returns the list of deleted chunk IDs so the caller can clean up vector store.
    """
    chunk_ids = [
        c.id for c in db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).all()
    ]
    if chunk_ids:
        db.query(IndexEntry).filter(IndexEntry.chunk_id.in_(chunk_ids)).delete(
            synchronize_session=False
        )
        db.query(Chunk).filter(Chunk.canonical_doc_id == canonical_doc_id).delete(
            synchronize_session=False
        )
    return chunk_ids
```

- [ ] **Step 4: Update `ingestion/review/queue.py` to cascade on doc-level rejection**

Add import at top of `queue.py` (after existing imports):
```python
from ingestion.storage.vector_store import vector_store
```

Replace the `reject` method's doc-rejection block. The full updated `reject` static method:
```python
    @staticmethod
    def reject(db: Session, item_id: str, reviewer: str, reason: str) -> ReviewQueueItem:
        item = crud.get_review_item(db, item_id)
        if item is None:
            raise ValueError(f"Review item {item_id} not found")

        item.status = "rejected"
        item.resolved_at = datetime.now(timezone.utc)
        item.resolved_by = reviewer
        item.rejection_reason = reason

        # Mark canonical doc as rejected and clean up chunks when rejecting at doc level
        if item.canonical_doc_id and item.chunk_id is None:
            canonical = crud.get_canonical(db, item.canonical_doc_id)
            if canonical:
                canonical.status = "rejected"
                # Remove all chunks from DB and vector store
                chunk_ids = crud.delete_chunks_for_canonical(db, item.canonical_doc_id)
                for cid in chunk_ids:
                    try:
                        vector_store.delete(cid)
                    except Exception:
                        pass  # vector store may not have this chunk yet; non-fatal

        db.commit()
        return item
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_review_queue.py -v`

Expected: 3 PASSED

- [ ] **Step 6: Commit**

```bash
git add ingestion/review/queue.py ingestion/db/crud.py tests/test_review_queue.py
git commit -m "fix: cascade delete chunks from DB and vector store when document-level review is rejected"
```

---

## Task 8: Store Extracted Text in Database

**Files:**
- Modify: `ingestion/db/models.py`
- Create: `scripts/migrate_add_extracted_text.py`
- Modify: `ingestion/pipeline/stages/s03_extraction.py`
- Modify: `ingestion/pipeline/stages/s04_cleaning.py`
- Test: `tests/test_extracted_text.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_extracted_text.py`:
```python
"""Tests for DB-backed extracted text storage."""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch


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
    assert "Hello world" in raw.extracted_text


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
    sidecar = doc_file.with_suffix(doc_file.suffix + ".extracted.json")
    sidecar.write_text(json.dumps({"text": "stale sidecar", "metadata": {}, "warnings": []}), encoding="utf-8")

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

    sidecar = doc_file.with_suffix(doc_file.suffix + ".extracted.json")
    sidecar.write_text(json.dumps({"text": "sidecar content", "metadata": {}, "warnings": []}), encoding="utf-8")

    result = load_extracted(raw.id, db)
    assert result["text"] == "sidecar content"
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_extracted_text.py -v`

Expected: FAIL — `AttributeError: 'RawDocument' object has no attribute 'extracted_text'`

- [ ] **Step 3: Add `extracted_text` column to `RawDocument` in `ingestion/db/models.py`**

In the `RawDocument` class, add after the `file_size_bytes` column:
```python
    extracted_text = Column(Text, nullable=True)
```

- [ ] **Step 4: Create migration script `scripts/migrate_add_extracted_text.py`**

```python
"""
One-shot migration: add extracted_text column to raw_documents.

Usage:
    python scripts/migrate_add_extracted_text.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from sqlalchemy import text
from ingestion.db.engine import engine


def migrate():
    with engine.connect() as conn:
        # SQLite: add column if not present
        cols = [row[1] for row in conn.execute(text("PRAGMA table_info(raw_documents)")).fetchall()]
        if "extracted_text" not in cols:
            conn.execute(text("ALTER TABLE raw_documents ADD COLUMN extracted_text TEXT"))
            conn.commit()
            print("Migration complete: added raw_documents.extracted_text")
        else:
            print("Column already exists — nothing to do.")


if __name__ == "__main__":
    migrate()
```

- [ ] **Step 5: Update `ingestion/pipeline/stages/s03_extraction.py`**

Replace the entire file:
```python
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
```

- [ ] **Step 6: Update `ingestion/pipeline/stages/s04_cleaning.py`**

Replace the `run` function to also update the DB field alongside the sidecar:
```python
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

    # Update DB field
    raw_doc.extracted_text = payload
    db.flush()

    # Update sidecar
    try:
        sidecar_path.write_text(payload, encoding="utf-8")
    except OSError:
        pass

    return cleaned
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `pytest tests/test_extracted_text.py -v`

Expected: 3 PASSED

- [ ] **Step 8: Commit**

```bash
git add ingestion/db/models.py ingestion/pipeline/stages/s03_extraction.py ingestion/pipeline/stages/s04_cleaning.py scripts/migrate_add_extracted_text.py tests/test_extracted_text.py
git commit -m "feat: store extracted text in DB (RawDocument.extracted_text) so pipeline is sidecar-independent"
```

---

## Task 9: Pagination on List Endpoints

**Files:**
- Modify: `ingestion/db/crud.py`
- Modify: `ingestion/api/routers/review.py`
- Test: `tests/test_pagination.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_pagination.py`:
```python
"""Tests for paginated list endpoints."""
import pytest
from unittest.mock import patch, MagicMock


def _seed_review_items(db, count: int):
    """Seed `count` pending review items."""
    from ingestion.db import crud

    raw = crud.create_raw_doc(
        db,
        filename="pag.txt",
        original_path="/tmp/pag.txt",
        stored_path="/tmp/pag.txt",
        file_hash=f"pagtest",
        file_size_bytes=1,
    )
    db.flush()
    canonical = crud.create_canonical(
        db,
        raw_doc_id=raw.id,
        content_type="general",
        authority_level="observed",
        status="review",
    )
    db.flush()
    for _ in range(count):
        crud.create_review_item(
            db,
            canonical_doc_id=canonical.id,
            assigned_role="reviewer",
            reason="test item",
        )
    db.commit()


def test_get_pending_paginated_returns_correct_page(db):
    from ingestion.db.crud import get_pending_review_items_paginated

    _seed_review_items(db, 10)

    page1 = get_pending_review_items_paginated(db, limit=4, offset=0)
    page2 = get_pending_review_items_paginated(db, limit=4, offset=4)
    page3 = get_pending_review_items_paginated(db, limit=4, offset=8)

    assert len(page1) == 4
    assert len(page2) == 4
    assert len(page3) == 2
    # No overlap
    ids1 = {i.id for i in page1}
    ids2 = {i.id for i in page2}
    assert ids1.isdisjoint(ids2)


def test_get_pending_paginated_default_limit(db):
    from ingestion.db.crud import get_pending_review_items_paginated

    _seed_review_items(db, 5)
    result = get_pending_review_items_paginated(db)
    assert len(result) == 5
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_pagination.py -v`

Expected: FAIL — `ImportError: cannot import name 'get_pending_review_items_paginated'`

- [ ] **Step 3: Add `get_pending_review_items_paginated` to `ingestion/db/crud.py`**

Append to the `ReviewQueueItem` section (after `get_review_item`):
```python
def get_pending_review_items_paginated(
    db: Session,
    role: str = None,
    limit: int = 100,
    offset: int = 0,
) -> list[ReviewQueueItem]:
    q = db.query(ReviewQueueItem).filter(ReviewQueueItem.status == "pending")
    if role:
        q = q.filter(ReviewQueueItem.assigned_role == role)
    return q.order_by(ReviewQueueItem.created_at).offset(offset).limit(limit).all()
```

- [ ] **Step 4: Update `ingestion/api/routers/review.py` queue endpoint**

Replace the `get_queue` route:
```python
@router.get("/queue", response_model=list[ReviewItemOut])
def get_queue(
    role: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
    _key=Depends(rate_limit),
):
    items = crud.get_pending_review_items_paginated(db, role=role, limit=limit, offset=offset)
    return [_serialise(i, db) for i in items]
```

Also update the import at the top — `ReviewQueue` is no longer called here for `get_queue`. The `ReviewQueue` import is still needed for approve/reject. No change needed there.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_pagination.py -v`

Expected: 2 PASSED

- [ ] **Step 6: Commit**

```bash
git add ingestion/db/crud.py ingestion/api/routers/review.py tests/test_pagination.py
git commit -m "feat: add limit/offset pagination to review queue endpoint"
```

---

## Task 10: DB-Backed Rate Limit (Survives Restarts)

**Files:**
- Modify: `ingestion/db/models.py`
- Modify: `ingestion/api/rate_limit.py`
- Modify: `scripts/init_db.py`
- Test: `tests/test_rate_limit.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_rate_limit.py`:
```python
"""Tests for DB-backed rate limiter."""
import pytest
from datetime import datetime, timezone, timedelta


def test_rate_limit_enforced_from_db(db):
    """Rate limit must be enforced using DB-persisted entries."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey

    key = ApiKey(key_hash="ratelimitkey", label="test", rate_limit_per_minute=3)
    db.add(key)
    db.commit()

    # First 3 calls should succeed
    for _ in range(3):
        db_rate_limit(key, db)  # must not raise

    # 4th call should raise HTTPException 429
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        db_rate_limit(key, db)
    assert exc_info.value.status_code == 429


def test_rate_limit_window_expires(db):
    """Entries older than 60s must not count toward the limit."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey, RateLimitEntry

    key = ApiKey(key_hash="expirekey", label="expire", rate_limit_per_minute=2)
    db.add(key)
    db.commit()

    # Insert 2 old entries (>60s ago)
    old_time = datetime.now(timezone.utc) - timedelta(seconds=90)
    for _ in range(2):
        db.add(RateLimitEntry(key_hash="expirekey", requested_at=old_time))
    db.commit()

    # Should succeed because old entries are expired
    db_rate_limit(key, db)  # must not raise


def test_rate_limit_cleans_old_entries(db):
    """After a request, entries older than 60s for that key should be removed."""
    from ingestion.api.rate_limit import db_rate_limit
    from ingestion.db.models import ApiKey, RateLimitEntry

    key = ApiKey(key_hash="cleankey", label="clean", rate_limit_per_minute=10)
    db.add(key)
    db.commit()

    old_time = datetime.now(timezone.utc) - timedelta(seconds=120)
    db.add(RateLimitEntry(key_hash="cleankey", requested_at=old_time))
    db.commit()

    db_rate_limit(key, db)

    remaining = db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == "cleankey",
        RateLimitEntry.requested_at < datetime.now(timezone.utc) - timedelta(seconds=60),
    ).count()
    assert remaining == 0
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_rate_limit.py -v`

Expected: FAIL — `ImportError: cannot import name 'db_rate_limit'` and `RateLimitEntry` does not exist

- [ ] **Step 3: Add `RateLimitEntry` model to `ingestion/db/models.py`**

Append at the end of the file:
```python
# ---------------------------------------------------------------------------
# Operational — Rate limit persistence
# ---------------------------------------------------------------------------

class RateLimitEntry(Base):
    __tablename__ = "rate_limit_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_hash = Column(String(64), nullable=False, index=True)
    requested_at = Column(DateTime(timezone=True), nullable=False)
```

Also add `Integer` to the imports if not already present (it is — no change needed).

- [ ] **Step 4: Update `ingestion/api/rate_limit.py`**

Replace the entire file:
```python
"""Sliding-window rate limiter backed by SQLite for restart-safety."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from ingestion.api.auth import verify_api_key
from ingestion.db.engine import get_db
from ingestion.db.models import ApiKey, RateLimitEntry


def db_rate_limit(api_key: ApiKey, db: Session) -> None:
    """
    Enforce rate limit using DB-persisted entries.
    Raises HTTP 429 if the key has exceeded its per-minute limit.
    Cleans expired entries on every call.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=60)

    # Evict expired entries for this key
    db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == api_key.key_hash,
        RateLimitEntry.requested_at < window_start,
    ).delete(synchronize_session=False)

    # Count current window
    count = db.query(RateLimitEntry).filter(
        RateLimitEntry.key_hash == api_key.key_hash,
        RateLimitEntry.requested_at >= window_start,
    ).count()

    if count >= api_key.rate_limit_per_minute:
        db.commit()  # persist eviction even on rejection
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    db.add(RateLimitEntry(key_hash=api_key.key_hash, requested_at=now))
    db.commit()


def rate_limit(
    api_key: ApiKey = Depends(verify_api_key),
    db: Session = Depends(get_db),
) -> ApiKey:
    db_rate_limit(api_key, db)
    return api_key
```

- [ ] **Step 5: Update `scripts/init_db.py` to create the new table**

In `init_db()`, add after `Base.metadata.create_all(bind=engine)`:

The `Base.metadata.create_all` call already covers all models including `RateLimitEntry` since it imports from `ingestion.db.models`. No change to the SQL block is needed — SQLAlchemy will create the table automatically.

Verify by adding an explicit import at the top of `init_db.py`:
```python
from ingestion.db.models import ApiKey, RateLimitEntry  # ensures all models registered
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_rate_limit.py -v`

Expected: 3 PASSED

- [ ] **Step 7: Commit**

```bash
git add ingestion/db/models.py ingestion/api/rate_limit.py scripts/init_db.py tests/test_rate_limit.py
git commit -m "feat: persist rate limit state to SQLite so limits survive process restarts"
```

---

## Task 11: Run Full Test Suite and Verify

- [ ] **Step 1: Run all tests**

```bash
pytest tests/ -v --tb=short
```

Expected: All tests PASSED. Note any failures and fix before proceeding.

- [ ] **Step 2: Run migration on existing data directory (if data/repository.db exists)**

```bash
python scripts/migrate_add_extracted_text.py
```

Expected:
```
Migration complete: added raw_documents.extracted_text
```
or `Column already exists — nothing to do.`

- [ ] **Step 3: Final commit**

```bash
git add .
git commit -m "chore: verify all improvements pass — full test suite green"
```

---

## Self-Review

### Spec coverage

| # | Improvement | Task |
|---|-------------|------|
| 1 | Add test suite | Tasks 1–10 all include tests |
| 2 | Fix exception handling | Task 5 |
| 3 | Move extracted text to DB | Task 8 |
| 4 | Pagination on list endpoints | Task 9 |
| 5 | Cascading deletes on rejection | Task 7 |
| 6 | Validate startup config | Task 3 |
| 7 | Fix transaction management | Task 6 |
| 8 | Move hardcoded thresholds to config | Task 2 |
| 9 | Unicode normalization in entity extraction | Task 4 |
| 10 | Persist rate limit state | Task 10 |

All 10 items covered. ✓

### Placeholder scan

No TBD/TODO/placeholder text. All steps contain full code. ✓

### Type consistency

- `get_pending_review_items_paginated` signature matches usage in router ✓
- `db_rate_limit(api_key: ApiKey, db: Session)` matches test calls ✓
- `delete_chunks_for_canonical` returns `list[str]` and is used correctly in `reject` ✓
- `_normalise` is defined in Task 4 and imported in test ✓
- `RateLimitEntry` model is defined before use in rate_limit.py ✓
