"""Tests for pipeline runner transaction safety."""
from unittest.mock import patch
import pytest


def test_failed_stage_does_not_leave_session_unusable(db):
    """
    If stage N fails, the session must still be usable afterward.
    """
    from ingestion.pipeline.runner import PipelineRunner
    from ingestion.db import crud

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
    from ingestion.db.models import RawDocument
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


def test_summary_records_failed_stage(db):
    """Summary dict must mark the failed stage as FAILED."""
    from ingestion.pipeline.runner import PipelineRunner
    from ingestion.db import crud

    raw = crud.create_raw_doc(
        db,
        filename="test3.txt",
        original_path="/tmp/test3.txt",
        stored_path="/tmp/test3.txt",
        file_hash="beefdead",
        file_size_bytes=3,
    )
    db.commit()

    def fake_run_stage(stage_name, raw_doc_id):
        raise RuntimeError("everything fails")

    runner = PipelineRunner(db)
    with patch.object(runner, "_run_stage", side_effect=fake_run_stage):
        summary = runner.run(raw.id)

    first_stage = list(summary.keys())[0]
    assert "FAILED" in summary[first_stage]
