"""PipelineRunner — orchestrates stages with resumable state."""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.orm import Session

from ingestion.db.crud import get_completed_stages, upsert_pipeline_run
from ingestion.pipeline.state import STAGES

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, db: Session):
        self.db = db

    def run(self, raw_doc_id: str) -> dict:
        """
        Run all pipeline stages for raw_doc_id, skipping already-completed stages.
        Returns a summary dict with stage outcomes.
        """
        completed = get_completed_stages(self.db, raw_doc_id)
        summary = {}

        for stage_name in STAGES:
            if stage_name in completed:
                summary[stage_name] = "skipped (already completed)"
                continue

            upsert_pipeline_run(self.db, raw_doc_id, stage_name, "running")

            try:
                result = self._run_stage(stage_name, raw_doc_id)
                upsert_pipeline_run(self.db, raw_doc_id, stage_name, "completed")
                summary[stage_name] = f"completed: {result}"
                logger.info(f"[{raw_doc_id}] {stage_name} completed")
            except Exception as e:
                upsert_pipeline_run(self.db, raw_doc_id, stage_name, "failed", error_msg=str(e))
                summary[stage_name] = f"FAILED: {e}"
                logger.error(f"[{raw_doc_id}] {stage_name} failed: {e}")
                self.db.rollback()
                break  # Pause at failed stage; do not continue

        return summary

    def _run_stage(self, stage_name: str, raw_doc_id: str):
        db = self.db
        if stage_name == "s01_acquisition":
            # Acquisition is special — it takes a path, not raw_doc_id
            # By the time we get here, raw_doc already exists.
            return "already acquired"
        if stage_name == "s02_format_detection":
            from ingestion.pipeline.stages import s02_format_detection
            return s02_format_detection.run(raw_doc_id, db)
        if stage_name == "s03_extraction":
            from ingestion.pipeline.stages import s03_extraction
            return s03_extraction.run(raw_doc_id, db)
        if stage_name == "s04_cleaning":
            from ingestion.pipeline.stages import s04_cleaning
            return s04_cleaning.run(raw_doc_id, db)
        if stage_name == "s05_classification":
            from ingestion.pipeline.stages import s05_classification
            return s05_classification.run(raw_doc_id, db)
        if stage_name == "s06_metadata":
            from ingestion.pipeline.stages import s06_metadata
            return s06_metadata.run(raw_doc_id, db)
        if stage_name == "s07_entity_extraction":
            from ingestion.pipeline.stages import s07_entity_extraction
            return s07_entity_extraction.run(raw_doc_id, db)
        if stage_name == "s08_deduplication":
            from ingestion.pipeline.stages import s08_deduplication
            return s08_deduplication.run(raw_doc_id, db)
        if stage_name == "s09_relationship":
            from ingestion.pipeline.stages import s09_relationship
            return s09_relationship.run(raw_doc_id, db)
        if stage_name == "s10_chunking":
            from ingestion.pipeline.stages import s10_chunking
            return s10_chunking.run(raw_doc_id, db)
        if stage_name == "s11_confidence":
            from ingestion.pipeline.stages import s11_confidence
            return s11_confidence.run(raw_doc_id, db)
        if stage_name == "s12_publication":
            from ingestion.pipeline.stages import s12_publication
            return s12_publication.run(raw_doc_id, db)
        raise ValueError(f"Unknown stage: {stage_name}")


def ingest_file(path: Path, db: Session) -> dict:
    """
    Full ingestion entry point for a file path.
    Handles acquisition then runs the full pipeline.
    """
    from ingestion.pipeline.stages.s01_acquisition import run as acquire
    from ingestion.db.crud import upsert_pipeline_run

    raw_doc_id = acquire(path, db)
    upsert_pipeline_run(db, raw_doc_id, "s01_acquisition", "completed")

    runner = PipelineRunner(db)
    summary = runner.run(raw_doc_id)
    return {"raw_doc_id": raw_doc_id, "stages": summary}
