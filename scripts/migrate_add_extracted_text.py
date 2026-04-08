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
        # SQLite: check if column already exists
        cols = [row[1] for row in conn.execute(text("PRAGMA table_info(raw_documents)")).fetchall()]
        if "extracted_text" not in cols:
            conn.execute(text("ALTER TABLE raw_documents ADD COLUMN extracted_text TEXT"))
            conn.commit()
            print("Migration complete: added raw_documents.extracted_text")
        else:
            print("Column already exists — nothing to do.")


if __name__ == "__main__":
    migrate()
