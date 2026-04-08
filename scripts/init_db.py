"""
Initialise the database: create all tables, set up FTS5, and seed a default API key.

Usage:
    python scripts/init_db.py
    python scripts/init_db.py --seed-key my-secret-key --label "dev-key"
"""
from __future__ import annotations

import argparse
import secrets
import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).parents[1]))

from sqlalchemy import text

from ingestion.config import settings
from ingestion.db.engine import Base, engine, SessionLocal
from ingestion.db.models import ApiKey, RateLimitEntry  # noqa: F401 — ensures all models are registered
from ingestion.storage.file_hash import sha256_string


def init_db():
    # Ensure data directories exist
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/chroma").mkdir(parents=True, exist_ok=True)
    Path("data/models").mkdir(parents=True, exist_ok=True)

    # Create all tables
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")

    # Create FTS5 virtual table for keyword search
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(chunk_id UNINDEXED, text)
        """))

        # Triggers to keep FTS in sync
        conn.execute(text("""
            CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
                INSERT INTO chunks_fts(rowid, chunk_id, text) VALUES (new.rowid, new.id, new.text);
            END
        """))
        conn.execute(text("""
            CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, chunk_id, text) VALUES ('delete', old.rowid, old.id, old.text);
                INSERT INTO chunks_fts(rowid, chunk_id, text) VALUES (new.rowid, new.id, new.text);
            END
        """))
        conn.execute(text("""
            CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, chunk_id, text) VALUES ('delete', old.rowid, old.id, old.text);
            END
        """))
        conn.commit()
    print("FTS5 virtual table and triggers created.")


def seed_api_key(raw_key: str, label: str):
    db = SessionLocal()
    try:
        key_hash = sha256_string(raw_key)
        existing = db.query(ApiKey).filter(ApiKey.key_hash == key_hash).first()
        if existing:
            print(f"API key '{label}' already exists.")
            return
        api_key = ApiKey(
            key_hash=key_hash,
            label=label,
            rate_limit_per_minute=settings.default_rate_limit_per_minute,
        )
        db.add(api_key)
        db.commit()
        print(f"API key created  — label: {label}")
        print(f"  Raw key (save this, it won't be shown again): {raw_key}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Initialise the repository database")
    parser.add_argument("--seed-key", default=None, help="Raw API key to seed (default: generate random)")
    parser.add_argument("--label", default="default", help="Label for the seeded API key")
    parser.add_argument("--no-seed", action="store_true", help="Skip API key seeding")
    args = parser.parse_args()

    init_db()

    if not args.no_seed:
        raw_key = args.seed_key or secrets.token_hex(32)
        seed_api_key(raw_key, args.label)


if __name__ == "__main__":
    main()
