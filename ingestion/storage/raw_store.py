"""Saves uploaded files to the raw store directory."""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from ingestion.config import settings
from ingestion.storage.file_hash import sha256_file


class RawStore:
    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or settings.raw_store_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, source_path: Path) -> tuple[Path, str]:
        """
        Copy source_path into the raw store under data/raw/YYYY-MM/<hash8>_<filename>.
        Returns (stored_path, file_hash).
        """
        file_hash = sha256_file(source_path)
        month_dir = self.base_dir / datetime.now(timezone.utc).strftime("%Y-%m")
        month_dir.mkdir(parents=True, exist_ok=True)

        dest_filename = f"{file_hash[:8]}_{source_path.name}"
        dest_path = month_dir / dest_filename

        if not dest_path.exists():
            shutil.copy2(source_path, dest_path)

        return dest_path, file_hash

    def save_bytes(self, data: bytes, filename: str) -> tuple[Path, str]:
        """Save raw bytes with the given filename. Used for uploaded content."""
        import hashlib
        file_hash = hashlib.sha256(data).hexdigest()
        month_dir = self.base_dir / datetime.now(timezone.utc).strftime("%Y-%m")
        month_dir.mkdir(parents=True, exist_ok=True)

        dest_filename = f"{file_hash[:8]}_{filename}"
        dest_path = month_dir / dest_filename

        if not dest_path.exists():
            dest_path.write_bytes(data)

        return dest_path, file_hash


raw_store = RawStore()
