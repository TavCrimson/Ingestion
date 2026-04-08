"""
Ingest one or more files through the pipeline.

Usage:
    python scripts/run_pipeline.py path/to/file.pdf
    python scripts/run_pipeline.py path/to/docs/          # all files in a directory
    python scripts/run_pipeline.py file.pdf --resume      # resume from last failed stage
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from ingestion.db.engine import SessionLocal
from ingestion.extractors.registry import supported_extensions
from ingestion.pipeline.runner import ingest_file


def process_file(path: Path, db) -> None:
    print(f"\n{'='*60}")
    print(f"Ingesting: {path.name}")
    print(f"{'='*60}")
    result = ingest_file(path, db)
    print(f"raw_doc_id: {result['raw_doc_id']}")
    for stage, outcome in result["stages"].items():
        status = "OK" if "FAILED" not in str(outcome) else "FAIL"
        outcome_str = str(outcome)[:120].encode("ascii", errors="replace").decode("ascii")
        print(f"  [{status}] {stage}: {outcome_str}")


def main():
    parser = argparse.ArgumentParser(description="Run the ingestion pipeline")
    parser.add_argument("paths", nargs="+", help="File or directory paths to ingest")
    parser.add_argument("--ext", nargs="*", help="File extensions to include (default: all supported)")
    args = parser.parse_args()

    allowed_exts = set(args.ext) if args.ext else set(supported_extensions())

    files: list[Path] = []
    for raw_path in args.paths:
        p = Path(raw_path)
        if p.is_dir():
            for ext in allowed_exts:
                files.extend(p.rglob(f"*{ext}"))
        elif p.is_file():
            files.append(p)
        else:
            print(f"Warning: {raw_path} not found, skipping")

    if not files:
        print("No files to ingest.")
        sys.exit(0)

    print(f"Found {len(files)} file(s) to ingest.")
    db = SessionLocal()
    try:
        for path in files:
            try:
                process_file(path, db)
            except KeyboardInterrupt:
                print("\nInterrupted. Pipeline state preserved — re-run to resume.")
                sys.exit(1)
            except Exception as e:
                print(f"Error processing {path}: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
