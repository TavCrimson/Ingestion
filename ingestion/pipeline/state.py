"""Pipeline stage names and state tracking helpers."""
from __future__ import annotations

STAGES = [
    "s01_acquisition",
    "s02_format_detection",
    "s03_extraction",
    "s04_cleaning",
    "s05_classification",
    "s06_metadata",
    "s07_entity_extraction",
    "s08_deduplication",
    "s09_relationship",
    "s10_chunking",
    "s11_confidence",
    "s12_publication",
]
