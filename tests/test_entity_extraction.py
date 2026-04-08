"""Tests for entity extraction and Unicode normalisation."""
from ingestion.pipeline.stages.s07_entity_extraction import extract_entities, _normalise


def test_normalise_strips_accents():
    assert _normalise("Café") == _normalise("Cafe")


def test_normalise_lowercases():
    assert _normalise("Fusion") == "fusion"


def test_normalise_handles_ligatures():
    # 'ﬁ' (U+FB01 LATIN SMALL LIGATURE FI) should normalise to 'fi'
    assert "fi" in _normalise("ﬁle")


def test_duplicate_entities_not_created_for_accent_variants():
    """Café and Cafe must produce the same normalised name."""
    assert _normalise("Café") == _normalise("Cafe")


def test_extract_entities_returns_list():
    results = extract_entities("This document mentions Fusion and its competitors.")
    assert isinstance(results, list)
