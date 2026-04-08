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
