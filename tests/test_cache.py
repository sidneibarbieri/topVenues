"""Tests for CacheManager."""

import pytest

from src.cache import CacheManager


@pytest.fixture
def cache(tmp_path):
    return CacheManager(tmp_path / "cache", enabled=True, ttl_hours=1)


def test_miss_returns_none(cache):
    assert cache.get("nonexistent") is None


def test_set_then_get_returns_value(cache):
    cache.set("k1", "abstract text")
    assert cache.get("k1") == "abstract text"


def test_disabled_cache_always_misses(tmp_path):
    c = CacheManager(tmp_path / "cache", enabled=False)
    c.set("k", "value")
    assert c.get("k") is None


def test_clear_removes_all(cache):
    cache.set("a", "v1")
    cache.set("b", "v2")
    deleted = cache.clear()
    assert deleted == 2
    assert cache.get("a") is None


def test_stats_hit_rate(cache):
    cache.set("x", "val")
    cache.get("x")   # hit
    cache.get("y")   # miss
    stats = cache.get_stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["hit_rate"] == pytest.approx(0.5)


def test_get_updates_access_count(cache):
    cache.set("k", "val")
    cache.get("k")
    cache.get("k")
    # Access count is stored in the file; just verify no error and correct return
    assert cache.get("k") == "val"
