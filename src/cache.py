"""Local file-based cache for abstracts."""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

from .models import CacheEntry


class CacheManager:
    """Persists abstracts on disk with TTL expiry."""

    def __init__(self, cache_dir: Path, enabled: bool = True, ttl_hours: int = 168):
        self.cache_dir = Path(cache_dir)
        self.enabled = enabled
        self.ttl = timedelta(hours=ttl_hours)
        self.hits = 0
        self.misses = 0

        if self.enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, key: str) -> str | None:
        if not self.enabled:
            return None

        cache_path = self._cache_path(key)
        if not cache_path.exists():
            self.misses += 1
            return None

        with open(cache_path, encoding="utf-8") as fh:
            entry = CacheEntry(**json.load(fh))

        if entry.expires_at and datetime.now() > entry.expires_at:
            cache_path.unlink()
            self.misses += 1
            return None

        entry.access_count += 1
        entry.last_accessed = datetime.now()
        self._write_entry(cache_path, entry)

        self.hits += 1
        return entry.value

    def set(self, key: str, value: str) -> None:
        if not self.enabled:
            return
        entry = CacheEntry(
            key=key,
            value=value,
            expires_at=datetime.now() + self.ttl if self.ttl else None,
        )
        self._write_entry(self._cache_path(key), entry)

    def clear(self) -> int:
        if not self.cache_dir.exists():
            return 0
        count = sum(1 for f in self.cache_dir.glob("*.json") if f.unlink() is None)
        self.hits = self.misses = 0
        return count

    def get_stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "enabled": self.enabled,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / total if total else 0,
            "cache_dir": str(self.cache_dir),
        }

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{hashlib.md5(key.encode()).hexdigest()}.json"

    def _write_entry(self, path: Path, entry: CacheEntry) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(entry.model_dump(), fh, default=str, indent=2)
