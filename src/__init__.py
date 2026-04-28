"""Top Venues Paper Collector - Python version.

A modern, async paper collection system with web interface.
"""

__version__ = "2.0.0"

from .collector import Collector
from .models import (
    AbstractLogEntry,
    AbstractStatus,
    CacheEntry,
    CheckpointData,
    Configuration,
    DownloadLogEntry,
    DownloadStatus,
    EventType,
    Paper,
    PaperType,
    SearchFilters,
)

__all__ = [
    "Collector",
    "Configuration",
    "Paper",
    "SearchFilters",
    "EventType",
    "PaperType",
    "DownloadStatus",
    "AbstractStatus",
    "DownloadLogEntry",
    "AbstractLogEntry",
    "CacheEntry",
    "CheckpointData",
    "__version__",
]
