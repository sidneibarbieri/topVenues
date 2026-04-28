"""Pydantic DTOs."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventType(str, Enum):
    CCS = "ccs"
    ASIACCS = "asiaccs"
    USS = "uss"
    NDSS = "ndss"
    SP = "sp"
    EUROSP = "eurosp"
    HOTNETS = "hotnets"
    SACMAT = "sacmat"
    ACM_CSUR = "acm_csur"
    IEEE_COMST = "ieee_comst"
    FNT_PRIVSEC = "fnt_privsec"


class PaperType(str, Enum):
    ARTICLE = "article"
    PROCEEDINGS = "proceedings"
    EDITORSHIP = "editorship"
    UNKNOWN = "unknown"


class DownloadStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    VALID = "valid"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"
    CORRUPT = "corrupt"


class AbstractStatus(str, Enum):
    OK = "ok"
    FAIL = "fail"
    NOT_APPLICABLE = "n.a."
    PENDING = "pending"


class Paper(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    score: float | None = None
    paper_id: str = Field(..., alias="ID")
    authors: str | None = None
    title: str
    venue: str | None = None
    pages: str | None = None
    year: int
    paper_type: PaperType = Field(default=PaperType.ARTICLE, alias="Type")
    access: str | None = None
    key: str | None = None
    ee: str | None = None
    url: str | None = None
    event: str | None = None
    abstract: str | None = None

    @field_validator("year")
    @classmethod
    def validate_year(cls, v: int) -> int:
        if not 1900 <= v <= 2100:
            raise ValueError(f"Year {v} is out of reasonable range")
        return v


class DownloadLogEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event: str
    year: int
    file_name: str = Field(..., alias="File")
    url: str
    http_code: int | None = Field(None, alias="HTTP_Code")
    status: DownloadStatus
    message: str | None = None
    timestamp: datetime = Field(default_factory=datetime.now)


class AbstractLogEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    paper_id: str = Field(..., alias="ID")
    event: str
    ee_url: str = Field(..., alias="EE")
    status: AbstractStatus
    abstract: str | None = None
    message: str | None = None
    source: str
    timestamp: datetime = Field(default_factory=datetime.now)


class CacheEntry(BaseModel):
    key: str
    value: str
    created_at: datetime = Field(default_factory=datetime.now)
    expires_at: datetime | None = None
    access_count: int = 0
    last_accessed: datetime | None = None


class CheckpointData(BaseModel):
    phase: str
    timestamp: datetime = Field(default_factory=datetime.now)
    events_processed: list[tuple[str, int]] = Field(default_factory=list)
    papers_processed: int = 0
    last_event: str | None = None
    last_year: int | None = None
    papers_with_abstracts: int = 0
    custom_data: dict[str, Any] = Field(default_factory=dict)


class Configuration(BaseModel):
    events: list[str] = Field(
        default_factory=lambda: [
            "ccs",
            "asiaccs",
            "uss",
            "ndss",
            "sp",
            "eurosp",
            "hotnets",
            "sacmat",
            "acm_csur",
            "ieee_comst",
            "fnt_privsec",
        ]
    )
    year_start: int = 2019
    years: list[int] = Field(default_factory=list)

    def effective_years(self) -> list[int]:
        """Return years to process."""
        if self.years:
            return list(self.years)
        return list(range(self.year_start, datetime.now().year + 1))

    ieee_comst_topics: list[str] = Field(
        default_factory=lambda: [
            "network",
            "networks",
            "IoT",
            "cloud",
            "edge",
            "wireless",
            "5G",
            "6G",
            "network security",
            "cybersecurity",
            "privacy",
            "blockchain",
            "distributed",
        ]
    )

    request_timeout: int = 120
    default_interval: list[float] = Field(default_factory=lambda: [5.0, 15.0])
    acm_wait_min: float = 60.0
    acm_wait_max: float = 300.0
    batch_size: int = 10

    acm_failure_threshold: int = 3
    acm_backoff_initial: float = 60.0
    acm_backoff_max: float = 600.0

    max_retries: int = 3
    retry_backoff_factor: float = 2.0

    cache_enabled: bool = True
    cache_ttl_hours: int = 168

    checkpoint_enabled: bool = True
    checkpoint_interval: int = 5

    base_dir: str = "."
    data_dir: str = "data/dataset"
    log_dir: str = "data/log"
    json_dir: str = "data/json"
    cache_dir: str = "data/cache"
    checkpoint_dir: str = "data/checkpoints"

    user_agents: list[str] = Field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/129.0.2792.79 Safari/537.36",
        ]
    )

    headers: dict[str, str] = Field(
        default_factory=lambda: {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }
    )


class SearchFilters(BaseModel):
    title_contains: str | None = None
    abstract_contains: str | None = None
    author_contains: str | None = None
    event: str | None = None
    year: int | None = None
    technology: str | None = None

    def has_any_filter(self) -> bool:
        return any(
            [
                self.title_contains,
                self.abstract_contains,
                self.author_contains,
                self.event,
                self.year,
                self.technology,
            ]
        )


class Statistics(BaseModel):
    total_papers: int
    papers_with_abstracts: int
    papers_without_abstracts: int
    by_event: dict[str, int] = Field(default_factory=dict)
    by_year: dict[int, int] = Field(default_factory=dict)
    abstract_sources: dict[str, int] = Field(default_factory=dict)
    last_updated: datetime | None = None


class AbstractImportResult(BaseModel):
    """Result of importing abstracts from CSV."""

    scanned: int
    matched: int
    updated: int
    skipped_existing: int
    missing_in_db: int
