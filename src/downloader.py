"""Async DBLP JSON downloader."""

import asyncio
import csv
import json
import random
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenError
from .models import Configuration, DownloadLogEntry, DownloadStatus
from .venue_config import VenueStrategyRegistry


class JSONDownloader:
    def __init__(self, config: Configuration, log_dir: Path):
        self.config = config
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.client: httpx.AsyncClient | None = None
        self.download_log: list[DownloadLogEntry] = []
        self.venue_registry = VenueStrategyRegistry()
        self.circuit_breaker = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=3, recovery_timeout=120.0, expected_exception=httpx.ReadError
            )
        )

    async def __aenter__(self) -> "JSONDownloader":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.request_timeout),
            headers=self.config.headers,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self.client:
            await self.client.aclose()

    async def download_all(
        self,
        json_dir: Path,
        progress_callback: Callable | None = None,
    ) -> list[DownloadLogEntry]:
        json_dir = Path(json_dir)
        json_dir.mkdir(parents=True, exist_ok=True)
        self.download_log = []

        years = self.config.effective_years()
        total = len(self.config.events) * len(years)
        completed = 0

        for event in self.config.events:
            for year in years:
                await self._download_single(event, year, json_dir)
                completed += 1
                if progress_callback:
                    progress_callback(completed, total, event, year)

        self._save_log()
        return self.download_log

    async def _download_single(self, event: str, year: int, json_dir: Path) -> DownloadLogEntry:
        file_name = json_dir / f"data_{event}{year}.json"
        timestamp = datetime.now()

        if file_name.exists() and self._validate_json(file_name):
            entry = DownloadLogEntry(
                event=event,
                year=year,
                file_name=str(file_name),
                url="",
                status=DownloadStatus.VALID,
                message="Valid file exists",
                timestamp=timestamp,
            )
            self.download_log.append(entry)
            return entry

        urls = self._get_event_urls(event, year)
        if not urls:
            entry = DownloadLogEntry(
                event=event,
                year=year,
                file_name=str(file_name),
                url="",
                status=DownloadStatus.SKIPPED,
                message="No URLs configured",
                timestamp=timestamp,
            )
            self.download_log.append(entry)
            return entry

        for url in urls:
            entry = await self._try_download_url(event, year, url, file_name, timestamp)
            if entry.status == DownloadStatus.DOWNLOADED:
                self.download_log.append(entry)
                return entry

        entry = DownloadLogEntry(
            event=event,
            year=year,
            file_name=str(file_name),
            url=urls[-1],
            status=DownloadStatus.FAILED,
            message="All download attempts failed",
            timestamp=timestamp,
        )
        self.download_log.append(entry)
        return entry

    async def _try_download_url(
        self,
        event: str,
        year: int,
        url: str,
        file_name: Path,
        timestamp: datetime,
    ) -> DownloadLogEntry:
        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = await self.circuit_breaker.call(
                    self.client.get, url, headers={"User-Agent": self._random_user_agent()}
                )

                if response.status_code == 429:
                    await asyncio.sleep(10)
                    continue
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                json_link: str | None = None
                for link in soup.find_all("a", href=True):
                    if "format=json" in link["href"]:
                        href = link["href"]
                        json_link = href if href.startswith("http") else f"https://dblp.org{href}"
                        break

                if not json_link:
                    return DownloadLogEntry(
                        event=event,
                        year=year,
                        file_name=str(file_name),
                        url=url,
                        status=DownloadStatus.FAILED,
                        message="JSON link not found on page",
                        timestamp=timestamp,
                    )

                json_response = await self.circuit_breaker.call(
                    self.client.get, json_link, headers={"User-Agent": self._random_user_agent()}
                )
                if json_response.status_code != 200:
                    continue

                file_name.write_text(json_response.text, encoding="utf-8")

                if self._validate_json(file_name):
                    return DownloadLogEntry(
                        event=event,
                        year=year,
                        file_name=str(file_name),
                        url=url,
                        http_code=json_response.status_code,
                        status=DownloadStatus.DOWNLOADED,
                        message=f"Downloaded successfully (attempt {attempt})",
                        timestamp=timestamp,
                    )

                file_name.unlink()
                return DownloadLogEntry(
                    event=event,
                    year=year,
                    file_name=str(file_name),
                    url=url,
                    status=DownloadStatus.CORRUPT,
                    message="Downloaded file is corrupt",
                    timestamp=timestamp,
                )

            except CircuitBreakerOpenError:
                return DownloadLogEntry(
                    event=event,
                    year=year,
                    file_name=str(file_name),
                    url=url,
                    status=DownloadStatus.FAILED,
                    message="Circuit breaker OPEN - DBLP unstable",
                    timestamp=timestamp,
                )
            except (httpx.HTTPError, OSError) as error:
                if attempt == self.config.max_retries:
                    raise
                await asyncio.sleep(2**attempt)

            await asyncio.sleep(2**attempt)

        return DownloadLogEntry(
            event=event,
            year=year,
            file_name=str(file_name),
            url=url,
            status=DownloadStatus.FAILED,
            message="Max retries exceeded",
            timestamp=timestamp,
        )

    def _get_event_urls(self, event: str, year: int) -> list[str]:
        strategy = self.venue_registry.get_strategy(event)
        return strategy.get_urls(event, year, self.config)

    def _validate_json(self, file_path: Path) -> bool:
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
            return (
                isinstance(data, dict)
                and data.get("result") is not None
                and "hits" in data["result"]
            )
        except (json.JSONDecodeError, KeyError, OSError):
            return False

    def _random_user_agent(self) -> str:
        return random.choice(self.config.user_agents)

    def _save_log(self) -> None:
        log_file = self.log_dir / "download_log.csv"
        fieldnames = ["Event", "Year", "File", "URL", "HTTP_Code", "Status", "Message", "Timestamp"]
        with open(log_file, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for entry in self.download_log:
                writer.writerow(
                    {
                        "Event": entry.event,
                        "Year": entry.year,
                        "File": entry.file_name,
                        "URL": entry.url,
                        "HTTP_Code": entry.http_code or "",
                        "Status": entry.status.value,
                        "Message": entry.message or "",
                        "Timestamp": entry.timestamp.isoformat(),
                    }
                )
