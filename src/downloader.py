"""Async DBLP JSON downloader."""

import asyncio
import csv
import json
import random
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenError
from .models import Configuration, DownloadLogEntry, DownloadStatus
from .venue_config import VenueStrategyRegistry

DBLP_API_PAGE_SIZE = 1000


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
                failure_threshold=3,
                recovery_timeout=120.0,
                expected_exception=(httpx.ReadError, httpx.TimeoutException),
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
        toc_key = self._toc_key_from_dblp_url(url)
        if toc_key:
            return await self._try_download_toc_api(event, year, toc_key, file_name, timestamp)

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = await self.circuit_breaker.call(
                    self.client.get, url, headers={"User-Agent": self._random_user_agent()}
                )

                if response.status_code == 429:
                    await asyncio.sleep(10)
                    continue
                if self._is_permanent_client_error(response.status_code):
                    return DownloadLogEntry(
                        event=event,
                        year=year,
                        file_name=str(file_name),
                        url=url,
                        http_code=response.status_code,
                        status=DownloadStatus.FAILED,
                        message=f"Permanent HTTP {response.status_code}",
                        timestamp=timestamp,
                    )
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
                if self._is_permanent_client_error(json_response.status_code):
                    return DownloadLogEntry(
                        event=event,
                        year=year,
                        file_name=str(file_name),
                        url=json_link,
                        http_code=json_response.status_code,
                        status=DownloadStatus.FAILED,
                        message=f"Permanent HTTP {json_response.status_code}",
                        timestamp=timestamp,
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
            except (httpx.HTTPError, OSError) as exc:
                if attempt == self.config.max_retries:
                    return DownloadLogEntry(
                        event=event,
                        year=year,
                        file_name=str(file_name),
                        url=url,
                        status=DownloadStatus.FAILED,
                        message=f"{type(exc).__name__}: {exc}",
                        timestamp=timestamp,
                    )
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

    async def _try_download_toc_api(
        self,
        event: str,
        year: int,
        toc_key: str,
        file_name: Path,
        timestamp: datetime,
    ) -> DownloadLogEntry:
        hits: list[dict] = []
        first_payload: dict | None = None
        total = 0
        first = 0

        while True:
            json_url = self._toc_api_url(toc_key, first)
            payload, error = await self._fetch_json_payload(json_url)
            if error:
                return DownloadLogEntry(
                    event=event,
                    year=year,
                    file_name=str(file_name),
                    url=json_url,
                    status=DownloadStatus.FAILED,
                    message=error,
                    timestamp=timestamp,
                )
            if payload is None:
                return DownloadLogEntry(
                    event=event,
                    year=year,
                    file_name=str(file_name),
                    url=json_url,
                    status=DownloadStatus.FAILED,
                    message="Empty DBLP API payload",
                    timestamp=timestamp,
                )

            first_payload = first_payload or payload
            hit_block = payload.get("result", {}).get("hits", {})
            total = int(hit_block.get("@total", "0") or 0)
            sent = int(hit_block.get("@sent", "0") or 0)
            page_hits = hit_block.get("hit", [])
            if isinstance(page_hits, dict):
                page_hits = [page_hits]
            hits.extend(page_hits)

            if total == 0:
                return DownloadLogEntry(
                    event=event,
                    year=year,
                    file_name=str(file_name),
                    url=json_url,
                    status=DownloadStatus.FAILED,
                    message="No DBLP records found for TOC",
                    timestamp=timestamp,
                )
            if sent == 0 or first + sent >= total:
                break
            first += sent

        if first_payload is None or len(hits) != total:
            return DownloadLogEntry(
                event=event,
                year=year,
                file_name=str(file_name),
                url=self._toc_api_url(toc_key, 0),
                status=DownloadStatus.CORRUPT,
                message=f"Incomplete DBLP API page set: {len(hits)}/{total}",
                timestamp=timestamp,
            )

        combined = first_payload
        combined["result"]["hits"]["hit"] = hits
        combined["result"]["hits"]["@sent"] = str(len(hits))
        combined["result"]["hits"]["@computed"] = str(len(hits))
        combined["result"]["hits"]["@first"] = "0"
        file_name.write_text(json.dumps(combined, ensure_ascii=False), encoding="utf-8")

        if not self._validate_json(file_name):
            file_name.unlink(missing_ok=True)
            return DownloadLogEntry(
                event=event,
                year=year,
                file_name=str(file_name),
                url=self._toc_api_url(toc_key, 0),
                status=DownloadStatus.CORRUPT,
                message="Downloaded DBLP API payload is invalid",
                timestamp=timestamp,
            )

        return DownloadLogEntry(
            event=event,
            year=year,
            file_name=str(file_name),
            url=self._toc_api_url(toc_key, 0),
            http_code=200,
            status=DownloadStatus.DOWNLOADED,
            message=f"Downloaded {len(hits)} DBLP records via TOC API",
            timestamp=timestamp,
        )

    async def _fetch_json_payload(self, url: str) -> tuple[dict | None, str | None]:
        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = await self.client.get(
                    url, headers={"User-Agent": self._random_user_agent()}
                )
                if response.status_code == 429:
                    await asyncio.sleep(10)
                    continue
                if self._is_permanent_client_error(response.status_code):
                    return None, f"Permanent HTTP {response.status_code}"
                if response.status_code != 200:
                    continue
                return response.json(), None
            except (httpx.HTTPError, json.JSONDecodeError, OSError) as exc:
                if attempt == self.config.max_retries:
                    return None, f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(2**attempt)

            await asyncio.sleep(2**attempt)
        return None, "Max retries exceeded"

    def _get_event_urls(self, event: str, year: int) -> list[str]:
        strategy = self.venue_registry.get_strategy(event)
        return strategy.get_urls(event, year, self.config)

    def _validate_json(self, file_path: Path) -> bool:
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or data.get("result") is None:
                return False
            hits = data["result"].get("hits")
            if not isinstance(hits, dict):
                return False
            hit_rows = hits.get("hit")
            if isinstance(hit_rows, dict):
                return True
            return isinstance(hit_rows, list) and len(hit_rows) > 0
        except (json.JSONDecodeError, KeyError, OSError):
            return False

    @staticmethod
    def _toc_key_from_dblp_url(url: str) -> str | None:
        prefix = "https://dblp.org/"
        if not url.startswith(prefix):
            return None
        path = url[len(prefix) :]
        if not path.startswith("db/") or not path.endswith(".html"):
            return None
        return path.removesuffix(".html") + ".bht"

    @staticmethod
    def _toc_api_url(toc_key: str, first: int) -> str:
        query = quote(f"toc:{toc_key}:")
        return (
            "https://dblp.org/search/publ/api?"
            f"q={query}&h={DBLP_API_PAGE_SIZE}&f={first}&format=json"
        )

    @staticmethod
    def _is_permanent_client_error(status_code: int) -> bool:
        """Return true for client errors that should not be retried."""
        return 400 <= status_code < 500 and status_code != 429

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
