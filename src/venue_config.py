"""Venue URL configuration strategies."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Configuration


class VenueURLStrategy(ABC):
    """Abstract base for venue URL generation strategies."""

    @abstractmethod
    def get_urls(self, event: str, year: int, config: "Configuration") -> list[str]:
        """Generate URLs for a given event and year."""
        pass


class ConferenceURLStrategy(VenueURLStrategy):
    """Standard conference URL pattern: /conf/{event}/{event}{year}.html"""

    def get_urls(self, event: str, year: int, config: "Configuration") -> list[str]:
        return [f"https://dblp.org/db/conf/{event}/{event}{year}.html"]


class MultiURLStrategy(VenueURLStrategy):
    """Strategy for events with multiple possible URLs."""

    def __init__(self, url_templates: list[str]):
        self.url_templates = url_templates

    def get_urls(self, event: str, year: int, config: "Configuration") -> list[str]:
        return [template.format(event=event, year=year) for template in self.url_templates]


class JournalVolumeStrategy(VenueURLStrategy):
    """Strategy for journals using volume-based URLs."""

    def __init__(self, journal_prefix: str, volume_map: dict[int, str]):
        self.journal_prefix = journal_prefix
        self.volume_map = volume_map

    def get_urls(self, event: str, year: int, config: "Configuration") -> list[str]:
        volume = self.volume_map.get(year)
        if not volume:
            return []
        return [
            f"https://dblp.org/db/journals/{self.journal_prefix}/{self.journal_prefix}{volume}.html"
        ]


class VenueStrategyRegistry:
    """Registry mapping event identifiers to URL strategies."""

    def __init__(self):
        self._strategies: dict[str, VenueURLStrategy] = {}
        self._register_default_strategies()

    def _register_default_strategies(self) -> None:
        self._strategies["asiaccs"] = MultiURLStrategy(
            [
                "https://dblp.org/db/conf/asiaccs/asiaccs{year}.html",
                "https://dblp.org/db/conf/ccs/asiaccs{year}.html",
            ]
        )

        self._strategies["sacmat"] = ConferenceURLStrategy()
        self._strategies["hotnets"] = ConferenceURLStrategy()
        self._strategies["ccs"] = ConferenceURLStrategy()
        self._strategies["uss"] = ConferenceURLStrategy()
        self._strategies["ndss"] = ConferenceURLStrategy()
        self._strategies["sp"] = ConferenceURLStrategy()
        self._strategies["eurosp"] = ConferenceURLStrategy()

        self._strategies["acm_csur"] = JournalVolumeStrategy(
            "csur",
            {
                2026: "58",
                2025: "57",
                2024: "56",
                2023: "55",
                2022: "54",
                2021: "53",
                2020: "52",
                2019: "52",
            },
        )

        self._strategies["ieee_comst"] = JournalVolumeStrategy(
            "comsur",
            {
                2026: "28",
                2025: "27",
                2024: "26",
                2023: "25",
                2022: "24",
                2021: "23",
                2020: "22",
                2019: "21",
            },
        )

        self._strategies["fnt_privsec"] = JournalVolumeStrategy(
            "ftsec",
            {
                2026: "8",
                2025: "8",
                2024: "7",
                2023: "6",
                2022: "5",
                2021: None,
                2020: "2",
                2019: "2",
            },
        )

    def get_strategy(self, event: str) -> VenueURLStrategy:
        """Get URL strategy for an event. Falls back to standard conference pattern."""
        return self._strategies.get(event, ConferenceURLStrategy())

    def register_strategy(self, event: str, strategy: VenueURLStrategy) -> None:
        """Register a custom strategy for an event."""
        self._strategies[event] = strategy
