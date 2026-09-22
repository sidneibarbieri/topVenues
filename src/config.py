"""Configuration management with YAML support."""

import os
from pathlib import Path

import yaml

from .models import Configuration
from .profiles import (
    PROFILE_ENV_VAR,
    PROJECT_ROOT,
    profile_config_path,
    select_profile_id,
)


class ConfigManager:
    """Loads and persists application configuration from a YAML file."""

    def __init__(self, config_path: Path = Path("config.yaml")):
        self.config_path = config_path
        self._config: Configuration | None = None

    def load(self) -> Configuration:
        if self._config is not None:
            return self._config
        if self.config_path.exists():
            with open(self.config_path, encoding="utf-8") as fh:
                self._config = Configuration(**yaml.safe_load(fh))
        else:
            self._config = Configuration()
            self.save(self._config)
        return self._config

    def save(self, config: Configuration) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as fh:
            yaml.dump(
                config.model_dump(by_alias=False),
                fh,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

    def get(self) -> Configuration:
        return self._config if self._config is not None else self.load()

    def reload(self) -> Configuration:
        self._config = None
        return self.load()


_config_manager: ConfigManager | None = None


def get_config_manager() -> ConfigManager:
    global _config_manager
    if _config_manager is None:
        requested_profile = os.getenv(PROFILE_ENV_VAR)
        config_path = (
            profile_config_path(select_profile_id(requested_profile), PROJECT_ROOT)
            if requested_profile
            else PROJECT_ROOT / "config.yaml"
        )
        if not config_path.is_file():
            raise FileNotFoundError(config_path)
        _config_manager = ConfigManager(config_path)
    return _config_manager


def set_configuration_path(config_path: Path) -> None:
    """Select one configuration for the current process before collectors start."""
    global _config_manager
    _config_manager = ConfigManager(Path(config_path))


def activate_profile(profile_id: str | None = None, root: Path = PROJECT_ROOT) -> str:
    """Select a named profile for subsequent configuration loads."""
    selected = select_profile_id(profile_id)
    config_path = profile_config_path(selected, root)
    if not config_path.is_file():
        raise FileNotFoundError(config_path)
    set_configuration_path(config_path)
    return selected


def load_configuration(config_path: Path | None = None) -> Configuration:
    if config_path:
        return ConfigManager(config_path).load()
    return get_config_manager().load()
