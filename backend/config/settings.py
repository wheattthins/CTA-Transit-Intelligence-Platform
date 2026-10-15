"""Application settings loaded from environment variables (and an optional .env file).

All configuration lives here so the rest of the code never reads os.environ directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRAIN_TRACKER_ARRIVALS_URL = "https://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

# CTA asks developers to use the API responsibly. Predictions refresh roughly once a
# minute, so polling a station faster than every 30 seconds adds load without new data.
MIN_POLL_INTERVAL_SECONDS = 30

DEFAULT_STATION_IDS = ("41320",)  # Belmont (Red, Brown, Purple Express)


class SettingsError(ValueError):
    """Raised when configuration values are missing or invalid."""


@dataclass(frozen=True)
class Settings:
    api_key: str | None = field(repr=False)  # repr=False keeps the key out of logs/tracebacks
    station_ids: tuple[str, ...] = DEFAULT_STATION_IDS
    poll_interval_seconds: int = 60
    request_timeout_seconds: float = 10.0
    max_attempts: int = 3
    raw_data_dir: Path = PROJECT_ROOT / "data" / "raw"
    sample_runs_dir: Path = PROJECT_ROOT / "data" / "sample_runs"
    log_level: str = "INFO"
    arrivals_url: str = TRAIN_TRACKER_ARRIVALS_URL
    # Unity Catalog location raw files are uploaded to: /Volumes/<catalog>/<schema>/<volume>/
    databricks_catalog: str = "workspace"
    databricks_schema: str = "cta_bronze"
    databricks_landing_volume: str = "landing"

    @property
    def landing_volume_path(self) -> str:
        return f"/Volumes/{self.databricks_catalog}/{self.databricks_schema}/{self.databricks_landing_volume}"

    def require_api_key(self) -> str:
        if not self.api_key:
            raise SettingsError(
                "CTA_API_KEY is not set. Copy .env.example to .env and add your key, "
                "or run with --sample to use the bundled test fixture."
            )
        return self.api_key


def parse_station_ids(raw: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Normalize station IDs from a comma-separated string or list, validating format.

    CTA parent station IDs (``mapid``) are five-digit numbers in the 4xxxx range.
    """
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    ids = tuple(dict.fromkeys(p.strip() for p in parts if p.strip()))  # dedupe, keep order
    if not ids:
        raise SettingsError("At least one station ID is required.")
    for sid in ids:
        if not (sid.isdigit() and len(sid) == 5 and sid.startswith("4")):
            raise SettingsError(
                f"Invalid station ID {sid!r}: expected a 5-digit CTA parent station ID (4xxxx)."
            )
    return ids


def validate_poll_interval(seconds: int) -> int:
    if seconds < MIN_POLL_INTERVAL_SECONDS:
        raise SettingsError(
            f"Poll interval must be at least {MIN_POLL_INTERVAL_SECONDS}s (got {seconds}s)."
        )
    return seconds


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer (got {value!r}).") from exc


def _resolve_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_settings(env_file: Path | None = PROJECT_ROOT / ".env") -> Settings:
    """Build Settings from the environment. Values already in the environment win over .env."""
    if env_file is not None and env_file.exists():
        load_dotenv(env_file, override=False)

    max_attempts = _int_env("CTA_MAX_ATTEMPTS", 3)
    if max_attempts < 1:
        raise SettingsError("CTA_MAX_ATTEMPTS must be at least 1.")
    timeout = _int_env("CTA_REQUEST_TIMEOUT_SECONDS", 10)
    if timeout <= 0:
        raise SettingsError("CTA_REQUEST_TIMEOUT_SECONDS must be positive.")

    return Settings(
        api_key=(os.getenv("CTA_API_KEY") or "").strip() or None,
        station_ids=parse_station_ids(os.getenv("CTA_STATION_IDS") or ",".join(DEFAULT_STATION_IDS)),
        poll_interval_seconds=validate_poll_interval(_int_env("CTA_POLL_INTERVAL_SECONDS", 60)),
        request_timeout_seconds=float(timeout),
        max_attempts=max_attempts,
        raw_data_dir=_resolve_path(os.getenv("CTA_RAW_DATA_DIR"), PROJECT_ROOT / "data" / "raw"),
        log_level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
        databricks_catalog=_identifier_env("DATABRICKS_CATALOG", "workspace"),
        databricks_schema=_identifier_env("DATABRICKS_SCHEMA", "cta_bronze"),
        databricks_landing_volume=_identifier_env("DATABRICKS_LANDING_VOLUME", "landing"),
    )


def _identifier_env(name: str, default: str) -> str:
    """Unity Catalog names: letters, digits and underscores only (keeps paths/SQL safe)."""
    value = (os.getenv(name) or default).strip()
    if not value.replace("_", "").isalnum():
        raise SettingsError(f"{name} must contain only letters, digits and underscores (got {value!r}).")
    return value
