"""Upload raw observation files to a Databricks Unity Catalog volume (the Bronze landing zone).

Collection runs outside Databricks (GitHub Actions, or your machine) because Databricks Free Edition
limits outbound internet access.
This script moves the raw JSON files into the lakehouse, where the Bronze notebook ingests them.

    python -m backend.uploaders.volume_uploader --dry-run   # show what would be uploaded
    python -m backend.uploaders.volume_uploader             # upload new files

Properties:
  * idempotent: lists each remote partition folder once and uploads only missing files,
    so re-running never duplicates or overwrites anything
  * preserves the local partition layout (collection_date=/station_id=) under the volume
  * never uploads SAMPLE fixture output
  * authenticates with DATABRICKS_HOST / DATABRICKS_TOKEN from the environment or .env
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Protocol

from backend.config.settings import PROJECT_ROOT, Settings, SettingsError, load_settings

logger = logging.getLogger("cta.uploader")

# Observation files (Bronze input) and collector run summaries (operational monitoring).
RAW_FILE_GLOBS = ("arrivals_*.json", "collector_run_*.json")
EXIT_OK, EXIT_PARTIAL_FAILURE, EXIT_CONFIG_ERROR = 0, 1, 2


class RemoteFileStore(Protocol):
    def ensure_volume(self, catalog: str, schema: str, volume: str) -> None: ...
    def list_file_names(self, directory: str) -> set[str]: ...
    def upload(self, local_path: Path, remote_path: str) -> bool:
        """Upload without overwriting. Returns False if the remote file already exists."""
        ...


class DatabricksVolumeStore:
    """RemoteFileStore backed by the Databricks SDK (Unity Catalog Files API)."""

    def __init__(self) -> None:
        from databricks.sdk import WorkspaceClient

        # Reads DATABRICKS_HOST and DATABRICKS_TOKEN (or a ~/.databrickscfg profile).
        self._client = WorkspaceClient()

    def ensure_volume(self, catalog: str, schema: str, volume: str) -> None:
        from databricks.sdk.errors import NotFound
        from databricks.sdk.service.catalog import VolumeType

        try:
            self._client.schemas.get(f"{catalog}.{schema}")
        except NotFound:
            logger.info("Creating schema %s.%s", catalog, schema)
            self._client.schemas.create(name=schema, catalog_name=catalog, comment="CTA Bronze layer")
        try:
            self._client.volumes.read(f"{catalog}.{schema}.{volume}")
        except NotFound:
            logger.info("Creating managed volume %s.%s.%s", catalog, schema, volume)
            self._client.volumes.create(
                catalog_name=catalog,
                schema_name=schema,
                name=volume,
                volume_type=VolumeType.MANAGED,
                comment="Landing zone for raw CTA API observation files",
            )

    def list_file_names(self, directory: str) -> set[str]:
        from databricks.sdk.errors import NotFound

        try:
            return {
                entry.name
                for entry in self._client.files.list_directory_contents(directory)
                if not entry.is_directory and entry.name
            }
        except NotFound:
            return set()

    def upload(self, local_path: Path, remote_path: str) -> bool:
        from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

        try:
            with local_path.open("rb") as fh:
                self._client.files.upload(remote_path, fh, overwrite=False)
            return True
        except (AlreadyExists, ResourceAlreadyExists):
            return False


@dataclass
class UploadSummary:
    uploaded: list[str] = field(default_factory=list)
    already_present: int = 0
    skipped_samples: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def find_partitions(local_root: Path) -> dict[PurePosixPath, list[Path]]:
    """Group local raw files by their directory, relative to ``local_root``."""
    partitions: dict[PurePosixPath, list[Path]] = {}
    paths = sorted({p for pattern in RAW_FILE_GLOBS for p in local_root.rglob(pattern)})
    for path in paths:
        rel_dir = PurePosixPath(path.parent.relative_to(local_root).as_posix())
        partitions.setdefault(rel_dir, []).append(path)
    return partitions


def is_sample_file(path: Path) -> bool:
    try:
        return bool(json.loads(path.read_text(encoding="utf-8"))["metadata"]["is_sample"])
    except (OSError, ValueError, KeyError, TypeError):
        return False  # unreadable files still go to Bronze, which flags parse errors


def upload_new_files(
    store: RemoteFileStore, local_root: Path, volume_root: str, *, dry_run: bool = False
) -> UploadSummary:
    summary = UploadSummary()
    for rel_dir, files in find_partitions(local_root).items():
        remote_dir = f"{volume_root}/{rel_dir}" if str(rel_dir) != "." else volume_root
        existing = store.list_file_names(remote_dir)
        for local_path in files:
            if local_path.name in existing:
                summary.already_present += 1
                continue
            remote_path = f"{remote_dir}/{local_path.name}"
            if is_sample_file(local_path):
                logger.warning("Skipping SAMPLE file (not live data): %s", local_path.name)
                summary.skipped_samples.append(remote_path)
                continue
            if dry_run:
                logger.info("[dry-run] would upload %s", remote_path)
                summary.uploaded.append(remote_path)
                continue
            try:
                if store.upload(local_path, remote_path):
                    logger.debug("Uploaded %s", remote_path)
                    summary.uploaded.append(remote_path)
                else:
                    summary.already_present += 1
            except Exception as exc:  # noqa: BLE001 - keep going; report every failure at the end
                logger.error("Failed to upload %s: %s", local_path.name, exc)
                summary.failed.append((remote_path, str(exc)))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m backend.uploaders.volume_uploader",
        description="Upload new raw CTA observation files to the Databricks landing volume.",
    )
    parser.add_argument("--dry-run", action="store_true", help="List files that would be uploaded; change nothing.")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Overrides LOG_LEVEL.")
    return parser


def main(argv: Sequence[str] | None = None, store: RemoteFileStore | None = None, settings: Settings | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        settings = settings or load_settings()
    except SettingsError as exc:
        logging.basicConfig(level="INFO")
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR
    logging.basicConfig(
        level=args.log_level or settings.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stderr,
    )
    # The SDK logs request details at DEBUG; keep auth headers out of our logs.
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)

    local_root = settings.raw_data_dir
    if not local_root.exists():
        logger.error("Raw data directory not found: %s", local_root)
        return EXIT_CONFIG_ERROR

    volume_root = settings.landing_volume_path
    try:
        store = store or DatabricksVolumeStore()
        if not args.dry_run:
            store.ensure_volume(settings.databricks_catalog, settings.databricks_schema, settings.databricks_landing_volume)
        logger.info("Syncing %s -> %s%s", _display(local_root), volume_root, " (dry run)" if args.dry_run else "")
        summary = upload_new_files(store, local_root, volume_root, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001 - top-level CLI handler: classify and report
        name = type(exc).__name__
        if name in {"Unauthenticated", "PermissionDenied"} or "default auth" in str(exc).lower():
            logger.error(
                "Databricks authentication failed (%s). Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env "
                "- see docs/databricks_setup.md.", name,
            )
        else:
            logger.error("Upload failed: %s: %s", name, exc)
        return EXIT_CONFIG_ERROR

    verb = "Would upload" if args.dry_run else "Uploaded"
    logger.info(
        "%s %d file(s); %d already present; %d sample file(s) skipped; %d failed.",
        verb, len(summary.uploaded), summary.already_present, len(summary.skipped_samples), len(summary.failed),
    )
    return EXIT_PARTIAL_FAILURE if summary.failed else EXIT_OK


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    sys.exit(main())
