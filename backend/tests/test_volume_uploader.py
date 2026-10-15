import json
from pathlib import Path

from backend.config.settings import Settings
from backend.uploaders import volume_uploader as vu

VOLUME = "/Volumes/workspace/cta_bronze/landing"
PARTITION = "train_tracker/arrivals/collection_date=2026-10-15/station_id=41320"


class FakeStore:
    def __init__(self, existing: set[str] | None = None, fail_on: set[str] | None = None):
        self.files: set[str] = set(existing or ())  # full remote paths
        self.fail_on = fail_on or set()
        self.uploads: list[str] = []
        self.list_calls: list[str] = []
        self.volume_ensured = False

    def ensure_volume(self, catalog, schema, volume):
        self.volume_ensured = (catalog, schema, volume) == ("workspace", "cta_bronze", "landing")

    def list_file_names(self, directory):
        self.list_calls.append(directory)
        return {p.rsplit("/", 1)[1] for p in self.files if p.rsplit("/", 1)[0] == directory}

    def upload(self, local_path: Path, remote_path: str) -> bool:
        if local_path.name in self.fail_on:
            raise RuntimeError("network down")
        if remote_path in self.files:
            return False
        self.files.add(remote_path)
        self.uploads.append(remote_path)
        return True


def _write(root: Path, name: str, *, is_sample: bool = False, partition: str = PARTITION) -> Path:
    path = root / partition / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"metadata": {"is_sample": is_sample}, "response": {"ctatt": {}}}), encoding="utf-8")
    return path


def test_uploads_new_files_preserving_partition_layout(tmp_path):
    _write(tmp_path, "arrivals_41320_a.json")
    store = FakeStore()

    summary = vu.upload_new_files(store, tmp_path, VOLUME)

    assert store.uploads == [f"{VOLUME}/{PARTITION}/arrivals_41320_a.json"]
    assert summary.already_present == 0 and not summary.failed


def test_rerun_is_idempotent(tmp_path):
    _write(tmp_path, "arrivals_41320_a.json")
    _write(tmp_path, "arrivals_41320_b.json")
    store = FakeStore()

    vu.upload_new_files(store, tmp_path, VOLUME)
    second = vu.upload_new_files(store, tmp_path, VOLUME)

    assert len(store.uploads) == 2
    assert second.uploaded == [] and second.already_present == 2


def test_lists_each_partition_once(tmp_path):
    for i in range(3):
        _write(tmp_path, f"arrivals_41320_{i}.json")
    _write(tmp_path, "arrivals_40380_x.json", partition=PARTITION.replace("41320", "40380"))
    store = FakeStore()
    vu.upload_new_files(store, tmp_path, VOLUME)
    assert len(store.list_calls) == 2


def test_sample_files_are_never_uploaded(tmp_path):
    _write(tmp_path, "arrivals_41320_sample.json", is_sample=True)
    store = FakeStore()
    summary = vu.upload_new_files(store, tmp_path, VOLUME)
    assert store.uploads == [] and len(summary.skipped_samples) == 1


def test_non_matching_and_temp_files_are_ignored(tmp_path):
    _write(tmp_path, ".tmp_abc.json")
    _write(tmp_path, "notes.json")
    store = FakeStore()
    vu.upload_new_files(store, tmp_path, VOLUME)
    assert store.uploads == []


def test_dry_run_uploads_nothing(tmp_path):
    _write(tmp_path, "arrivals_41320_a.json")
    store = FakeStore()
    summary = vu.upload_new_files(store, tmp_path, VOLUME, dry_run=True)
    assert store.uploads == [] and len(summary.uploaded) == 1


def test_failures_are_reported_and_other_files_continue(tmp_path):
    _write(tmp_path, "arrivals_41320_a.json")
    _write(tmp_path, "arrivals_41320_b.json")
    store = FakeStore(fail_on={"arrivals_41320_a.json"})
    summary = vu.upload_new_files(store, tmp_path, VOLUME)
    assert len(summary.failed) == 1 and len(store.uploads) == 1


def test_main_ensures_volume_and_returns_status(tmp_path):
    _write(tmp_path, "arrivals_41320_a.json")
    store = FakeStore()
    settings = Settings(api_key=None, raw_data_dir=tmp_path)
    assert vu.main([], store=store, settings=settings) == vu.EXIT_OK
    assert store.volume_ensured and len(store.uploads) == 1

    failing = FakeStore(fail_on={"arrivals_41320_a.json"})
    assert vu.main([], store=failing, settings=settings) == vu.EXIT_PARTIAL_FAILURE


def test_main_missing_raw_dir_is_config_error(tmp_path):
    settings = Settings(api_key=None, raw_data_dir=tmp_path / "missing")
    assert vu.main([], store=FakeStore(), settings=settings) == vu.EXIT_CONFIG_ERROR


def test_works_with_real_collector_output(tmp_path):
    """Files produced by the Phase 1 collector are picked up with their partition path."""
    from backend.collectors.train_tracker import SampleArrivalsSource, collect_station

    out = tmp_path / "raw"
    outcome = collect_station(SampleArrivalsSource(), "41320", output_dir=out, is_sample=False)
    store = FakeStore()
    vu.upload_new_files(store, out, VOLUME)
    rel = outcome.saved_path.relative_to(out).as_posix()
    assert store.uploads == [f"{VOLUME}/{rel}"]
