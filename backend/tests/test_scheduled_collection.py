import json
import os
import time

from backend.collectors import train_tracker as tt
from backend.config.settings import Settings
from backend.jobs import scheduled_collection as sc
from backend.jobs.run_lock import RunLock
from backend.services.cta_client import CTATransientError
from backend.tests.test_volume_uploader import FakeStore


class FlakySource:
    """Sample responses, failing on the listed call numbers (1-based)."""

    def __init__(self, fail_calls=()):
        self.sample = tt.SampleArrivalsSource()
        self.fail_calls = set(fail_calls)
        self.calls = 0

    def get_arrivals(self, station_id):
        self.calls += 1
        if self.calls in self.fail_calls:
            raise CTATransientError(f"Network error requesting station {station_id}: down")
        return self.sample.get_arrivals(station_id)


class FakeClock:
    """monotonic() that advances only when sleep() is called."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _settings(tmp_path):
    return Settings(api_key="unused", raw_data_dir=tmp_path / "raw", poll_interval_seconds=60)


def _run(tmp_path, argv, source=None, store=None, clock=None):
    clock = clock or FakeClock()
    store = store if store is not None else FakeStore()
    code = sc.main(
        argv,
        settings=_settings(tmp_path),
        source=source or FlakySource(),
        store_factory=lambda: store,
        lock_path=tmp_path / ".lock",
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        log_path=None,
    )
    return code, store


def _summary(tmp_path):
    files = list((tmp_path / "raw" / "collector_runs").rglob("collector_run_*.json"))
    assert len(files) == 1
    return json.loads(files[0].read_text(encoding="utf-8"))


def test_session_collects_uploads_and_writes_summary(tmp_path):
    code, store = _run(tmp_path, ["--duration-minutes", "10", "--upload-every-minutes", "5"])

    assert code == sc.EXIT_OK
    summary = _summary(tmp_path)
    assert summary["requests"] == 11 and summary["succeeded"] == 11 and summary["failed"] == 0
    assert summary["files_written"] == 11
    # every observation was uploaded before the summary was written, then the summary itself
    assert summary["files_uploaded"] == 11 and len(store.uploads) == 12
    assert any("collector_runs/run_date=" in p for p in store.uploads)
    assert sum("/arrivals_" in p for p in store.uploads) == 11


def test_uploads_happen_periodically_not_every_cycle(tmp_path):
    store = FakeStore()
    calls = []
    original = sc.upload_new_files

    def counting(*a, **k):
        calls.append(1)
        return original(*a, **k)

    sc.upload_new_files = counting
    try:
        _run(tmp_path, ["--duration-minutes", "10", "--upload-every-minutes", "5"], store=store)
    finally:
        sc.upload_new_files = original
    # cycles at t=0..10 min: uploads at 0, 5, 10, then final data + summary -> 5, not 11+
    assert len(calls) == 5


def test_failures_are_recorded_with_reasons(tmp_path):
    code, _ = _run(tmp_path, ["--duration-minutes", "3", "--no-upload"], source=FlakySource(fail_calls={2}))
    assert code == sc.EXIT_PARTIAL_FAILURE
    s = _summary(tmp_path)
    assert s["failed"] == 1 and s["succeeded"] == 3
    f = s["failures"][0]
    assert f["error_type"] == "CTATransientError" and f["station_id"] == "41320"
    assert f["attempted_at_utc"].endswith("Z")
    assert s["uploads_disabled_reason"] == "not requested"


def test_upload_problems_never_stop_collection(tmp_path):
    def broken_factory():
        raise RuntimeError("default auth: cannot configure default credentials")

    code = sc.main(
        ["--duration-minutes", "2"],
        settings=_settings(tmp_path),
        source=FlakySource(),
        store_factory=broken_factory,
        lock_path=tmp_path / ".lock",
        sleep=FakeClock().sleep,
        monotonic=FakeClock().monotonic,
        log_path=None,
    )
    assert code == sc.EXIT_OK
    s = _summary(tmp_path)
    assert s["succeeded"] == 3 and "default auth" in s["uploads_disabled_reason"]


def test_second_run_is_skipped_while_first_holds_lock(tmp_path):
    lock = RunLock(tmp_path / ".lock", stale_after_seconds=3600)
    assert lock.acquire()
    try:
        code, store = _run(tmp_path, ["--duration-minutes", "1"])
    finally:
        lock.release()
    assert code == sc.EXIT_SKIPPED and store.uploads == []


def test_stale_lock_is_replaced(tmp_path):
    path = tmp_path / ".lock"
    path.write_text("{}")
    old = time.time() - 7200
    os.utime(path, (old, old))
    lock = RunLock(path, stale_after_seconds=3600)
    assert lock.acquire()
    lock.release()
    assert not path.exists()


def test_fresh_lock_is_respected(tmp_path):
    path = tmp_path / ".lock"
    path.write_text("{}")
    assert not RunLock(path, stale_after_seconds=3600).acquire()
    assert path.exists()


def test_summary_contains_no_api_key(tmp_path):
    _run(tmp_path, ["--duration-minutes", "1", "--no-upload"], source=FlakySource(fail_calls={1}))
    text = next((tmp_path / "raw" / "collector_runs").rglob("*.json")).read_text()
    assert "unused" not in text  # the api_key value in _settings
