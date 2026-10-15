import json
from dataclasses import replace
from pathlib import Path

import pytest

from backend.collectors import train_tracker as tt
from backend.config.settings import Settings
from backend.services.cta_client import CTAApiError, CTATransientError
from backend.tests.conftest import load_fixture


class StubSource:
    """Returns the sample response for every call, or raises queued errors first."""

    def __init__(self, *errors: Exception):
        self.sample = tt.SampleArrivalsSource()
        self.errors = list(errors)
        self.calls: list[str] = []

    def get_arrivals(self, station_id: str):
        self.calls.append(station_id)
        if self.errors:
            raise self.errors.pop(0)
        return self.sample.get_arrivals(station_id)


def _saved_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.json"))


def test_collect_station_saves_raw_response(tmp_path, sample_payload):
    outcome = tt.collect_station(StubSource(), "41320", output_dir=tmp_path, is_sample=True)

    assert outcome.success and outcome.prediction_count == 5
    files = _saved_files(tmp_path)
    assert files == [outcome.saved_path]
    envelope = json.loads(files[0].read_text(encoding="utf-8"))
    assert envelope["response"] == sample_payload
    assert envelope["metadata"]["is_sample"] is True
    assert envelope["metadata"]["source_system"].endswith("_SAMPLE")


def test_collect_station_no_save(tmp_path):
    outcome = tt.collect_station(StubSource(), "41320", output_dir=tmp_path, save=False)
    assert outcome.success and outcome.saved_path is None
    assert _saved_files(tmp_path) == []


def test_cta_error_response_is_saved_and_reported(tmp_path):
    sample = tt.SampleArrivalsSource().get_arrivals("41320")
    error_resp = replace(sample, payload=load_fixture("error_invalid_key.json"))
    source = StubSource(CTAApiError("101", "Invalid API key", response=error_resp))

    outcome = tt.collect_station(source, "41320", output_dir=tmp_path)

    assert not outcome.success and outcome.error_code == "101"
    assert outcome.saved_path is not None and outcome.saved_path.exists()


def test_network_failure_is_reported_without_crashing(tmp_path):
    outcome = tt.collect_station(StubSource(CTATransientError("down")), "41320", output_dir=tmp_path)
    assert not outcome.success and "down" in outcome.error
    assert _saved_files(tmp_path) == []


def test_session_runs_bounded_iterations_on_fixed_cadence(tmp_path):
    sleeps: list[float] = []
    source = StubSource()
    outcomes = tt.run_session(
        source, ["41320"], iterations=3, interval_seconds=60,
        output_dir=tmp_path, sleep=sleeps.append, monotonic=lambda: 0.0,
    )
    assert len(outcomes) == 3 and all(o.success for o in outcomes)
    assert source.calls == ["41320"] * 3
    assert sleeps == [60, 60]  # no sleep after the final cycle
    assert len(_saved_files(tmp_path)) == 3  # each observation is a separate file


def test_session_stops_on_fatal_error(tmp_path):
    sample = tt.SampleArrivalsSource().get_arrivals("41320")
    fatal = CTAApiError("102", "Daily limit exceeded", response=sample)
    source = StubSource(fatal)
    outcomes = tt.run_session(
        source, ["41320"], iterations=5, interval_seconds=60,
        output_dir=tmp_path, sleep=lambda s: None,
    )
    assert len(outcomes) == 1 and outcomes[0].error_code == "102"


def test_session_continues_after_transient_error(tmp_path):
    source = StubSource(CTATransientError("blip"))
    outcomes = tt.run_session(
        source, ["41320"], iterations=2, interval_seconds=60,
        output_dir=tmp_path, sleep=lambda s: None,
    )
    assert [o.success for o in outcomes] == [False, True]


def test_sample_source_rejects_unknown_station():
    with pytest.raises(Exception, match="Sample mode"):
        tt.SampleArrivalsSource().get_arrivals("40380")


# ---- CLI ------------------------------------------------------------------


def _settings(tmp_path, api_key=None) -> Settings:
    return Settings(api_key=api_key, raw_data_dir=tmp_path / "raw", sample_runs_dir=tmp_path / "sample_runs")


def test_cli_sample_mode_writes_to_sample_dir_only(tmp_path, capsys):
    code = tt.main(["--sample"], settings=_settings(tmp_path))

    assert code == tt.EXIT_OK
    assert len(_saved_files(tmp_path / "sample_runs")) == 1
    assert not (tmp_path / "raw").exists()
    assert "SAMPLE DATA" in capsys.readouterr().out


def test_cli_live_mode_without_key_is_config_error(tmp_path):
    assert tt.main([], settings=_settings(tmp_path)) == tt.EXIT_CONFIG_ERROR


@pytest.mark.parametrize(
    "argv",
    [["--interval", "5"], ["--station", "123"], ["--iterations", "0"], ["--sample", "--duration-minutes", "-1"]],
)
def test_cli_rejects_invalid_arguments(tmp_path, argv):
    assert tt.main(argv, settings=_settings(tmp_path, api_key="k")) == tt.EXIT_CONFIG_ERROR


def test_resolve_iterations_from_duration():
    args = tt.build_arg_parser().parse_args(["--duration-minutes", "10"])
    assert tt.resolve_iterations(args, interval=60) == 11  # polls at t=0,1,...,10 min
