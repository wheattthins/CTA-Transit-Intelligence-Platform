import pytest

from backend.config.settings import (
    SettingsError,
    load_settings,
    parse_station_ids,
    validate_poll_interval,
)


def test_parse_station_ids_trims_and_dedupes():
    assert parse_station_ids(" 41320, 40380 ,41320") == ("41320", "40380")


@pytest.mark.parametrize("bad", ["", "abc", "4132", "31320", "413200"])
def test_parse_station_ids_rejects_invalid(bad):
    with pytest.raises(SettingsError):
        parse_station_ids(bad)


def test_poll_interval_minimum_enforced():
    assert validate_poll_interval(30) == 30
    with pytest.raises(SettingsError):
        validate_poll_interval(10)


def test_load_settings_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("CTA_API_KEY", "secret-from-env")
    monkeypatch.setenv("CTA_STATION_IDS", "41320,40380")
    monkeypatch.setenv("CTA_POLL_INTERVAL_SECONDS", "90")
    settings = load_settings(env_file=None)
    assert settings.require_api_key() == "secret-from-env"
    assert settings.station_ids == ("41320", "40380")
    assert settings.poll_interval_seconds == 90
    assert "secret-from-env" not in repr(settings)


def test_missing_api_key_gives_helpful_error(monkeypatch):
    monkeypatch.delenv("CTA_API_KEY", raising=False)
    with pytest.raises(SettingsError, match="--sample"):
        load_settings(env_file=None).require_api_key()


def test_non_integer_env_value_is_config_error(monkeypatch):
    monkeypatch.setenv("CTA_POLL_INTERVAL_SECONDS", "fast")
    with pytest.raises(SettingsError):
        load_settings(env_file=None)
