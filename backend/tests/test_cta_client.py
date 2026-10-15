import logging

import pytest
import requests

from backend.services.cta_client import (
    CTAApiError,
    CTAResponseError,
    CTATransientError,
    redact,
)
from backend.tests.conftest import FAKE_KEY, FakeResponse, load_fixture


def test_successful_request_returns_payload_and_metadata(make_client, sample_payload):
    client, session = make_client(FakeResponse(200, sample_payload))

    result = client.get_arrivals("41320")

    assert result.payload == sample_payload
    assert result.http_status == 200
    assert result.attempts == 1
    assert result.requested_at_utc.tzinfo is not None
    assert session.calls[0]["params"] == {"key": FAKE_KEY, "mapid": "41320", "outputType": "JSON"}
    assert session.calls[0]["timeout"] == 5


def test_request_params_in_result_never_include_api_key(make_client, sample_payload):
    client, _ = make_client(FakeResponse(200, sample_payload))
    result = client.get_arrivals("41320")
    assert "key" not in result.request_params
    assert FAKE_KEY not in repr(result)
    assert FAKE_KEY not in repr(client)


def test_invalid_key_raises_non_retryable_api_error(make_client):
    client, session = make_client(FakeResponse(200, load_fixture("error_invalid_key.json")))

    with pytest.raises(CTAApiError) as exc_info:
        client.get_arrivals("41320")

    assert exc_info.value.code == "101"
    assert exc_info.value.response is not None  # body kept so it can be saved
    assert len(session.calls) == 1  # not retried


def test_cta_server_error_is_retried_then_succeeds(make_client, sample_payload):
    client, session = make_client(
        FakeResponse(200, load_fixture("error_server.json")),
        FakeResponse(200, sample_payload),
    )
    result = client.get_arrivals("41320")
    assert result.attempts == 2
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    "failure",
    [
        requests.ConnectionError("connection refused"),
        requests.Timeout("read timed out"),
        FakeResponse(503, {}),
        FakeResponse(429, {}),
    ],
    ids=["connection", "timeout", "http503", "http429"],
)
def test_transient_failures_are_retried(make_client, sample_payload, failure):
    client, session = make_client(failure, FakeResponse(200, sample_payload))
    assert client.get_arrivals("41320").attempts == 2
    assert len(session.calls) == 2


def test_gives_up_after_max_attempts(make_client):
    client, session = make_client(*[requests.ConnectionError("down")] * 3, max_attempts=3)
    with pytest.raises(CTATransientError):
        client.get_arrivals("41320")
    assert len(session.calls) == 3


def test_http_4xx_is_not_retried(make_client):
    client, session = make_client(FakeResponse(404, {}))
    with pytest.raises(CTAResponseError):
        client.get_arrivals("41320")
    assert len(session.calls) == 1


def test_non_json_body_raises_response_error(make_client):
    client, _ = make_client(FakeResponse(200, text="<html>maintenance</html>"))
    with pytest.raises(CTAResponseError, match="not valid JSON"):
        client.get_arrivals("41320")


def test_missing_ctatt_root_raises_response_error(make_client):
    client, _ = make_client(FakeResponse(200, {"unexpected": True}))
    with pytest.raises(CTAResponseError, match="ctatt"):
        client.get_arrivals("41320")


def test_api_key_is_redacted_from_network_errors_and_logs(make_client, caplog):
    leaky = requests.ConnectionError(
        f"Max retries exceeded with url: /api/1.0/ttarrivals.aspx?key={FAKE_KEY}&mapid=41320"
    )
    client, _ = make_client(leaky, leaky, leaky)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(CTATransientError) as exc_info:
        client.get_arrivals("41320")

    assert FAKE_KEY not in str(exc_info.value)
    assert exc_info.value.__cause__ is None  # original (leaky) exception not chained
    assert FAKE_KEY not in caplog.text
    assert "REDACTED" in str(exc_info.value)


def test_redact_handles_empty_secret():
    assert redact("abc", None) == "abc"
    assert redact("key=abc", "abc") == "key=***REDACTED***"
