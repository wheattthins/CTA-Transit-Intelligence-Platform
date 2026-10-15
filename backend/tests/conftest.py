import json
from pathlib import Path
from typing import Any

import pytest
import requests

from backend.config.settings import PROJECT_ROOT

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_PATH = PROJECT_ROOT / "data" / "samples" / "SAMPLE_ttarrivals_belmont_41320.json"

FAKE_KEY = "test-key-abc123-SECRET"


def load_fixture(name: str) -> dict[str, Any]:
    path = SAMPLE_PATH if name == "sample" else FIXTURES / name
    return json.loads(path.read_text(encoding="utf-8"))


class FakeResponse:
    def __init__(self, status_code: int = 200, json_body: Any = None, text: str | None = None):
        self.status_code = status_code
        self._json = json_body
        self._text = text
        self.headers = {"Content-Type": "application/json" if text is None else "text/html"}

    def json(self) -> Any:
        if self._text is not None:
            raise ValueError("not json")
        return self._json


class FakeSession:
    """Stands in for requests.Session: returns queued responses or raises queued exceptions."""

    def __init__(self, *outcomes: FakeResponse | Exception):
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, str], timeout: float) -> FakeResponse:
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def sample_payload() -> dict[str, Any]:
    return load_fixture("sample")


@pytest.fixture
def make_client():
    from backend.services.cta_client import CTAClient

    def _make(*outcomes: FakeResponse | Exception, max_attempts: int = 3):
        session = FakeSession(*outcomes)
        client = CTAClient(
            FAKE_KEY,
            base_url="https://example.test/ttarrivals.aspx",
            timeout_seconds=5,
            max_attempts=max_attempts,
            session=session,  # type: ignore[arg-type]
            backoff_initial_seconds=0,  # no real waiting in tests
            backoff_max_seconds=0,
        )
        return client, session

    return _make


# Re-exported for test modules
__all__ = ["FAKE_KEY", "FakeResponse", "FakeSession", "load_fixture", "requests"]
