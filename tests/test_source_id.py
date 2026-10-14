"""CANTON_SOURCE_ID must be honoured when it is set in .env. runner.cli() loads .env after the runtime
module is imported, so the source id has to be read at emit time, not at import time.

Runs under pytest and standalone (python tests/test_source_id.py)."""
import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv
from sentinel_canton.runner import build_detector, run_over_fixture
from sentinel_canton.runtime import DEFAULT_SOURCE_ID, source_id

KEY = "CANTON_SOURCE_ID"
FIXTURE = ROOT / "tests" / "fixtures" / "all_scenarios.jsonl"


def _clear(monkeypatch):
    # setenv then delenv: monkeypatch restores the variable to absent afterwards, including any value
    # load_dotenv() sets during the test, which it would otherwise leave behind in os.environ
    monkeypatch.setenv(KEY, "placeholder")
    monkeypatch.delenv(KEY)


def _write_dotenv(tmp_path, value):
    env_file = tmp_path / ".env"
    env_file.write_text(f"{KEY}={value}\n")
    return env_file


def _first_event_sid():
    # a low stakeholder limit makes the stream raise alerts, whichever sid they are stamped with
    detector = build_detector("stakeholder_anomaly", {"max_stakeholders_default": 1})
    events = asyncio.run(run_over_fixture(detector, FIXTURE))
    assert events, "expected the detector to emit at least one event"
    return events[0].sid


def test_defaults_to_extractor_source(monkeypatch):
    _clear(monkeypatch)
    assert source_id() == DEFAULT_SOURCE_ID == "ext:sentinel"


def test_dotenv_value_is_used_by_emitted_events(monkeypatch, tmp_path):
    _clear(monkeypatch)
    load_dotenv(_write_dotenv(tmp_path, "ext:from-dotenv"))
    assert source_id() == "ext:from-dotenv"
    assert _first_event_sid() == "ext:from-dotenv"


def test_variable_already_in_environment_wins_over_dotenv(monkeypatch, tmp_path):
    _clear(monkeypatch)
    monkeypatch.setenv(KEY, "ext:from-shell")
    load_dotenv(_write_dotenv(tmp_path, "ext:from-dotenv"))  # override=False by default
    assert source_id() == "ext:from-shell"
    assert _first_event_sid() == "ext:from-shell"


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-q", "--no-cov"]))
