"""Core runtime + runner tests: drive a detector over the recorded fixtures with no Kafka.

Proves the standalone path fixture -> CantonUpdate -> detector.on_update -> Event works, that the
runtime's dedup-by-update_id holds, and that the vendored model parses every fixture. Runs under
pytest and standalone (python tests/test_runner.py)."""
import asyncio
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.event import Event
from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, _parameters, _schema_for, build_detector, run_over_fixture

FIX = ROOT / "tests" / "fixtures"


def _run(name, fixture):
    return asyncio.run(run_over_fixture(build_detector(name), FIX / fixture))


def test_dedup_drops_repeated_update_id():
    # feeding the same file twice must not double the alerts: every update is processed once,
    # so the second feed adds nothing
    updates = [
        CantonUpdate.model_validate_json(line)
        for line in (FIX / "all_scenarios.jsonl").read_text().splitlines()
        if line.strip()
    ]
    updates = [update for update in updates if update.update_id]  # snapshots carry none, are never deduped
    params = {"max_stakeholders_default": 1}  # low limit, so the stream raises alerts

    async def run(batches):
        detector = build_detector("stakeholder_anomaly", params)
        sink = ListEventSink()
        detector.outputs.events = sink
        detector.init()
        await detector.on_init()
        for update in batches:
            await detector.handle_update(update)
        return sink.events

    once = asyncio.run(run(updates))
    twice = asyncio.run(run(updates + updates))
    assert once, "expected the low limit to raise at least one alert"
    assert len(twice) == len(once)


def test_unknown_detector_raises():
    try:
        build_detector("does-not-exist")
    except SystemExit:
        return
    raise AssertionError("expected SystemExit for an unknown detector")


def test_unknown_parameter_key_raises():
    try:
        build_detector("stakeholder_anomaly", {"max_stakeholder_default": 5})  # missing the 's'
    except SystemExit as e:
        assert "max_stakeholder_default" in str(e)
        return
    raise AssertionError("expected SystemExit for a mistyped parameter key")


def test_wrong_parameter_type_raises():
    try:
        build_detector("stakeholder_anomaly", {"max_stakeholders_default": [1, 2]})
    except SystemExit:
        return
    raise AssertionError("expected SystemExit for a wrong-typed parameter")


def test_valid_parameters_and_runner_keys_pass():
    # a correct key and a runner-injected key build cleanly
    build_detector("stakeholder_anomaly", {"max_stakeholders_default": 5})
    build_detector("reassignment_limbo", {"state_path": "/tmp/x.json"})


def test_schema_for_detector_without_manifest_is_none():
    assert _schema_for("no_such_detector") is None


class _FailingInit:
    """Minimal stand-in for a detector whose init() raises."""
    name = "AnyDetector"

    def __init__(self, exc):
        self._exc = exc

    def init(self):
        raise self._exc

    async def on_init(self):
        pass


def test_detector_init_errors_propagate_unchanged():
    # the runner adds no error mapping of its own: a detector's exception surfaces with its own type
    from sentinel_canton.runner import _start_detector
    try:
        asyncio.run(_start_detector(_FailingInit(KeyError("some_key"))))
    except KeyError:
        return
    raise AssertionError("expected the original KeyError to propagate")


def test_main_requires_detector_name():
    from sentinel_canton.runner import main
    saved = os.environ.pop("CANTON_DETECTOR", None)
    try:
        try:
            asyncio.run(main())
        except SystemExit as exit_error:
            assert "CANTON_DETECTOR is required" in str(exit_error)
            return
        raise AssertionError("expected SystemExit when CANTON_DETECTOR is unset")
    finally:
        if saved is not None:
            os.environ["CANTON_DETECTOR"] = saved


def _parameters_with(env, name="capability_drift"):
    keys = ("CANTON_ADMIN_API_URL", "CANTON_PARAMETERS")
    saved = {k: os.environ.pop(k, None) for k in keys}
    os.environ.update(env)
    try:
        return _parameters(name)
    finally:
        for k in keys:
            os.environ.pop(k, None)
            if saved[k] is not None:
                os.environ[k] = saved[k]


def test_admin_api_url_defaults_to_local_node():
    assert _parameters_with({})["admin_api_url"] == "localhost:5012"
    assert _parameters_with({"CANTON_ADMIN_API_URL": "node:6012"})["admin_api_url"] == "node:6012"


def test_admin_api_url_empty_disables_and_parameters_win():
    assert _parameters_with({"CANTON_ADMIN_API_URL": ""})["admin_api_url"] == ""
    env = {"CANTON_ADMIN_API_URL": "node:6012", "CANTON_PARAMETERS": '{"admin_api_url": "x:1"}'}
    assert _parameters_with(env)["admin_api_url"] == "x:1"


def test_admin_api_url_not_injected_for_other_detectors():
    assert "admin_api_url" not in _parameters_with({}, name="topology_drift")


def test_model_parses_all_fixtures():
    n = sum(1 for f in FIX.glob("*.jsonl") for line in f.read_text().splitlines()
            if line.strip() and CantonUpdate.model_validate_json(line))
    assert n > 0


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback; print(f"FAIL {t.__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
