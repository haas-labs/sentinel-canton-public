"""Behavioural tests for the three stateless detectors over the recorded scenario stream.

Each detector runs over all_scenarios.jsonl (the same stream the original local profiles used) and
must raise its specific alert. informee_anomaly's per-template rule needs its expected-parties parameter, exactly as the
original profile supplied it (its structural rule runs by default and is silent on this stream); the parties are read from the
fixture so the test is not brittle to id changes.
"""
import asyncio
import pathlib
import sys
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonEventType, CantonUpdate
from sentinel_canton.runner import build_detector, run_over_fixture
from sentinel_canton.runtime import hash_id

FIX = ROOT / "tests" / "fixtures"
STREAM = FIX / "all_scenarios.jsonl"


def _run(name, params=None):
    return asyncio.run(run_over_fixture(build_detector(name, params), STREAM))


def _iou_parties():
    """Client / counterparty / stranger ids as they appear on Iou creations in the fixture."""
    counts = Counter()
    for line in STREAM.read_text().splitlines():
        if not line.strip():
            continue
        u = CantonUpdate.model_validate_json(line)
        for e in u.events:
            if e.type == CantonEventType.CREATED and (e.template_id or "").endswith("Iou:Iou"):
                for p in set(e.signatories) | set(e.observers):
                    counts[p] += 1

    def top(prefix):
        return max((p for p in counts if p.startswith(prefix)), key=lambda p: counts[p])

    return top("client::"), top("counterparty::"), top("stranger::")


def test_stakeholder_anomaly_fires_on_bloated_contract():
    events = _run("stakeholder_anomaly")  # code default limit 10; the bloat scenario has 62
    alerts = [e for e in events if e.type == "canton_stakeholder_anomaly"]
    assert alerts, "expected a stakeholder anomaly on the 62-stakeholder contract"
    assert any(int(e.metadata.get("count", 0)) > int(e.metadata.get("threshold", 0)) for e in alerts)


def test_topology_drift_fires_on_hosting_change():
    events = _run("topology_drift")  # memory state; baseline is in the stream
    drift = [e for e in events if e.type == "canton_topology_drift"]
    assert drift, "expected a topology drift when a counterparty's hosting changes"
    assert {e.metadata.get("change") for e in drift} & {"added", "revoked"}


def test_informee_anomaly_needs_params_then_catches_the_stranger():
    client, counterparty, stranger = _iou_parties()

    # code default: only the structural rule runs, and the Iou stream has no structural leak
    assert not [e for e in _run("informee_anomaly") if e.type == "canton_informee_anomaly"]

    # with the expected-parties parameter (as the original profile supplied), the stranger is caught
    params = {"expected_parties_by_template": {"Iou:Iou": [client, counterparty]}}
    alerts = [e for e in _run("informee_anomaly", params) if e.type == "canton_informee_anomaly"]
    assert alerts, "expected an informee anomaly once expected parties are configured"
    assert any(hash_id(stranger) in e.metadata.get("unexpected_party_hash", "") for e in alerts), \
        "the stranger party should appear (hashed) among the unexpected parties"


def _all_parties():
    """Every party id that appears anywhere in the input stream."""
    ps = set()
    for line in STREAM.read_text().splitlines():
        if not line.strip():
            continue
        u = CantonUpdate.model_validate_json(line)
        for e in u.events:
            ps |= set(e.signatories) | set(e.observers) | set(e.witness_parties) | set(e.acting_parties)
            if e.party:
                ps.add(e.party)
    return ps


def test_alerts_never_carry_raw_party_ids():
    """Party ids leave a detector only hashed. No alert metadata value may contain any raw party id
    from the input stream. synchronizer_id also contains '::' but is not a party and is emitted in the
    clear by design, so we check against the actual party set rather than a '::' heuristic."""
    parties = _all_parties()
    assert parties, "the fixture should contain party ids"
    client, counterparty, _ = _iou_parties()
    params = {"expected_parties_by_template": {"Iou:Iou": [client, counterparty]}}
    runs = [_run("stakeholder_anomaly"), _run("topology_drift"), _run("informee_anomaly", params)]
    for events in runs:
        for e in events:
            for v in e.metadata.values():
                leaked = [p for p in parties if p in str(v)]
                assert not leaked, f"raw party id(s) leaked in alert metadata: {leaked} in {v!r}"


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
