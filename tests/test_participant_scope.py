"""Several trunks on one topic: every record carries the node it was read from, and a
detector scoped with participant_id only judges its own node. The fixtures are replayed as two trunks
would publish them: once stamped node A, once node B, interleaved, with the same update ids."""
import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, _parameters, build_detector
from sentinel_canton.runtime import hash_id

FIX = ROOT / "tests" / "fixtures"
NODE_A, NODE_B = "node-a::1220aa", "node-b::1220bb"


def _updates(name, participant_id=None):
    out = []
    for line in (FIX / name).read_text().splitlines():
        if line.strip():
            u = CantonUpdate.model_validate_json(line)
            u.participant_id = participant_id
            out.append(u)
    return out


def _two_trunks(name, b_first=False):
    out = []
    for a, b in zip(_updates(name, NODE_A), _updates(name, NODE_B)):
        out += [b, a] if b_first else [a, b]
    return out


def _feed(name, params, updates):
    async def run():
        detector = build_detector(name, params)
        sink = ListEventSink()
        detector.outputs.events = sink
        detector.init()
        await detector.on_init()
        for u in updates:
            await detector.handle_update(u)
        return detector, sink.events
    return asyncio.run(run())


def test_detector_of_node_a_does_not_alert_on_node_b():
    _, events = _feed("stakeholder_anomaly", {"participant_id": NODE_A}, _updates("371_stakeholder_bloat.jsonl", NODE_B))
    assert events == []


def test_same_transaction_from_two_nodes_alerts_once_per_scoped_detector():
    _, single = _feed("stakeholder_anomaly", {}, _updates("371_stakeholder_bloat.jsonl"))
    assert single
    for b_first in (False, True):
        for node in (NODE_A, NODE_B):
            _, events = _feed("stakeholder_anomaly", {"participant_id": node}, _two_trunks("371_stakeholder_bloat.jsonl", b_first))
            assert len(events) == len(single), f"{node}'s copy must not be dropped as a duplicate of the other node's"
            assert all(e.metadata["source_participant_hash"] == hash_id(node) for e in events)


def test_unscoped_detector_handles_each_nodes_copy_once():
    _, single = _feed("stakeholder_anomaly", {}, _updates("371_stakeholder_bloat.jsonl"))
    feed = _two_trunks("371_stakeholder_bloat.jsonl")
    _, events = _feed("stakeholder_anomaly", {}, feed + feed)  # the second pass is a republish
    assert len(events) == 2 * len(single)


def test_snapshot_of_node_b_does_not_reach_node_a():
    snapshots = [u for u in _updates("snapshot_bootstrap.jsonl", NODE_B) if u.kind.value == "snapshot"]
    assert snapshots
    detector, events = _feed("stakeholder_anomaly", {"participant_id": NODE_A, "max_stakeholders_default": 0}, snapshots)
    assert events == [] and detector.seen["updates"] == 0


def test_topology_drift_seen_by_node_b_does_not_alert_node_a():
    feed = _updates("quiet_bootstrap.jsonl", NODE_A) + _updates("369_topology_drift.jsonl", NODE_B)
    _, events = _feed("topology_drift", {"participant_id": NODE_A}, feed)
    assert [e.type for e in events if e.category == "ALERT"] == []
    _, events = _feed("topology_drift", {"participant_id": NODE_B}, _updates("quiet_bootstrap.jsonl", NODE_B) + _updates("369_topology_drift.jsonl", NODE_B))
    assert [e.type for e in events if e.type == "canton_topology_drift"] == ["canton_topology_drift"] * 2


def test_topology_drift_from_two_trunks_alerts_each_node_once():
    bootstrap, drift = _two_trunks("quiet_bootstrap.jsonl", b_first=True), _two_trunks("369_topology_drift.jsonl", b_first=True)
    for node in (NODE_A, NODE_B):
        _, events = _feed("topology_drift", {"participant_id": node}, bootstrap + drift)
        assert [e.type for e in events if e.category == "ALERT"] == ["canton_topology_drift", "canton_topology_drift"]


def test_records_without_participant_id_are_still_judged():
    _, events = _feed("stakeholder_anomaly", {"participant_id": NODE_A}, _updates("371_stakeholder_bloat.jsonl"))
    assert events and all("source_participant_hash" not in e.metadata for e in events)


def test_participant_id_from_the_env_and_parameters_win(monkeypatch):
    monkeypatch.delenv("CANTON_PARAMETERS", raising=False)
    monkeypatch.setenv("CANTON_PARTICIPANT_ID", NODE_A)
    assert _parameters("stakeholder_anomaly")["participant_id"] == NODE_A
    monkeypatch.setenv("CANTON_PARAMETERS", '{"participant_id": "%s"}' % NODE_B)
    assert _parameters("stakeholder_anomaly")["participant_id"] == NODE_B
    build_detector("stakeholder_anomaly", {"participant_id": NODE_A})  # a runner key: passes validation


def test_empty_participant_id_saved_from_the_ui_does_not_override_the_env(monkeypatch):
    # The platform form saves the CantonSchema default (participant_id=""): the node set by Helm
    # (CANTON_PARTICIPANT_ID) must still scope the detector, or it alerts on another node's data.
    monkeypatch.setenv("CANTON_PARTICIPANT_ID", NODE_A)
    monkeypatch.setenv("CANTON_PARAMETERS", '{"participant_id": ""}')
    params = _parameters("stakeholder_anomaly")
    assert params["participant_id"] == NODE_A
    detector, events = _feed("stakeholder_anomaly", params, _updates("371_stakeholder_bloat.jsonl", NODE_B))
    assert detector.participant_id == NODE_A
    assert events == []


def test_participant_id_from_the_env_is_stripped(monkeypatch):
    monkeypatch.delenv("CANTON_PARAMETERS", raising=False)
    monkeypatch.setenv("CANTON_PARTICIPANT_ID", "  %s\n" % NODE_A)
    assert _parameters("stakeholder_anomaly")["participant_id"] == NODE_A
    monkeypatch.setenv("CANTON_PARTICIPANT_ID", "   ")
    assert "participant_id" not in _parameters("stakeholder_anomaly")
