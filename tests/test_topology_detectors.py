"""Topology drift and the Ledger-API half of capability drift, against the fixtures captured from
a real participant. The quiet fixture is the baseline: every party's first hosting is recorded as
an informational event, and drift alerts start from the next change.

capability_drift is built with admin_api_url empty, so only its Ledger-API half runs here; its
Admin-API half is covered in test_capability_threshold.py."""
import re

from wire import detector as _build, meta, run, updates


def detector(name, **parameters):
    if name == "capability_drift":
        parameters.setdefault("admin_api_url", "")
    return _build(name, **parameters)


def _no_clear_ids(events):
    for e in events:
        assert not re.search(r"(client|counterparty|stranger|obs\d+)::", str(meta(e))), "party ids never leave in the clear"


# ------------------------------------------------------------------------------- topology drift

def test_quiet_traffic_only_records_baselines():
    d = detector("topology_drift")
    events = run(d, "quiet_bootstrap.jsonl")
    assert events and all(e.type == "canton_counterparty_first_seen" and e.category == "EVENT" for e in events)
    assert len(events) == 10  # five parties on two synchronizers
    assert d.seen["alerts"] == 0
    _no_clear_ids(events)


def test_counterparty_gaining_and_losing_a_host_alerts_twice():
    d = detector("topology_drift")
    run(d, "quiet_bootstrap.jsonl")
    d.outputs.events.events.clear()
    events = run(d, "369_topology_drift.jsonl")
    assert [e.type for e in events] == ["canton_topology_drift", "canton_topology_drift"]
    added, revoked = events
    assert added.category == "ALERT" and added.severity == 0.5
    assert meta(added)["change"] == "added" and meta(added)["participant_name"] == "p1"
    assert meta(added)["hosts_before"] == ["p2"] and sorted(meta(added)["hosts_after"]) == ["p1", "p2"]
    assert meta(added)["permission"] == "CONFIRMATION"
    assert meta(revoked)["change"] == "revoked" and meta(revoked)["hosts_after"] == ["p2"]
    assert meta(added)["party_hash"] == meta(revoked)["party_hash"] and len(meta(added)["party_hash"]) == 64
    assert meta(added)["synchronizer_id"].startswith("sync1::")
    assert meta(added)["policy"]["name"] == "Canton-TopologyDrift"
    _no_clear_ids(events)


def test_monitored_party_hosting_is_tracked_but_not_judged():
    client = [u for u in updates("quiet_bootstrap.jsonl") if u.events and u.events[0].party and u.events[0].party.startswith("client::")][0].events[0].party
    d = detector("topology_drift", monitored_parties=[client])
    events = run(d, "quiet_bootstrap.jsonl")
    assert len(events) == 8, "the client's own two hostings produce no event"


def test_first_contact_can_be_an_alert():
    d = detector("topology_drift", alert_on_first_contact=True)
    events = run(d, "369_topology_drift.jsonl")  # no baseline at all
    assert events[0].type == "canton_counterparty_first_seen" and events[0].category == "ALERT"


def test_state_survives_a_restart(tmp_path):
    path = str(tmp_path / "topology.json")
    d = detector("topology_drift", state_path=path)
    run(d, "quiet_bootstrap.jsonl")
    d2 = detector("topology_drift", state_path=path)  # a fresh sentry, same file
    events = run(d2, "369_topology_drift.jsonl")
    assert [e.type for e in events] == ["canton_topology_drift", "canton_topology_drift"], "no first_seen: the baseline was reloaded"


# ------------------------------------------------------------------------------- capability drift (Ledger API half)

def test_permission_upgrade_alerts_once_and_observation_host_does_not():
    d = detector("capability_drift")
    run(d, "quiet_bootstrap.jsonl")
    d.outputs.events.events.clear()
    events = run(d, "370_capability_drift.jsonl")
    assert [e.type for e in events] == ["canton_capability_drift"], "Observation host added: no alert; upgraded to Submission: alert"
    ev = events[0]
    assert meta(ev)["field"] == "permission" and meta(ev)["old"] == "OBSERVATION" and meta(ev)["new"] == "SUBMISSION"
    assert meta(ev)["source"] == "ledger_api" and meta(ev)["participant_name"] == "p1" and ev.severity == 0.5
    _no_clear_ids(events)


def test_new_host_with_confirmation_alerts():
    d = detector("capability_drift")
    run(d, "quiet_bootstrap.jsonl")
    d.outputs.events.events.clear()
    events = run(d, "369_topology_drift.jsonl")  # p1 added as CONFIRMATION host, then revoked
    assert [meta(e)["new"] for e in events] == ["CONFIRMATION"] and meta(events[0]).get("old") is None


def test_baseline_and_quiet_traffic_do_not_alert():
    d = detector("capability_drift")
    assert run(d, "quiet_bootstrap.jsonl") == []
    d = detector("capability_drift", alert_on_permission_upgrade=False)
    run(d, "quiet_bootstrap.jsonl")
    assert run(d, "370_capability_drift.jsonl") == []


def test_all_scenarios_end_to_end():
    t = detector("topology_drift")
    c = detector("capability_drift")
    t_alerts = [e for e in run(t, "all_scenarios.jsonl") if e.category == "ALERT"]
    c_alerts = [e for e in run(c, "all_scenarios.jsonl") if e.category == "ALERT"]
    assert [meta(e)["change"] for e in t_alerts] == ["added", "revoked", "added"], "369 twice, then 370's Observation host"
    assert [(meta(e).get("old"), meta(e)["new"]) for e in c_alerts] == [(None, "CONFIRMATION"), ("OBSERVATION", "SUBMISSION")]
