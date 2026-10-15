"""Stakeholder / observer anomaly against the fixtures captured from a real participant: the
bloated Iou must alert once, normal traffic must not, a per-template limit overrides the default,
and monitored parties scope the check to the contracts they are a stakeholder of."""
import re

from wire import detector, meta, run

BLOAT = "371_stakeholder_bloat.jsonl"


def test_bloated_iou_alerts_once():
    events = run(detector("stakeholder_anomaly"), BLOAT)
    assert len(events) == 1
    ev = events[0]
    assert ev.type == "canton_stakeholder_anomaly" and ev.category == "ALERT" and ev.severity == 0.5
    assert meta(ev)["count"] == 62 and meta(ev)["observers_count"] == 61 and meta(ev)["signatories_count"] == 1  # 1 signatory + 61 observers, all distinct
    assert meta(ev)["threshold"] == 10 and meta(ev)["source"] == "stream"
    assert meta(ev)["template_id"].endswith(":Iou:Iou") and meta(ev)["package_name"] == "CantonExamples"
    text = str(meta(ev))
    assert len(meta(ev)["contract_hash"]) == 64 and "contract_id" not in meta(ev)
    assert not re.search(r"(client|counterparty|stranger|obs\d+)::", text), "party ids never leave in the clear"
    assert meta(ev)["synchronizer_id"].startswith("sync1::"), "synchronizer ids are public and stay in the clear"
    assert meta(ev)["offset"] and meta(ev)["update_id"] and ev.ts > 1_700_000_000_000
    assert meta(ev)["policy"] == {"name": "Canton-StakeholderAnomaly", "version": "0.2.1"}


def test_configured_severity_overrides_the_default_and_auto_keeps_it():
    assert run(detector("stakeholder_anomaly", severity=0.25), BLOAT)[0].severity == 0.25, "MEDIUM, as configured"
    assert run(detector("stakeholder_anomaly", severity=-1), BLOAT)[0].severity == 0.5, "AUTO: the detector default, HIGH"


def test_quiet_traffic_does_not_alert():
    d = detector("stakeholder_anomaly")
    assert run(d, "quiet_bootstrap.jsonl") == []
    assert run(d, "snapshot_bootstrap.jsonl") == []
    assert d.seen["created"] == 2 and d.seen["alerts"] == 0


def test_per_template_limit_overrides_the_default():
    assert run(detector("stakeholder_anomaly", max_stakeholders_by_template={"Iou:Iou": 100}), BLOAT) == []
    d = detector("stakeholder_anomaly", max_stakeholders_default=100, max_stakeholders_by_template={"Iou:Iou": 5})
    assert len(run(d, BLOAT)) == 1


def test_monitored_parties_scope_a_contract_to_its_stakeholders():
    """A reader that may read every party on the synchronizer streams other parties' contracts too;
    a configured monitored party judges only the contracts it is a stakeholder of."""
    stakeholder = "counterparty::12201fe11e83326abfaf115657eedee557e647a5a5043f5d5c7f821709daef529940"
    # a monitored party that is a stakeholder of the bloated contract: still judged
    assert len(run(detector("stakeholder_anomaly", monitored_parties=[stakeholder]), BLOAT)) == 1
    # a monitored party absent from the contract: skipped, no alert
    assert run(detector("stakeholder_anomaly", monitored_parties=["stranger::1220dead"]), BLOAT) == []


def test_all_scenarios_alert_exactly_once():
    events = run(detector("stakeholder_anomaly"), "all_scenarios.jsonl")
    assert [meta(e)["count"] for e in events] == [62]
