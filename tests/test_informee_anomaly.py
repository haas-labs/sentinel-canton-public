"""Canton informee-tree anomaly.

Ported from ext-sentinel-py-detectors tests/test_canton_informee_anomaly.py (PR #838/#839) and
extended with the edge cases from the review. Iou fixtures were captured from the trunk-canton
localnet (scenario 373: the client shares an Iou with 'stranger', then transfers it).
"""
import asyncio
import json
import pathlib
import re
import sys
import tempfile

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, build_detector
from sentinel_canton.runtime import hash_id

sys.path.insert(0, str(ROOT / "tests" / "fixtures"))
from make_informee_tree import BANK, CLERK, GUEST, JANUS, MIRO, REGULATOR, SHOP  # hand-built canton-contracts fixtures (see that module)

FIX = ROOT / "tests" / "fixtures"
ALERT = "canton_informee_anomaly"


def _updates(name):
    return [CantonUpdate.model_validate_json(l) for l in (FIX / name).read_text().splitlines() if l.strip()]


def _party(prefix):
    for u in _updates("quiet_bootstrap.jsonl"):
        for e in u.events:
            if e.party and e.party.startswith(prefix + "::"):
                return e.party
    raise AssertionError(prefix)


CLIENT, COUNTERPARTY, STRANGER = _party("client"), _party("counterparty"), _party("stranger")
IOU_EXPECTED = {"expected_parties_by_template": {"Iou:Iou": [CLIENT, COUNTERPARTY]}}


def _detector(**parameters):
    d = build_detector("informee_anomaly", parameters)
    d.outputs.events = ListEventSink()
    d.init()
    return d


def _run(d, *names):
    async def go():
        for name in names:
            for u in _updates(name):
                await d.handle_update(u)
    asyncio.run(go())
    return [e for e in d.outputs.events.events if e.type == ALERT]


def meta(event):
    """Metadata as the platform flattens it, with JSON lists decoded back."""
    out = {}
    for k, v in event.metadata.items():
        try:
            out[k] = json.loads(v) if v[:1] in "[{" else v
        except ValueError:
            out[k] = v
    return out


# ---------------------------------------------------------------- ported from upstream

def test_share_with_a_stranger_alerts_with_the_choice_and_the_party():
    events = _run(_detector(**IOU_EXPECTED), "373_informee_anomaly.jsonl")
    assert len(events) == 2
    share, transfer = events
    assert share.category == "ALERT" and share.severity == 0.5
    assert meta(share)["choice"] == "Share" and meta(share)["reason"] == "unexpected"
    assert meta(share)["offset"] == "265" and meta(transfer)["offset"] == "268"
    assert meta(share)["unexpected_party_hash"] == [hash_id(STRANGER)] and meta(share)["missing_party_hash"] == []
    assert meta(share)["node_id"] == "1", "the child created event of the exercise"
    assert meta(transfer)["choice"] == "Transfer"
    assert meta(share)["template_id"].endswith(":Iou:Iou")
    assert not re.search(r"(client|counterparty|stranger)::", str(meta(share))), "ids leave only as hashes"
    assert meta(share)["policy"]["name"] == "Canton Informee Anomaly"


def test_bloated_iou_is_also_a_visibility_breach():
    # the bloated Iou lists 60 unknown observers: those are unexpected parties too
    events = _run(_detector(**IOU_EXPECTED), "371_stakeholder_bloat.jsonl")
    assert len(events) == 1 and len(meta(events[0])["unexpected_party_hash"]) == 60
    assert "choice" not in meta(events[0])


def test_quiet_traffic_is_silent():
    d = _detector(**IOU_EXPECTED)
    assert _run(d, "quiet_bootstrap.jsonl", "snapshot_bootstrap.jsonl", "372_reassignment_limbo.jsonl") == []
    assert d.seen["checked"] >= 2 and d.seen["alerts"] == 0


def test_unconfigured_template_is_ignored():
    d = _detector(expected_parties_by_template={"Other:Thing": [CLIENT]})
    assert _run(d, "373_informee_anomaly.jsonl") == [] and d.seen["checked"] == 0


def test_monitored_parties_are_always_allowed_and_missing_is_opt_in():
    params = dict(monitored_parties=[CLIENT], expected_parties_by_template={"Iou:Iou": [COUNTERPARTY, STRANGER]})
    assert _run(_detector(**params), "373_informee_anomaly.jsonl") == [], "client is monitored, stranger is expected"
    events = _run(_detector(**params, alert_on_missing=True), "373_informee_anomaly.jsonl")
    assert events and all(meta(e)["reason"] == "missing" for e in events), "before the Share, stranger is not there yet"


def test_full_template_id_also_matches():
    tid = [e.template_id for u in _updates("373_informee_anomaly.jsonl") for e in u.events if e.template_id][0]
    assert len(_run(_detector(expected_parties_by_template={tid: [CLIENT, COUNTERPARTY]}), "373_informee_anomaly.jsonl")) == 2


# ---------------------------------------------------------------- alert shape: missing and mixed

def test_missing_only_has_reason_missing_and_no_unexpected_wording():
    d = _detector(alert_on_missing=True, expected_parties_by_template={"Iou:Iou": [CLIENT, COUNTERPARTY, STRANGER]})
    first = _run(d, "373_informee_anomaly.jsonl")[0]
    assert meta(first)["offset"] == "262" and meta(first)["reason"] == "missing"
    assert meta(first)["missing_party_hash"] == [hash_id(STRANGER)] and meta(first)["unexpected_party_hash"] == []
    assert "unexpected" not in meta(first)["desc"] and "missing" in meta(first)["desc"]


def test_unexpected_and_missing_together_name_both():
    d = _detector(alert_on_missing=True, expected_parties_by_template={"Iou:Iou": [CLIENT, COUNTERPARTY, "auditor::1220"]})
    share = [e for e in _run(d, "373_informee_anomaly.jsonl") if meta(e).get("choice") == "Share"][0]
    assert meta(share)["reason"] == "unexpected+missing"
    assert meta(share)["unexpected_party_hash"] == [hash_id(STRANGER)]
    assert meta(share)["missing_party_hash"] == [hash_id("auditor::1220")]
    assert "1 unexpected party" in meta(share)["desc"] and "1 expected party missing" in meta(share)["desc"]


# ---------------------------------------------------------------- the tree

def _ev(type, node_id, last=None, **kw):
    from sentinel.models.chains.canton.update import CantonEvent
    return CantonEvent.model_validate({"type": type, "offset": 1, "node_id": node_id, "last_descendant_node_id": last, **kw})


def test_parent_exercise_innermost_wins_and_edges():
    from sentinel_canton.detectors.informee_anomaly.sentry import InformeeAnomalyDetector as D
    events = [
        _ev("exercised", 0, 3, choice="Outer"),
        _ev("exercised", 1, 2, choice="Inner"),
        _ev("created", 2),
        _ev("created", 3),
        _ev("created", 4),
        _ev("created", None),
    ]
    assert D.parent_exercise(events, 2).choice == "Inner", "innermost enclosing exercise"
    assert D.parent_exercise(events, 3).choice == "Outer", "outside Inner's range, inside Outer's"
    assert D.parent_exercise(events, 4) is None, "a sibling after the tree"
    assert D.parent_exercise(events, 5) is None, "no node_id, no tree"
    assert D.parent_exercise(events, 0) is None, "a root exercise has no parent"


# ---------------------------------------------------------------- structural rule (canton-contracts)
# A child create or exercise witnessed by a party that is not one of its own informees. No
# configuration: it runs by default. Fixtures mirror informee_tree_anomaly/Test/Trigger.daml.

def test_leaking_desk_slip_alerts_once_with_the_choice():
    events = _run(_detector(), "informee_tree_leak.jsonl")
    assert len(events) == 1, "only FileLeaking's child Slip; the desk create and the Rewrite are not hits"
    m = meta(events[0])
    assert m["rule"] == "structural" and m["reason"] == "unexpected" and m["choice"] == "FileLeaking"
    assert m["template_id"].endswith("Desk:Slip") and m["offset"] == "11" and m["node_id"] == "1"
    assert m["unexpected_party_hash"] == [hash_id(GUEST)] and m["contract_hash"]
    assert m["acting_parties_hash"] == [hash_id(CLERK)]


def test_quiet_desks_are_silent():
    d = _detector()
    assert _run(d, "informee_tree_quiet.jsonl") == [], "Plain, Copied and Joint desks: witnesses equal stakeholders"
    assert d.seen["structural_checked"] == 6, "three filings and three rewrites, each a child create"


def test_child_exercise_witnessed_by_an_outsider_alerts_and_unknown_target_is_skipped():
    events = _run(_detector(), "informee_tree_child_exercise.jsonl")
    kinds = sorted((meta(e)["offset"], meta(e)["choice"]) for e in events)
    # offset 32: the child exercise Rewrite (target Slip known: clerk only) and its child create both leak to guest
    assert kinds == [("32", "Rewrite"), ("32", "RewriteVia")], kinds
    rewrite = [e for e in events if meta(e)["choice"] == "RewriteVia"][0]
    assert meta(rewrite)["node_id"] == "1" and meta(rewrite)["template_id"].endswith("Desk:Slip")


def test_structural_missing_only_for_monitored_stakeholders_and_opt_in():
    # guest is monitored: on the CopiedDesk Slip (offset 24) guest is a reader, drop it from the witnesses
    lines = (FIX / "informee_tree_quiet.jsonl").read_text().splitlines()
    u = json.loads(lines[4]); u["events"][1]["witness_parties"] = [CLERK]
    with tempfile.TemporaryDirectory() as d:
        tmp = pathlib.Path(d) / "missing.jsonl"  # absolute, so FIX / tmp is tmp
        tmp.write_text(json.dumps(u) + "\n")
        assert _run(_detector(monitored_parties=[GUEST]), tmp) == [], "missing is opt-in"
        events = _run(_detector(monitored_parties=[GUEST], alert_on_missing=True), tmp)
        assert [meta(e)["reason"] for e in events] == ["missing"] and meta(events[0])["missing_party_hash"] == [hash_id(GUEST)]
        assert _run(_detector(alert_on_missing=True), tmp) == [], "unmonitored stakeholders are filtered by the Ledger API"


def test_iou_share_and_transfer_are_not_structural_hits():
    # Share: stranger is a real observer, witnesses are filtered to the client. Transfer: the client
    # witnesses the new Iou (stranger's) only because it exercised Transfer; a parent's actors see
    # what their choice creates, so that is not a leak
    d = _detector()
    assert _run(d, "373_informee_anomaly.jsonl") == [] and d.seen["structural_checked"] == 2


def test_structural_check_can_be_disabled():
    assert _run(_detector(structural_check=False), "informee_tree_leak.jsonl") == []


def test_allowlist_alert_is_tagged_and_not_doubled_by_structural():
    tmpl = {"InformeeTreeAnomaly.Desk:Slip": [CLERK]}
    events = _run(_detector(expected_parties_by_template=tmpl), "informee_tree_leak.jsonl")
    assert sorted(meta(e)["rule"] for e in events) == ["allowlist", "structural"], "two rules, one alert each"


# ---------------------------------------------------------------- divulgence: trusted parties
# A regulator that observes an asset also witnesses what a consuming choice does to it. That is normal
# Canton behaviour, so a client can list it in trusted_parties and the structural rule stays quiet.

def test_regulator_witnessing_a_transfer_alerts_until_it_is_trusted():
    events = _run(_detector(), "informee_tree_divulgence.jsonl")
    assert [meta(alert)["unexpected_party_hash"] for alert in events] == [[hash_id(REGULATOR)]]


def test_trusted_regulator_does_not_alert():
    detector = _detector(trusted_parties=[REGULATOR])
    assert _run(detector, "informee_tree_divulgence.jsonl") == [] and detector.seen["structural_checked"] == 1


# ---------------------------------------------------------------- divulgence: an access contract that reaches too far
# Janus owes the bank USDCx; bank and regulator may see that. Miro, who is only selling a car, is given a
# test-ride contract that also reaches the holding and the loan. By default a controller is exempt (it sees
# what its own choice does), so Miro is only caught with strict_actors.

def test_a_controller_is_exempt_by_default_so_the_test_ride_leak_is_not_seen():
    assert _run(_detector(trusted_parties=[REGULATOR]), "informee_tree_test_ride.jsonl") == []


def test_strict_actors_flags_miro_seeing_janus_holdings_but_not_the_regulator_or_bank():
    detector = _detector(trusted_parties=[REGULATOR], strict_actors=True)
    events = _run(detector, "informee_tree_test_ride.jsonl")
    assert [meta(alert)["choice"] for alert in events] == ["TestRide", "TestRide"]
    assert [meta(alert)["unexpected_party_hash"] for alert in events] == [[hash_id(MIRO)]] * 2


def test_leak_alert_names_the_exposed_parties_and_says_how_to_stop_it():
    # whose contracts were seen, so the user and the bank can be told (every stakeholder of the exposed
    # contract, regulator included), and what to do about it
    events = _run(_detector(trusted_parties=[REGULATOR], strict_actors=True), "informee_tree_test_ride.jsonl")
    for alert in events:
        alert_metadata = meta(alert)
        assert sorted(alert_metadata["exposed_party_hash"]) == sorted(hash_id(party) for party in (JANUS, BANK, REGULATOR))
        assert "revoke" in alert_metadata["recommended_action"] and "reject" in alert_metadata["recommended_action"]
        assert "miro::" not in json.dumps(alert.metadata) and "janus::" not in json.dumps(alert.metadata)


def test_missing_only_alert_carries_no_leak_remediation():
    # guest is a monitored stakeholder that does not witness the Slip: a gap, not a leak, so no advice
    lines = (FIX / "informee_tree_quiet.jsonl").read_text().splitlines()
    update = json.loads(lines[4]); update["events"][1]["witness_parties"] = [CLERK]
    with tempfile.TemporaryDirectory() as directory:
        tmp = pathlib.Path(directory) / "missing.jsonl"
        tmp.write_text(json.dumps(update) + "\n")
        events = _run(_detector(monitored_parties=[GUEST], alert_on_missing=True), tmp)
    assert [meta(alert)["reason"] for alert in events] == ["missing"]
    assert "recommended_action" not in events[0].metadata and "exposed_party_hash" not in events[0].metadata


def test_strict_actors_costs_the_iou_transfer_alert_unless_the_client_is_trusted():
    # why strict_actors is opt-in: the client witnesses the new Iou it created by exercising Transfer
    events = _run(_detector(strict_actors=True), "373_informee_anomaly.jsonl")
    assert [(meta(alert)["choice"], meta(alert)["unexpected_party_hash"]) for alert in events] == [("Transfer", [hash_id(CLIENT)])]
    assert _run(_detector(strict_actors=True, trusted_parties=[CLIENT]), "373_informee_anomaly.jsonl") == []


def test_trusted_party_is_also_exempt_from_the_template_allowlist():
    tmpl = {"InformeeTreeAnomaly.Desk:Slip": [CLERK]}
    assert _run(_detector(expected_parties_by_template=tmpl, trusted_parties=[GUEST]), "informee_tree_leak.jsonl") == []


def test_trusted_parties_must_be_a_list_of_party_ids():
    with pytest.raises(SystemExit):
        _detector(trusted_parties="regulator::1220")
    with pytest.raises(SystemExit):
        _detector(strict_actors="sometimes")


def test_a_well_scoped_test_ride_grant_raises_no_alert_even_in_strict_mode():
    # Miro lends his car to Janus. The grant touches only Miro's contracts, so Janus's loan stays private
    # to him, the bank and the regulator, and nothing is divulged to Miro
    detector = _detector(trusted_parties=[REGULATOR], strict_actors=True)
    assert _run(detector, "informee_tree_test_ride_scoped.jsonl") == [] and detector.seen["structural_checked"] == 1


def test_the_scoped_and_the_over_broad_grant_differ_only_in_what_the_grant_reaches():
    detector = _detector(trusted_parties=[REGULATOR], strict_actors=True)
    assert len(_run(detector, "informee_tree_test_ride.jsonl")) == 2
    assert _run(_detector(trusted_parties=[REGULATOR], strict_actors=True), "informee_tree_test_ride_scoped.jsonl") == []


# ---------------------------------------------------------------- learning (learn_from_first_n)
# After N contracts of an unconfigured template, a party is expected when it appeared in at least
# learn_threshold (default 0.5, the average presence) of them. Learning contracts never alert.

def _iou_stream(dirpath, party_sets):
    """One top-level Iou create per party set, shaped like the first update of the 373 capture."""
    base = json.loads((FIX / "373_informee_anomaly.jsonl").read_text().splitlines()[0])
    lines = []
    for n, ps in enumerate(party_sets):
        u = json.loads(json.dumps(base))
        u["offset"] = u["events"][0]["offset"] = 1000 + n
        u["update_id"] = f"1220{n:060x}"
        u["events"][0].update(contract_id=f"00{n:062x}", signatories=[COUNTERPARTY], observers=sorted(ps), witness_parties=[CLIENT])
        lines.append(json.dumps(u))
    path = pathlib.Path(dirpath) / "ious.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return path


def test_learns_from_the_first_contract_then_alerts_on_the_stranger():
    d = _detector(learn_from_first_n=1)
    events = _run(d, "373_informee_anomaly.jsonl")
    assert [(meta(e)["offset"], meta(e)["rule"]) for e in events] == [("265", "learned"), ("268", "learned")]
    assert all(meta(e)["unexpected_party_hash"] == [hash_id(STRANGER)] for e in events)
    assert d.learned["Iou:Iou"] == {CLIENT, COUNTERPARTY}


def test_learning_contracts_never_alert():
    d = _detector(learn_from_first_n=3)
    assert _run(d, "373_informee_anomaly.jsonl") == [] and d.learned["Iou:Iou"] == {CLIENT, COUNTERPARTY, STRANGER}, \
        "stranger is in 2 of 3 contracts: at or above half, so learned as expected"


def test_threshold_is_the_share_of_learning_contracts_a_party_must_appear_in():
    with tempfile.TemporaryDirectory() as tmp:
        stream = _iou_stream(tmp, [{CLIENT}, {CLIENT}, {CLIENT}, {CLIENT, STRANGER}, {CLIENT, STRANGER}])
        strict = _run(_detector(learn_from_first_n=4), stream)
        assert [meta(e)["unexpected_party_hash"] for e in strict] == [[hash_id(STRANGER)]], "1 of 4 < 0.5"
        assert _run(_detector(learn_from_first_n=4, learn_threshold=0.25), stream) == [], "1 of 4 >= 0.25"


def test_configured_template_is_never_learned():
    d = _detector(learn_from_first_n=1, **IOU_EXPECTED)
    events = _run(d, "373_informee_anomaly.jsonl")
    assert [meta(e)["rule"] for e in events] == ["allowlist", "allowlist"] and d.learned == {}


def test_missing_party_against_the_learned_set():
    with tempfile.TemporaryDirectory() as tmp:
        stream = _iou_stream(tmp, [{CLIENT, STRANGER}, {CLIENT}])
        assert _run(_detector(learn_from_first_n=1), stream) == [], "missing is opt-in"
        events = _run(_detector(learn_from_first_n=1, alert_on_missing=True), stream)
        assert [(meta(e)["reason"], meta(e)["missing_party_hash"]) for e in events] == [("missing", [hash_id(STRANGER)])]


def test_learning_survives_a_restart():
    with tempfile.TemporaryDirectory() as tmp:
        state = pathlib.Path(tmp) / "informee.state.json"
        stream = _iou_stream(tmp, [{CLIENT}, {CLIENT}, {CLIENT, STRANGER}])
        first = _detector(learn_from_first_n=2, state_path=str(state))
        _run(first, stream)
        assert state.exists()
        # a fresh instance has the learned set before any update, and an in-progress count resumes
        again = _detector(learn_from_first_n=2, state_path=str(state))
        assert again.learned == {"Iou:Iou": {CLIENT, COUNTERPARTY}}
        half = _detector(learn_from_first_n=4, state_path=str(pathlib.Path(tmp) / "half.json"))
        _run(half, _iou_stream(tmp, [{CLIENT}, {CLIENT}]))
        resumed = _detector(learn_from_first_n=4, state_path=str(pathlib.Path(tmp) / "half.json"))
        assert resumed.learning["Iou:Iou"]["n"] == 2 and resumed.learned == {}


def test_no_learning_by_default():
    d = _detector()
    _run(d, "373_informee_anomaly.jsonl")
    assert d.learned == {} and d.learning == {}


def test_no_rule_leaks_a_raw_party_id():
    raw = re.compile(r"(client|counterparty|stranger|clerk|guest)::")
    runs = [
        _run(_detector(**IOU_EXPECTED, alert_on_missing=True), "373_informee_anomaly.jsonl"),
        _run(_detector(learn_from_first_n=1), "373_informee_anomaly.jsonl"),
        _run(_detector(monitored_parties=[GUEST], alert_on_missing=True), "informee_tree_leak.jsonl", "informee_tree_child_exercise.jsonl"),
    ]
    events = [e for run in runs for e in run]
    assert len(events) >= 6
    for e in events:
        assert not raw.search(json.dumps(e.metadata)), e.metadata


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
