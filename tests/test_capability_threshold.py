"""The Admin-API half of capability drift: a party's confirmation threshold dropping.

The Ledger API never carries the threshold, so the detector reads PartyToParticipant mappings from
the participant's Admin API. The two captures come from a real Canton 3.5 participant (the
client's) before and after the scenario: the counterparty hosted on two participants with
threshold 2, then the same hosts with threshold 1.

The reader is replaced by a fake built from the captures (build_reader is stubbed before
configure() runs), so these tests need neither grpc nor a participant."""
import asyncio
import inspect
import json

import pytest
from sentinel.manifest import Severity
from sentinel.models.config import Configuration

from sentinel_canton.admin_api import PartyHosting
from sentinel_canton.detectors.capability_drift.sentry import CapabilityDriftDetector
from sentinel_canton.topology_state import TopologyState
from wire import FIX, detector, meta, updates

POLLER = "capability-drift-threshold-poller"


def _rows(name):
    return [PartyHosting.from_dict(r) for r in json.loads((FIX / f"canton_370_admin_threshold_{name}.json").read_text())]


class FakeAdminApi:
    """Answers like the participant's Admin API, from the captured mappings; `state` picks the capture."""

    def __init__(self, state="two"):
        self.state, self.calls = state, []

    async def party_to_participant(self, synchronizer_id, party=""):
        # the mappings are answered for whichever synchronizer is asked: the Ledger API fixtures come
        # from an earlier instance of the same local network, with another synchronizer id
        self.calls.append((synchronizer_id, party))
        prefix = party.split("::")[0]
        return [PartyHosting.from_dict({**r.to_dict(), "synchronizer": synchronizer_id, "party": party if "::" in party else r.party})
                for r in _rows(self.state) if r.party.startswith(prefix)]


@pytest.fixture(autouse=True)
def fake_admin_api(monkeypatch):
    """Every reader configure() builds is a fresh FakeAdminApi."""
    monkeypatch.setattr(CapabilityDriftDetector, "build_reader", lambda self: FakeAdminApi())


def _detector(tmp_path, **parameters):
    return detector("capability_drift", state_path=str(tmp_path / "state.json"), admin_api_url="fake:5012", **parameters)


def _alerts(d):
    return d.outputs.events.events


def _poll(d):
    return asyncio.run(d.poll_thresholds())


SYNC = _rows("two")[0].synchronizer
COUNTERPARTY = next(r.party for r in _rows("two") if r.party.startswith("counterparty::"))


def test_the_captures_are_the_drop_the_scenario_describes():
    two = {r.party: r for r in _rows("two")}
    one = {r.party: r for r in _rows("one")}
    assert two[COUNTERPARTY].threshold == 2 and one[COUNTERPARTY].threshold == 1
    assert two[COUNTERPARTY].participants == one[COUNTERPARTY].participants, "same hosts, only the threshold moved"
    assert sorted(p.split("::")[0] for p in two[COUNTERPARTY].participants) == ["p1", "p2"]
    assert one[COUNTERPARTY].valid_from > two[COUNTERPARTY].valid_from


def test_first_poll_is_the_baseline_and_a_drop_alerts_critical(tmp_path):
    d = _detector(tmp_path)
    d.state.set_threshold("seed::x", SYNC, 1)  # the state knows the synchronizer, as after any topology event
    judged = _poll(d)
    assert judged == 3 and _alerts(d) == [], "the first read of every party is its baseline"
    assert d.state.threshold(COUNTERPARTY, SYNC) == 2
    d.reader.state = "one"
    _poll(d)
    (alert,) = _alerts(d)
    assert alert.category == "ALERT" and alert.type == "canton_capability_drift"
    assert alert.severity == float(Severity.CRITICAL)
    m = meta(alert)
    assert m["field"] == "threshold" and m["old"] == 2 and m["new"] == 1 and m["source"] == "admin_api"
    assert m["party_role"] == "counterparty" and m["hosts"] == ["p1", "p2"]
    assert "counterparty::" not in json.dumps(m) and m["party_hash"], "the party leaves hashed"
    assert m["synchronizer_id"] == SYNC and alert.ts == _rows("one")[0].valid_from or alert.ts > 0


def test_a_rise_and_a_repeat_do_not_alert(tmp_path):
    d = _detector(tmp_path)
    d.reader.state = "one"
    d.state.set_threshold("seed::x", SYNC, 1)
    _poll(d)
    d.reader.state = "two"
    _poll(d)  # 1 -> 2 is a stronger posture
    _poll(d)  # 2 -> 2
    assert _alerts(d) == []


def test_the_threshold_survives_a_restart(tmp_path):
    d = _detector(tmp_path)
    d.state.set_threshold("seed::x", SYNC, 1)
    _poll(d)
    again = _detector(tmp_path)
    assert again.state.threshold(COUNTERPARTY, SYNC) == 2, "persisted with the hosting state"
    again.reader.state = "one"
    _poll(again)
    assert len(_alerts(again)) == 1, "a restarted sentry still knows the baseline and still alerts on the drop"


def test_a_topology_event_rereads_that_party_only(tmp_path):
    d = _detector(tmp_path)
    feed = updates("370_capability_drift.jsonl")

    async def go():
        for u in feed:
            await d.on_update(u)

    asyncio.run(go())
    parties = {party for (_, party) in d.reader.calls}
    assert parties and all(p.startswith("counterparty::") for p in parties), "each event re-reads its own party's mapping"
    assert d.state.threshold(feed[0].events[0].party, feed[0].synchronizer_id) is not None


def test_the_drop_on_a_monitored_party_alerts_too(tmp_path):
    d = _detector(tmp_path, monitored_parties=[COUNTERPARTY])
    d.state.set_threshold("seed::x", SYNC, 1)
    _poll(d)
    d.reader.state = "one"
    _poll(d)
    (alert,) = _alerts(d)
    assert meta(alert)["party_role"] == "monitored"


def test_the_old_state_file_format_still_loads(tmp_path):
    path = tmp_path / "old.json"
    path.write_text(json.dumps({f"{COUNTERPARTY}|{SYNC}": {"p2::x": "SUBMISSION"}}))
    state = TopologyState(str(path))
    assert state.participants(COUNTERPARTY, SYNC) == {"p2::x": "SUBMISSION"} and state.thresholds == {}
    state.set_threshold(COUNTERPARTY, SYNC, 2)
    assert TopologyState(str(path)).threshold(COUNTERPARTY, SYNC) == 2


def test_the_reader_credentials_follow_the_three_access_shapes(tmp_path):
    pytest.importorskip("grpc")  # the [admin] extra: the real reader parses its descriptor with protobuf
    from sentinel_canton.admin_api import AdminTopologyReader

    plain = AdminTopologyReader("p:1")
    assert plain.secure is False and plain.credentials() is None, "your own node: no TLS, no certificate"
    tls = AdminTopologyReader("p:1", secure=True)
    assert tls.credentials() is not None and tls.client_cert is None
    cert, key = tmp_path / "c.pem", tmp_path / "k.pem"
    cert.write_bytes(b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n")
    key.write_bytes(b"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n")
    mtls = AdminTopologyReader("p:1", client_cert=str(cert), client_key=str(key))
    assert mtls.secure is True, "a client certificate implies TLS"
    assert mtls.credentials() is not None
    with pytest.raises(ValueError):
        AdminTopologyReader("p:1", client_cert=str(cert))


def _pollers():
    return [t for t in asyncio.all_tasks() if t.get_name() == POLLER and not t.done()]


def test_the_poller_is_scheduled_on_the_running_loop_not_at_init(tmp_path):
    """init() runs before the event loop exists, so asyncio.create_task there kills the process on
    every restart; the poller is scheduled by on_init()."""
    for method in (CapabilityDriftDetector.init, CapabilityDriftDetector.configure):
        assert "create_task" not in inspect.getsource(method)
    d = _detector(tmp_path)  # init() already ran, outside any loop
    assert getattr(d, "poller_task", None) is None

    async def go():
        await d.on_init()
        (task,) = _pollers()
        assert task is d.poller_task
        d.reader = None
        await d.on_init()
        await asyncio.sleep(0)  # let the cancellation land
        assert _pollers() == [], "no poller without an Admin API"

    asyncio.run(go())


def _condition(**config):
    return Configuration(**{
        "id": 991, "createdAt": 1, "updatedAt": 5, "status": "ACTIVE", "name": "c", "source": "ATTACK_DETECTOR",
        "contract": {"id": 1, "createdAt": 1, "updatedAt": 1, "projectId": 1, "tenantId": 1, "chainUid": "canton", "name": "p", "address": "client::1220aa"},
        "schema": {"id": 3, "createdAt": 1, "updatedAt": 1, "status": "ACTIVE", "name": "Canton-CapabilityDrift", "version": "0.2.1"},
        "config": {"admin_api_url": "fake:5012", **config},
    })


def test_a_config_change_reschedules_the_poller(tmp_path):
    """A live monitoring-condition change re-runs configure() (a fresh reader); the poller must be
    handed a new task, not left pointing at the reader configure() replaced."""
    d = _detector(tmp_path)

    async def go():
        await d.on_init()
        (before,) = _pollers()
        reader_before = d.reader
        await d.on_config_change(config=_condition())
        await asyncio.sleep(0)  # let the cancellation land
        live = _pollers()
        assert len(live) == 1 and live[0] is not before, "the old poller was replaced by exactly one fresh poller"
        assert d.reader is not reader_before and d.reader is not None, "the poller reads through the new reader"
        assert d.monitored_parties == {"client::1220aa"}, "the condition's entity became a monitored party"
        await d.on_config_change(config=_condition(admin_api_url=""))
        await asyncio.sleep(0)
        assert d.reader is None and _pollers() == [], "a condition without an Admin API stops the poller"
        d.poller_task and d.poller_task.cancel()

    asyncio.run(go())
