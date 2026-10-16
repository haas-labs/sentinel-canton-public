"""Behavioural tests for capability_drift, whose two halves run independently.

Ledger-API half (hosting-permission upgrades) runs over the recorded stream with no Admin API and no
grpc. Admin-API half (confirmation-threshold drops) is driven here with a PartyHosting value directly,
so it is exercised without a live Admin API or the optional [admin]/grpc dependency.
"""
import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel_canton.admin_api import PartyHosting
from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, build_detector, run_over_fixture

FIX = ROOT / "tests" / "fixtures"
STREAM = FIX / "all_scenarios.jsonl"


def _all_parties():
    ps = set()
    for line in STREAM.read_text().splitlines():
        if not line.strip():
            continue
        u = CantonUpdate.model_validate_json(line)
        for e in u.events:
            ps |= set(e.signatories) | set(e.observers) | set(e.witness_parties) | set(e.acting_parties)
            if e.party:
                ps.add(e.party)
            if e.participant:
                ps.add(e.participant)
    return ps


def test_permission_drift_fires_over_ledger_api():
    events = asyncio.run(run_over_fixture(build_detector("capability_drift"), STREAM))
    perm = [e for e in events if e.type == "canton_capability_drift" and e.metadata.get("field") == "permission"]
    assert perm, "expected a hosting-permission drift over the ledger stream"
    assert all(e.metadata.get("source") == "ledger_api" for e in perm)


def test_admin_api_half_off_without_url():
    # no admin_api_url -> no reader, so the Admin-API half (and grpc) is never touched
    d = build_detector("capability_drift")
    d.init()
    assert d.reader is None


def test_threshold_drop_fires_via_admin_api_half():
    d = build_detector("capability_drift")
    sink = ListEventSink()
    d.outputs.events = sink
    d.init()
    base = PartyHosting(party="counterparty::x", synchronizer="sync1::y", threshold=2,
                        participants={"p1::a": "CONFIRMATION", "p2::b": "CONFIRMATION"})
    dropped = PartyHosting(party="counterparty::x", synchronizer="sync1::y", threshold=1,
                          participants={"p1::a": "CONFIRMATION", "p2::b": "CONFIRMATION"})

    async def drive():
        await d.judge_threshold(base)     # first read is the baseline: no alert
        await d.judge_threshold(dropped)  # a lower threshold is the alert

    asyncio.run(drive())
    thr = [e for e in sink.events if e.metadata.get("field") == "threshold"]
    assert len(thr) == 1, "exactly one threshold-drop alert (baseline is silent)"
    assert thr[0].metadata.get("source") == "admin_api"
    assert int(thr[0].metadata["old"]) == 2 and int(thr[0].metadata["new"]) == 1


def test_admin_reader_parses_the_shipped_descriptor():
    """Construct the reader (no network) so the shipped topology_admin_v30.pb is actually parsed and
    the expected message/enum names resolve. Guards against a truncated or mismatched descriptor that
    would otherwise only fail at runtime inside init(). Needs the [admin] extra (grpc)."""
    try:
        import grpc  # noqa: F401  (the [admin] extra); skip when it is not installed
    except ImportError:
        return
    from sentinel_canton.admin_api import AdminTopologyReader

    reader = AdminTopologyReader("localhost:1")  # parses the .pb; the channel is opened lazily
    assert reader.Request is not None
    assert reader.Response is not None
    assert reader.op_replace == 1  # TOPOLOGY_CHANGE_OP_ADD_REPLACE


class DecodeError(Exception):
    """Stands in for google.protobuf.message.DecodeError, matched by class name in build_reader()."""


def _build_reader_failing_with(exc):
    """Run init() with AdminTopologyReader replaced by a constructor that raises exc."""
    import sentinel_canton.admin_api as admin_api

    def failing_reader(*args, **kwargs):
        raise exc

    detector = build_detector("capability_drift", {"admin_api_url": "localhost:1"})
    original = admin_api.AdminTopologyReader
    admin_api.AdminTopologyReader = failing_reader
    try:
        detector.init()
    finally:
        admin_api.AdminTopologyReader = original


def test_missing_admin_extra_gives_clear_message():
    exc = ModuleNotFoundError("No module named 'grpc'", name="grpc")
    try:
        _build_reader_failing_with(exc)
    except SystemExit as exit_error:
        assert "[admin]" in str(exit_error) and "CantonCapabilityDrift" in str(exit_error)
        return
    raise AssertionError("expected SystemExit naming the [admin] extra")


def test_bad_descriptor_keyerror_gives_clear_message():
    try:
        _build_reader_failing_with(KeyError("ListPartyToParticipantRequest"))
    except SystemExit as exit_error:
        assert "descriptor" in str(exit_error)
        return
    raise AssertionError("expected SystemExit mentioning the descriptor")


def test_bad_descriptor_decodeerror_gives_clear_message():
    # a truncated/unparseable .pb fails at ParseFromString with a protobuf DecodeError, not KeyError
    try:
        _build_reader_failing_with(DecodeError("Error parsing message"))
    except SystemExit as exit_error:
        assert "descriptor" in str(exit_error)
        return
    raise AssertionError("expected SystemExit mentioning the descriptor")


def test_unrelated_reader_errors_propagate():
    try:
        _build_reader_failing_with(ModuleNotFoundError("No module named 'nope'", name="nope"))
    except SystemExit:
        raise AssertionError("an unrelated import error must not be masked as a config error")
    except ModuleNotFoundError:
        pass
    try:
        _build_reader_failing_with(ValueError("boom"))
    except ValueError:
        return
    raise AssertionError("expected the original ValueError to propagate")


def test_no_raw_party_ids():
    parties = _all_parties()
    for e in asyncio.run(run_over_fixture(build_detector("capability_drift"), STREAM)):
        for v in e.metadata.values():
            leaked = [p for p in parties if p in str(v)]
            assert not leaked, f"raw party id(s) leaked: {leaked} in {v!r}"


class _FakeReader:
    """Stands in for AdminTopologyReader: each party_to_participant() returns the next threshold in
    the list for a fixed party/sync, recording its calls. Lets the poll loop run with no grpc/network."""
    def __init__(self, thresholds):
        self._thresholds = list(thresholds)
        self.calls = []

    async def party_to_participant(self, synchronizer_id, party=""):
        self.calls.append((synchronizer_id, party))
        thr = self._thresholds.pop(0)
        return [PartyHosting(party="counterparty::x", synchronizer=synchronizer_id, threshold=thr,
                             participants={"p1::a": "CONFIRMATION", "p2::b": "CONFIRMATION"})]


def test_poll_thresholds_baseline_then_drop_via_fake_reader():
    # drives the Admin-API POLL path (poll_thresholds -> party_to_participant -> judge_threshold),
    # not judge_threshold directly: first pass is the baseline, a lower threshold on the next is the alert
    d = build_detector("capability_drift")
    d.init()
    sink = ListEventSink()
    d.outputs.events = sink
    d.state.hosts[("counterparty::x", "sync1::y")] = {"p1::a": "CONFIRMATION"}  # make the sync known
    d.reader = _FakeReader([3, 2])

    async def drive():
        return await d.poll_thresholds(), await d.poll_thresholds()

    n1, n2 = asyncio.run(drive())
    assert n1 == 1 and n2 == 1  # one mapping judged per pass
    assert d.reader.calls == [("sync1::y", ""), ("sync1::y", "")]  # party_to_participant exercised
    thr = [e for e in sink.events if e.metadata.get("field") == "threshold"]
    assert len(thr) == 1, "baseline silent, one alert on the drop"
    assert int(thr[0].metadata["old"]) == 3 and int(thr[0].metadata["new"]) == 2
    assert thr[0].metadata.get("source") == "admin_api"


def test_admin_api_alert_is_stamped_with_the_detectors_node():
    # a poll has no update in hand: its alert still carries the node, like the Ledger-API half's
    from sentinel_canton.runtime import hash_id
    node = "node-a::1220aa"
    d = build_detector("capability_drift", {"participant_id": node})
    d.init()
    sink = ListEventSink()
    d.outputs.events = sink
    d.state.hosts[("counterparty::x", "sync1::y")] = {"p1::a": "CONFIRMATION"}
    d.reader = _FakeReader([3, 2])

    async def drive():
        await d.poll_thresholds()
        await d.poll_thresholds()

    asyncio.run(drive())
    (thr,) = [e for e in sink.events if e.metadata.get("field") == "threshold"]
    assert thr.metadata["source_participant_hash"] == hash_id(node)


def test_poll_thresholds_covers_multiple_syncs_and_hostings():
    # poll_thresholds is a double loop: every synchronizer, every PartyToParticipant mapping on it.
    # Seed two syncs and return two hostings per sync so a bug in either loop level is exercised.
    d = build_detector("capability_drift")
    d.init()
    sink = ListEventSink()
    d.outputs.events = sink
    for sync in ("syncA::1", "syncB::2"):
        d.state.hosts[("counterparty::x", sync)] = {"p1::a": "CONFIRMATION"}

    class MultiReader:
        def __init__(self):
            self.calls = []

        async def party_to_participant(self, synchronizer_id, party=""):
            self.calls.append(synchronizer_id)
            return [
                PartyHosting(party="counterparty::x", synchronizer=synchronizer_id, threshold=3,
                             participants={"p1::a": "CONFIRMATION"}),
                PartyHosting(party="counterparty::y", synchronizer=synchronizer_id, threshold=3,
                             participants={"p2::b": "CONFIRMATION"}),
            ]

    d.reader = MultiReader()
    judged = asyncio.run(d.poll_thresholds())
    assert judged == 4  # 2 synchronizers x 2 hostings each
    assert sorted(d.reader.calls) == ["syncA::1", "syncB::2"]  # both syncs polled


def test_threshold_poller_survives_reader_error_and_keeps_polling():
    # the poll loop must not die with the Admin API: a failing read is logged and polling continues
    d = build_detector("capability_drift")
    d.init()
    d.state.hosts[("counterparty::x", "sync1::y")] = {"p1::a": "CONFIRMATION"}
    d.admin_api_poll_seconds = 0  # tight loop; cancelled once it proves it kept going after the error
    calls = []
    kept_going = asyncio.Event()
    warned = []
    d.logger.warning = lambda msg, *a, **k: warned.append(msg)  # capture, so "logged" is pinned too

    class Reader:
        async def party_to_participant(self, synchronizer_id, party=""):
            calls.append(synchronizer_id)
            if len(calls) == 1:
                raise RuntimeError("admin api down")  # first pass fails -> must be swallowed
            kept_going.set()                           # reached only if the loop continued
            return []

    d.reader = Reader()

    async def drive():
        task = asyncio.create_task(d.threshold_poller())
        await asyncio.wait_for(kept_going.wait(), timeout=2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(drive())
    assert len(calls) >= 2  # polled again after the first read raised
    assert any("Admin API read failed" in w for w in warned)  # the error was logged, not just eaten


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
