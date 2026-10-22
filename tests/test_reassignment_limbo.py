"""Behavioural tests for reassignment_limbo, the first stateful detector.

Background: a Canton reassignment moves a contract between synchronizers in two non-atomic steps,
an unassign on the source and an assign on the target. Between them the contract is in flight and
usable on neither side. The detector tracks every open unassign and alerts when one stays open past
a window (ledger clock), alerts on abnormal unassign volume by one submitter, and persists the open
set across restarts.

How a reassignment ends up in limbo. Reference: "Connecting a Validator to Multiple Synchronizers",
section "Contract assignment and reassignment" and its Troubleshooting entry "Contract reassignment
fails", https://docs.canton.network/global-synchronizer/extension-synchronizers/linking-validator-multi-sync
The doc describes the move as two separate Ledger API commands:
`ledger_api.commands.reassignment.unassign(contractId, source, target)` returns an `unassignId`, and
`ledger_api.commands.reassignment.assign(unassignId, source, target)` completes the move. "The
contract is briefly unavailable between unassignment and reassignment." Nothing makes the second
command follow the first, so the contract stays unusable on both synchronizers whenever the assign is
never submitted or cannot succeed:
  - the target synchronizer is unreachable, or the validator is not connected to both the source and
    the target (the doc requires both). Disconnecting does not unassign anything; the contract just
    waits for the reconnection;
  - a stakeholder's validator is not connected to the target synchronizer ("All stakeholders of the
    contract must have validators connected to the target synchronizer"), so the assign cannot be
    confirmed;
  - the submitter's participant or application crashes, or is redeployed, between the two commands;
  - deliberately: a counterparty that is a stakeholder unassigns the client's contract and never
    assigns it, freezing the asset (the "freezing attack" in the monitored_parties tests). Not from
    the doc: until assignment_exclusivity only the unassign's submitter may assign, so during that
    period the other stakeholders cannot complete the move themselves.
Each case shows up on the stream the same way: an unassigned event whose reassignment_id never gets a
matching assigned event. That is exactly what these tests feed the detector.

Every test's docstring states the Canton behaviour it models, why that matters for detection, and
what exactly is asserted. Over the recorded stream the alert only fires with the demo's short window
(the code default of 900s is quiet), exactly as the original profile configured it.
"""
import asyncio
import contextlib
import json
import pathlib
import signal
import subprocess
import sys
import tempfile
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, _cancel_background_tasks, _consume, build_detector, run_over_fixture
import sentinel_canton.detectors.reassignment_limbo.sentry as sentry_module

FIX = ROOT / "tests" / "fixtures"
STREAM = FIX / "all_scenarios.jsonl"

SOURCE = "sync1::1220aa"
TARGET = "sync2::1220bb"
CLIENT = "client::1220cc"
COUNTERPARTY = "counterparty::1220dd"
STRANGER = "stranger::1220ee"
T0 = 1_000_000_000_000  # a ledger timestamp in ms; scenarios are expressed as offsets from it


def _run(params, fixture=STREAM):
    return asyncio.run(run_over_fixture(build_detector("reassignment_limbo", params), fixture))


def _all_parties():
    parties = set()
    for line in STREAM.read_text().splitlines():
        if not line.strip():
            continue
        update = CantonUpdate.model_validate_json(line)
        for event in update.events:
            parties |= set(event.signatories) | set(event.observers) | set(event.witness_parties) | set(event.acting_parties)
            if event.submitter:
                parties.add(event.submitter)
            if event.party:
                parties.add(event.party)
    return parties


# ---------------------------------------------------------------------------------- scenario helpers

def _unassign(reassignment_id, record_time, exclusivity=None, submitter=CLIENT, witnesses=(CLIENT,),
              update_id=None, snapshot=False):
    """An update carrying one unassigned event, shaped like the publisher's (see
    reassignment_limbo_case.jsonl): unassigned events name the stakeholders only as witness_parties.
    A snapshot update has no record_time and no update_id, like the bootstrap snapshot."""
    return CantonUpdate.model_validate({
        "kind": "snapshot" if snapshot else "reassignment",
        "offset": 10,
        "update_id": None if snapshot else (update_id or f"u-unassign-{reassignment_id}-{record_time}"),
        "synchronizer_id": SOURCE,
        "record_time": None if snapshot else record_time,
        "events": [{
            "type": "unassigned", "contract_id": f"c-{reassignment_id}", "template_id": "pkg:Iou:Iou",
            "signatories": [], "observers": [], "witness_parties": list(witnesses), "interface_ids": [],
            "acting_parties": [], "reassignment_id": reassignment_id, "source": SOURCE, "target": TARGET,
            "submitter": submitter, "reassignment_counter": 0, "assignment_exclusivity": exclusivity,
        }],
        "synchronizer_times": {},
    })


def _assign(reassignment_id, record_time):
    """An update carrying the assigned event that completes reassignment_id on the target."""
    return CantonUpdate.model_validate({
        "kind": "reassignment", "offset": 20, "update_id": f"u-assign-{reassignment_id}-{record_time}",
        "synchronizer_id": TARGET, "record_time": record_time,
        "events": [{
            "type": "assigned", "contract_id": f"c-{reassignment_id}", "template_id": "pkg:Iou:Iou",
            "signatories": [CLIENT], "observers": [], "witness_parties": [CLIENT], "interface_ids": [],
            "acting_parties": [], "reassignment_id": reassignment_id, "source": SOURCE, "target": TARGET,
            "submitter": CLIENT, "reassignment_counter": 1,
        }],
        "synchronizer_times": {},
    })


def _transaction(record_time):
    """An unrelated transaction: it only advances the ledger clock."""
    return CantonUpdate.model_validate({
        "kind": "transaction", "offset": 30, "update_id": f"u-tx-{record_time}", "synchronizer_id": SOURCE,
        "record_time": record_time,
        "events": [{"type": "created", "contract_id": f"c-tx-{record_time}", "template_id": "pkg:Iou:Iou",
                    "signatories": [CLIENT], "observers": [], "witness_parties": [], "interface_ids": [],
                    "acting_parties": []}],
        "synchronizer_times": {},
    })


def _detector(params):
    """A configured detector with an in-memory sink and no ticker (on_init is not run), so a scenario
    is driven only by the updates it is fed."""
    detector = build_detector("reassignment_limbo", params)
    detector.init()
    detector.outputs.events = ListEventSink()
    return detector


def _drive(detector, updates):
    """Feed the updates through the detector's real entry point and return what it emitted."""
    async def feed():
        for update in updates:
            await detector.handle_update(update)
    asyncio.run(feed())
    return detector.outputs.events.events


def _of_type(events, event_type):
    return [event for event in events if event.type == event_type]


class _Clock:
    """A monotonic clock the test moves by hand."""
    def __init__(self, seconds=1000.0):
        self.seconds = seconds

    def monotonic(self):
        return self.seconds


@contextlib.contextmanager
def _patched_io(clock=None, failing_replace=False):
    """Swap the detector module's own `time` and `os` for stand-ins, counting the state writes.
    Only the module's names are replaced, never the global modules, so asyncio's clock is untouched.
    Yields the list of (temp, destination) pairs passed to os.replace."""
    original_time, original_os = sentry_module.time, sentry_module.os
    replaces = []

    def replace(source, destination):
        if failing_replace:
            raise OSError("disk full")
        replaces.append((source, destination))
        original_os.replace(source, destination)

    sentry_module.os = types.SimpleNamespace(replace=replace)
    if clock is not None:
        sentry_module.time = types.SimpleNamespace(time=original_time.time, monotonic=clock.monotonic)
    try:
        yield replaces
    finally:
        sentry_module.time, sentry_module.os = original_time, original_os


# ---------------------------------------------------------------------------------- detection

def test_open_reassignment_alerts_after_window():
    """Canton: an unassign that is never followed by its assign leaves the contract in flight, usable
    on neither synchronizer.
    Why: that is the limbo itself, the frozen asset this detector exists to report.
    Asserts: over a self-contained fixture (an unassign, then a later update that moves the ledger
    clock past a 5s window), a canton_reassignment_limbo alert fires, and its elapsed time is past
    the window. The fixture is independent of the unrelated scenarios in all_scenarios.jsonl."""
    limbo = [event for event in _run({"pending_window_seconds": 5}, FIX / "reassignment_limbo_case.jsonl")
             if event.type == "canton_reassignment_limbo"]
    assert limbo, "expected a limbo alert for an unassign that stayed open past the window"
    first_alert = limbo[0]
    assert int(first_alert.metadata["elapsed_seconds"]) > int(first_alert.metadata["window_seconds"])


def test_default_window_is_quiet_over_the_stream():
    """Canton: in a healthy network both reassignment steps land within seconds of each other.
    Why: the 900s default must not page anyone over normal traffic; only the demo profile used 5s.
    Asserts: replaying the whole recorded stream with default parameters emits no limbo alert."""
    assert not [event for event in _run({}) if event.type == "canton_reassignment_limbo"]


def test_completed_reassignment_closes_the_alert():
    """Canton: a slow reassignment can still complete, once the target is reachable again or any
    stakeholder submits the assign after exclusivity.
    Why: an open limbo alert needs a matching all-clear, or operators keep chasing a resolved case.
    Asserts: over the recorded stream with a 5s window, a canton_reassignment_completed event is
    emitted for the reassignment whose assign arrived after the alert."""
    assert any(event.type == "canton_reassignment_completed"
               for event in _run({"pending_window_seconds": 5}))


def test_snapshot_pending_reassignment_is_inherited():
    """Canton: the bootstrap ACS snapshot lists reassignments that are already in flight when the
    detector starts, without a record_time; the unassigned event carries the target synchronizer's
    assignment_exclusivity timestamp.
    Why: a contract frozen before the detector started is just as frozen; it must be tracked, and its
    alert must say its start time is approximate.
    Asserts: after snapshot_pending_reassignment.jsonl there is exactly one open entry, marked
    inherited, whose clock starts at assignment_exclusivity. Once the ledger clock passes that plus
    the window, the limbo alert carries inherited_from_snapshot=true."""
    updates = [CantonUpdate.model_validate_json(line)
               for line in (FIX / "snapshot_pending_reassignment.jsonl").read_text().splitlines() if line.strip()]
    detector = _detector({"pending_window_seconds": 60})
    assert not _drive(detector, updates)
    assert len(detector.open) == 1
    entry = next(iter(detector.open.values()))
    exclusivity = 1788876673420  # the fixture's assignment_exclusivity
    assert entry["inherited"] is True and entry["since"] == exclusivity

    limbo = _of_type(_drive(detector, [_transaction(exclusivity + 61_000)]), "canton_reassignment_limbo")
    assert len(limbo) == 1 and limbo[0].metadata["inherited_from_snapshot"] == "true"


def test_window_starts_at_assignment_exclusivity():
    """Canton: until assignment_exclusivity only the unassign's submitter may assign; after it, any
    stakeholder can. A pending assign before exclusivity is expected, not a stall.
    Why: starting the window at exclusivity measures the time in which anyone could have completed
    the reassignment and nobody did, which avoids early false alarms on long exclusivity periods.
    Asserts: with exclusivity 10 minutes after the unassign and a 60s window, the ledger clock at
    unassign+2min raises nothing, and exclusivity+61s raises the alert. With
    use_assignment_exclusivity=false, unassign+2min already alerts."""
    exclusivity = T0 + 600_000
    detector = _detector({"pending_window_seconds": 60})
    assert not _drive(detector, [_unassign("r-1", T0, exclusivity=exclusivity), _transaction(T0 + 120_000)])
    assert _of_type(_drive(detector, [_transaction(exclusivity + 61_000)]), "canton_reassignment_limbo")

    from_record_time = _detector({"pending_window_seconds": 60, "use_assignment_exclusivity": False})
    events = _drive(from_record_time, [_unassign("r-1", T0, exclusivity=exclusivity), _transaction(T0 + 120_000)])
    assert _of_type(events, "canton_reassignment_limbo")


def test_completion_inside_window_is_silent():
    """Canton: the normal reassignment, where the assign follows the unassign within seconds.
    Why: completed-in-time reassignments are routine traffic (the router issues them automatically);
    reporting them would drown real alerts.
    Asserts: an unassign, its assign 30s later, then the ledger clock moving 10 minutes on with a 60s
    window emits no event at all and leaves nothing open."""
    detector = _detector({"pending_window_seconds": 60})
    events = _drive(detector, [_unassign("r-1", T0), _assign("r-1", T0 + 30_000), _transaction(T0 + 600_000)])
    assert events == []
    assert detector.open == {}


def test_completion_after_alert_reports_elapsed():
    """Canton: a stalled reassignment completes late, e.g. after the target synchronizer recovers.
    Why: the all-clear must say how long the asset was frozen, which matters for incident reports.
    Asserts: with a 60s window, the unassign at T0 alerts at T0+61s, and the assign at T0+100s emits
    canton_reassignment_completed at INFO severity with elapsed_seconds=100."""
    detector = _detector({"pending_window_seconds": 60})
    events = _drive(detector, [_unassign("r-1", T0), _transaction(T0 + 61_000), _assign("r-1", T0 + 100_000)])
    assert len(_of_type(events, "canton_reassignment_limbo")) == 1
    completed = _of_type(events, "canton_reassignment_completed")
    assert len(completed) == 1
    assert completed[0].severity == 0.0
    assert completed[0].metadata["elapsed_seconds"] == "100"


def test_limbo_alerts_once_per_reassignment():
    """Canton: a stuck reassignment stays stuck across many later updates.
    Why: one CRITICAL page per frozen contract, not one per update the stream delivers afterwards.
    Asserts: three updates past the 60s window yield exactly one limbo alert."""
    detector = _detector({"pending_window_seconds": 60})
    events = _drive(detector, [_unassign("r-1", T0), _transaction(T0 + 61_000),
                               _transaction(T0 + 120_000), _transaction(T0 + 300_000)])
    assert len(_of_type(events, "canton_reassignment_limbo")) == 1


def test_window_boundary_is_inclusive():
    """Canton: the assign can land at any ledger time after the unassign, including exactly at the
    end of the window.
    Why: an assign that arrives exactly at the window's end is in time. The alert must fire only once
    the window is actually exceeded, not one tick early.
    Asserts: with a 60s window, the ledger clock at exactly unassign+60s raises nothing, and 1ms later
    the limbo alert fires."""
    detector = _detector({"pending_window_seconds": 60})
    assert _drive(detector, [_unassign("r-1", T0), _transaction(T0 + 60_000)]) == []
    assert len(_of_type(_drive(detector, [_transaction(T0 + 60_001)]), "canton_reassignment_limbo")) == 1


def test_assign_without_seen_unassign_is_ignored():
    """Canton: the detector can join mid-stream and see an assign whose unassign happened before it
    started and was not in the snapshot.
    Why: there is nothing open to close, and no basis for an alert or an all-clear.
    Asserts: a lone assign emits nothing, opens nothing, and does not mark the state for writing."""
    detector = _detector({"pending_window_seconds": 60})
    assert _drive(detector, [_assign("r-unknown", T0)]) == []
    assert detector.open == {}
    assert detector.dirty is False


def test_duplicate_unassign_does_not_reset_clock():
    """Canton: the same unassign can be delivered again, e.g. a page republished with a new
    update_id, or the snapshot listing an unassign the stream already delivered.
    Why: restarting the clock on each redelivery would postpone the limbo alert forever.
    Asserts: a second unassign with the same reassignment_id 50s later leaves the entry's start at
    T0."""
    detector = _detector({"pending_window_seconds": 60})
    _drive(detector, [_unassign("r-1", T0), _unassign("r-1", T0 + 50_000, update_id="u-republished")])
    assert detector.open["r-1"]["since"] == T0


def test_check_survives_concurrent_mutation():
    """Canton/runtime: on the Kafka path the consume loop and the ticker share one event loop. The
    ticker's check() awaits emit() inside its loop over open reassignments, and meanwhile the consumer
    can open a new one.
    Why: iterating the live dict raised "dict changed size during iteration" and killed the ticker,
    which silently ended all wall-clock alerting.
    Asserts: with check() parked inside emit(), an on_unassigned() call adds an entry, check()
    finishes without raising, and the new entry is kept for the next check. This is a regression test."""
    async def scenario():
        detector = build_detector("reassignment_limbo", {"pending_window_seconds": 0, "check_interval_seconds": 9999})
        detector.init()
        detector.open = {"limbo-1": _open_item(), "limbo-2": _open_item()}
        sink = _GateSink()
        detector.outputs.events = sink
        update, unassigned = _an_unassigned()
        task = asyncio.create_task(detector.check(10_000_000, update))
        await sink.entered.wait()           # check() suspended inside its loop
        before = len(detector.open)
        await detector.on_unassigned(update, unassigned)   # consumer mutates detector.open mid-iteration
        sink.release.set()
        await task                           # pre-fix this raised RuntimeError
        assert len(detector.open) == before + 1   # the new reassignment was added, deferred to next check
    asyncio.run(scenario())


def test_ticker_fires_on_wall_clock_when_ledger_clock_is_stale():
    """Canton: the ledger clock only moves when updates arrive. A synchronizer outage, a classic cause
    of limbo, is exactly when the stream goes quiet.
    Why: without the wall-clock ticker, the window would never expire in the very case it exists for.
    Asserts: with the ledger clock stuck at 0, an open reassignment inside its window by ledger time,
    and the module's wall clock injected far past the window, the real ticker() loop emits a limbo
    alert with no update in hand."""
    async def scenario():
        detector = build_detector("reassignment_limbo", {"pending_window_seconds": 10, "check_interval_seconds": 0})
        detector.init()
        detector.now_ms = 0  # ledger clock stuck at 0 (no updates ever arrive)
        detector.open = {"r1": _open_item()}  # since=0; stays within the 10s window by ledger time
        got = asyncio.Event()
        events = []

        class Sink:
            async def send(self, event):
                events.append(event)
                got.set()

        detector.outputs.events = Sink()
        original_time = sentry_module.time.time
        sentry_module.time.time = lambda: 1_000_000.0  # wall clock = 1e6 s -> 1e9 ms, far past the 10s window
        try:
            task = asyncio.create_task(detector.ticker())
            await asyncio.wait_for(got.wait(), timeout=2)
        finally:
            sentry_module.time.time = original_time
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert events and events[0].type == "canton_reassignment_limbo"  # fired on wall clock, update=None

    asyncio.run(scenario())


def test_ticker_alert_is_stamped_with_the_detectors_node():
    """Canton: several trunks can share one topic, and every alert says which node it is about.
    Why: the ticker builds its own update; without the node its alerts would be the only unstamped ones.
    Asserts: a scoped detector's ticker-driven limbo alert carries source_participant_hash of its node."""
    from sentinel_canton.runtime import hash_id
    node = "node-a::1220aa"

    async def scenario():
        detector = build_detector("reassignment_limbo", {"pending_window_seconds": 0, "check_interval_seconds": 9999, "participant_id": node})
        detector.init()
        detector.open = {"r1": _open_item()}
        sink = ListEventSink()
        detector.outputs.events = sink
        await detector.check(10_000_000, None)  # what the ticker calls: no update in hand
        return sink.events

    events = asyncio.run(scenario())
    assert [e.type for e in events] == ["canton_reassignment_limbo"]
    assert events[0].metadata["source_participant_hash"] == hash_id(node)


# ---------------------------------------------------------------------------------- volume

def test_volume_alert_above_hourly_limit():
    """Canton: one party can unassign many contracts in a burst. Each one goes in flight, contends
    with other workflows, and freezes the counterparties' assets until it is assigned.
    Why: a burst by one submitter is a griefing or freezing pattern (or a runaway bot), worth a HIGH
    alert before any single reassignment reaches the limbo window.
    Asserts: with a limit of 3 per hour, the first three unassigns by one submitter are quiet, and
    the fourth emits one canton_reassignment_volume at HIGH (0.5) with count_in_window=4."""
    detector = _detector({"max_reassignments_per_party_per_hour": 3})
    events = _drive(detector, [_unassign(f"r-{index}", T0 + index * 1000) for index in range(4)])
    volume = _of_type(events, "canton_reassignment_volume")
    assert len(volume) == 1
    assert volume[0].severity == 0.5
    assert volume[0].metadata["count_in_window"] == "4"
    assert volume[0].metadata["threshold"] == "3"


def test_volume_is_per_submitter_and_slides():
    """Canton: several parties reassign independently, and a busy hour long ago is not a burst now.
    Why: the limit is per submitter over a sliding hour; otherwise ordinary multi-party traffic or
    old history would trip it.
    Asserts: with a limit of 2, the client and a counterparty each unassigning twice is quiet. A third
    client unassign two hours later is also quiet, because the earlier two slid out of the hour."""
    detector = _detector({"max_reassignments_per_party_per_hour": 2})
    events = _drive(detector, [
        _unassign("r-client-1", T0, submitter=CLIENT),
        _unassign("r-client-2", T0 + 1000, submitter=CLIENT),
        _unassign("r-counter-1", T0 + 2000, submitter=COUNTERPARTY, witnesses=(COUNTERPARTY,)),
        _unassign("r-counter-2", T0 + 3000, submitter=COUNTERPARTY, witnesses=(COUNTERPARTY,)),
        _unassign("r-client-3", T0 + 7_200_000, submitter=CLIENT),
    ])
    assert not _of_type(events, "canton_reassignment_volume")


def test_volume_disabled_by_default_and_ignores_snapshot():
    """Canton: the bootstrap snapshot lists every in-flight reassignment at once, which can be many.
    Why: the volume check is opt-in (0 disables it), and a snapshot is inherited history, not a live
    burst, so it must never count.
    Asserts: with default parameters, ten unassigns by one submitter raise no volume alert. With a
    limit of 1, three snapshot unassigns raise none and leave the volume window empty."""
    by_default = _detector({})
    assert not _of_type(_drive(by_default, [_unassign(f"r-{index}", T0 + index) for index in range(10)]),
                        "canton_reassignment_volume")

    from_snapshot = _detector({"max_reassignments_per_party_per_hour": 1})
    events = _drive(from_snapshot, [_unassign(f"r-{index}", None, exclusivity=T0, snapshot=True) for index in range(3)])
    assert not _of_type(events, "canton_reassignment_volume")
    assert len(from_snapshot.open) == 3 and not from_snapshot.recent_unassigns


# ---------------------------------------------------------------------------------- monitored_parties

def test_monitored_parties_ignores_unrelated_reassignments():
    """Canton: a participant can host many parties, and the detector sees every reassignment its
    Ledger API user can read, not only the client's.
    Why: with monitored_parties set, a stuck contract that concerns none of the client's parties is
    someone else's incident, not this client's alert.
    Asserts: an unassign submitted by, and witnessed only by, a stranger is never opened, and the
    ledger clock passing the window raises nothing."""
    detector = _detector({"pending_window_seconds": 60, "monitored_parties": [CLIENT]})
    events = _drive(detector, [_unassign("r-1", T0, submitter=STRANGER, witnesses=(STRANGER,)),
                               _transaction(T0 + 120_000)])
    assert detector.open == {}
    assert events == []


def test_monitored_party_as_witness_is_tracked():
    """Canton: any stakeholder can unassign a contract, so a counterparty can move the client's
    contract in flight and never assign it. The client then appears only as a witness (stakeholder)
    of the unassigned event.
    Why: that is the freezing attack against the client. Filtering on the submitter alone would hide
    it, so the filter matches stakeholders too.
    Asserts: with monitored_parties=[client], an unassign submitted by the counterparty and witnessed
    by the client is tracked, and it raises the limbo alert after the window."""
    detector = _detector({"pending_window_seconds": 60, "monitored_parties": [CLIENT]})
    events = _drive(detector, [_unassign("r-1", T0, submitter=COUNTERPARTY, witnesses=(COUNTERPARTY, CLIENT)),
                               _transaction(T0 + 61_000)])
    assert "r-1" in detector.open
    assert len(_of_type(events, "canton_reassignment_limbo")) == 1


def test_empty_monitored_parties_tracks_everything():
    """Canton: same setting as above, but the operator has not named the client's parties.
    Why: an empty list keeps the detector's behaviour from before the filter existed, namely
    tracking every visible reassignment, so existing deployments are unaffected.
    Asserts: with no monitored_parties, a stranger's unassign is tracked and alerts after the window."""
    detector = _detector({"pending_window_seconds": 60})
    events = _drive(detector, [_unassign("r-1", T0, submitter=STRANGER, witnesses=(STRANGER,)),
                               _transaction(T0 + 61_000)])
    assert len(_of_type(events, "canton_reassignment_limbo")) == 1


# ---------------------------------------------------------------------------------- persistence

def test_state_is_persisted_and_reloaded():
    """Canton: the two reassignment steps can be minutes or hours apart, longer than a detector's
    uptime between deploys.
    Why: an open unassign forgotten at restart is a limbo that will never be reported.
    Asserts: replaying the unassign-only scenario writes a state file holding exactly one open
    reassignment. A fresh instance with the same state_path has the identical open set before it
    sees any update."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        # replay only the unassign scenario: it opens a reassignment that never completes
        _run({"pending_window_seconds": 5, "state_path": str(state)}, FIX / "372_reassignment_limbo.jsonl")
        assert state.exists(), "state file should be written"
        open_after = json.loads(state.read_text())["open"]
        assert len(open_after) == 1, "the open unassign should be persisted"

        # a fresh instance with the same state_path loads the open set before any update
        reloaded = build_detector("reassignment_limbo", {"pending_window_seconds": 5, "state_path": str(state)})
        reloaded.init()
        assert reloaded.open == open_after, "the open set should survive a restart"


def test_ledger_clock_restored_after_restart():
    """Canton: after a restart the stream may stay quiet for a while, e.g. during a synchronizer
    outage.
    Why: the restored ledger clock is what open reassignments are measured against until new updates
    arrive. Restarting it at 0 would make every restored entry look young.
    Asserts: the record_time of the last update before the write comes back as now_ms in a fresh
    instance."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        params = {"pending_window_seconds": 60, "state_path": str(state), "save_interval_seconds": 0}
        _drive(_detector(params), [_unassign("r-1", T0 + 42_000)])
        assert _detector(params).now_ms == T0 + 42_000


def test_writes_are_throttled_on_a_busy_stream():
    """Runtime: the state write is synchronous on the event loop that also consumes Kafka (the PR
    review finding).
    Why: one full JSON write per update stalls the consumer on a busy topic. Writes are throttled to
    one per save_interval_seconds.
    Asserts: 50 unassigns arriving within one frozen monotonic instant, with a 60s save interval,
    cause exactly one state write. The rest stay pending (dirty) for the next allowed write."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 60})
        with _patched_io(clock=_Clock()) as replaces:
            _drive(detector, [_unassign(f"r-{index}", T0 + index) for index in range(50)])
        assert len(replaces) == 1
        assert detector.dirty is True


def test_first_write_not_throttled_right_after_boot():
    """Runtime: time.monotonic() counts from machine boot, so on a freshly started host (a CI runner)
    it is smaller than the save interval.
    Why: the throttle measured the first write against a last write at monotonic 0, so a detector
    started within save_interval_seconds of boot skipped its first write, and a crash then lost it.
    Asserts: with the monotonic clock at 0 and a 60s save interval, the first unassign is written
    and nothing is left pending."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 60})
        with _patched_io(clock=_Clock(seconds=0.0)) as replaces:
            _drive(detector, [_unassign("r-1", T0)])
        assert len(replaces) == 1
        assert detector.dirty is False


def test_held_back_change_written_when_interval_ends_on_quiet_stream():
    """Runtime: a change the throttle held back used to wait for the next save() call, which comes
    with the next update or the next ticker run (check_interval_seconds, 30 by default).
    Why: on a quiet stream that stretched the changes a crash can lose from save_interval_seconds to
    check_interval_seconds, more than the FAQ promises. The held-back change is now written when the
    interval ends.
    Asserts: with a 50ms save interval and a ticker that never runs, the second of two unassigns is
    held back, and without any further update it reaches the state file once the interval has passed."""
    async def scenario(state):
        detector = _detector({"state_path": str(state), "save_interval_seconds": 0.05, "check_interval_seconds": 3600})
        await detector.handle_update(_unassign("r-1", T0))
        await detector.handle_update(_unassign("r-2", T0 + 1000))
        held_back = set(json.loads(state.read_text())["open"])
        await asyncio.sleep(0.2)
        return held_back, set(json.loads(state.read_text())["open"])

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        held_back, after_interval = asyncio.run(scenario(state))
        assert held_back == {"r-1"}, "second change should be throttled"
        assert after_interval == {"r-1", "r-2"}


def test_held_back_write_replaced_when_event_loop_changes():
    """Runtime: the held-back write is a timer on the event loop that was running when the throttle
    held a change back. If that loop ends (each asyncio.run is a new loop) before the timer fires, the
    timer never fires.
    Why: trusting such a leftover timer would leave the next held-back change waiting for an update
    that may never come, which is the same gap the timer exists to close.
    Asserts: with a frozen monotonic clock, r-2 is held back on a first loop that then ends. On a
    second loop r-3 is held back too. Once the clock passes the interval, with no further update, the
    state file holds r-1, r-2 and r-3."""
    clock = _Clock()

    async def quiet_period(detector):
        await detector.handle_update(_unassign("r-3", T0 + 2000))
        clock.seconds += 60  # the throttle interval has passed; the timer fires on real loop time
        await asyncio.sleep(0.2)

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 0.05})
        with _patched_io(clock=clock):
            _drive(detector, [_unassign("r-1", T0), _unassign("r-2", T0 + 1000)])
            assert set(json.loads(state.read_text())["open"]) == {"r-1"}, "r-2 should be held back"
            asyncio.run(quiet_period(detector))
        assert set(json.loads(state.read_text())["open"]) == {"r-1", "r-2", "r-3"}


def test_held_back_write_cancelled_by_an_earlier_write():
    """Runtime: a busy stream usually gets to the next allowed write before the held-back write's
    timer fires.
    Why: that write already stores the held-back change, so the timer is cancelled rather than left
    behind to fire later.
    Asserts: r-1 is written and r-2 held back. Once the interval has passed, r-3 arrives and its write
    stores both changes. Waiting past the timer's deadline adds no write: two writes in total, and no
    timer is left pending."""
    clock = _Clock()

    async def scenario(detector):
        await detector.handle_update(_unassign("r-1", T0))
        await detector.handle_update(_unassign("r-2", T0 + 1000))
        clock.seconds += 60
        await detector.handle_update(_unassign("r-3", T0 + 2000))
        await asyncio.sleep(0.2)

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 0.05})
        with _patched_io(clock=clock) as replaces:
            asyncio.run(scenario(detector))
        assert len(replaces) == 2
        assert detector.pending_flush is None
        assert set(json.loads(state.read_text())["open"]) == {"r-1", "r-2", "r-3"}


def test_memory_only_mode_never_schedules_a_write():
    """Runtime: without state_path (and without CANTON_STATE_DIR) the detector keeps state in memory
    only, by design.
    Why: there is nothing to write, so the throttle must not leave timers on the event loop.
    Asserts: with no state_path, a burst of unassigns tracks all of them and schedules no write."""
    detector = _detector({"save_interval_seconds": 60})
    _drive(detector, [_unassign(f"r-{index}", T0 + index) for index in range(5)])
    assert len(detector.open) == 5
    assert detector.pending_flush is None


def test_alerted_state_survives_restart():
    """Canton: a stuck reassignment can stay stuck for longer than a deploy cycle, e.g. while a
    stakeholder's validator is not connected to the target synchronizer (see the module docstring).
    Why: after a restart the restored entry must not page again for the same frozen contract, and
    when the assign finally lands, the all-clear must still be emitted, measured from the original
    unassign.
    Asserts: the unassign alerts before the restart. A fresh instance on the same state file stays
    quiet past the window, then emits canton_reassignment_completed with elapsed_seconds=300 when the
    assign arrives, and the state file is left with nothing open."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        params = {"pending_window_seconds": 60, "state_path": str(state), "save_interval_seconds": 0}
        before = _drive(_detector(params), [_unassign("r-1", T0), _transaction(T0 + 61_000)])
        assert len(_of_type(before, "canton_reassignment_limbo")) == 1

        restarted = _detector(params)
        after = _drive(restarted, [_transaction(T0 + 120_000), _assign("r-1", T0 + 300_000)])
        assert not _of_type(after, "canton_reassignment_limbo"), "no second page after the restart"
        completed = _of_type(after, "canton_reassignment_completed")
        assert len(completed) == 1 and completed[0].metadata["elapsed_seconds"] == "300"
        assert json.loads(state.read_text())["open"] == {}


def test_shutdown_without_changes_does_not_write():
    """Runtime: the shutdown flush bypasses the throttle, and it runs on every clean stop.
    Why: a forced write of an unchanged open set would recreate or overwrite the state file for
    nothing, e.g. on a detector that only ever saw ordinary transactions.
    Asserts: start the ticker, feed only transactions, and cancel the background tasks. No state write
    happens, and no state file is created."""
    async def scenario(state):
        detector = _detector({"state_path": str(state), "save_interval_seconds": 3600, "check_interval_seconds": 3600})
        await detector.on_init()
        await detector.handle_update(_transaction(T0))
        await _cancel_background_tasks()

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with _patched_io() as replaces:
            asyncio.run(scenario(state))
        assert replaces == []
        assert not state.exists()


def test_updates_without_reassignments_do_not_write():
    """Runtime: most updates on a busy topic are ordinary transactions with no reassignment in them.
    Why: before the fix every such update rewrote an unchanged open set, twice. A write now happens
    only when the open set changed.
    Asserts: five transaction-only updates with save_interval_seconds=0 (no throttle) cause zero
    writes, and no state file is created."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 0})
        with _patched_io() as replaces:
            _drive(detector, [_transaction(T0 + index * 1000) for index in range(5)])
        assert replaces == []
        assert not state.exists()


def test_interval_zero_writes_once_per_change():
    """Runtime: on_update() used to save once inside check() and once more after it.
    Why: save_interval_seconds=0 is the write-on-every-change mode; it must mean one write per
    change, not two per update. This is a regression test for the double write.
    Asserts: one unassign with no throttle causes exactly one state write."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        detector = _detector({"state_path": str(state), "save_interval_seconds": 0})
        with _patched_io() as replaces:
            _drive(detector, [_unassign("r-1", T0)])
        assert len(replaces) == 1


def test_pending_state_flushed_on_shutdown():
    """Runtime: the runner stops by cancelling the detector's background tasks (the ticker) in both
    the Kafka and the fixture path.
    Why: changes the throttle held back must not be lost on a clean shutdown. The ticker flushes
    them, bypassing the throttle, when it is cancelled.
    Asserts: the first unassign is written and the second is held back by a 1h save interval. After
    the background tasks are cancelled, the state file holds both."""
    async def scenario(state):
        detector = _detector({"state_path": str(state), "save_interval_seconds": 3600, "check_interval_seconds": 3600})
        await detector.on_init()  # starts the ticker
        await detector.handle_update(_unassign("r-1", T0))
        await detector.handle_update(_unassign("r-2", T0 + 1000))
        assert set(json.loads(state.read_text())["open"]) == {"r-1"}, "second change should be throttled"
        await _cancel_background_tasks()
        return set(json.loads(state.read_text())["open"])

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        assert asyncio.run(scenario(state)) == {"r-1", "r-2"}


def test_state_write_is_atomic():
    """Runtime: a process can die, or a disk can fill up, in the middle of a write.
    Why: a truncated state file made load() raise at the next start, so the detector could not boot.
    The write goes to a temp file that is renamed into place.
    Asserts: after a successful write, no .tmp file is left and the state file parses as JSON. When
    the rename fails, the error surfaces, and the previous state file is intact and still loads."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        params = {"state_path": str(state), "save_interval_seconds": 0}
        detector = _detector(params)
        _drive(detector, [_unassign("r-1", T0)])
        assert not list(pathlib.Path(directory).glob("*.tmp"))
        assert set(json.loads(state.read_text())["open"]) == {"r-1"}

        with _patched_io(failing_replace=True):
            try:
                _drive(detector, [_unassign("r-2", T0 + 1000)])
                raise AssertionError("a failed write should surface")
            except OSError:
                pass
        assert set(_detector(params).open) == {"r-1"}, "the last good state must survive a failed write"


# ---------------------------------------------------------------------------------- process shutdown
#
# The reviewer's scenario, end to end in a real process (tests/shutdown_harness.py): an unassign and
# its assign 2s later, the assign still held back by the save interval, then the process is stopped.
# Kafka has already recorded the assign as read, so it is never delivered again: whatever the state
# file holds after the stop is what the restarted detector believes. A clean stop (SIGTERM, which is
# how Kubernetes stops a pod on a deploy, or SIGINT) must write the pending change first. A hard kill
# (SIGKILL; a Kubernetes OOMKilled container is the kernel sending SIGKILL, exit code 137) cannot be
# caught: those tests pin down what survives it and what does not.

HARNESS = ROOT / "tests" / "shutdown_harness.py"


def _open_in(state):
    return set(json.loads(state.read_text())["open"])


@contextlib.contextmanager
def _harness(state, save_interval_seconds):
    """Start the harness process and yield it once both records were handled ("ready")."""
    process = subprocess.Popen([sys.executable, str(HARNESS), str(state), str(save_interval_seconds)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        ready = process.stdout.readline()
        assert ready.strip() == "ready", f"harness did not start: {ready!r} {process.stderr.read() if process.poll() is not None else ''}"
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=20)
        process.stdout.close()
        process.stderr.close()


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_clean_stop_writes_held_back_assign(stop_signal):
    """Runtime: Kubernetes stops a pod with SIGTERM (a deploy, a drain, kubectl delete); a terminal
    stops a local run with SIGINT. The runner had no SIGTERM handler, so its finally blocks, and with
    them the state write, never ran (the PR review finding).
    Why: the assign was already committed in Kafka but missing from the state file, so the restarted
    detector raised a false CRITICAL limbo alert 15 minutes later for a contract that had arrived.
    Asserts: before the signal the state file still lists r-1 as open (the assign is held back by a
    1h save interval). After the signal the process exits 0, says which signal stopped it, and the
    state file has nothing open."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with _harness(state, 3600) as process:
            assert _open_in(state) == {"r-1"}, "the assign should still be held back"
            process.send_signal(stop_signal)
            output, _ = process.communicate(timeout=20)
        assert process.returncode == 0
        assert f"stopping on {stop_signal.name}" in output
        assert _open_in(state) == set()


def test_hard_kill_keeps_last_written_state_loadable():
    """Runtime: SIGKILL, and an OOM kill (the kernel's SIGKILL), end the process at once, possibly in
    the middle of a state write; no handler or finally block runs.
    Why: the state file must still be the last complete write, or the detector cannot start again.
    Asserts: the process dies by SIGKILL, no temp file is left behind, and a fresh detector loads the
    file and finds the unassign that was written before the kill."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with _harness(state, 3600) as process:
            process.send_signal(signal.SIGKILL)
            process.wait(timeout=20)
        assert process.returncode == -signal.SIGKILL
        assert not list(pathlib.Path(directory).glob("*.tmp"))
        assert set(_detector({"state_path": str(state)}).open) == {"r-1"}


def test_hard_kill_with_interval_zero_loses_nothing():
    """Runtime: save_interval_seconds=0 writes every change as it happens, as before the throttle.
    Why: an operator who cannot accept the hard-kill gap below can trade write volume for it.
    Asserts: with interval 0, the state file has nothing open after a SIGKILL that follows the
    assign."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with _harness(state, 0) as process:
            process.send_signal(signal.SIGKILL)
            process.wait(timeout=20)
        assert _open_in(state) == set()


@pytest.mark.xfail(strict=True, reason="known gap: a hard kill loses changes held back by the save "
                   "interval while Kafka has already committed them. Follow-up: commit Kafka offsets "
                   "only after the state is written, so a restart replays what the file is missing.")
def test_hard_kill_keeps_held_back_assign():
    """Runtime: SIGKILL or an OOM kill while the throttle holds the assign back.
    Why: this is the reviewer's false CRITICAL alert, reached through a hard kill instead of a
    deploy. A signal handler cannot help here; only committing Kafka offsets after the state write
    can. The test documents the gap: it fails today, and strict xfail turns it red once the gap is
    closed, so the marker is removed then.
    Asserts: after a SIGKILL that follows the held-back assign, the state file has nothing open."""
    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with _harness(state, 3600) as process:
            process.send_signal(signal.SIGKILL)
            process.wait(timeout=20)
        assert _open_in(state) == set()


def test_consume_writes_state_when_the_stream_fails():
    """Runtime: the consume loop can also end with an error, e.g. a broker failure surfacing from
    the consumer, or a bug in a detector.
    Why: an error that reaches Python's exception handling is not a hard kill: the pending state can
    still be written before the process exits.
    Asserts: the stream delivers the unassign and the held-back assign, then raises. The error
    propagates out of _consume, and the state file already has nothing open."""
    async def failing_stream():
        for offset, update in enumerate([_unassign("r-1", T0), _assign("r-1", T0 + 2000)]):
            yield types.SimpleNamespace(value=update.model_dump_json(), offset=offset)
        raise RuntimeError("broker connection lost")

    async def scenario(state):
        detector = _detector({"state_path": str(state), "save_interval_seconds": 3600, "check_interval_seconds": 3600})
        await detector.on_init()  # starts the ticker, whose cancellation writes the state
        await _consume(detector, failing_stream())

    with tempfile.TemporaryDirectory() as directory:
        state = pathlib.Path(directory) / "limbo.state.json"
        with pytest.raises(RuntimeError, match="broker connection lost"):
            asyncio.run(scenario(state))
        assert _open_in(state) == set()


# ---------------------------------------------------------------------------------- privacy

def test_no_raw_party_ids():
    """Canton: party ids reveal who trades with whom, and Canton's privacy model exists to hide that.
    Why: alerts leave the client's perimeter (Kafka topic, webhook), so ids are hashed first.
    Asserts: no alert emitted over the recorded stream contains any raw party id from that stream in
    any metadata value."""
    parties = _all_parties()
    for event in _run({"pending_window_seconds": 5}):
        for value in event.metadata.values():
            leaked = [party for party in parties if party in str(value)]
            assert not leaked, f"raw party id(s) leaked: {leaked} in {value!r}"


def _open_item():
    # the shape on_unassigned() writes; past-window (since=0) so check() alerts and awaits emit
    return dict(contract_id="c", template_id="Iou:Iou", source="s", target="t", submitter="p",
                counter=0, since=0, offset=1, update_id="u", inherited=False, alerted=False)


class _GateSink:
    """emit() awaits send(); parks check() inside its loop on the first alert until released."""
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.count = 0

    async def send(self, event):
        self.count += 1
        if self.count == 1:
            self.entered.set()
            await self.release.wait()


def _an_unassigned():
    for line in (FIX / "372_reassignment_limbo.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        update = CantonUpdate.model_validate_json(line)
        unassigned = next((event for event in update.events if event.type.value == "unassigned"), None)
        if unassigned is not None:
            return update, unassigned
    raise AssertionError("no unassigned event in fixture")


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    failed = 0
    for test in tests:
        try:
            test(); print(f"PASS {test.__name__}")
        except Exception as error:
            failed += 1
            import traceback; print(f"FAIL {test.__name__}: {error}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
