"""Reassignment limbo against the fixtures captured from a real participant. Ledger time is the
clock: the unassign happens at offset 259, the next updates (the informee scenario) arrive ten
seconds later, the assign twenty-two seconds later."""
import asyncio
import re

from wire import detector, meta, run, updates


def test_unassign_without_assign_alerts_after_the_window():
    d = detector("reassignment_limbo", pending_window_seconds=5)
    assert run(d, "372_reassignment_limbo.jsonl") == [], "just unassigned: still inside the window"
    assert len(d.open) == 1
    events = run(d, "373_informee_anomaly.jsonl")  # ledger time moves ten seconds on
    assert [e.type for e in events] == ["canton_reassignment_limbo"]
    ev = events[0]
    assert ev.category == "ALERT" and ev.severity == 0.75
    assert meta(ev)["source"].startswith("sync1::") and meta(ev)["target"].startswith("sync2::")
    assert meta(ev)["elapsed_seconds"] >= 5 and meta(ev)["window_seconds"] == 5
    assert meta(ev)["reassignment_counter"] == 1 and meta(ev)["inherited_from_snapshot"] is False
    assert len(meta(ev)["contract_hash"]) == 64 and len(meta(ev)["submitter_hash"]) == 64
    assert not re.search(r"(client|counterparty|stranger)::", str(meta(ev)))
    assert meta(ev)["unassign_offset"] == 259 and meta(ev)["offset"] > 259, "reported on the update that revealed it"
    assert meta(ev)["policy"]["name"] == "Canton-ReassignmentLimbo"


def test_assign_in_time_closes_silently():
    d = detector("reassignment_limbo", pending_window_seconds=900)
    events = run(d, "372_reassignment_limbo.jsonl", "373_informee_anomaly.jsonl", "372b_reassignment_complete.jsonl")
    assert events == [] and d.open == {} and d.seen["closed"] == 1


def test_late_assign_closes_an_alerted_limbo_with_an_event():
    d = detector("reassignment_limbo", pending_window_seconds=5)
    events = run(d, "372_reassignment_limbo.jsonl", "373_informee_anomaly.jsonl", "372b_reassignment_complete.jsonl")
    assert [e.type for e in events] == ["canton_reassignment_limbo", "canton_reassignment_completed"]
    done = events[1]
    assert done.category == "EVENT" and meta(done)["elapsed_seconds"] >= 20 and d.open == {}


def test_pending_reassignment_is_inherited_from_the_snapshot():
    d = detector("reassignment_limbo", pending_window_seconds=5)
    assert run(d, "snapshot_pending_reassignment.jsonl") == []
    assert len(d.open) == 1 and next(iter(d.open.values()))["inherited"] is True
    events = run(d, "373_informee_anomaly.jsonl")
    assert [e.type for e in events] == ["canton_reassignment_limbo"] and meta(events[0])["inherited_from_snapshot"] is True


def test_open_set_survives_a_restart(tmp_path):
    path = str(tmp_path / "limbo.json")
    d = detector("reassignment_limbo", pending_window_seconds=5, state_path=path)
    run(d, "372_reassignment_limbo.jsonl")
    d2 = detector("reassignment_limbo", pending_window_seconds=5, state_path=path)
    assert len(d2.open) == 1, "reloaded from the file"
    events = run(d2, "373_informee_anomaly.jsonl")
    assert [e.type for e in events] == ["canton_reassignment_limbo"]
    d3 = detector("reassignment_limbo", pending_window_seconds=5, state_path=path)
    assert run(d3, "373_informee_anomaly.jsonl") == [], "already alerted: not again after another restart"


def test_ticker_uses_the_wall_clock_when_the_stream_is_quiet():
    d = detector("reassignment_limbo", pending_window_seconds=5)
    run(d, "372_reassignment_limbo.jsonl")
    since = next(iter(d.open.values()))["since"]
    asyncio.run(d.check(since + 6_000, None))  # what the ticker does
    events = d.outputs.events.events
    assert [e.type for e in events] == ["canton_reassignment_limbo"] and meta(events[0])["offset"] == 259


def test_volume_rule_counts_unassigns_per_submitter():
    d = detector("reassignment_limbo", pending_window_seconds=900, max_reassignments_per_party_per_hour=1)
    first = [u for u in updates("372_reassignment_limbo.jsonl") if u.kind.value == "reassignment"][0]
    second = first.model_copy(deep=True)
    second.update_id, second.offset = "1220second", first.offset + 3
    second.events[0].reassignment_id, second.events[0].contract_id = "00second", "00othercontract"

    async def feed():
        await d.on_update(first)
        await d.on_update(second)

    asyncio.run(feed())
    events = d.outputs.events.events
    assert [e.type for e in events] == ["canton_reassignment_volume"]
    assert meta(events[0])["count_in_window"] == 2 and meta(events[0])["threshold"] == 1


def test_all_scenarios_with_default_window_stay_quiet():
    d = detector("reassignment_limbo")
    assert run(d, "all_scenarios.jsonl") == [] and d.open == {}
