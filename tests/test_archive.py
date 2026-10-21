"""An archived contract in the Canton stream (Create/Exercise/Archive), against the updates
captured from a real participant when the payer archived an Iou the client held.

The adapter asks for the LEDGER_EFFECTS shape, so the archive arrives as the consuming exercise of
the built-in Archive choice; the same update in the ACS_DELTA shape is an ArchivedEvent, and the
model normalises both."""
import json

from sentinel.models.chains.canton.update import CantonEventType, CantonUpdate, CantonUpdateKind

from wire import FIX, detector, run, updates


def test_ledger_effects_shape_carries_the_archive_as_a_consuming_exercise():
    created, archived = updates("367_archive.jsonl")
    assert created.kind == CantonUpdateKind.TRANSACTION and [e.type for e in created.events] == [CantonEventType.CREATED]
    assert archived.kind == CantonUpdateKind.TRANSACTION and len(archived.events) == 1
    ev = archived.events[0]
    assert ev.type == CantonEventType.EXERCISED and ev.choice == "Archive" and ev.consuming is True
    assert ev.contract_id == created.events[0].contract_id, "the exercise names the contract it consumes"
    assert ev.template_id == created.events[0].template_id
    assert ev.payload is None and ev.payload_hash, "the choice argument is hashed, never carried"
    assert archived.offset > created.offset and archived.update_id != created.update_id


def test_acs_delta_shape_of_the_same_update_is_an_archived_event():
    raw = json.loads((FIX / "canton_367_archive_acs_delta.json").read_text())
    created, archived = [CantonUpdate.from_json_api(u) for u in raw]
    effects_created, effects_archived = updates("367_archive.jsonl")
    assert archived.update_id == effects_archived.update_id and archived.offset == effects_archived.offset
    ev = archived.events[0]
    assert ev.type == CantonEventType.ARCHIVED
    assert ev.contract_id == created.events[0].contract_id == effects_created.events[0].contract_id
    assert ev.template_id == effects_created.events[0].template_id
    # the user reads as the client only, so the participant reports the client as the sole witness
    assert ev.witness_parties and all(w.startswith("client::") for w in ev.witness_parties)
    assert set(ev.witness_parties) <= set(created.events[0].observers)


def test_an_archive_raises_no_stakeholder_alert():
    d = detector("stakeholder_anomaly", max_stakeholders_default=10)
    assert run(d, "367_archive.jsonl") == []
