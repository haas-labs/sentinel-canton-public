"""`participant_id` is a field of every Canton detector's Schema, so the platform
form shows it ("Participant (node)") and saving a condition from the UI keeps it."""

import importlib

import pytest

DETECTORS = ["topology_drift", "capability_drift", "stakeholder_anomaly", "reassignment_limbo", "informee_anomaly"]


@pytest.mark.parametrize("detector", DETECTORS)
def test_the_form_has_the_participant_field(detector):
    manifest = importlib.import_module(f"sentinel_canton.detectors.{detector}.manifest")
    prop = manifest.Schema.model_json_schema()["properties"]["participant_id"]
    assert prop["title"] == "Participant (node)"
    assert prop["default"] == ""
    assert "Empty = any node" in prop["description"]
    assert manifest.Schema().participant_id == ""


@pytest.mark.parametrize("detector", DETECTORS)
def test_the_faq_says_which_node_it_watches(detector):
    manifest = importlib.import_module(f"sentinel_canton.detectors.{detector}.manifest")
    assert manifest.metadata.faq[-1].name == "Which node does it watch?"
