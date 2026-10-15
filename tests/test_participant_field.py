"""`participant_id` is a field of every Canton detector's Schema, so the platform
form shows it ("Participant (node)") and saving a condition from the UI keeps it. Conditions saved
on 0.2.0, before the field, must still reach the detectors: the platform matches a condition to a
detector by schema name and by the 0.2 line, not one patch."""

import importlib

import pytest
from sentinel.utils.version import is_bugfix

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
def test_a_condition_without_the_field_still_validates_as_any_node(detector):
    manifest = importlib.import_module(f"sentinel_canton.detectors.{detector}.manifest")
    assert manifest.Schema().participant_id == ""
    node = "validator-01::1220aa"
    assert manifest.Schema(participant_id=node).participant_id == node


@pytest.mark.parametrize("detector", DETECTORS)
def test_conditions_saved_on_0_2_0_keep_reaching_the_detectors(detector):
    version = importlib.import_module(f"sentinel_canton.detectors.{detector}.manifest").metadata.version
    line = ".".join(version.split(".")[:2])  # what a deployment pins: the major.minor line
    assert is_bugfix(main_version=line, version=version)
    assert is_bugfix(main_version=line, version="0.2.0"), f"{detector} {version} drops the 0.2.0 conditions"
    assert is_bugfix(main_version=line, version="0.2.1"), f"{detector} {version} drops the 0.2.1 conditions"


@pytest.mark.parametrize("detector", DETECTORS)
def test_the_faq_says_which_node_it_watches(detector):
    manifest = importlib.import_module(f"sentinel_canton.detectors.{detector}.manifest")
    assert manifest.metadata.faq[-1].name == "Which node does it watch?"
