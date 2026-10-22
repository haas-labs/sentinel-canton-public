"""Shared helpers for the scenario tests: the recorded fixtures, a detector wired to an in-memory
sink, and what an alert looks like on the wire, read back as Python values.

The runtime flattens event metadata to a map of strings (the platform reads it as
Map<String,String> and drops anything else). meta() decodes it back: JSON for numbers, lists,
objects and booleans, the text otherwise; a key the detector set to None is absent. Not collected
by pytest (no test_ prefix)."""
import asyncio
import json
import pathlib
from typing import Any, Dict, List

from sentinel.models.chains.canton.update import CantonUpdate
from sentinel_canton.runner import ListEventSink, build_detector

FIX = pathlib.Path(__file__).resolve().parent / "fixtures"


def meta(event) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in (event.metadata or {}).items():
        if isinstance(value, str):
            try:
                out[key] = json.loads(value)
            except ValueError:
                out[key] = value
        else:
            out[key] = value
    return out


def updates(name: str) -> List[CantonUpdate]:
    return [CantonUpdate.model_validate_json(l) for l in (FIX / name).read_text().splitlines() if l.strip()]


def detector(name: str, **parameters):
    """A detector built and initialised as the runner does, its events collected in d.outputs.events.events."""
    d = build_detector(name, parameters)
    d.outputs.events = ListEventSink()
    d.init()
    return d


def run(d, *names) -> list:
    """Feed the fixtures through on_update; returns every event the detector has emitted so far."""
    async def go():
        for name in names:
            for u in updates(name):
                await d.on_update(u)
    asyncio.run(go())
    return list(d.outputs.events.events)
