"""A reassignment_limbo process for the shutdown tests in test_reassignment_limbo.py, which signal it.

It drives the runner's real consume loop (_consume, the one run_kafka uses) over two records: an
unassign and its assign 2s later, then the stream goes quiet. The first write is never throttled, so
the unassign reaches the state file at once, while the assign is held back by the save interval:
the reviewer's scenario. "ready" on stdout means both records were handled; the test then sends its
signal. Not collected by pytest (no test_ prefix).

Usage: python shutdown_harness.py <state_path> <save_interval_seconds>
"""
import asyncio
import pathlib
import sys
import types

TESTS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))

from test_reassignment_limbo import T0, _assign, _unassign
from sentinel_canton.runner import ListEventSink, _consume, _start_detector, build_detector


async def quiet_stream():
    """The two records, then a topic with nothing more to deliver."""
    for offset, update in enumerate([_unassign("r-1", T0), _assign("r-1", T0 + 2000)]):
        yield types.SimpleNamespace(value=update.model_dump_json(), offset=offset)
    print("ready", flush=True)  # the generator resumes only after the last record was handled
    await asyncio.Event().wait()


async def main(state_path: str, save_interval_seconds: float) -> None:
    detector = build_detector("reassignment_limbo", {
        "state_path": state_path,
        "save_interval_seconds": save_interval_seconds,
        "check_interval_seconds": 3600,  # the ticker never runs a check: only the signal path writes
    })
    detector.outputs.events = ListEventSink()
    await _start_detector(detector)
    await _consume(detector, quiet_stream())


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], float(sys.argv[2])))
