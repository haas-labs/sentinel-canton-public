"""Standalone runner: consume CantonUpdate records, drive a detector, publish its Events.

Input: a Kafka topic (CANTON_TX_TOPIC, the stream produced by the trunk-canton adapter) or, for
tests and local runs, a file or directory of *.jsonl fixtures. Output: a Kafka topic
(CANTON_EVENTS_TOPIC) or an in-memory sink. The detector is selected by CANTON_DETECTOR.
"""
import asyncio
import importlib
import json
import os
import pathlib
import signal
from typing import List, Optional

from pydantic import ValidationError

from sentinel_canton.detectors import REGISTRY
from sentinel.models.event import Event
from sentinel.models.chains.canton.update import CantonUpdate

# Parameter keys the runner injects itself (not user-facing); excluded from the unknown-key check.
_RUNNER_KEYS = {"state_path"}

DEFAULT_ADMIN_API_URL = "localhost:5012"  # a local participant's Admin API


def _parameters(name: str) -> dict:
    raw = os.environ.get("CANTON_PARAMETERS", "").strip()
    params = json.loads(raw) if raw else {}
    # Only capability_drift accepts admin_api_url; CANTON_ADMIN_API_URL= (empty) disables it.
    if name == "capability_drift" and "admin_api_url" not in params:
        params["admin_api_url"] = os.environ.get("CANTON_ADMIN_API_URL", DEFAULT_ADMIN_API_URL)
    return params


def _schema_for(name: str):
    """The detector's manifest Schema (a pydantic model of its parameters), or None when the detector
    ships no manifest. The runtime only reads `metadata` from the manifest, so the Schema is otherwise
    unused here; we load it to validate CANTON_PARAMETERS."""
    try:
        manifest = importlib.import_module(f"sentinel_canton.detectors.{name}.manifest")
    except ModuleNotFoundError:
        return None
    return getattr(manifest, "Schema", None)


def _validate_parameters(name: str, params: dict) -> None:
    """Fail fast on a mistyped or wrong-typed CANTON_PARAMETERS key instead of silently falling back
    to the code default. Checks the user's keys against the manifest Schema (unknown keys, then types).
    Runs on the user-supplied params only, before the runner injects its own keys (see _RUNNER_KEYS)."""
    schema = _schema_for(name)
    if schema is None:
        return
    valid = set(schema.model_fields)
    unknown = set(params) - valid - _RUNNER_KEYS
    if unknown:
        raise SystemExit(
            f"unknown parameter(s) for {name!r}: {sorted(unknown)}; valid keys: {sorted(valid)}"
        )
    try:
        schema.model_validate(params)
    except ValidationError as e:
        raise SystemExit(f"invalid CANTON_PARAMETERS for {name!r}: {e}")


def build_detector(name: str, parameters: dict = None):
    if name not in REGISTRY:
        raise SystemExit(f"unknown detector {name!r}; known: {sorted(REGISTRY)}")
    params = dict(parameters or {})
    _validate_parameters(name, params)
    # A stateful detector persists to parameters["state_path"]. Default it under CANTON_STATE_DIR so
    # the mounted volume is used without hand-writing the path; an explicit state_path always wins.
    state_dir = os.environ.get("CANTON_STATE_DIR")
    if state_dir and not params.get("state_path"):
        params["state_path"] = str(pathlib.Path(state_dir) / f"{name}.state.json")
    # keep the class's own name so the emitted Event `did` matches the original build; the registry
    # key (name) above only names the state file.
    return REGISTRY[name](parameters=params)


async def _start_detector(detector) -> None:
    detector.init()
    await detector.on_init()


class ListEventSink:
    """Collects emitted events in memory (tests / fs runs)."""

    def __init__(self):
        self.events: List[Event] = []

    async def send(self, event: Event) -> None:
        self.events.append(event)


class KafkaEventSink:
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    async def send(self, event: Event) -> None:
        await self._producer.send_and_wait(self._topic, value=event.model_dump_json(exclude_none=True).encode())


async def run_over_fixture(detector, path) -> List[Event]:
    """Feed every CantonUpdate in a .jsonl file (or all *.jsonl in a directory) through the detector
    and return the events it emits. Deterministic, no Kafka: used by tests and local runs."""
    sink = ListEventSink()
    detector.outputs.events = sink
    await _start_detector(detector)
    path = pathlib.Path(path)
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    for f in files:
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                await detector.handle_update(CantonUpdate.model_validate_json(line))
    await _cancel_background_tasks()
    return sink.events


async def _cancel_background_tasks():
    """Cancel every other task in this loop (e.g. a detector's on_init ticker) so it closes cleanly
    without a "Task was destroyed but it is pending" warning. Both entry points run under their own
    asyncio.run, so "every other task" is exactly the detector-spawned background work."""
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def run_kafka(detector) -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    broker = os.environ.get("CANTON_KAFKA_BOOTSTRAP") or "localhost:19092"
    tx_topic = os.environ.get("CANTON_TX_TOPIC") or "canton.localnet.tx"
    events_topic = os.environ.get("CANTON_EVENTS_TOPIC") or "canton.localnet.events"
    # per-detector group by default, so two detectors on one topic don't steal each other's messages
    group = os.environ.get("CANTON_CONSUMER_GROUP") or f"sentinel-canton-{detector.name}"
    consumer = AIOKafkaConsumer(tx_topic, bootstrap_servers=broker, group_id=group,
                                auto_offset_reset="earliest", enable_auto_commit=True)
    producer = AIOKafkaProducer(bootstrap_servers=broker, acks="all")
    await consumer.start()
    await producer.start()
    detector.outputs.events = KafkaEventSink(producer, events_topic)
    await _start_detector(detector)
    print(f"detector {detector.name} consuming {tx_topic} -> {events_topic} @ {broker}")
    try:
        await _consume(detector, consumer)
    finally:
        # after _consume has stopped the detector's tasks, so its state is written before the
        # consumer's final offset commit
        await consumer.stop()
        await producer.stop()


class ShutdownSignal:
    """Which stop signal arrived, if any: tells a requested stop from any other cancellation."""

    def __init__(self):
        self.received: Optional[str] = None


def _stop_on_signals(task: asyncio.Task) -> ShutdownSignal:
    """Cancel task on SIGTERM or SIGINT. Kubernetes stops a pod with SIGTERM, and without a handler
    the process never reaches its finally blocks: a stateful detector's pending state is never
    written. As PID 1 in a container, Python ignores an unhandled SIGTERM altogether, so the pod
    also waited out its whole grace period and was then SIGKILLed."""
    shutdown = ShutdownSignal()
    loop = asyncio.get_running_loop()

    def stop(stop_signal: signal.Signals) -> None:
        shutdown.received = stop_signal.name
        task.cancel()

    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stop, stop_signal)
    return shutdown


async def _consume(detector, messages) -> None:
    """Drive the detector over raw records (anything with .value and .offset, e.g. aiokafka's
    ConsumerRecord) until the stream ends or SIGTERM/SIGINT stops it. Either way, and on an error,
    the detector's background tasks are stopped first, which is where stateful detectors write
    their pending state. A signal is a clean stop: it returns normally, so the process exits 0."""
    shutdown = _stop_on_signals(asyncio.current_task())
    try:
        async for msg in messages:
            try:
                update = CantonUpdate.model_validate_json(msg.value)
            except Exception as e:
                print(f"skipping unparseable record at offset {msg.offset}: {e}")
                continue
            await detector.handle_update(update)
    except asyncio.CancelledError:
        if shutdown.received is None:
            raise
        print(f"stopping on {shutdown.received}", flush=True)
    finally:
        await _cancel_background_tasks()  # stop the detector's on_init ticker, if any


async def main() -> None:
    name = os.environ.get("CANTON_DETECTOR")
    if not name:
        raise SystemExit(f"CANTON_DETECTOR is required; known: {sorted(REGISTRY)}")
    detector = build_detector(name, _parameters(name))
    if (os.environ.get("CANTON_INPUT") or "kafka") == "fs":
        events = await run_over_fixture(detector, os.environ["CANTON_INPUT_PATH"])
        print(f"{detector.name}: emitted {len(events)} events over the fixtures")
    else:
        await run_kafka(detector)


def cli() -> None:
    from dotenv import load_dotenv

    load_dotenv()  # ./.env if present; variables already in the environment win
    asyncio.run(main())


if __name__ == "__main__":
    cli()
