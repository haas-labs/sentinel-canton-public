"""Standalone runtime for the Canton detectors.

The detectors were written against the private Sentinel SDK's CantonUpdateDetector, which extends a
heavy AsyncCoreSentry (multiprocessing, channels, databases, metrics, croniter, the monitoring-
conditions system). None of that is needed to run a detector: the detection logic only touches two
seams -- configure() for parameters in, and emit()/outputs.events.send() for alerts out.

This module reimplements CantonUpdateDetector as a thin base with no SDK dependency: the pure helpers
(flat_metadata, hash_id, emit) are copied verbatim, and the framework runtime is replaced by the
runner in runner.py, which wires inputs/outputs and drives the update loop. The monitoring-conditions
machinery is dropped; parameters come once from configure() (profile/env).
"""
import hashlib
import json
import logging
import os
import time
from collections import deque
from types import SimpleNamespace
from typing import Any, Deque, Dict, Optional, Set

from sentinel.manifest import Severity
from sentinel.models.event import Blockchain, Event
from sentinel.models.chains.canton.update import CantonUpdate

CANTON = Blockchain(network="canton", chain_id="canton")

DEFAULT_SOURCE_ID = "ext:sentinel"  # the Extractor's sentinel source


def source_id() -> str:
    """
    Event source id, stamped on every emitted Event as `sid`. Override with CANTON_SOURCE_ID when
    feeding a different sink. Read on each call, not at import: runner.cli() loads ./.env after this
    module is imported, so an import-time read would miss a value set there.
    """
    return os.environ.get("CANTON_SOURCE_ID", DEFAULT_SOURCE_ID)

RECENT_UPDATES = 10_000  # update_ids remembered for deduplication


def flat_metadata(metadata: Dict[str, Any]) -> Dict[str, str]:
    """
    The platform reads an event's metadata as a map of strings and drops, without a log line,
    any record whose metadata holds a list, an object or a number. Numbers become their text,
    lists and objects their JSON; None is left out.
    """
    out: Dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, str):
            out[key] = value
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif isinstance(value, (int, float)):
            out[key] = str(value)
        else:
            out[key] = json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return out


def severity_or(configured: Optional[float], default: Severity) -> float:
    """The platform's scale: INFO 0, LOW 0.15, MEDIUM 0.25, HIGH 0.5, CRITICAL 0.75. A configured
    severity of -1 (AUTO, the manifest default) means the detector's own default."""
    value = float(configured if configured is not None else -1)
    return value if value >= 0 else float(default)


def hash_id(value: Optional[str]) -> Optional[str]:
    """
    Party, contract and participant ids never leave a Canton sentry in the clear: every alert
    carries their sha256 instead, under a key with a _hash suffix.
    """
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class CantonUpdateDetector:
    """Base for sentries over a Canton participant's update stream. Subclasses override configure()
    to read parameters, on_update() for the detection logic, and optionally on_init() for background
    tasks; they emit alerts through emit()."""

    name = "CantonUpdateDetector"
    description = "Base detector over a Canton participant's update stream"

    policy_name: Optional[str] = None
    policy_version: Optional[str] = None

    def __init__(self, name: str = None, description: str = None, parameters: Dict = None, **kwargs) -> None:
        self.name = name or type(self).name
        self.description = description or type(self).description
        self.parameters = dict(parameters or {})
        self.logger = logging.getLogger("canton://" + self.name)
        self._recent_ids: Deque[str] = deque(maxlen=RECENT_UPDATES)
        self._recent_set: Set[str] = set()
        # inputs/outputs are wired by the runner; init() binds the update handler, emit() uses events.
        self.inputs = SimpleNamespace(updates=SimpleNamespace(on_update=None))
        self.outputs = SimpleNamespace(events=None)

    def init(self) -> None:
        """Sync setup: bind the update handler and apply parameters. The runner calls this once."""
        self.inputs.updates.on_update = self.handle_update
        self.profile_parameters = dict(self.parameters or {})
        self.configure(dict(self.profile_parameters))

    async def on_init(self) -> None:
        """Async startup hook (e.g. to spawn a background ticker). Base is a no-op."""

    async def handle_update(self, update: CantonUpdate) -> None:
        """Dedup by update_id (a republished page is harmless), then dispatch to on_update.
        Snapshots carry no update_id and are never deduplicated."""
        if update.update_id:
            if update.update_id in self._recent_set:
                self.logger.info(f"update {update.update_id[:12]}… already seen, skipped")
                return
            if len(self._recent_ids) == self._recent_ids.maxlen:
                self._recent_set.discard(self._recent_ids[0])
            self._recent_ids.append(update.update_id)
            self._recent_set.add(update.update_id)
        await self.on_update(update)

    async def on_update(self, update: CantonUpdate) -> None:
        """The detection entry point. Subclasses override this."""

    def configure(self, parameters: Dict) -> None:
        """Detectors override this to read their parameters."""

    @property
    def policy(self) -> Dict[str, Any]:
        return {"name": self.policy_name, "version": self.policy_version}

    async def emit(
        self,
        type: str,
        severity: float,
        update: CantonUpdate,
        category: str = "ALERT",
        desc: Optional[str] = None,
        **metadata: Any,
    ) -> Event:
        """
        Build and send the Event every Canton detector emits alike: blockchain fixed to canton,
        ts = the update's record_time, and offset / update_id / synchronizer_id always in metadata so
        an alert is traceable to the ledger. Metadata is flattened to strings, with the description
        under `desc` and the update id under `tx_hash` (how the platform reads the message and builds
        its explorer link). Callers pass ids already hashed (see hash_id).
        """
        event = Event(
            did=self.name,
            sid=source_id(),
            category=category,
            type=type,
            severity=severity,
            desc=desc,
            ts=update.record_time or int(time.time() * 1000),
            blockchain=CANTON,
            metadata=flat_metadata(
                {
                    "desc": desc,
                    "tx_hash": update.update_id,
                    "offset": update.offset,
                    "update_id": update.update_id,
                    "synchronizer_id": update.synchronizer_id,
                    "policy": self.policy,
                    **metadata,
                }
            ),
        )
        await self.outputs.events.send(event)
        return event
