import asyncio
import json
import os
import pathlib
import time
from collections import Counter, deque
from typing import Deque, Dict, Optional, Tuple

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, CantonUpdate, CantonUpdateKind
from sentinel.manifest import Severity
from sentinel_canton.runtime import CantonUpdateDetector, hash_id, severity_or

from sentinel_canton.detectors.reassignment_limbo import manifest


class ReassignmentLimboDetector(CantonUpdateDetector):
    """
    Canton is several synchronizers. Moving a contract between two is a two-step operation:
    unassign on the source, then assign on the target, and between the steps the contract sits in
    a limbo where neither side can use it. A reassignment that never completes freezes the asset,
    by bug or by attack. This detector keeps every open unassign it sees, from the stream and from
    the bootstrap snapshot, and alerts when one stays open longer than the window. The two steps
    can be minutes or hours apart, so the open set is persisted and survives a restart.

    Clock: ledger time, the latest record_time seen (checkpoints included); the wall clock only
    fills in when the stream is quiet.
    """

    name = "CantonReassignmentLimbo"
    description = "Alerts when an unassigned contract is not assigned on the target synchronizer in time"

    def configure(self, parameters: Dict) -> None:
        parameters = parameters or {}
        self.monitored_parties = set(parameters.get("monitored_parties") or [])
        self.window_ms = int(parameters.get("pending_window_seconds", 900)) * 1000
        self.use_exclusivity = bool(parameters.get("use_assignment_exclusivity", True))
        self.volume_limit = int(parameters.get("max_reassignments_per_party_per_hour", 0))
        self.severity = severity_or(parameters.get("severity"), Severity.CRITICAL)
        self.severity_volume = severity_or(parameters.get("severity_volume"), Severity.HIGH)
        self.check_interval = int(parameters.get("check_interval_seconds", 30))
        self.save_interval = float(parameters.get("save_interval_seconds", 5))
        self.state_path = pathlib.Path(parameters["state_path"]) if parameters.get("state_path") else None
        self.open: Dict[str, Dict] = {}
        self.dirty = False  # the open set changed since the last write
        # time.monotonic() of the last write. monotonic() counts from boot, so a 0 here would throttle
        # the first write on a freshly started host
        self.last_save = float("-inf")
        # the deferred write armed by a throttled save, and the loop it runs on
        self.pending_flush: Optional[asyncio.TimerHandle] = None
        self.pending_flush_loop: Optional[asyncio.AbstractEventLoop] = None
        self.now_ms: int = 0  # ledger clock
        self.recent_unassigns: Deque[Tuple[int, str]] = deque()
        self.seen = Counter()
        self.policy_name, self.policy_version = manifest.metadata.name, manifest.metadata.version
        self.load()

    # ------------------------------------------------------------------ persistence
    def load(self) -> None:
        if self.state_path and self.state_path.exists():
            raw = json.loads(self.state_path.read_text())
            self.open = raw.get("open", {})
            self.now_ms = int(raw.get("now_ms", 0))

    def save(self, force: bool = False) -> None:
        """Write the open set, but only when it changed, and at most once per save_interval: the
        write is synchronous on the event loop, so writing on every update would stall the consumer
        on a busy topic. A change the throttle holds back is written when the interval ends, not on
        the next update or tick, so a quiet stream loses no more than save_interval on a crash.
        force skips the throttle (shutdown flush). The write goes to a temp file and is renamed
        into place, so a crash mid-write never leaves a truncated state file."""
        if not self.state_path or not self.dirty:
            return
        elapsed = time.monotonic() - self.last_save
        if not force and elapsed < self.save_interval:
            self.arm_pending_flush(self.save_interval - elapsed)
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_path.with_name(self.state_path.name + ".tmp")
        temp_path.write_text(json.dumps({"open": self.open, "now_ms": self.now_ms}, indent=1))
        os.replace(temp_path, self.state_path)
        self.dirty = False
        self.last_save = time.monotonic()
        if self.pending_flush is not None:
            self.pending_flush.cancel()
            self.pending_flush = None

    def arm_pending_flush(self, delay_seconds: float) -> None:
        """Schedule one write for when the throttle interval ends. A timer from a loop that is gone
        (each asyncio.run is a new loop) never fires, so it is replaced rather than trusted."""
        loop = asyncio.get_running_loop()
        if self.pending_flush is not None and self.pending_flush_loop is loop:
            return
        self.pending_flush = loop.call_later(delay_seconds, self.flush_pending)
        self.pending_flush_loop = loop

    def flush_pending(self) -> None:
        self.pending_flush = None
        self.save()

    # ------------------------------------------------------------------ lifecycle
    async def on_init(self) -> None:
        asyncio.create_task(self.ticker(), name="reassignment-limbo-ticker")
        # let the ticker enter its try block: a task cancelled before its first step never runs its
        # finally, and the shutdown flush would be skipped
        await asyncio.sleep(0)

    async def ticker(self) -> None:
        """When no update arrives the ledger clock stands still; the wall clock takes over. The
        ticker also writes changes the throttle held back, and flushes them when it is cancelled
        at shutdown."""
        try:
            while True:
                await asyncio.sleep(self.check_interval)
                await self.check(max(self.now_ms, int(time.time() * 1000)), None)
        finally:
            self.save(force=True)

    # ------------------------------------------------------------------ stream
    async def on_update(self, update: CantonUpdate) -> None:
        self.seen["updates"] += 1
        if update.record_time:
            self.now_ms = max(self.now_ms, update.record_time)
        for t in update.synchronizer_times.values():
            self.now_ms = max(self.now_ms, t or 0)
        for event in update.events:
            if event.type == CantonEventType.UNASSIGNED:
                await self.on_unassigned(update, event)
            elif event.type == CantonEventType.ASSIGNED:
                await self.on_assigned(update, event)
        await self.check(self.now_ms, update)  # check() saves

    def concerns_monitored_party(self, event: CantonEvent) -> bool:
        """An unassign concerns the client when one of its parties submitted it or is a stakeholder.
        Unassigned events carry the stakeholders only as witness_parties; signatories and observers
        are checked too in case the publisher fills them. Matching stakeholders, not only the
        submitter, catches a counterparty that freezes the client's contract."""
        parties = set(event.witness_parties) | set(event.signatories) | set(event.observers)
        if event.submitter:
            parties.add(event.submitter)
        return bool(parties & self.monitored_parties)

    async def on_unassigned(self, update: CantonUpdate, event: CantonEvent) -> None:
        rid = event.reassignment_id
        if not rid or rid in self.open:
            return
        if self.monitored_parties and not self.concerns_monitored_party(event):
            return
        if update.kind == CantonUpdateKind.SNAPSHOT:
            # inherited at start: the snapshot has no record_time, the exclusivity timestamp is the
            # only ledger time we have for it; failing that, the clock starts now
            since = event.assignment_exclusivity or self.now_ms or int(time.time() * 1000)
        else:
            since = update.record_time or self.now_ms or int(time.time() * 1000)
            if self.use_exclusivity and event.assignment_exclusivity:
                since = max(since, event.assignment_exclusivity)
        self.open[rid] = dict(
            contract_id=event.contract_id,
            template_id=event.template_id,
            source=event.source,
            target=event.target,
            submitter=event.submitter,
            counter=event.reassignment_counter,
            since=since,
            offset=update.offset,
            update_id=update.update_id,
            inherited=update.kind == CantonUpdateKind.SNAPSHOT,
            alerted=False,
        )
        self.dirty = True
        self.seen["opened"] += 1
        if update.kind != CantonUpdateKind.SNAPSHOT and self.volume_limit and event.submitter:
            await self.check_volume(update, event.submitter, since)

    async def on_assigned(self, update: CantonUpdate, event: CantonEvent) -> None:
        item = self.open.pop(event.reassignment_id or "", None)
        if item is None:
            return  # an assign whose unassign we never saw: nothing was open
        self.dirty = True
        self.seen["closed"] += 1
        if not item["alerted"]:
            return  # completed inside the window: silent
        elapsed = max(0, (update.record_time or self.now_ms) - item["since"]) // 1000
        await self.emit(
            "canton_reassignment_completed",
            float(Severity.INFO),
            update,
            category="EVENT",
            desc=f"Reassignment {event.reassignment_id[:12]}… completed after {elapsed}s; the limbo alert is closed",
            contract_hash=hash_id(item["contract_id"]),
            reassignment_id=event.reassignment_id,
            source=item["source"],
            target=item["target"],
            elapsed_seconds=elapsed,
        )

    async def check(self, now_ms: int, update: Optional[CantonUpdate]) -> None:
        # snapshot the entries: emit() below awaits, and on the Kafka path the consume loop shares
        # this event loop with the ticker, so a concurrent update may add or remove open reassignments
        # mid-iteration. Iterating self.open directly would then raise "dict changed size".
        for rid, item in list(self.open.items()):
            if item["alerted"] or now_ms - item["since"] <= self.window_ms:
                continue
            item["alerted"] = True
            self.dirty = True
            self.seen["alerts"] += 1
            elapsed = (now_ms - item["since"]) // 1000
            self.logger.warning(f"reassignment limbo: {rid[:12]}… open for {elapsed}s (window {self.window_ms // 1000}s)")
            await self.emit(
                "canton_reassignment_limbo",
                self.severity,
                update or self.synthetic_update(item),
                desc=f"Contract unassigned {elapsed}s ago and not yet assigned on the target synchronizer (window {self.window_ms // 1000}s)",
                contract_hash=hash_id(item["contract_id"]),
                template_id=item["template_id"],
                reassignment_id=rid,
                source=item["source"],
                target=item["target"],
                submitter_hash=hash_id(item["submitter"]),
                reassignment_counter=item["counter"],
                unassigned_at=item["since"],
                elapsed_seconds=elapsed,
                window_seconds=self.window_ms // 1000,
                inherited_from_snapshot=item["inherited"],
                unassign_offset=item["offset"],
                unassign_update_id=item["update_id"],
            )
        self.save()

    def synthetic_update(self, item: Dict) -> CantonUpdate:
        """The ticker has no update in hand: point the alert at the unassign itself, on this detector's node."""
        return CantonUpdate(kind=CantonUpdateKind.REASSIGNMENT, offset=item["offset"] or 0, update_id=item["update_id"], synchronizer_id=item["source"], record_time=self.now_ms or None, participant_id=self.participant_id)

    async def check_volume(self, update: CantonUpdate, submitter: str, at_ms: int) -> None:
        hour = 3_600_000
        self.recent_unassigns.append((at_ms, submitter))
        while self.recent_unassigns and self.recent_unassigns[0][0] < at_ms - hour:
            self.recent_unassigns.popleft()
        count = sum(1 for t, s in self.recent_unassigns if s == submitter)
        if count > self.volume_limit:
            self.seen["volume_alerts"] += 1
            await self.emit(
                "canton_reassignment_volume",
                self.severity_volume,
                update,
                desc=f"{count} unassigns by one party in the last hour (limit {self.volume_limit})",
                party_hash=hash_id(submitter),
                count_in_window=count,
                threshold=self.volume_limit,
                window_seconds=3600,
            )
