import asyncio
import time
from collections import Counter
from typing import Dict, List, Optional

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, CantonUpdate, CantonUpdateKind, ParticipantPermission
from sentinel.manifest import Severity
from sentinel_canton.runtime import CantonUpdateDetector, hash_id, severity_or

from sentinel_canton.admin_api import PartyHosting
from sentinel_canton.detectors.capability_drift import manifest
from sentinel_canton.topology_state import TopologyState, participant_name, rank


class CapabilityDriftDetector(CantonUpdateDetector):
    """
    The topology records, per hosting participant, what it may do for a party: Observation,
    Confirmation or Submission; and, per party, how many hosts must confirm (the threshold).
    A host gaining a stronger permission, or a lower threshold, makes the party easier to hijack:
    a joint account that needed two signatures and now needs one, plus a new proxy with power to
    move funds. Permission changes arrive on the Ledger API; the threshold only through the Admin
    API (TopologyManagerReadService.ListPartyToParticipant), so that half runs only when
    admin_api_url is configured: every party's threshold is re-read on a timer, and a party's own
    mapping is re-read right after one of its topology events. Thresholds are judged for the
    client's own parties too: a drop there is the most direct weakening of the client's posture.
    """

    name = "CantonCapabilityDrift"
    description = "Alerts when a counterparty's hosting permissions or confirmation threshold get weaker"

    def init(self) -> None:
        super().init()  # configures from the profile (builds the reader), then applies the condition in force

    def build_reader(self):
        """Build the Admin-API reader, turning its two cryptic failures into a clear message. Without
        the `[admin]` extra the reader's `import grpc` raises ModuleNotFoundError; a malformed or
        version-mismatched topology_admin_v30.pb raises protobuf DecodeError (unparseable, matched by
        class name since google.protobuf is absent without the extra) or KeyError (a message or enum
        type is absent)."""
        try:
            from sentinel_canton.admin_api import AdminTopologyReader

            return AdminTopologyReader(
                self.admin_api_url,
                secure=self.admin_api_secure,
                client_cert=self.admin_api_client_cert,
                client_key=self.admin_api_client_key,
            )
        except ModuleNotFoundError as error:
            if (error.name or "").split(".")[0] in ("grpc", "google"):
                raise SystemExit(
                    f"detector {self.name!r} needs the optional '[admin]' extra for its Admin-API "
                    f"path (missing module {error.name!r}). Install it with: pip install '.[admin]'  "
                    f"(or clear admin_api_url to run the Ledger-API half only)."
                )
            raise
        except Exception as error:
            if isinstance(error, KeyError) or type(error).__name__ == "DecodeError":
                raise SystemExit(
                    f"detector {self.name!r} failed to load its Admin-API descriptor "
                    f"(topology_admin_v30.pb): {error!r}. The shipped descriptor may be malformed or "
                    f"mismatched; reinstall the package."
                )
            raise

    async def on_init(self) -> None:
        # init() runs before the event loop exists; the poller can only be scheduled once it does
        self.reschedule_poller()

    async def on_config_change(self, record=None, config=None) -> None:
        # a live monitoring-condition change re-runs configure() (a new reader) inside the loop;
        # point the poller at it. configure() itself must not touch the poller: at init it runs
        # before the loop, and on_init() owns the first scheduling.
        await super().on_config_change(record, config)
        self.reschedule_poller()

    def reschedule_poller(self) -> None:
        """Cancel any running threshold poller and start one iff a reader exists; a no-op before the loop."""
        task = getattr(self, "poller_task", None)
        if task is not None and not task.done():
            task.cancel()
        self.poller_task = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no event loop yet (first start, inside init); on_init() will schedule it
        if self.reader is not None:
            self.poller_task = loop.create_task(self.threshold_poller(), name="capability-drift-threshold-poller")

    def configure(self, parameters: Dict) -> None:
        parameters = parameters or {}
        self.monitored_parties = set(parameters.get("monitored_parties") or [])
        self.known_counterparties = dict(parameters.get("known_counterparties") or {})
        self.admin_api_url = parameters.get("admin_api_url") or None
        self.admin_api_secure = bool(parameters.get("admin_api_secure", False))
        self.admin_api_client_cert = parameters.get("admin_api_client_cert") or None
        self.admin_api_client_key = parameters.get("admin_api_client_key") or None
        self.admin_api_poll_seconds = int(parameters.get("admin_api_poll_seconds", 60))
        self.alert_on_permission_upgrade = bool(parameters.get("alert_on_permission_upgrade", True))
        self.severity = severity_or(parameters.get("severity"), Severity.HIGH)
        self.severity_threshold = severity_or(parameters.get("severity_threshold"), Severity.CRITICAL)
        self.state = TopologyState(parameters.get("state_path") or None)
        # rebuilt on every (re)configure, so a condition change reaches the poller; tests may replace it afterwards
        self.reader = self.build_reader() if self.admin_api_url else None
        self.last_offset = 0
        self.seen = Counter()
        self.policy_name, self.policy_version = manifest.metadata.name, manifest.metadata.version

    async def on_update(self, update: CantonUpdate) -> None:
        self.seen["updates"] += 1
        self.last_offset = max(self.last_offset, update.offset or 0)
        for event in update.events:
            if not event.party or event.type not in (
                CantonEventType.AUTHORIZATION_ADDED,
                CantonEventType.AUTHORIZATION_CHANGED,
                CantonEventType.AUTHORIZATION_REVOKED,
                CantonEventType.AUTHORIZATION_ONBOARDING,
            ):
                continue
            sync = update.synchronizer_id or ""
            old = self.state.permission(event.party, sync, event.participant)
            known = self.state.known(event.party, sync)
            self.state.apply(event, sync)
            if self.reader is not None:
                await self.reread_threshold(update, event.party, sync)  # a topology event may carry a new threshold
            if event.party in self.monitored_parties or not known:
                continue  # baseline, or the client's own node
            await self.judge_permission(update, event, old)

    async def judge_permission(self, update: CantonUpdate, event: CantonEvent, old: Optional[ParticipantPermission]) -> None:
        new = event.permission
        if event.type == CantonEventType.AUTHORIZATION_REVOKED or new is None:
            return
        upgraded_existing = old is not None and rank(new) > rank(old)
        strong_new_host = old is None and rank(new) >= rank(ParticipantPermission.CONFIRMATION)
        if not (upgraded_existing and self.alert_on_permission_upgrade) and not strong_new_host:
            return
        self.seen["alerts"] += 1
        self.logger.warning(f"capability drift: {participant_name(event.participant)} {old.value if old else 'none'} -> {new.value} at offset {update.offset}")
        await self.emit(
            "canton_capability_drift",
            self.severity,
            update,
            desc=f"Hosting permission for a counterparty went from {old.value if old else 'none'} to {new.value} on {participant_name(event.participant)}",
            field="permission",
            old=old.value if old else None,
            new=new.value,
            source="ledger_api",
            party_hash=hash_id(event.party),
            party_label=self.known_counterparties.get(event.party),
            participant_hash=hash_id(event.participant),
            participant_name=participant_name(event.participant),
        )

    # ------------------------------------------------------------------ threshold (Admin API)
    async def threshold_poller(self) -> None:
        """Re-read every party's threshold on a timer; the Admin API is the only place it exists."""
        while True:
            try:
                await self.poll_thresholds()
            except Exception as err:  # the Ledger API half must not die with the Admin API
                self.logger.warning(f"capability drift: Admin API read failed: {err}")
            await asyncio.sleep(self.admin_api_poll_seconds)

    async def poll_thresholds(self) -> int:
        """One pass over every synchronizer the state knows; returns how many mappings were judged."""
        judged = 0
        for sync in self.state.synchronizers():
            for hosting in await self.reader.party_to_participant(sync):
                await self.judge_threshold(hosting)
                judged += 1
        self.seen["threshold_polls"] += 1
        return judged

    async def reread_threshold(self, update: CantonUpdate, party: str, sync: str) -> None:
        try:
            for hosting in await self.reader.party_to_participant(sync, party=party):
                await self.judge_threshold(hosting, update)
        except Exception as err:
            self.logger.warning(f"capability drift: Admin API read for a party failed: {err}")

    async def judge_threshold(self, hosting: PartyHosting, update: Optional[CantonUpdate] = None) -> None:
        """
        The first read of a party is its baseline. A lower threshold than the baseline is an alert:
        fewer of the party's hosts are now enough to confirm on its behalf.
        """
        previous = self.state.set_threshold(hosting.party, hosting.synchronizer, hosting.threshold)
        if previous is None:
            self.seen["threshold_baseline"] += 1
            return
        if hosting.threshold >= previous:
            return
        self.seen["alerts"] += 1
        self.logger.warning(f"capability drift: threshold of {hosting.party.split('::')[0]} dropped {previous} -> {hosting.threshold} on {hosting.synchronizer.split('::')[0]}")
        await self.emit(
            "canton_capability_drift",
            self.severity_threshold,
            update or self.poll_update(hosting),
            desc=f"Confirmation threshold for a party dropped from {previous} to {hosting.threshold} with {len(hosting.participants)} hosting participants",
            field="threshold",
            old=previous,
            new=hosting.threshold,
            source="admin_api",
            party_role="monitored" if hosting.party in self.monitored_parties else "counterparty",
            party_hash=hash_id(hosting.party),
            party_label=self.known_counterparties.get(hosting.party),
            hosts=sorted(participant_name(p) for p in hosting.participants),
        )

    def poll_update(self, hosting: PartyHosting) -> CantonUpdate:
        """A poll has no update in hand: point the alert at the last offset seen and the mapping's own time."""
        return CantonUpdate(
            kind=CantonUpdateKind.CHECKPOINT,
            offset=self.last_offset,
            synchronizer_id=hosting.synchronizer,
            record_time=hosting.valid_from or int(time.time() * 1000),
        )
