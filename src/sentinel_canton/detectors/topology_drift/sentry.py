from collections import Counter
from typing import Dict

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, CantonUpdate
from sentinel.manifest import Severity
from sentinel_canton.runtime import CantonUpdateDetector, hash_id, severity_or

from sentinel_canton.detectors.topology_drift import manifest
from sentinel_canton.topology_state import TopologyState, participant_name

TOPOLOGY = {
    CantonEventType.AUTHORIZATION_ADDED,
    CantonEventType.AUTHORIZATION_CHANGED,
    CantonEventType.AUTHORIZATION_REVOKED,
    CantonEventType.AUTHORIZATION_ONBOARDING,
}


class TopologyDriftDetector(CantonUpdateDetector):
    """
    A party does not act by itself: it is hosted on a participant that stores its contracts and
    signs for it, and the network keeps a public registry of which party is hosted where. This
    detector keeps that registry for every party the client's participant sees and alerts when a
    counterparty gains a hosting participant it never had, or loses one, mid-relationship.
    """

    name = "CantonTopologyDrift"
    description = "Alerts when a counterparty changes the participant that hosts it"

    def configure(self, parameters: Dict) -> None:
        parameters = parameters or {}
        self.monitored_parties = set(parameters.get("monitored_parties") or [])
        self.known_counterparties = dict(parameters.get("known_counterparties") or {})
        self.alert_on_first_contact = bool(parameters.get("alert_on_first_contact", False))
        self.severity = severity_or(parameters.get("severity"), Severity.HIGH)
        self.state = TopologyState(parameters.get("state_path") or None)
        self.seen = Counter()
        self.policy_name, self.policy_version = manifest.metadata.name, manifest.metadata.version

    async def on_update(self, update: CantonUpdate) -> None:
        self.seen["updates"] += 1
        for event in update.events:
            if event.type not in TOPOLOGY or not event.party:
                continue
            self.seen["topology_events"] += 1
            if event.party in self.monitored_parties:
                self.state.apply(event, update.synchronizer_id or "")
                continue  # the client's own hosting is the operator's business, not a counterparty signal
            await self.judge(update, event)

    async def judge(self, update: CantonUpdate, event: CantonEvent) -> None:
        sync = update.synchronizer_id or ""
        known = self.state.known(event.party, sync)
        before = self.state.apply(event, sync)
        after = self.state.participants(event.party, sync)
        common = dict(
            party_hash=hash_id(event.party),
            party_label=self.known_counterparties.get(event.party),
            participant_hash=hash_id(event.participant),
            participant_name=participant_name(event.participant),
            hosts_before=[participant_name(p) for p in before],
            hosts_after=[participant_name(p) for p in after],
            permission=event.permission.value if event.permission else None,
        )
        if not known:
            # first time this party shows up: record the baseline, do not call it drift
            self.seen["first_seen"] += 1
            await self.emit(
                "canton_counterparty_first_seen",
                self.severity if self.alert_on_first_contact else float(Severity.INFO),
                update,
                category="ALERT" if self.alert_on_first_contact else "EVENT",
                desc=f"Counterparty first seen, hosted on {participant_name(event.participant)}",
                change=event.type.value.replace("authorization_", ""),
                **common,
            )
            return
        if event.type in (CantonEventType.AUTHORIZATION_ADDED, CantonEventType.AUTHORIZATION_ONBOARDING) and event.participant not in before:
            change = "added" if event.type == CantonEventType.AUTHORIZATION_ADDED else "onboarding"
        elif event.type == CantonEventType.AUTHORIZATION_REVOKED and event.participant in before:
            change = "revoked"
        else:
            return  # permission change on an existing host: capability drift's business
        self.seen["alerts"] += 1
        self.logger.warning(f"topology drift: {change} {participant_name(event.participant)} for a counterparty at offset {update.offset}")
        await self.emit(
            "canton_topology_drift",
            self.severity,
            update,
            desc=f"Counterparty hosting changed: {change} {participant_name(event.participant)} (was {[participant_name(p) for p in before]})",
            change=change,
            **common,
        )
