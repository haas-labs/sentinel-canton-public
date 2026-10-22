"""
Hosting state shared by the topology detectors (369 topology drift, 370 capability drift).

Canton's topology maps a party to the participants that host it, per synchronizer, each with a
permission (Observation < Confirmation < Submission). The Ledger API streams every change as a
ParticipantAuthorization* event, for every party on the synchronizer, so the client's participant
sees its counterparties' hosting move. This state is the baseline those changes are judged against.

Persistence: with `path` set, the whole state is written as JSON after every change, so a
restarted sentry does not re-alert on what it already knew.
"""

import json
import pathlib
from typing import Dict, List, Optional, Tuple

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, ParticipantPermission

RANK = {ParticipantPermission.OBSERVATION: 0, ParticipantPermission.CONFIRMATION: 1, ParticipantPermission.SUBMISSION: 2}


def rank(permission: Optional[ParticipantPermission]) -> int:
    return RANK.get(permission, -1)


class TopologyState:
    def __init__(self, path: Optional[str] = None) -> None:
        self.path = pathlib.Path(path) if path else None
        # (party, synchronizer) -> participant -> permission
        self.hosts: Dict[Tuple[str, str], Dict[str, str]] = {}
        # (party, synchronizer) -> confirmation threshold, as last read from the Admin API
        self.thresholds: Dict[Tuple[str, str], int] = {}
        self.load()

    # ---------------------------------------------------------------- persistence
    def load(self) -> None:
        if self.path and self.path.exists():
            raw = json.loads(self.path.read_text())
            if "hosts" not in raw:  # the first format: hosts only, at the top level
                raw = {"hosts": raw, "thresholds": {}}
            self.hosts = {tuple(k.split("|", 1)): v for k, v in raw["hosts"].items()}
            self.thresholds = {tuple(k.split("|", 1)): int(v) for k, v in raw.get("thresholds", {}).items()}

    def save(self) -> None:
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps({
                "hosts": {"|".join(k): v for k, v in self.hosts.items()},
                "thresholds": {"|".join(k): v for k, v in self.thresholds.items()},
            }, indent=1))

    # ---------------------------------------------------------------- queries
    def known(self, party: str, synchronizer: str) -> bool:
        return (party, synchronizer) in self.hosts

    def participants(self, party: str, synchronizer: str) -> Dict[str, str]:
        return dict(self.hosts.get((party, synchronizer), {}))

    def permission(self, party: str, synchronizer: str, participant: str) -> Optional[ParticipantPermission]:
        value = self.hosts.get((party, synchronizer), {}).get(participant)
        return ParticipantPermission(value) if value else None

    def synchronizers(self) -> List[str]:
        return sorted({sync for (_, sync) in list(self.hosts) + list(self.thresholds) if sync})

    def threshold(self, party: str, synchronizer: str) -> Optional[int]:
        return self.thresholds.get((party, synchronizer))

    def set_threshold(self, party: str, synchronizer: str, threshold: int) -> Optional[int]:
        """Record the threshold read from the Admin API; returns the previous one, None on first read."""
        before = self.thresholds.get((party, synchronizer))
        self.thresholds[(party, synchronizer)] = int(threshold)
        self.save()
        return before

    # ---------------------------------------------------------------- mutation
    def apply(self, event: CantonEvent, synchronizer: str) -> Dict[str, str]:
        """Apply a topology event; returns the hosting of the party BEFORE the event."""
        key = (event.party, synchronizer)
        before = dict(self.hosts.get(key, {}))
        if event.type in (CantonEventType.AUTHORIZATION_ADDED, CantonEventType.AUTHORIZATION_CHANGED, CantonEventType.AUTHORIZATION_ONBOARDING):
            self.hosts.setdefault(key, {})[event.participant] = (event.permission or ParticipantPermission.UNSPECIFIED).value
        elif event.type == CantonEventType.AUTHORIZATION_REVOKED:
            self.hosts.setdefault(key, {}).pop(event.participant, None)
        self.save()
        return before


def participant_name(participant_id: Optional[str]) -> Optional[str]:
    """The human prefix of a participant id (p1 of p1::1220…): public topology, safe to show."""
    return participant_id.split("::")[0] if participant_id else None
