import json
import pathlib
from collections import Counter, OrderedDict
from typing import Dict, List, Optional, Set

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, CantonUpdate
from sentinel.manifest import Severity
from sentinel_canton.runtime import CantonUpdateDetector, hash_id, severity_or

from sentinel_canton.detectors.informee_anomaly import manifest

KNOWN_CONTRACTS = 100_000  # stakeholders remembered per contract, to judge child exercises on them
LEAK_ACTION = (
    "A party that is not a stakeholder of this contract can see it: reject or revoke the access "
    "contract or choice that exposed it, and tell the exposed parties"
)


def short_template(template_id: Optional[str]) -> Optional[str]:
    """Module:Entity, without the package id."""
    return template_id.split(":", 1)[1] if template_id and ":" in template_id else template_id


def parties(n: int, adjective: str) -> str:
    return f"{n} {adjective} {'party' if n == 1 else 'parties'}"


def reason_of(unexpected: List[str], missing: List[str]) -> str:
    return "+".join(r for r, found in (("unexpected", unexpected), ("missing", missing)) if found)


def describe(event: CantonEvent, parent_choice: Optional[str], unexpected: List[str], missing: List[str]) -> str:
    template = short_template(event.template_id)
    if event.type == CantonEventType.EXERCISED:
        subject = f"Exercise of {event.choice} on template {template}"
    else:
        subject = f"Contract of template {template}"
    origin = f"under choice {parent_choice}" if parent_choice else "at top level"
    findings = []
    if unexpected:
        findings.append(f"reached {parties(len(unexpected), 'unexpected')}")
    if missing:
        findings.append(f"has {parties(len(missing), 'expected')} missing")
    return f"{subject} {origin} " + "; ".join(findings)


class InformeeAnomalyDetector(CantonUpdateDetector):
    """
    Informee-tree anomaly signals within authorized local views. Two rules run side by side, each
    alert tagged with the one that fired:

    - structural (default on, no configuration): a sub-action under a choice whose witness_parties
      include a party that is not its own informee. A child create's informees are its signatories
      and observers; a child exercise's are its acting parties plus the stakeholders of its target,
      known from the create seen earlier in the stream (an unknown target is skipped). The acting
      parties of every enclosing exercise are also allowed: a controller sees what its choice does
      (373: the client transfers an Iou and witnesses the new one, not a leak). This is the
      canton-contracts LeakingDesk case: guest observes the desk and so witnesses the Slip the
      clerk files, without being a stakeholder of it. trusted_parties (a regulator, an auditor) never
      raise it, and strict_actors stops exempting the acting parties, which catches a counterparty
      whose access contract reaches contracts it is not a stakeholder of. The alert then carries
      exposed_party_hash (the stakeholders whose contract was seen) and a recommended_action.
    - allowlist: the client configures, per template, the parties allowed to be stakeholders or
      witnesses; a contract of that template created with any other party, at top level or as a
      child of a choice, alerts. The client's monitored parties are always allowed.
    - learned (learn_from_first_n > 0): a template that is not configured is learned from its first
      N contracts, then checked like the allowlist. A party is expected when it appeared in at least
      learn_threshold of those N contracts (default 0.5, the average presence). Learning contracts
      never alert; learned sets and in-progress counts persist to state_path.

    Missing parties alert only when alert_on_missing is set. The Ledger API filters witness_parties
    to the parties the reader can read as, so the structural rule only reports a missing stakeholder
    that is one of the monitored parties. The reason is "unexpected+missing" when one sub-action has
    both.
    """

    name = "CantonInformeeAnomaly"
    description = "Alerts when a sub-action reaches a party that is not its own informee or not expected for its template"

    def configure(self, parameters: Dict) -> None:
        parameters = parameters or {}
        self.monitored_parties: Set[str] = set(parameters.get("monitored_parties") or [])
        self.expected: Dict[str, Set[str]] = {k: set(v) for k, v in (parameters.get("expected_parties_by_template") or {}).items()}
        self.trusted: Set[str] = set(parameters.get("trusted_parties") or [])
        self.strict_actors = bool(parameters.get("strict_actors", False))
        self.alert_on_missing = bool(parameters.get("alert_on_missing", False))
        self.structural_check = bool(parameters.get("structural_check", True))
        self.severity = severity_or(parameters.get("severity"), Severity.HIGH)
        # A party is expected when it appears in at least learn_threshold (a share, default 0.5 =
        # average presence) of the first N contracts.
        self.learn_n = int(parameters.get("learn_from_first_n", 0) or 0)
        self.learn_threshold = float(parameters.get("learn_threshold", 0.5))
        self.state_path = pathlib.Path(parameters["state_path"]) if parameters.get("state_path") else None
        self.learning: Dict[str, Dict] = {}  # template -> {"n": contracts seen, "counts": {party: contracts}}
        self.learned: Dict[str, Set[str]] = {}
        self.known: "OrderedDict[str, Set[str]]" = OrderedDict()
        self.seen = Counter()
        self.policy_name, self.policy_version = manifest.metadata.name, manifest.metadata.version
        self.load()

    # ------------------------------------------------------------------ persistence
    def load(self) -> None:
        if self.state_path and self.state_path.exists():
            raw = json.loads(self.state_path.read_text())
            self.learning = raw.get("learning", {})
            self.learned = {k: set(v) for k, v in raw.get("learned", {}).items()}

    def save(self) -> None:
        if self.state_path:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            learned = {k: sorted(v) for k, v in self.learned.items()}
            self.state_path.write_text(json.dumps({"learning": self.learning, "learned": learned}, indent=1))

    # ------------------------------------------------------------------ rules
    def expected_for(self, template_id: Optional[str]) -> Optional[Set[str]]:
        if not template_id:
            return None
        if template_id in self.expected:
            return self.expected[template_id]
        return self.expected.get(short_template(template_id))

    def remember(self, event: CantonEvent) -> None:
        if not event.contract_id:
            return
        self.known[event.contract_id] = set(event.signatories) | set(event.observers)
        self.known.move_to_end(event.contract_id)
        if len(self.known) > KNOWN_CONTRACTS:
            self.known.popitem(last=False)

    def own_informees(self, event: CantonEvent) -> Optional[Set[str]]:
        """The sub-action's own informees, or None when they cannot be known from the stream."""
        if event.type == CantonEventType.CREATED:
            return set(event.signatories) | set(event.observers)
        if event.type == CantonEventType.EXERCISED:
            target = self.known.get(event.contract_id)
            return None if target is None else target | set(event.acting_parties)
        return None

    async def on_update(self, update: CantonUpdate) -> None:
        self.seen["updates"] += 1
        events = update.events
        for i, event in enumerate(events):
            if event.type == CantonEventType.CREATED:
                self.remember(event)
            parent = self.parent_exercise(events, i)
            if event.type == CantonEventType.CREATED:
                await self.check_allowlist(update, event, parent)
            if self.structural_check and parent is not None:
                await self.check_structural(update, event, parent, self.ancestor_actors(events, i))

    async def check_allowlist(self, update: CantonUpdate, event: CantonEvent, parent: Optional[CantonEvent]) -> None:
        present = set(event.signatories) | set(event.observers) | set(event.witness_parties)
        present -= self.monitored_parties | self.trusted
        rule, expected = "allowlist", self.expected_for(event.template_id)
        if expected is None:
            rule, expected = "learned", self.learn(event.template_id, present)
        if expected is None:
            return  # template not under this invariant, or still learning
        self.seen["checked"] += 1
        unexpected = sorted(present - expected)
        missing = sorted(expected - present - self.monitored_parties) if self.alert_on_missing else []
        if unexpected or missing:
            await self.alert(update, event, parent, unexpected, missing, rule=rule)

    def learn(self, template_id: Optional[str], present: Set[str]) -> Optional[Set[str]]:
        """The learned expected set, or None while the template's first N contracts are still counted."""
        if not self.learn_n or not template_id:
            return None
        template = short_template(template_id)
        if template in self.learned:
            return self.learned[template]
        sample = self.learning.setdefault(template, {"n": 0, "counts": {}})
        sample["n"] += 1
        for p in present:
            sample["counts"][p] = sample["counts"].get(p, 0) + 1
        if sample["n"] >= self.learn_n:
            self.learned[template] = {p for p, c in sample["counts"].items() if c / sample["n"] >= self.learn_threshold}
            del self.learning[template]
            self.logger.info(f"informee anomaly: learned {len(self.learned[template])} expected parties for {template}")
        self.save()
        return None

    async def check_structural(self, update: CantonUpdate, event: CantonEvent, parent: CantonEvent, actors: Set[str]) -> None:
        own = self.own_informees(event)
        if own is None:
            return
        self.seen["structural_checked"] += 1
        witnesses = set(event.witness_parties)
        unexpected = sorted(witnesses - own - (set() if self.strict_actors else actors) - self.trusted)
        missing = sorted((own & self.monitored_parties) - witnesses) if self.alert_on_missing else []
        if unexpected or missing:
            await self.alert(update, event, parent, unexpected, missing, rule="structural", exposed=sorted(own) if unexpected else None)

    @staticmethod
    def enclosing(events: List[CantonEvent], index: int) -> List[CantonEvent]:
        """The exercised events whose subtree (node_id .. last_descendant_node_id) holds this event, outermost first."""
        child = events[index]
        if child.node_id is None:
            return []
        return [
            ev for ev in events[:index]
            if ev.type == CantonEventType.EXERCISED and ev.node_id is not None and ev.last_descendant_node_id is not None
            and ev.node_id < child.node_id <= ev.last_descendant_node_id
        ]

    @classmethod
    def ancestor_actors(cls, events: List[CantonEvent], index: int) -> Set[str]:
        return {p for ev in cls.enclosing(events, index) for p in ev.acting_parties}

    @classmethod
    def parent_exercise(cls, events: List[CantonEvent], index: int) -> Optional[CantonEvent]:
        """The innermost exercised event enclosing this event."""
        chain = cls.enclosing(events, index)
        return chain[-1] if chain else None

    async def alert(self, update: CantonUpdate, event: CantonEvent, parent: Optional[CantonEvent], unexpected: List[str], missing: List[str], rule: str, exposed: Optional[List[str]] = None) -> None:
        self.seen["alerts"] += 1
        choice = parent.choice if parent else None
        reason = reason_of(unexpected, missing)
        self.logger.warning(f"informee anomaly ({rule}): {short_template(event.template_id)} via {choice or 'top level'}: {reason} at offset {update.offset}")
        await self.emit(
            "canton_informee_anomaly",
            self.severity,
            update,
            desc=describe(event, choice, unexpected, missing),
            contract_hash=hash_id(event.contract_id),
            template_id=event.template_id,
            package_name=event.package_name,
            choice=choice,
            acting_parties_hash=[hash_id(p) for p in (parent.acting_parties if parent else [])],
            unexpected_party_hash=[hash_id(p) for p in unexpected],
            missing_party_hash=[hash_id(p) for p in missing],
            reason=reason,
            node_id=event.node_id,
            rule=rule,
            exposed_party_hash=[hash_id(party) for party in exposed] if exposed else None,
            recommended_action=LEAK_ACTION if unexpected else None,
        )
