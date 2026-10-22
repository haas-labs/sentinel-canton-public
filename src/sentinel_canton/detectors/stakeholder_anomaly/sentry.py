from collections import Counter
from typing import Dict

from sentinel.models.chains.canton.update import CantonEvent, CantonEventType, CantonUpdate, CantonUpdateKind
from sentinel.manifest import Severity
from sentinel_canton.runtime import CantonUpdateDetector, hash_id, severity_or

from sentinel_canton.detectors.stakeholder_anomaly import manifest


class StakeholderAnomalyDetector(CantonUpdateDetector):
    """
    A Daml contract lists its signatories (who authorised it) and observers (who may see it), and
    every one of those parties' participants must store it. A normal contract has two or three
    stakeholders; one with dozens is either an attempt to bloat every listed participant's
    database or Sybil-style spam from many fresh identities. This detector counts them on every
    created event the client sees and alerts above the template's limit.
    """

    name = "CantonStakeholderAnomaly"
    description = "Alerts when a created contract lists more stakeholders than the template's policy allows"

    def configure(self, parameters: Dict) -> None:
        parameters = parameters or {}
        self.monitored_parties = set(parameters.get("monitored_parties") or [])
        self.default_limit = int(parameters.get("max_stakeholders_default", 10))
        self.limits_by_template = {k: int(v) for k, v in (parameters.get("max_stakeholders_by_template") or {}).items()}
        self.severity = severity_or(parameters.get("severity"), Severity.HIGH)
        self.seen = Counter()
        self.policy_name, self.policy_version = manifest.metadata.name, manifest.metadata.version

    def limit_for(self, template_id: str) -> int:
        if template_id in self.limits_by_template:
            return self.limits_by_template[template_id]
        short = template_id.split(":", 1)[1] if ":" in template_id else template_id  # Module:Entity
        return self.limits_by_template.get(short, self.default_limit)

    async def on_update(self, update: CantonUpdate) -> None:
        self.seen["updates"] += 1
        for event in update.events:
            if event.type != CantonEventType.CREATED:
                continue
            if self.monitored_parties and not (set(event.stakeholders) & self.monitored_parties):
                continue  # only judge contracts a monitored party is a stakeholder of
            self.seen["created"] += 1
            count = len(event.stakeholders)
            limit = self.limit_for(event.template_id or "")
            if count <= limit:
                continue
            self.seen["alerts"] += 1
            await self.alert(update, event, count, limit)
        if self.seen["updates"] % 100 == 0:
            self.logger.info(dict(self.seen))

    async def alert(self, update: CantonUpdate, event: CantonEvent, count: int, limit: int) -> None:
        template = event.template_id.split(":", 1)[1] if event.template_id and ":" in event.template_id else event.template_id
        self.logger.warning(f"stakeholder anomaly: {template} with {count} stakeholders (limit {limit}) at offset {update.offset}")
        await self.emit(
            "canton_stakeholder_anomaly",
            self.severity,
            update,
            desc=f"Contract of template {template} created with {count} stakeholders; the limit is {limit}",
            contract_hash=hash_id(event.contract_id),
            template_id=event.template_id,
            package_name=event.package_name,
            signatories_count=len(event.signatories),
            observers_count=len(event.observers),
            count=count,
            threshold=limit,
            source="snapshot" if update.kind == CantonUpdateKind.SNAPSHOT else "stream",
        )
