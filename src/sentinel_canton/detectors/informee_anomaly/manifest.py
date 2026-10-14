from typing import Dict, List

from pydantic import Field
from sentinel.manifest import MetadataModel, NetworkTag, Status, Tag

from sentinel_canton.schema import CantonSchema


class Schema(CantonSchema):
    """
    Configuration of the Canton informee-tree anomaly detector.
    """

    monitored_parties: List[str] = Field(default_factory=list, title="Monitored parties", description="The client's parties")
    expected_parties_by_template: Dict[str, List[str]] = Field(default_factory=dict, title="Expected parties per template", description="Template (full id or Module:Entity) to the parties allowed as observers or witnesses")
    learn_from_first_n: int = Field(default=0, ge=0, title="Learn from first N", description="When a template is not configured, learn its expected set from its first N contracts, which never alert (0 = do not learn)")
    learn_threshold: float = Field(default=0.5, gt=0, le=1, title="Learning threshold", description="Share of the first N contracts a party must appear in to be learned as expected (0.5 = average presence)")
    alert_on_missing: bool = Field(default=False, title="Alert on missing party", description="Also alert when an expected party is absent from a sub-action")
    structural_check: bool = Field(default=True, title="Structural check", description="Alert when a sub-action under a choice is witnessed by a party that is not its own informee (needs no configuration)")
    trusted_parties: List[str] = Field(default_factory=list, title="Trusted parties", description="Parties, such as a regulator or auditor, that may witness any sub-action without raising a structural alert")
    strict_actors: bool = Field(default=False, title="Strict actors", description="Do not exempt the acting parties of the enclosing choices from the structural check, so a controller that sees contracts it is not a stakeholder of alerts (off by default: a controller normally sees what its own choice does)")


metadata = MetadataModel(
    name="Canton Informee Anomaly",
    title="Canton Informee Anomaly",
    version="0.2.1",
    status=Status.ACTIVE,
    icon="https://cdn-icons-png.flaticon.com/512/11494/11494910.png",
    description=(
        "Alerts when, within the client's own view, a sub-action under a choice is witnessed by a party "
        "that is not its own informee (no configuration), or a party appears on a template where it is "
        "not expected (configured or learned from the first N contracts), or an expected party is missing. "
        "Information reaching someone it should not, or a workflow not built as the template promised."
    ),
    tags=[Tag.SECURITY, Tag.COMPLIANCE],
    network_tags=[NetworkTag.CANTON],
    faq=[
        {"name": "What can it see?", "value": "Only the sub-actions, on the node it watches, where one of the client's parties is an informee; that is Canton's sub-transaction privacy."},
        {"name": "Which node does it watch?", "value": "Only the node set in Participant (node). Leave it empty when a single node feeds the topic; set it when several nodes publish to the same topic, so the alerts never come from another company's node."},
    ],
)
