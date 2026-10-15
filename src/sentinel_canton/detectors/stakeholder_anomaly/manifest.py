from typing import Dict, List

from pydantic import Field
from sentinel.manifest import MetadataModel, NetworkTag, Status, Tag

from sentinel_canton.schema import CantonSchema


class Schema(CantonSchema):
    """
    Configuration of the Canton stakeholder/observer anomaly detector.
    Until it is registered as a monitoring condition, the same fields are read
    from the sentry's `parameters` in the profile.
    """

    monitored_parties: List[str] = Field(
        default_factory=list,
        title="Monitored parties",
        description="The client's parties, as hosted on the participant the adapter reads from",
    )
    max_stakeholders_default: int = Field(
        default=10,
        title="Default stakeholder limit",
        description="Alert when a new contract lists more signatories plus observers than this",
    )
    max_stakeholders_by_template: Dict[str, int] = Field(
        default_factory=dict,
        title="Limit per template",
        description="Overrides by template: full id (package:Module:Entity) or Module:Entity",
    )


metadata = MetadataModel(
    name="Canton-StakeholderAnomaly",
    title="Canton Stakeholder Anomaly",
    author="hacken",
    version="0.2.1",
    status=Status.ACTIVE,
    icon="https://directus.messari.io/assets/ff4a67ec-8a37-436b-97d6-4a9e26ad33e3",
    description=(
        "Alerts when a contract the client is party to is created with more signatories plus "
        "observers than the template's policy allows. Every listed party must store the contract, "
        "so a bloated stakeholder list is either database bloat or Sybil-style spam."
    ),
    tags=[Tag.SECURITY],
    network_tags=[NetworkTag.CANTON],
    faq=[
        {
            "name": "Which contracts are checked?",
            "value": "Every created contract the monitored parties see, from the stream and from the bootstrap snapshot.",
        },
        {
            "name": "How is the limit chosen?",
            "value": "max_stakeholders_by_template first (full template id or Module:Entity), then max_stakeholders_default.",
        },
        {"name": "Which node does it watch?", "value": "Only the node set in Participant (node). Leave it empty when a single node feeds the topic; set it when several nodes publish to the same topic, so the alerts never come from another company's node."},
    ],
)
