from typing import Dict, List

from pydantic import Field
from sentinel.manifest import MetadataModel, NetworkTag, Status, Tag

from sentinel_canton.schema import CantonSchema


class Schema(CantonSchema):
    """Configuration of the Canton topology drift detector."""

    monitored_parties: List[str] = Field(default_factory=list, title="Monitored parties", description="The client's parties; every other party is a counterparty")
    known_counterparties: Dict[str, str] = Field(default_factory=dict, title="Known counterparties", description="Party id to label; optional, the label is echoed in alerts")
    alert_on_first_contact: bool = Field(default=False, title="Alert on first contact", description="Raise an ALERT, not just an EVENT, when a never-seen counterparty appears")
    state_path: str = Field(default="", title="State file", description="Where the hosting baseline is persisted between restarts (empty = memory only)")


metadata = MetadataModel(
    name="Canton Topology Drift",
    title="Canton Topology Drift",
    version="0.1.2",
    status=Status.ACTIVE,
    icon="https://cdn-icons-png.flaticon.com/512/11494/11494910.png",
    description=(
        "Alerts when a counterparty changes the participant that hosts it mid-relationship, or is "
        "newly hosted on a participant never seen for it. A party changing hosting is the Canton "
        "equivalent of a supplier changing bank and address at once."
    ),
    tags=[Tag.SECURITY],
    network_tags=[NetworkTag.CANTON],
    faq=[
        {"name": "What does it see?", "value": "Every party hosted on the synchronizers the client's participant is connected to; the topology is public by design."},
        {"name": "First contact?", "value": "A never-seen party records its baseline and emits an informational event; drift alerts start from the next change."},
    ],
)
