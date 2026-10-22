from typing import List

from pydantic import Field
from sentinel.manifest import MetadataModel, NetworkTag, Severity, Status, Tag

from sentinel_canton.schema import CantonSchema


class Schema(CantonSchema):
    """Configuration of the Canton reassignment limbo detector."""

    monitored_parties: List[str] = Field(default_factory=list, title="Monitored parties", description="The client's parties: only unassigns they submitted or are a stakeholder of are tracked (empty = all)")
    pending_window_seconds: int = Field(default=900, title="Pending window (s)", description="An unassign with no assign after this long is a limbo alert")
    use_assignment_exclusivity: bool = Field(default=True, title="Start at exclusivity", description="Start the window at assignment_exclusivity when it is later than the unassign itself")
    max_reassignments_per_party_per_hour: int = Field(default=0, title="Volume limit", description="Unassigns by one submitter per hour above this is a volume alert (0 disables)")
    severity_volume: float = Field(default=Severity.HIGH, title="Severity (volume)", description="Severity of a volume alert")
    check_interval_seconds: int = Field(default=30, title="Check interval (s)", description="How often open reassignments are checked against the wall clock when no update arrives")
    state_path: str = Field(default="", title="State file", description="Where open reassignments are persisted between restarts (empty = memory only)")
    save_interval_seconds: float = Field(default=5, title="Save interval (s)", description="The state file is written at most this often, and only when the open set changed (0 = on every change)")


metadata = MetadataModel(
    name="Canton-ReassignmentLimbo",
    title="Canton Reassignment Limbo",
    author="hacken",
    version="0.2.2",
    status=Status.ACTIVE,
    icon="https://directus.messari.io/assets/ff4a67ec-8a37-436b-97d6-4a9e26ad33e3",
    description=(
        "Alerts when a contract unassigned from one synchronizer is not assigned on the target "
        "within the window: between the two steps it is usable on neither side, and a stalled "
        "reassignment freezes the asset. Also alerts on abnormal reassignment volume by one party."
    ),
    tags=[Tag.SECURITY, Tag.FINANCIAL],
    network_tags=[NetworkTag.CANTON],
    faq=[
        {"name": "Which clock?", "value": "Ledger time: the latest record_time seen, including checkpoints; the wall clock only fills in when no update arrives."},
        {"name": "After a restart?", "value": "Open reassignments come back from the state file, and the bootstrap snapshot lists pending ones too."},
        {"name": "How much is lost on a crash?", "value": "Up to save_interval_seconds of open/close changes on a hard kill (SIGKILL, OOM, node loss); a clean stop (SIGTERM, SIGINT) writes them first, and on a quiet stream a held-back change is written when the interval ends. save_interval_seconds=0 loses nothing, at the cost of a write per change. A lost close can raise a false limbo alert after the restart; a lost open comes back only through the bootstrap snapshot."},
        {"name": "Which node does it watch?", "value": "Only the node set in Participant (node). Leave it empty when a single node feeds the topic; set it when several nodes publish to the same topic, so the alerts never come from another company's node."},
    ],
)
