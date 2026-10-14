from typing import Dict, List

from pydantic import Field
from sentinel.manifest import BaseSchema, MetadataModel, NetworkTag, Severity, Status, Tag


class Schema(BaseSchema):
    """Configuration of the Canton key and capability drift detector."""

    monitored_parties: List[str] = Field(default_factory=list, title="Monitored parties", description="The client's parties; every other party is a counterparty")
    known_counterparties: Dict[str, str] = Field(default_factory=dict, title="Known counterparties", description="Party id to label; optional")
    admin_api_url: str = Field(default="", title="Admin API", description="Participant Admin API (host:port, gRPC); enables threshold monitoring (empty = permission changes only)")
    admin_api_secure: bool = Field(default=False, title="Admin API over TLS", description="Open the Admin API channel with TLS (system roots); implied by a client certificate")
    admin_api_client_cert: str = Field(default="", title="Admin API client certificate", description="PEM file presented to the Admin API (mTLS); empty for a participant that does not ask for one")
    admin_api_client_key: str = Field(default="", title="Admin API client key", description="PEM private key of the client certificate; set together with it")
    admin_api_poll_seconds: int = Field(default=60, title="Admin API poll (s)", description="How often every party's confirmation threshold is re-read from the Admin API")
    alert_on_permission_upgrade: bool = Field(default=True, title="Alert on permission upgrade", description="Observation to Confirmation or Submission on an existing host")
    severity_threshold: float = Field(default=Severity.CRITICAL, title="Severity (threshold)", description="Severity when the confirmation threshold drops")
    state_path: str = Field(default="", title="State file", description="Where the permission baseline is persisted between restarts (empty = memory only)")


metadata = MetadataModel(
    name="Canton Capability Drift",
    title="Canton Capability Drift",
    version="0.1.3",
    status=Status.ACTIVE,
    icon="https://cdn-icons-png.flaticon.com/512/11494/11494910.png",
    description=(
        "Alerts when a counterparty becomes easier to hijack: a hosting participant gains "
        "Confirmation or Submission permission it did not have, or (with Admin API access) the "
        "party's confirmation threshold drops."
    ),
    tags=[Tag.SECURITY],
    network_tags=[NetworkTag.CANTON],
    faq=[
        {"name": "Why two modes?", "value": "Permission changes arrive on the Ledger API; the confirmation threshold does not, it is only readable through the participant's Admin API."},
    ],
)
