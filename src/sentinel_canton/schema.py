from pydantic import Field
from sentinel.manifest import BaseSchema


class CantonSchema(BaseSchema):
    """
    The fields every Canton detector reads, whatever its rule. `participant_id` is read by the
    runtime's CantonUpdateDetector before the detector sees an update: declaring it here puts it
    on the platform form of all five detectors, so editing a condition in the UI keeps it.
    """

    participant_id: str = Field(
        default="",
        title="Participant (node)",
        description=(
            "Participant id of the node this detector watches, as the node reports it "
            "(name::fingerprint). Empty = any node on the topic."
        ),
    )
