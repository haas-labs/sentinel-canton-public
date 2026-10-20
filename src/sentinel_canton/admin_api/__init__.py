"""
The participant's Admin API, for the one fact the Ledger API never carries: a party's confirmation
threshold (how many of its hosting participants must confirm a transaction).

`TopologyManagerReadService.ListPartyToParticipant` on the client's participant returns the
PartyToParticipant mapping of every party on a synchronizer, threshold included: topology is
public across the synchronizer, so the client's own node knows its counterparties' thresholds. The
service is gRPC only. Rather than committing generated stubs for the whole Canton proto tree, the
message types are built at runtime from a FileDescriptorSet compiled from the Canton 3.5.16
release (`topology_admin_v30.pb`, 56 KB), and the call goes through a generic channel.

Access: this is the third tier of the README (the Admin API is exposed only by an operator that
owns the participant). Connect to it as follows:
- Your own node: enable the Admin API on the participant and pass its plaintext address as `url`
  (e.g. `localhost:<admin-port>`), with `secure=False`.
- MainNet/TestNet: use the Admin API endpoint your operator gives you, with `secure=True` (TLS
  with the system roots). If the operator requires mTLS, also pass `client_cert`/`client_key`
  (PEM files, both or neither).
"""

import pathlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sentinel.models.chains.canton.update import ParticipantPermission

DESCRIPTOR = pathlib.Path(__file__).with_name("topology_admin_v30.pb")
SERVICE = "com.digitalasset.canton.topology.admin.v30.TopologyManagerReadService"
PKG = "com.digitalasset.canton.topology.admin.v30"
PERMISSIONS = {  # Enums.ParticipantPermission of the Canton protocol
    1: ParticipantPermission.SUBMISSION,
    2: ParticipantPermission.CONFIRMATION,
    3: ParticipantPermission.OBSERVATION,
}


@dataclass
class PartyHosting:
    """One PartyToParticipant mapping as the Admin API reports it (head state of a synchronizer)."""

    party: str
    synchronizer: str
    threshold: int
    participants: Dict[str, str] = field(default_factory=dict)  # participant uid -> permission value
    valid_from: Optional[int] = None  # epoch ms

    def to_dict(self) -> Dict[str, Any]:
        return {"party": self.party, "synchronizer": self.synchronizer, "threshold": self.threshold, "participants": dict(self.participants), "valid_from": self.valid_from}

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "PartyHosting":
        return cls(party=raw["party"], synchronizer=raw["synchronizer"], threshold=int(raw["threshold"]), participants=dict(raw.get("participants") or {}), valid_from=raw.get("valid_from"))


class AdminTopologyReader:
    """
    Reads PartyToParticipant mappings from a participant's Admin API. One instance per participant;
    the channel is opened on first use and closed by `close()`.
    """

    def __init__(
        self,
        url: str,
        descriptor: pathlib.Path = DESCRIPTOR,
        secure: bool = False,
        client_cert: Optional[str] = None,
        client_key: Optional[str] = None,
        timeout: float = 30.0,  # a first call to a remote participant (e.g. over a VPN) took more than 10 s
    ) -> None:
        import grpc
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

        if bool(client_cert) != bool(client_key):
            raise ValueError("Admin API: client_cert and client_key go together (both or neither)")
        self.url, self.timeout = url, timeout
        self.secure = secure or bool(client_cert)  # a client certificate implies TLS
        self.client_cert, self.client_key = client_cert or None, client_key or None
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(pathlib.Path(descriptor).read_bytes())
        pool = descriptor_pool.DescriptorPool()
        for f in fds.file:
            pool.Add(f)
        self.Request = message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{PKG}.ListPartyToParticipantRequest"))
        self.Response = message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{PKG}.ListPartyToParticipantResponse"))
        self.op_replace = pool.FindEnumTypeByName("com.digitalasset.canton.protocol.v30.Enums.TopologyChangeOp").values_by_name["TOPOLOGY_CHANGE_OP_ADD_REPLACE"].number
        self._grpc = grpc
        self._channel = None

    def credentials(self):
        """None for a plain channel; TLS with the system roots; plus the client certificate for mTLS."""
        if not self.secure:
            return None
        if self.client_cert:
            return self._grpc.ssl_channel_credentials(
                private_key=pathlib.Path(self.client_key).read_bytes(),
                certificate_chain=pathlib.Path(self.client_cert).read_bytes(),
            )
        return self._grpc.ssl_channel_credentials()

    def channel(self):
        if self._channel is None:
            creds = self.credentials()
            self._channel = self._grpc.aio.secure_channel(self.url, creds) if creds else self._grpc.aio.insecure_channel(self.url)
        return self._channel

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def party_to_participant(self, synchronizer_id: str, party: str = "") -> List[PartyHosting]:
        """Head state of the synchronizer's PartyToParticipant mappings, optionally for one party (prefix filter)."""
        req = self.Request()
        req.base_query.store.synchronizer.id = synchronizer_id
        req.base_query.head_state.SetInParent()
        req.base_query.operation = self.op_replace
        req.filter_party = party
        call = self.channel().unary_unary(f"/{SERVICE}/ListPartyToParticipant", request_serializer=self.Request.SerializeToString, response_deserializer=self.Response.FromString)
        res = await call(req, timeout=self.timeout)
        out = []
        for r in res.results:
            valid_from = None
            if r.context.HasField("valid_from"):
                valid_from = int(r.context.valid_from.ToDatetime(tzinfo=timezone.utc).timestamp() * 1000)
            out.append(PartyHosting(
                party=r.item.party,
                synchronizer=synchronizer_id,
                threshold=int(r.item.threshold),
                participants={h.participant_uid: PERMISSIONS.get(h.permission, ParticipantPermission.UNSPECIFIED).value for h in r.item.participants},
                valid_from=valid_from,
            ))
        return out
