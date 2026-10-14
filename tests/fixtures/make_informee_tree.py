"""Hand-built fixtures for the canton-contracts `informee_tree_anomaly` package.

Mirrors InformeeTreeAnomaly/Test/Trigger.daml transaction by transaction (one `debug "TX ..."` line
each), in the CantonUpdate shape trunk-canton emits (see 373_informee_anomaly.jsonl). The extractor is
assumed to read as BOTH clerk and guest, so the Ledger API does not filter witness_parties.

    python tests/fixtures/make_informee_tree.py     # rewrites the six informee_tree_*.jsonl
"""
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
PKG = "7e1d2c3b4a5968778695a4b3c2d1e0f0e1d2c3b4a5968778695a4b3c2d1e0f0"
CLERK = "clerk::1220" + "c1" * 32
GUEST = "guest::1220" + "9e" * 32
JANUS = "janus::1220" + "a1" * 32
BANK = "bank::1220" + "b2" * 32
REGULATOR = "regulator::1220" + "e3" * 32
SHOP = "shop::1220" + "f4" * 32
MIRO = "miro::1220" + "d5" * 32
SYNC = "sync1::1220" + "dd" * 32


def tid(entity):
    return f"{PKG}:InformeeTreeAnomaly.Desk:{entity}"


def cid(n):
    return f"00{n:062x}"


def created(node, contract, entity, signatories, observers, witnesses):
    return {"type": "created", "node_id": node, "contract_id": cid(contract), "template_id": tid(entity),
            "package_name": "informee-tree-anomaly", "signatories": signatories, "observers": observers,
            "witness_parties": witnesses, "acting_parties": []}


def exercised(node, last, contract, entity, choice, acting, witnesses, consuming=True):
    return {"type": "exercised", "node_id": node, "last_descendant_node_id": last, "contract_id": cid(contract),
            "template_id": tid(entity), "package_name": "informee-tree-anomaly", "signatories": [], "observers": [],
            "witness_parties": witnesses, "choice": choice, "acting_parties": acting, "consuming": consuming}


def tx(offset, *events):
    for e in events:
        e["offset"] = offset
    return {"kind": "transaction", "offset": offset, "update_id": f"1220{offset:060x}", "synchronizer_id": SYNC,
            "record_time": 1788876000000 + offset * 1000, "events": list(events), "synchronizer_times": {}}


# triggerInformeeTreeAnomaly
LEAK = [
    # TX create LeakingDesk | witnesses are clerk and guest, not a hit
    tx(10, created(0, 1, "LeakingDesk", [CLERK], [GUEST], [CLERK, GUEST])),
    # TX FileLeaking | HIT: guest witnesses the Slip and is not a stakeholder
    tx(11, exercised(0, 1, 1, "LeakingDesk", "FileLeaking", [CLERK], [CLERK, GUEST]),
           created(1, 2, "Slip", [CLERK], [], [CLERK, GUEST])),
    # TX Rewrite leaking Slip | witnesses are clerk only, not a hit
    tx(12, exercised(0, 1, 2, "Slip", "Rewrite", [CLERK], [CLERK]),
           created(1, 3, "Slip", [CLERK], [], [CLERK])),
]

# quietTraffic
QUIET = [
    # TX create PlainDesk | guest is payload only
    tx(20, created(0, 20, "PlainDesk", [CLERK], [], [CLERK])),
    # TX FilePlain | Slip witnesses match stakeholders
    tx(21, exercised(0, 1, 20, "PlainDesk", "FilePlain", [CLERK], [CLERK]),
           created(1, 21, "Slip", [CLERK], [], [CLERK])),
    # TX Rewrite plain Slip | holder only
    tx(22, exercised(0, 1, 21, "Slip", "Rewrite", [CLERK], [CLERK]),
           created(1, 22, "Slip", [CLERK], [], [CLERK])),
    # TX create CopiedDesk | guest observes the desk
    tx(23, created(0, 23, "CopiedDesk", [CLERK], [GUEST], [CLERK, GUEST])),
    # TX FileCopied | guest is also an observer of the Slip
    tx(24, exercised(0, 1, 23, "CopiedDesk", "FileCopied", [CLERK], [CLERK, GUEST]),
           created(1, 24, "Slip", [CLERK], [GUEST], [CLERK, GUEST])),
    # TX Rewrite copied Slip | holder and guest
    tx(25, exercised(0, 1, 24, "Slip", "Rewrite", [CLERK], [CLERK, GUEST]),
           created(1, 25, "Slip", [CLERK], [GUEST], [CLERK, GUEST])),
    # TX create JointDesk | clerk and guest sign
    tx(26, created(0, 26, "JointDesk", [CLERK, GUEST], [], [CLERK, GUEST])),
    # TX FileJoint | guest cosigns the Slip
    tx(27, exercised(0, 1, 26, "JointDesk", "FileJoint", [CLERK, GUEST], [CLERK, GUEST]),
           created(1, 27, "Slip", [CLERK, GUEST], [], [CLERK, GUEST])),
    # TX Rewrite joint Slip | holder and guest
    tx(28, exercised(0, 1, 27, "Slip", "Rewrite", [CLERK, GUEST], [CLERK, GUEST]),
           created(1, 28, "Slip", [CLERK, GUEST], [], [CLERK, GUEST])),
]

# Not in canton-contracts yet (needs a desk choice that exercises Rewrite on an existing Slip):
# a CHILD EXERCISE witnessed by a party that is neither its actor nor a stakeholder of its target.
CHILD_EXERCISE = [
    # the Slip, holder clerk only
    tx(30, created(0, 30, "Slip", [CLERK], [], [CLERK])),
    # the desk guest observes
    tx(31, created(0, 31, "LeakingDesk", [CLERK], [GUEST], [CLERK, GUEST])),
    # a desk choice rewrites the Slip: the child exercise inherits guest as a witness -> HIT
    tx(32, exercised(0, 2, 31, "LeakingDesk", "RewriteVia", [CLERK], [CLERK, GUEST], consuming=False),
           exercised(1, 2, 30, "Slip", "Rewrite", [CLERK], [CLERK, GUEST]),
           created(2, 32, "Slip", [CLERK], [], [CLERK, GUEST])),
    # same shape on a Slip never seen in the stream: target stakeholders unknown, child exercise skipped
    tx(33, exercised(0, 1, 31, "LeakingDesk", "RewriteVia", [CLERK], [CLERK, GUEST], consuming=False),
           exercised(1, 1, 99, "Slip", "Rewrite", [CLERK], [CLERK, GUEST])),
]

# Divulgence: a regulator that observes the holding is expected to see everything that happens to it
# (normal Canton behaviour); a counterparty with an access contract that reaches further is not.
DIVULGENCE = [
    # TX Janus holds USDCx: the bank signs, Janus and the regulator observe
    tx(40, created(0, 40, "Holding", [BANK], [JANUS, REGULATOR], [BANK, JANUS, REGULATOR])),
    # TX Janus pays for groceries: the consuming Transfer creates the shop's Holding, which the regulator
    # witnesses without being its observer (divulgence, not a leak)
    tx(41, exercised(0, 1, 40, "Holding", "Transfer", [JANUS], [BANK, JANUS, REGULATOR]),
           created(1, 41, "Holding", [BANK], [SHOP], [BANK, JANUS, REGULATOR, SHOP])),
]

# Janus borrows USDCx from the bank (the regulator sees it, which is fine), then agrees a test ride with
# Miro. The access contract Miro is given reaches Janus's holding and loan: a wrongly scoped contract.
TEST_RIDE = [
    tx(50, created(0, 50, "Loan", [BANK, JANUS], [REGULATOR], [BANK, JANUS, REGULATOR])),
    tx(51, created(0, 51, "Holding", [BANK], [JANUS, REGULATOR], [BANK, JANUS, REGULATOR])),
    tx(52, created(0, 52, "TestRideGrant", [JANUS], [MIRO], [JANUS, MIRO])),
    # Miro exercises TestRide; the grant lets it act for Janus on the holding and the loan -> HIT x2
    tx(53, exercised(0, 2, 52, "TestRideGrant", "TestRide", [MIRO], [JANUS, MIRO], consuming=False),
           exercised(1, 1, 51, "Holding", "Inspect", [JANUS], [BANK, JANUS, REGULATOR, MIRO], consuming=False),
           exercised(2, 2, 50, "Loan", "Disclose", [JANUS], [BANK, JANUS, REGULATOR, MIRO], consuming=False)),
]

# The same test ride, designed well: Miro lends the car, the grant only touches Miro's own contracts, so
# Janus's loan and holding (which the regulator and bank may see) never enter the subtree. No alert.
TEST_RIDE_SCOPED = [
    tx(60, created(0, 60, "Loan", [BANK, JANUS], [REGULATOR], [BANK, JANUS, REGULATOR])),
    tx(61, created(0, 61, "Holding", [BANK], [JANUS, REGULATOR], [BANK, JANUS, REGULATOR])),
    tx(62, created(0, 62, "Car", [MIRO], [], [MIRO])),
    tx(63, created(0, 63, "TestRideGrant", [MIRO], [JANUS], [MIRO, JANUS])),
    # Janus exercises TestRide; it only creates a log of the ride signed by both, and never touches the car
    tx(64, exercised(0, 1, 63, "TestRideGrant", "TestRide", [JANUS], [MIRO, JANUS], consuming=False),
           created(1, 64, "RideLog", [MIRO, JANUS], [], [MIRO, JANUS])),
]


if __name__ == "__main__":
    for name, updates in (("leak", LEAK), ("quiet", QUIET), ("child_exercise", CHILD_EXERCISE), ("divulgence", DIVULGENCE), ("test_ride", TEST_RIDE), ("test_ride_scoped", TEST_RIDE_SCOPED)):
        path = HERE / f"informee_tree_{name}.jsonl"
        path.write_text("".join(json.dumps(u, separators=(",", ":")) + "\n" for u in updates))
        print(f"wrote {path.name}: {len(updates)} updates")
