"""The detectors on the platform's base, as a consumer that embeds them binds them: the runtime's
CantonUpdateDetector and hash_id are swapped for the SDK's (sentinel.sentry.v2.canton) before any
detector module is imported, so every detector class extends the platform base, with its
monitoring-condition machinery. Run in its own process by test_platform_base.py, because the swap
is global. Prints "ok" when every check passed. Not collected by pytest (no test_ prefix)."""
import asyncio
import importlib
import pathlib
import sys
from unittest.mock import MagicMock

import sentinel_canton.runtime as runtime
from sentinel.sentry.v2.canton import CantonUpdateDetector as PlatformDetector, hash_id as platform_hash_id

runtime.CantonUpdateDetector = PlatformDetector
runtime.hash_id = platform_hash_id

from sentinel.models.config import Configuration  # noqa: E402
from sentinel_canton.detectors import REGISTRY  # noqa: E402

TESTS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))
from wire import meta, updates  # noqa: E402


class Sink:
    def __init__(self):
        self.events = []

    async def send(self, event):
        self.events.append(event)


def condition(schema_name, config, status="ACTIVE", cid=991, updated_at=5):
    return Configuration(**{
        "id": cid, "createdAt": 1, "updatedAt": updated_at, "status": status, "name": "c", "source": "ATTACK_DETECTOR",
        "contract": {"id": 1, "createdAt": 1, "updatedAt": 1, "projectId": 1, "tenantId": 1, "chainUid": "canton", "name": "p", "address": "client::1220aa"},
        "schema": {"id": 3, "createdAt": 1, "updatedAt": 1, "status": "ACTIVE", "name": schema_name, "version": "0.2.1"},
        "config": config,
    })


def bare(cls, **parameters):
    """A detector without the SDK's channels and process machinery, configured from the profile."""
    d = cls.__new__(cls)
    d.name, d.logger, d.parameters = cls.name, MagicMock(), dict(parameters)
    d.policy_config_id = None
    d.outputs = MagicMock()
    d.outputs.events, d.outputs.webhook = Sink(), None
    d._recent_ids, d._recent_set = __import__("collections").deque(maxlen=10_000), set()
    d.profile_parameters = dict(parameters)
    d.scope_participant(d.profile_parameters)
    d.configure(dict(parameters))
    return d


def check_every_detector_extends_the_platform_base_under_its_manifest_name():
    for key, cls in REGISTRY.items():
        assert PlatformDetector in cls.__mro__, f"{key} does not extend the platform base"
        manifest = importlib.import_module(f"sentinel_canton.detectors.{key}.manifest")
        d = bare(cls, admin_api_url="") if key == "capability_drift" else bare(cls)
        assert d.policy_name == manifest.metadata.name and d.policy_name.startswith("Canton-"), key
        assert d.policy_version == manifest.metadata.version, key


def check_a_condition_under_the_manifest_name_is_applied_and_another_name_is_not():
    cls = REGISTRY["stakeholder_anomaly"]
    d = bare(cls)
    d.apply_configuration(condition("Canton Stakeholder Anomaly", {"max_stakeholders_default": 100}))
    assert d.default_limit == 10 and d.policy_config_id is None, "a condition of another schema name is ignored"
    d.apply_configuration(condition("Canton-StakeholderAnomaly", {"max_stakeholders_default": 100}))
    assert d.default_limit == 100 and d.policy_config_id == 991
    assert d.monitored_parties == {"client::1220aa"}, "the condition's entity became a monitored party"

    async def feed():
        for u in updates("371_stakeholder_bloat.jsonl"):
            await d.on_update(u)  # not handle_update: the second feed would be deduplicated

    d.apply_configuration(condition("Canton-StakeholderAnomaly", {"max_stakeholders_default": 10}, updated_at=6))
    asyncio.run(feed())
    assert d.outputs.events.events == [], "the bloated contract has no monitored party as stakeholder"
    d.apply_configuration(condition("Canton-StakeholderAnomaly", {}, status="DISABLED"))
    asyncio.run(feed())
    (alert,) = d.outputs.events.events
    assert meta(alert)["policy"]["name"] == "Canton-StakeholderAnomaly" and alert.cid is None


def check_a_config_change_rebuilds_the_reader_and_reschedules_the_poller():
    cls = REGISTRY["capability_drift"]
    readers = []

    def fake_reader(self):
        readers.append(object())
        reader = MagicMock()
        reader.party_to_participant = MagicMock(side_effect=lambda *a, **k: asyncio.sleep(0, []))
        return reader

    cls.build_reader = fake_reader
    d = bare(cls, admin_api_url="fake:5012")
    assert d.reader is not None and len(readers) == 1

    def pollers():
        return [t for t in asyncio.all_tasks() if t.get_name() == "capability-drift-threshold-poller" and not t.done()]

    async def go():
        await d.on_init()
        (before,) = pollers()
        reader_before = d.reader
        await d.on_config_change(config=condition("Canton-CapabilityDrift", {"admin_api_url": "fake:5013"}))
        await asyncio.sleep(0)
        (after,) = pollers()
        assert after is not before and d.reader is not reader_before and len(readers) == 2
        assert d.admin_api_url == "fake:5013" and d.policy_config_id == 991
        await d.on_config_change(config=condition("Canton-CapabilityDrift", {}, status="DISABLED"))
        await asyncio.sleep(0)
        assert d.admin_api_url == "fake:5012" and len(pollers()) == 1, "back to the profile's reader and one poller"
        for t in pollers():
            t.cancel()

    asyncio.run(go())


def check_a_condition_field_survives_init():
    """The base configures from the profile and then applies the condition in force; a detector's
    init must not configure again, or it undoes the condition."""
    from sentinel.core.v2.sentry import AsyncCoreSentry

    AsyncCoreSentry.init = lambda self: None  # the SDK's channels and databases are not under test
    cls = REGISTRY["informee_anomaly"]
    d = cls.__new__(cls)
    d.name, d.parameters, d.logger, d.policy_config_id = cls.name, {"monitored_parties": []}, MagicMock(), None
    d.inputs, d.databases = MagicMock(), MagicMock()
    db = MagicMock()
    db._config_db = {101: condition("Canton-InformeeAnomaly", {"expected_parties_by_template": {"Splice.Wallet.TransferOffer:TransferOffer": []}}, cid=101)}
    d.databases.monitoring_conditions = db
    cls.init(d)
    assert d.expected == {"Splice.Wallet.TransferOffer:TransferOffer": set()}
    assert d.monitored_parties == {"client::1220aa"} and d.policy_config_id == 101


if __name__ == "__main__":
    for name, check in list(globals().items()):
        if name.startswith("check_"):
            check()
    print("ok")
