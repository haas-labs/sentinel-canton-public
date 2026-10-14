# sentinel-canton

Open-source detectors for [Canton](https://www.canton.network/) that run over the **CantonUpdate
stream** produced by the [trunk-canton](https://github.com/haas-labs/trunk-canton) adapter. Each
detector consumes a participant's normalized update stream from Kafka, applies a rule, and emits
alert `Event`s to an output topic.

The data models (`CantonUpdate`, `Event`, manifest enums) come from the public
[Sentinel SDK](https://github.com/haas-labs/ext-sentinel-py-sdk); the SDK's heavy processor runtime
(`AsyncCoreSentry`) is replaced by a thin async runner in this repo, so detectors run standalone
without the SDK's launch/profile machinery.

## How it works

```
trunk-canton ──CantonUpdate JSON──▶ Kafka (canton.*.tx) ──▶ sentinel-canton ──Event JSON──▶ Kafka (canton.*.events)
```

A detector subclasses `CantonUpdateDetector` (`src/sentinel_canton/runtime.py`) and overrides:
- `configure(parameters)` — read its parameters (monitored parties, thresholds, ...),
- `on_update(update)` — the rule, operating on a `CantonUpdate`,
- `on_init()` — optional, for background tasks,
and emits alerts via `emit(...)`. The runner (`runner.py`) wires input/output and drives the loop.

Detectors emit alert `Event`s to the Kafka events topic only. Forwarding them to an HTTP intake, a
webhook, or a SIEM is left to the consumer of that topic. That forwarding is out of scope for this repo.

## Detectors

Selected with `CANTON_DETECTOR`:

| Name | What it alerts on |
|---|---|
| `stakeholder_anomaly` | a created contract listing more stakeholders than the template's limit |
| `informee_anomaly` | a sub-action witnessed by a party that is not its own informee; a contract reaching a party not expected for its template (configured or learned) |
| `topology_drift` | a counterparty gaining or losing a hosting participant mid-relationship |
| `reassignment_limbo` | an unassigned contract not re-assigned on the target synchronizer within a window |
| `capability_drift` | a counterparty's hosting permission strengthening, or its confirmation threshold dropping |


Each detector ships a `manifest.py` with a `Schema` (its parameters, used to validate
`CANTON_PARAMETERS`) and a `MetadataModel` (name, version, icon, tags, FAQ, status). Standalone only
`name` and `version` are read at runtime; the rest is the detector's **registration contract for the
Extractor platform** and is kept intact so a detector can be registered there without a second source.

### Parameters per detector

Pass as JSON in `CANTON_PARAMETERS`. Examples:

- `stakeholder_anomaly`:
  `{"max_stakeholders_default": 10, "max_stakeholders_by_template": {"Iou:Iou": 5}}`
- `informee_anomaly` (the structural rule needs none; the per-template rule needs a list, or `learn_from_first_n`):
  `{"expected_parties_by_template": {"Iou:Iou": ["client::<id>", "counterparty::<id>"]}}`
  A regulator or auditor that observes your assets also witnesses what happens beneath them
  (divulgence), which is normal in Canton. List such parties in `trusted_parties` so they never raise
  the structural alert: `{"trusted_parties": ["regulator::<id>"]}`. Any other party that sees a
  contract it is not a stakeholder of alerts, with `exposed_party_hash` (whose contracts were seen) and
  a `recommended_action` (reject or revoke the access that caused it). A choice's acting parties are
  exempt by default, because a controller sees what its own choice does. Set `"strict_actors": true`
  to alert on them too, which is how a counterparty whose access contract reaches your holdings is
  caught; it also alerts on a client that witnesses the contract its own transfer created, so trust
  that party or leave it off.
  With `learn_from_first_n` set, a template that is not configured is learned from its first N
  contracts (which never alert): a party is then expected when it appeared in at least
  `learn_threshold` of them, a share in (0, 1] that defaults to `0.5` (the average presence):
  `{"learn_from_first_n": 5, "learn_threshold": 0.5}`
- `topology_drift`:
  `{"monitored_parties": ["client::<id>"], "known_counterparties": {"counterparty::<id>": "Acme"}}`
- `reassignment_limbo` (stateful; set `state_path` or `CANTON_STATE_DIR` to persist across restarts;
  the state file is written at most every `save_interval_seconds`, and only when it changed;
  with `monitored_parties` set, only unassigns those parties submitted or are a stakeholder of are
  tracked, empty = all):
  `{"monitored_parties": ["client::<id>"], "pending_window_seconds": 900, "check_interval_seconds": 30, "max_reassignments_per_party_per_hour": 20, "save_interval_seconds": 5}`
- `capability_drift` (the threshold half needs the `[admin]` extra and `admin_api_url`; the
  permission half runs without either):
  `{"monitored_parties": ["client::<id>"], "admin_api_url": "localhost:5012", "admin_api_poll_seconds": 60}`

Set `monitored_parties` to the client's own party ids. If it is empty, `topology_drift` treats the
client's own hosting changes as counterparty signals (it emits first-seen / drift for the client).

## Install / build

Requires Python 3.10+ and pip 21.3+ (editable installs read `pyproject.toml`, PEP 660). An older
pip fails before the Python version check is even reached, e.g. on the stock macOS Python 3.9 /
pip 21.2: use a Python 3.10+ environment and run `python -m pip install --upgrade pip` first if its
pip is older than 21.3.

```bash
pip install .                 # core; add .[dev] for tests, .[admin] for capability_drift's Admin-API path
docker build -f deploy/Dockerfile -t sentinel-canton:0.1.0 .
```

## Run

Configuration is by environment variable. `sentinel-canton` also reads a `.env` file in the
working directory (real environment variables take precedence). `.env.example` targets a default
local Canton node:

```bash
cp .env.example .env
```

| Variable | Default | Meaning |
|---|---|---|
| `CANTON_DETECTOR` | — (required) | Which detector to run (see the registry) |
| `CANTON_INPUT` | `kafka` | `kafka` to consume the topic, `fs` to replay fixtures |
| `CANTON_KAFKA_BOOTSTRAP` | `localhost:19092` | Kafka bootstrap servers |
| `CANTON_TX_TOPIC` | `canton.localnet.tx` | Input stream (trunk-canton output) |
| `CANTON_EVENTS_TOPIC` | `canton.localnet.events` | Output alert topic |
| `CANTON_CONSUMER_GROUP` | `sentinel-canton` | Consumer group |
| `CANTON_ADMIN_API_URL` | `localhost:5012` | capability_drift's participant Admin API (needs `[admin]`); empty disables it; `admin_api_url` in `CANTON_PARAMETERS` wins |
| `CANTON_PARAMETERS` | `` (empty) | JSON of detector parameters; empty = defaults |
| `CANTON_INPUT_PATH` | — | fs mode: a `.jsonl` file or directory of fixtures |
| `CANTON_STATE_DIR` | — | dir for stateful detectors' `state_path` (defaults `<dir>/<detector>.state.json` when set) |
| `CANTON_SOURCE_ID` | `ext:sentinel` | the `sid` stamped on every emitted event; override when feeding a sink other than the Extractor |

```bash
# consume the live stream
CANTON_DETECTOR=stakeholder_anomaly sentinel-canton
# replay a fixture with no Kafka
CANTON_DETECTOR=stakeholder_anomaly CANTON_INPUT=fs CANTON_INPUT_PATH=tests/fixtures/all_scenarios.jsonl sentinel-canton
```

## Deploy (Helm)

One release per detector; select it with `--set detector=...`:

```bash
helm install stakeholder deploy/helm/sentinel-canton \
  --set image.repository=<registry>/sentinel-canton --set image.tag=0.1.0 \
  --set detector=stakeholder_anomaly \
  --set canton.kafkaBootstrap=<broker>:9092 \
  --set canton.txTopic=canton.devnet.tx
```

Stateful detectors (reassignment_limbo, topology_drift) persist JSON state: set
`--set persistence.enabled=true`. Without it a restart forgets every open reassignment, so a contract
frozen across the restart is never reported. On a deploy the pod gets SIGTERM, and the runner writes
the pending state before exiting; a hard kill (OOMKilled, SIGKILL) can still lose up to
`save_interval_seconds` of changes.

## Test

```bash
pip install -e ".[dev]" && pytest        # prints a coverage report; `pytest --no-cov` skips it, or: python tests/test_runner.py
```

Tests drive detectors over the recorded fixtures in `tests/fixtures/` (no Kafka), asserting the
alerts each scenario should produce. The live end-to-end demo runs trunk-canton against a Canton
localnet and points these detectors at the resulting topic.

## Known limitations

- `capability_drift`'s confirmation-threshold half reads the participant's Admin API over gRPC: it
  runs only when `admin_api_url` is set and the `[admin]` extra is installed. Without them the
  detector runs its Ledger-API (hosting-permission) half only.
- Some detectors need parameters for part of their rules (e.g. `informee_anomaly`'s per-template rule
  needs `expected_parties_by_template` or `learn_from_first_n`); with empty `CANTON_PARAMETERS` they
  run with code defaults, which for that part is a no-op. Supply parameters via `CANTON_PARAMETERS` (JSON).

## License

Apache License 2.0. See [LICENSE](LICENSE). Copyright 2026 Hacken <https://hacken.io>.
