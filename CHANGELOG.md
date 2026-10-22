# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [0.2.0] - Unreleased

### Breaking changes

- **Detector schema names follow the platform's registered schemas.** Each detector's
  `metadata.name` changed, and its version moved to the `0.2` line:

  | Detector (`CANTON_DETECTOR`) | Old name (version) | New name (version) |
  |---|---|---|
  | `topology_drift` | `Canton Topology Drift` (0.1.1) | `Canton-TopologyDrift` (0.2.1) |
  | `capability_drift` | `Canton Capability Drift` (0.1.3) | `Canton-CapabilityDrift` (0.2.1) |
  | `stakeholder_anomaly` | `Canton Stakeholder Anomaly` (0.1.1) | `Canton-StakeholderAnomaly` (0.2.1) |
  | `reassignment_limbo` | `Canton Reassignment Limbo` (0.2.0) | `Canton-ReassignmentLimbo` (0.2.2) |
  | `informee_anomaly` | `Canton Informee Anomaly` (0.2.0) | `Canton-InformeeAnomaly` (0.2.2) |

  The name is what alerts carry in `policy.name` and what a monitoring condition is matched by.
  **What to change:** anything that filters alerts on `policy.name`, and any monitoring condition
  registered against an old name, must use the new name. The `CANTON_DETECTOR` values and the Helm
  `detector` values are unchanged.
- **Sentinel SDK 0.5.87 or later is required** (it adds `CantonUpdate.participant_id`).
  `pyproject.toml` now pins `ext-sentinel-py-sdk` to the `v0.5.87` tag instead of the default
  branch. **What to change:** an environment that installs an older SDK must upgrade it.

### Added

- Per-node scope: a detector can be set to the participant it watches with
  `CANTON_PARTICIPANT_ID`, the `participant_id` key in `CANTON_PARAMETERS` (a non-empty value
  there wins), or the Helm value `canton.participantId`. Updates from any other node, snapshots
  included, are dropped before they reach the detector's state or its alerts. Records without
  `participant_id` are still accepted. Empty keeps the previous behavior (every node on the topic),
  and a warning is logged once a second node appears.
- `participant_id` field ("Participant (node)") on the schema of all five detectors, through a
  shared `CantonSchema` base, so the scope can be set and kept on a monitoring condition.
- Alerts carry `source_participant_hash`: the sha256 of the node the update came from.
- `on_config_change()` on the standalone runtime: a monitoring condition handed to the detector
  is applied the same way as on the SDK base (an active condition lands on top of the profile
  parameters, any other status restores them).
- Release workflow: pushing a version tag (`X.Y.Z`, matching the version in `pyproject.toml`)
  builds and publishes the wheel.
- FAQ entry "Which node does it watch?" on every detector.

### Changed

- Deduplication is by `(participant_id, update_id)` instead of `update_id` alone, since the same
  transaction seen by two nodes has one `update_id`.
- `stakeholder_anomaly`: with `monitored_parties` set, only contracts where a monitored party is a
  stakeholder are judged.
- `capability_drift`: the Admin API reader is rebuilt on every reconfiguration and the threshold
  poller follows it, so a condition change reaches the poller; its alerts carry the node whose
  Admin API was polled.
- `reassignment_limbo`: alerts raised by the ticker carry the detector's node.
- Helm chart and image version 0.2.0.

## [0.1.0] - 2026-10-14

Initial public release.
