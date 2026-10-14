"""Detector registry: maps CANTON_DETECTOR names to detector classes. The runner selects one to run.

capability_drift's Admin-API half needs the optional `[admin]` extra
(grpc); its Ledger-API half runs without it."""
from sentinel_canton.detectors.capability_drift.sentry import CapabilityDriftDetector
from sentinel_canton.detectors.informee_anomaly.sentry import InformeeAnomalyDetector
from sentinel_canton.detectors.reassignment_limbo.sentry import ReassignmentLimboDetector
from sentinel_canton.detectors.stakeholder_anomaly.sentry import StakeholderAnomalyDetector
from sentinel_canton.detectors.topology_drift.sentry import TopologyDriftDetector

REGISTRY = {
    "stakeholder_anomaly": StakeholderAnomalyDetector,
    "informee_anomaly": InformeeAnomalyDetector,
    "topology_drift": TopologyDriftDetector,
    "reassignment_limbo": ReassignmentLimboDetector,
    "capability_drift": CapabilityDriftDetector,
}
