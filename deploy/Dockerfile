# Canton detectors reference image. One image runs any detector; pick it with CANTON_DETECTOR.
FROM python:3.12-slim

WORKDIR /app

# git is needed to install the Sentinel SDK, which is a git dependency (not on PyPI).
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
# Build without the admin extra by default (keeps grpc/protobuf out of the image). To include
# capability_drift's Admin-API path, build with: --build-arg EXTRAS=[admin]
ARG EXTRAS=""
RUN pip install --no-cache-dir ".${EXTRAS}"

# State is memory-only unless a state directory is provided: stateful detectors (reassignment_limbo,
# topology_drift) persist JSON only when CANTON_STATE_DIR points at a mounted volume. The Helm chart
# sets it (and mounts a PVC) when persistence.enabled=true; do not set it here unconditionally, or the
# state would be written to the ephemeral container layer and lost on restart.

ENTRYPOINT ["sentinel-canton"]
