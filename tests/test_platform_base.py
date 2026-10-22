"""The detectors also run on the platform's base (the SDK's CantonUpdateDetector), where a monitoring
condition can arrive or change while the detector runs. platform_harness.py binds them to that base
in its own process and checks what only shows there: the condition is matched by the manifest name,
applied on top of the profile, survives init, and a capability_drift condition change rebuilds the
Admin-API reader and its poller."""
import inspect
import pathlib
import subprocess
import sys

import pytest

from sentinel_canton.detectors import REGISTRY

HARNESS = pathlib.Path(__file__).resolve().parent / "platform_harness.py"


def test_the_detectors_behave_on_the_platform_base():
    result = subprocess.run([sys.executable, str(HARNESS)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0 and result.stdout.strip() == "ok", result.stderr


@pytest.mark.parametrize("cls", REGISTRY.values(), ids=lambda c: c.__name__)
def test_init_does_not_configure_after_the_base_applied_the_condition(cls):
    init = cls.__dict__.get("init")
    if init is None:
        return  # the base's init: configures once, then applies the condition in force
    source = inspect.getsource(init)
    assert "super().init()" in source
    assert "self.configure(" not in source, f"{cls.__name__}.init undoes the condition the base just applied"
