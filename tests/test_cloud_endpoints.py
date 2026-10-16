"""The ARM and Fabric endpoints: public-cloud defaults, environment overrides, and refusal of unsafe values."""

from __future__ import annotations

import importlib

import pytest

from adf2fabric import cloud


@pytest.fixture
def reload_cloud(monkeypatch):
    def _reload(**env):
        for name in (cloud.ENV_ARM, cloud.ENV_FABRIC):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return importlib.reload(cloud)

    yield _reload
    monkeypatch.delenv(cloud.ENV_ARM, raising=False)
    monkeypatch.delenv(cloud.ENV_FABRIC, raising=False)
    importlib.reload(cloud)  # leave the module on its defaults for other tests


def test_defaults_are_the_public_cloud(reload_cloud):
    c = reload_cloud()
    assert c.ARM_ENDPOINT == "https://management.azure.com"
    assert c.ARM_HOST == "management.azure.com"
    assert c.ARM_RESOURCE == "https://management.azure.com/"
    assert c.ARM_SCOPE == "https://management.azure.com/.default"
    assert c.FABRIC_API == "https://api.fabric.microsoft.com/v1"
    assert c.FABRIC_SCOPE == "https://api.fabric.microsoft.com/.default"


def test_overrides_flow_into_every_derived_value(reload_cloud):
    c = reload_cloud(ADF2FABRIC_ARM_ENDPOINT="https://Arm.Example.test/",
                     ADF2FABRIC_FABRIC_ENDPOINT="https://fabric.example.test")
    assert c.ARM_HOST == "arm.example.test"
    assert c.ARM_RESOURCE == "https://arm.example.test/"
    assert c.ARM_SCOPE == "https://arm.example.test/.default"
    assert c.FABRIC_API == "https://fabric.example.test/v1"
    assert c.FABRIC_SCOPE == "https://fabric.example.test/.default"


@pytest.mark.parametrize("bad", [
    "http://management.azure.com",            # not https
    "https://management.azure.com/sub",       # a path
    "https://user:pw@management.azure.com",  # credentials
    "https://management.azure.com?x=1",      # a query
    "management.azure.com",                  # no scheme
])
def test_unsafe_values_are_refused(reload_cloud, bad):
    with pytest.raises(ValueError):
        reload_cloud(ADF2FABRIC_ARM_ENDPOINT=bad)
