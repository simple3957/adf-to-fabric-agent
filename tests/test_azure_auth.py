"""Every Azure authentication mechanism: resolution from env-var references, refusals, and wiring.

azure-identity classes are replaced by recorders, so nothing here touches the network or a real credential.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from adf2fabric import cli as cli_mod
from adf2fabric.agent.discovery.adls import AdlsDiscoverer, AdlsDiscoveryRequest
from adf2fabric.connectivity import _azure
from adf2fabric.connectivity._azure import (
    AUTH_MECHANISMS,
    ENTRA_MECHANISMS,
    STORAGE_ONLY_MECHANISMS,
    CredentialUnavailable,
    build_credential,
)
from adf2fabric.project.models import AzureCredentialRef, ConnectivityStatus
from tests.test_adls_discovery import FakeFS, FakeService, _setup


class Recorder:
    """Stands in for an azure.identity credential class and remembers how it was built."""

    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs


@pytest.fixture
def fake_identity(monkeypatch):
    import azure.identity as ai

    for name in ("AzureCliCredential", "ClientSecretCredential", "CertificateCredential", "ManagedIdentityCredential",
                 "InteractiveBrowserCredential", "DeviceCodeCredential", "DefaultAzureCredential"):
        monkeypatch.setattr(ai, name, type(name, (Recorder,), {}))
    return ai


def ref(auth, **kw):
    return AzureCredentialRef(auth=auth, **kw)


def test_mechanism_sets_are_consistent():
    assert set(AUTH_MECHANISMS) == set(ENTRA_MECHANISMS) | set(STORAGE_ONLY_MECHANISMS)
    assert not set(ENTRA_MECHANISMS) & set(STORAGE_ONLY_MECHANISMS)


def test_azure_cli_with_and_without_tenant(fake_identity):
    plain, _ = build_credential(ref("azure_cli"))
    assert plain.kwargs == {}
    import os
    os.environ["T_ID"] = "tenant-1"
    try:
        tenanted, _ = build_credential(ref("azure_cli", tenant_id_env="T_ID"))
    finally:
        del os.environ["T_ID"]
    assert tenanted.kwargs == {"tenant_id": "tenant-1"}


def test_client_secret_reads_three_env_vars_and_returns_the_secret_for_redaction(fake_identity, monkeypatch):
    for k, v in (("T", "tid"), ("C", "cid"), ("S", "s3cret")):
        monkeypatch.setenv(k, v)
    cred, secret = build_credential(ref("client_secret", tenant_id_env="T", client_id_env="C", client_secret_env="S"))
    assert cred.args == ("tid", "cid", "s3cret") and secret == "s3cret"


def test_client_certificate(fake_identity, monkeypatch):
    monkeypatch.setenv("T", "tid")
    monkeypatch.setenv("C", "cid")
    monkeypatch.setenv("P", "/certs/app.pem")
    monkeypatch.setenv("PW", "pfxpass")
    cred, secret = build_credential(ref("client_certificate", tenant_id_env="T", client_id_env="C",
                                        certificate_path_env="P", certificate_password_env="PW"))
    assert cred.args == ("tid", "cid") and cred.kwargs == {"certificate_path": "/certs/app.pem", "password": "pfxpass"}
    assert secret == "pfxpass"
    nopass, secret = build_credential(ref("client_certificate", tenant_id_env="T", client_id_env="C",
                                          certificate_path_env="P"))
    assert nopass.kwargs["password"] is None and secret == ""


def test_managed_identity_system_and_user_assigned(fake_identity, monkeypatch):
    assert build_credential(ref("managed_identity"))[0].kwargs == {}
    monkeypatch.setenv("MI", "uami-client-id")
    assert build_credential(ref("managed_identity", client_id_env="MI"))[0].kwargs == {"client_id": "uami-client-id"}
    monkeypatch.delenv("MI")
    with pytest.raises(CredentialUnavailable) as e:
        build_credential(ref("managed_identity", client_id_env="MI"))
    assert e.value.status is ConnectivityStatus.missing_credential and "MI" in e.value.detail


@pytest.mark.parametrize("mechanism, cls", [("interactive_browser", "InteractiveBrowserCredential"),
                                            ("device_code", "DeviceCodeCredential")])
def test_interactive_mechanisms_take_an_optional_tenant(fake_identity, monkeypatch, mechanism, cls):
    assert build_credential(ref(mechanism))[0].kwargs == {}
    monkeypatch.setenv("T", "tid")
    cred, secret = build_credential(ref(mechanism, tenant_id_env="T"))
    assert type(cred).__name__ == cls and cred.kwargs == {"tenant_id": "tid"} and secret == ""


def test_azure_identity_default_chain(fake_identity):
    assert type(build_credential(ref("azure_identity"))[0]).__name__ == "DefaultAzureCredential"


def test_access_token_is_presented_as_a_credential_and_redacted(monkeypatch):
    monkeypatch.setenv("TOK", "eyJ.token.value")
    cred, secret = build_credential(ref("access_token", access_token_env="TOK"))
    assert secret == "eyJ.token.value"
    assert cred.get_token("https://api.fabric.microsoft.com/.default").token == "eyJ.token.value"


@pytest.mark.parametrize("auth, fields, env, missing", [
    ("client_secret", {"tenant_id_env": "T"}, {}, "client_id_env, client_secret_env"),  # named in the model
    ("client_certificate", {"tenant_id_env": "T", "client_id_env": "C"}, {}, "certificate_path_env"),
    ("access_token", {}, {}, "access_token_env"),
    ("account_key", {}, {}, "account_key_env"),
    ("sas_token", {}, {}, "sas_token_env"),
    ("connection_string", {}, {}, "connection_string_env"),
])
def test_missing_references_are_named(fake_identity, auth, fields, env, missing):
    with pytest.raises(CredentialUnavailable) as e:
        build_credential(ref(auth, **fields), storage_ok=True)
    assert e.value.status is ConnectivityStatus.missing_config and missing in e.value.detail


def test_unset_env_vars_are_named_without_values(fake_identity, monkeypatch):
    monkeypatch.delenv("S_UNSET", raising=False)
    with pytest.raises(CredentialUnavailable) as e:
        build_credential(ref("access_token", access_token_env="S_UNSET"))
    assert e.value.status is ConnectivityStatus.missing_credential and "S_UNSET" in e.value.detail


def test_unknown_mechanism_lists_every_valid_one():
    with pytest.raises(CredentialUnavailable) as e:
        build_credential(ref("kerberos"))
    assert all(m in e.value.detail for m in AUTH_MECHANISMS)


# --- storage-only mechanisms ---------------------------------------------------------------------------------


def test_storage_credentials_are_plain_strings_only_where_storage_is_allowed(monkeypatch):
    monkeypatch.setenv("KEY", "base64key==")
    monkeypatch.setenv("SAS", "?sv=2022&sig=abc")
    assert build_credential(ref("account_key", account_key_env="KEY"), storage_ok=True) == ("base64key==", "base64key==")
    assert build_credential(ref("sas_token", sas_token_env="SAS"), storage_ok=True) == ("sv=2022&sig=abc", "sv=2022&sig=abc")


def test_connection_string_yields_its_key_or_sas(monkeypatch):
    monkeypatch.setenv("CS1", "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=k3y==;EndpointSuffix=core.windows.net")
    monkeypatch.setenv("CS2", "BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=sv=1&sig=z")
    monkeypatch.setenv("CS3", "AccountName=a")
    assert build_credential(ref("connection_string", connection_string_env="CS1"), storage_ok=True)[0] == "k3y=="
    assert build_credential(ref("connection_string", connection_string_env="CS2"), storage_ok=True)[0] == "sv=1&sig=z"
    with pytest.raises(CredentialUnavailable) as e:
        build_credential(ref("connection_string", connection_string_env="CS3"), storage_ok=True)
    assert "AccountKey" in e.value.detail


@pytest.mark.parametrize("auth", STORAGE_ONLY_MECHANISMS)
def test_storage_mechanisms_are_refused_for_arm_and_fabric(auth):
    with pytest.raises(CredentialUnavailable) as e:  # default storage_ok=False = ARM / Fabric callers
        build_credential(ref(auth, account_key_env="X", sas_token_env="X", connection_string_env="X"))
    assert "storage (ADLS Gen2) only" in e.value.detail and "azure_cli" in e.value.detail


def test_adls_discovery_hands_the_account_key_to_the_storage_client(tmp_path, monkeypatch):
    monkeypatch.setenv("ADLS_KEY", "base64key==")
    store, _ = _setup(tmp_path)
    seen = {}

    def factory(url, credential):
        seen["url"], seen["credential"] = url, credential
        return FakeService([FakeFS("raw")])

    result = AdlsDiscoverer(store, service_factory=factory).run(AdlsDiscoveryRequest(
        project_name="test-proj", storage_account="acct1",
        credential=ref("account_key", account_key_env="ADLS_KEY")))
    assert result.success and seen == {"url": "https://acct1.dfs.core.windows.net", "credential": "base64key=="}
    assert "base64key" not in (result.discovery.model_dump_json())  # the secret is never persisted


# --- CLI wiring ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("command", ["intake", "discovery-adf", "deploy"])
def test_azure_commands_offer_every_entra_mechanism_and_no_storage_ones(command):
    params = {p.name: p for p in cli_mod.cli.commands[command].params}
    assert set(params["auth"].type.choices) == set(ENTRA_MECHANISMS)
    for name in ("tenant_id_env", "client_id_env", "client_secret_env", "certificate_path_env",
                 "certificate_password_env", "access_token_env"):
        assert name in params
    # storage-only secrets are not offered where ARM / Fabric need a token
    assert not {"account_key_env", "sas_token_env", "connection_string_env"} & set(params)
    assert params["auth"].default == "azure_cli"


def test_credential_from_keeps_only_given_names():
    c = cli_mod.credential_from("client_secret", tenant_id_env="T", client_id_env=None, client_secret_env="S")
    assert c == AzureCredentialRef(auth="client_secret", tenant_id_env="T", client_secret_env="S")
