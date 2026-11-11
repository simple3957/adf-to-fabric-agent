"""Every Azure authentication mechanism: resolution from env-var references, refusals, and wiring.

azure-identity classes are replaced by recorders, so nothing here touches the network or a real credential.
"""

from __future__ import annotations

import pytest

from adf2fabric import cli as cli_mod
from adf2fabric.connectivity._azure import (
    AUTH_MECHANISMS,
    ENTRA_MECHANISMS,
    STORAGE_ONLY_MECHANISMS,
    CredentialUnavailable,
    build_credential,
)
from adf2fabric.project.models import AzureCredentialRef


def ref(auth, **kw):
    return AzureCredentialRef(auth=auth, **kw)


def test_mechanism_sets_are_consistent():
    assert set(AUTH_MECHANISMS) == set(ENTRA_MECHANISMS) | set(STORAGE_ONLY_MECHANISMS)
    assert not set(ENTRA_MECHANISMS) & set(STORAGE_ONLY_MECHANISMS)


def test_access_token_is_presented_as_a_credential_and_redacted(monkeypatch):
    monkeypatch.setenv("TOK", "eyJ.token.value")
    cred, secret = build_credential(ref("access_token", access_token_env="TOK"))
    assert secret == "eyJ.token.value"
    assert cred.get_token("https://api.fabric.microsoft.com/.default").token == "eyJ.token.value"


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
