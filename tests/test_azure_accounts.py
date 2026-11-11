"""Several signed-in accounts at once: ADF can use one, Fabric another, neither changes the CLI's default."""

from __future__ import annotations


PAL, GMAIL, OTHER = "sathvik.kamegaonkar@pal.tech", "sathvik7593@gmail.com", "someone@example.com"
LIST = "az account list"


def count(runner, prefix):
    return sum(1 for args, _ in runner.calls if " ".join(args).startswith(prefix))


# --- who is signed in ---------------------------------------------------------------------------------------------


# --- picking an account for a token -------------------------------------------------------------------------------


# --- the cache ----------------------------------------------------------------------------------------------------


# --- the API ------------------------------------------------------------------------------------------------------


# --- what a failed token says -------------------------------------------------------------------------------------


def test_a_plain_expired_session_keeps_its_old_message():
    from adf2fabric.connections.providers.azure_cli import token_failure

    msg, actions = token_failure("ERROR: Please run 'az login' to setup account.")
    assert "session has expired" in msg and actions


# --- two saved sign-ins for one username (Azure CLI issue 20168) -------------------------------------------------


# --- tokens are reused until shortly before they expire -----------------------------------------------------------


