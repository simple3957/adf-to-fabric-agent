"""Git repository provider: public and private HTTPS repositories, a branch, tag or commit, and a verified SHA.

Authentication for private repositories is an HTTPS access token sent as an HTTP header through git's ``GIT_CONFIG_*``
environment variables. The token is never placed in the URL, never on a command line, never written into the clone's
``.git/config``, and every line of git output is redacted before it is returned. SSH keys are not supported (they would need
key files and agent forwarding this tool does not manage); the error says so.

Supported hosts need no special casing: the header is standard HTTP Basic, and the default user name follows each host's
convention (GitHub ``x-access-token``, GitLab ``oauth2``, Bitbucket ``x-token-auth``, Azure DevOps empty). Override it if yours differs.
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from adf2fabric.connections.models import Check, CheckStatus, ConnectionState, TestReport
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.runner import CliResult, Runner, run_cli

_SHA = re.compile(r"^[0-9a-fA-F]{7,40}$")
_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@+-]{0,200}$")
_HOST_USER = (("github.com", "x-access-token"), ("gitlab", "oauth2"), ("bitbucket.org", "x-token-auth"),
              ("dev.azure.com", ""), ("visualstudio.com", ""))


class GitSourceError(Exception):
    """Safe to show; carries what to do about it."""

    def __init__(self, message: str, actions: Optional[list[str]] = None):
        super().__init__(message)
        self.actions = actions or []


@dataclass
class GitAuth:
    token: Optional[str] = None  # None = anonymous (public repository)
    username: Optional[str] = None  # None = the host's convention


@dataclass
class ResolvedRevision:
    requested: str
    kind: str  # branch | tag | commit
    sha: Optional[str] = None  # known for branches and tags (from ls-remote); for a commit it is confirmed after fetch


def clean_url(url: str) -> tuple[str, str]:
    """``(token-free https URL, host)``. Rejects credentials in the URL and non-HTTPS transports."""
    raw = (url or "").strip()
    u = urlparse(raw)
    if u.scheme in ("ssh", "git", "file", "ftp") or raw.startswith("git@"):
        raise GitSourceError("Only HTTPS repository URLs are supported (SSH keys are not).",
                             ["Use the https:// clone URL and, for a private repository, an access token."])
    if u.scheme != "https" or not u.hostname:
        raise GitSourceError("The repository URL must be an https:// address.")
    if u.username or u.password:
        raise GitSourceError("Do not put credentials in the repository URL.",
                             ["Remove the user/token from the URL and supply the token in the authentication field."])
    path = u.path.rstrip("/")
    return f"https://{u.hostname}{f':{u.port}' if u.port else ''}{path}", u.hostname.lower()


def default_username(host: str) -> str:
    for needle, user in _HOST_USER:
        if needle in host:
            return user
    return "git"


def git_env(auth: GitAuth, host: str) -> dict[str, str]:
    """Environment for git: no terminal prompts, and the token (if any) as an Authorization header that git reads from the
    environment, so it never appears in argv, the URL, or the repository's config."""
    env = {"GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"}
    if auth.token:
        user = auth.username if auth.username is not None else default_username(host)
        basic = base64.b64encode(f"{user}:{auth.token}".encode()).decode()
        env.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.extraHeader",
                    "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}"})
    return env


def _explain(res: CliResult, host: str, authed: bool) -> GitSourceError:
    text = res.text.lower()
    if not res.found:
        return GitSourceError("git is not installed.", ["Install Git and make sure `git` is on PATH."])
    if res.timed_out:
        return GitSourceError(f"{host} did not answer in time.", ["Check your network and the repository address."])
    if "could not resolve host" in text or ("unable to access" in text and "connect" in text):
        return GitSourceError(f"{host} could not be reached.", ["Check the address and your network or proxy."])
    if any(k in text for k in ("authentication failed", "could not read username", "invalid username or password", "403", "401")):
        if authed:
            return GitSourceError("The host rejected the access token.",
                                  ["Check the token has read access to this repository and has not expired.",
                                   "Check the user name convention for this host."])
        return GitSourceError("The repository is private or does not exist; anonymous access was refused.",
                              ["Provide an access token with read access to the repository."])
    if "not found" in text or "repository" in text and "does not exist" in text:
        return GitSourceError("The repository was not found.", ["Check the URL; for a private repository, supply an access token."])
    if "remote branch" in text and "not found" in text or "couldn't find remote ref" in text:
        return GitSourceError("That branch or tag does not exist in the repository.", ["Check the revision name."])
    return GitSourceError("git could not complete the request: " + redact(res.text)[:160])


class GitProvider:
    id = "git"

    def __init__(self, runner: Runner = run_cli):
        self._run = runner

    def _git(self, args: list[str], auth: GitAuth, host: str, timeout: float = 300) -> CliResult:
        env = git_env(auth, host)
        # redact the token AND the base64 header value git might echo, both taken from the very environment git receives
        secrets = (auth.token, env["GIT_CONFIG_VALUE_0"].rsplit(" ", 1)[-1]) if auth.token else ()
        return self._run(["git", *args], timeout=timeout, env_extra=env, secrets=secrets)

    def is_available(self) -> bool:
        return self._run(["git", "--version"], timeout=20).found

    # ---- resolve + test ---------------------------------------------------------------------------------------
    def resolve(self, url: str, revision: str, auth: GitAuth) -> ResolvedRevision:
        """Check the repository is reachable with this authentication and that the revision exists."""
        cleaned, host = clean_url(url)
        rev = (revision or "").strip() or "HEAD"
        if rev != "HEAD" and not _SHA.match(rev) and not _REF.match(rev):
            raise GitSourceError("The branch, tag or commit name contains characters git does not allow.")
        r = self._git(["ls-remote", "--heads", "--tags", "--", cleaned], auth, host, timeout=60)
        if not r.ok:
            raise _explain(r, host, bool(auth.token))
        refs: dict[str, str] = {}
        for line in r.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                refs[parts[1]] = parts[0]
                if parts[1].endswith("^{}"):  # an annotated tag points at the commit through the peeled ref
                    refs[parts[1][:-3]] = parts[0]
        if _SHA.match(rev):
            return ResolvedRevision(rev, "commit", None)
        if rev == "HEAD":
            r2 = self._git(["ls-remote", "--", cleaned, "HEAD"], auth, host, timeout=60)
            first = r2.stdout.split()[0] if r2.ok and r2.stdout.split() else None
            return ResolvedRevision("HEAD", "branch", first)
        for prefix, kind in (("refs/heads/", "branch"), ("refs/tags/", "tag")):
            if prefix + rev in refs:
                return ResolvedRevision(rev, kind, refs[prefix + rev])
        raise GitSourceError(f"'{rev}' is not a branch or tag of this repository.",
                             ["Check the name, or give a commit SHA instead."])

    def test(self, url: str, revision: str, auth: GitAuth) -> TestReport:
        rep = TestReport(target=url.split("?")[0], state=ConnectionState.validation_failed)
        try:
            res = self.resolve(url, revision, auth)
        except GitSourceError as exc:
            rep.error, rep.actions = str(exc), exc.actions
            rep.checks.append(Check(name="repository access", status=CheckStatus.failed, detail=str(exc)))
            return rep
        rep.verified.append("the repository is reachable" + (" with the supplied token" if auth.token else " anonymously"))
        rep.verified.append(f"{res.kind} '{res.requested}' exists" + (f" at {res.sha[:8]}" if res.sha else ""))
        if res.kind == "commit":
            rep.not_verified.append("a commit SHA is confirmed only when fetched")
        rep.checks.append(Check(name="repository access", status=CheckStatus.passed))
        rep.state = ConnectionState.connection_tested
        return rep

    # ---- fetch ------------------------------------------------------------------------------------------------
    def fetch(self, url: str, revision: str, dest: Path, auth: GitAuth) -> str:
        """Fetch ``revision`` into ``dest`` (which must not exist) and return the commit SHA actually checked out."""
        cleaned, host = clean_url(url)
        res = self.resolve(url, revision, auth)
        base = ["-c", "core.longpaths=true"]
        if res.kind in ("branch", "tag"):
            args = [*base, "clone", "--depth", "1"]
            if res.requested != "HEAD":
                args += ["--branch", res.requested]
            r = self._git([*args, "--", cleaned, str(dest)], auth, host)
            if not r.ok:
                raise _explain(r, host, bool(auth.token))
        else:
            dest.mkdir(parents=True, exist_ok=True)
            for a in (["init", "-q", str(dest)], ["-C", str(dest), "remote", "add", "origin", cleaned]):
                r = self._git(a, auth, host)
                if not r.ok:
                    raise _explain(r, host, bool(auth.token))
            r = self._git(["-C", str(dest), *base, "fetch", "--depth", "1", "origin", res.requested], auth, host)
            if not r.ok:  # some hosts refuse fetching an arbitrary SHA shallowly: fall back to a full fetch
                r = self._git(["-C", str(dest), *base, "fetch", "origin"], auth, host)
                if not r.ok:
                    raise _explain(r, host, bool(auth.token))
                r = self._git(["-C", str(dest), "checkout", "-q", "--detach", res.requested], auth, host)
            else:
                r = self._git(["-C", str(dest), "checkout", "-q", "--detach", "FETCH_HEAD"], auth, host)
            if not r.ok:
                raise GitSourceError(f"The commit '{res.requested}' could not be fetched or checked out.",
                                     ["Check the SHA exists in this repository."])
        head = self._git(["-C", str(dest), "rev-parse", "HEAD"], auth, host)
        sha = head.stdout.strip()
        if not head.ok or not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise GitSourceError("The fetched revision's commit SHA could not be read.")
        if res.kind == "commit" and not sha.startswith(res.requested.lower()):
            raise GitSourceError("The fetched commit does not match the requested SHA.")
        return sha
