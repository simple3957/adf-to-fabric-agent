"""Input guards: identifiers, GUIDs, SSRF-safe URLs, path scrubbing.

Every value a client controls and the server then USES (an id that becomes a path, a URL the server fetches, a subscription or
workspace the server calls with its own credentials) passes through one of these before use. They reject; they do not repair.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from pathlib import Path
from typing import Callable, Iterable, Optional
from urllib.parse import urlsplit

ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,127}$")  # a project / linked-service / factory display name: no separators, no traversal
GUID = re.compile(r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.()\- ]{0,89}$")  # Azure resource group / factory names
_ABS_PATH = re.compile(r"([A-Za-z]:[\\/][^\s'\"<>|]*|(?<![\w:/])/(?:home|Users|tmp|var|etc|opt|usr|mnt|private)[^\s'\"<>|]*)")
_BLOCKED_HOSTS = {"localhost", "metadata.google.internal", "metadata", "instance-data"}
_BLOCKED_SUFFIXES = (".internal", ".local", ".localhost", ".lan", ".corp", ".home")


class Rejected(ValueError):
    """The input was refused. The message is safe to show: it never echoes the offending value back in full."""


def valid_id(value: object, what: str = "id") -> str:
    if not isinstance(value, str) or not ID.match(value):
        raise Rejected(f"The {what} must be 1-64 letters, digits, '-' or '_'.")
    return value


def valid_name(value: object, what: str = "name") -> str:
    if not isinstance(value, str) or not NAME.match(value) or ".." in value:
        raise Rejected(f"The {what} may use letters, digits, spaces, '_', '-' and '.', and must start with a letter or digit.")
    return value


def valid_guid(value: object, what: str = "id") -> str:
    if not isinstance(value, str) or not GUID.match(value):
        raise Rejected(f"The {what} must be a GUID.")
    return value


def valid_resource_name(value: object, what: str = "resource name") -> str:
    if not isinstance(value, str) or not RESOURCE_NAME.match(value) or ".." in value:
        raise Rejected(f"The {what} is not a valid Azure resource name.")
    return value


def safe_child(base: Path, *parts: str) -> Path:
    """``base / parts`` guaranteed to stay inside ``base`` (no traversal through ``..``, absolute parts or symlinks)."""
    base_r = base.resolve()
    target = base_r.joinpath(*parts).resolve()
    if base_r != target and base_r not in target.parents:
        raise Rejected("That path is outside the allowed folder.")
    return target


# --- SSRF -----------------------------------------------------------------------------------------------------------------


def _addresses(host: str, resolver: Callable[[str], list[str]]) -> list[ipaddress._BaseAddress]:
    try:
        return [ipaddress.ip_address(host.strip("[]"))]
    except ValueError:
        pass
    out = []
    for a in resolver(host):
        try:
            out.append(ipaddress.ip_address(a.split("%")[0]))
        except ValueError:
            continue
    return out


def _default_resolver(host: str) -> list[str]:
    try:
        return sorted({i[4][0] for i in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})
    except OSError:
        return []


def _public(ip: ipaddress._BaseAddress) -> bool:
    return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
                or (ip.version == 6 and (ip.ipv4_mapped is not None and not _public(ip.ipv4_mapped))))


def check_public_https_url(url: object, allowed_hosts: Iterable[str] = (), resolver: Callable[[str], list[str]] = _default_resolver) -> str:
    """The URL if it is safe for the SERVER to fetch on a client's behalf, else ``Rejected``.

    Requires https, no credentials in the URL, no unusual port, a host that is not an internal name, and (after DNS resolution)
    only public addresses, so a client cannot aim the server at ``localhost``, a private network or a cloud metadata address. If
    an allow-list is configured the host must be on it. Note: DNS can change between this check and the fetch; the fetch runs
    ``git`` against the URL, so the allow-list is the stronger control where one can be set.
    """
    if not isinstance(url, str) or len(url) > 2048:
        raise Rejected("The repository URL is missing or too long.")
    if any(c.isspace() or ord(c) < 32 or ord(c) == 127 or c == "\\" for c in url.strip()):  # urlsplit silently drops line breaks and tabs, so it would pass what git then receives
        raise Rejected("The repository URL may not contain spaces, line breaks, control characters or backslashes.")
    u = urlsplit(url.strip())
    if u.scheme != "https":
        raise Rejected("Only https:// repository URLs are accepted.")
    if u.username or u.password:
        raise Rejected("Credentials must not be part of the URL; supply a token separately.")
    host = (u.hostname or "").lower()
    if not host or u.port not in (None, 443):
        raise Rejected("The repository URL must name a host and use the standard https port.")
    if host in _BLOCKED_HOSTS or host.endswith(_BLOCKED_SUFFIXES):
        raise Rejected("That host name is not allowed.")
    allow = [h.lower() for h in allowed_hosts]
    if allow and host not in allow and not any(host.endswith("." + a) for a in allow):
        raise Rejected("That host is not on the list of allowed Git hosts.")
    addrs = _addresses(host, resolver)
    if not addrs:
        raise Rejected("The host name could not be resolved.")
    if not all(_public(a) for a in addrs):
        raise Rejected("That host resolves to a non-public address, which is not allowed.")
    return url.strip()


# --- scrubbing --------------------------------------------------------------------------------------------------------------


def scrub(text: str, roots: Iterable[Path] = ()) -> str:
    """Replace server paths in a message so internal layout is not disclosed."""
    out = text or ""
    for r in roots:
        for form in {str(r), str(r).replace("\\", "/"), str(r.resolve()), str(r.resolve()).replace("\\", "/")}:
            if form and form not in ("/", "."):
                out = out.replace(form, "<server>")
    return _ABS_PATH.sub("<path>", out)
