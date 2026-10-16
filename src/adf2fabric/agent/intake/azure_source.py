"""Intake Mode A: retrieve ADF artifact JSON from a live Azure Data Factory.

    resolve identity -> acquire ARM token (reusing the `az login` session)
    -> read the factory + every artifact collection (paginated, throttle-aware)
    -> redact secret VALUES -> write the ADF Git folder layout into a staging dir
    -> apply the optional pipeline selection -> validate -> commit locally
    -> swap into projects/<id>/source/ -> record project + source manifest -> STOP

The result is shaped exactly like a cloned ADF repository (``pipeline/``,
``dataset/``, ``linkedService/``, ``trigger/``, ``factory/`` ...) and is a local
git repository, so the existing Discovery stages consume it unchanged — including
the commit-SHA integrity check. The export is built in a staging directory and only
replaces ``source/`` once it is complete, so a failed retrieval never destroys a
previously good source.

Everything here is read-only against Azure: GET requests only. Source definitions
are preserved byte-for-byte (the ARM item as returned) except for secret VALUES,
which are masked and every masking is recorded in the manifest. Tokens are never
logged, persisted, or placed in an error message.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from adf2fabric import cloud
from adf2fabric.agent.discovery.adf import arm
from adf2fabric.agent.discovery.repository.artifacts import _SECRET_KEY_HINTS
from adf2fabric.agent.intake import source_validation as sv
from adf2fabric.agent.intake.models import AzureIntakeRequest, IntakeResult
from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.project.discovery import AdfArtifactKind
from adf2fabric.project.models import (
    AzureCredentialRef,
    ConnectivityStatus,
    Project,
    RepositoryMetadata,
)
from adf2fabric.project.source import (
    AzureFactoryIdentity,
    FindingSeverity,
    RedactionRecord,
    RetrievalFailure,
    SourceFinding,
    SourceManifest,
    SourceSelection,
    SourceType,
)
from adf2fabric.project.store import ProjectStore, slugify
from adf2fabric.repository import authentication, git

PROVIDER_NAME = "azure-adf"
EXPORT_BRANCH = "adf-live"
REDACTED = "[REDACTED]"

# Kinds a user may request. ``factory`` carries the global parameters. Pipelines
# are always retrieved (they are what the migration is about).
FACTORY_KIND = AdfArtifactKind.factory
CDC_KIND = AdfArtifactKind.change_data_capture
CDC_SEGMENT = "adfcdcs"  # Microsoft.DataFactory/factories/adfcdcs (ARM reference)
KIND_CHOICES = [k.value for k, _ in arm.ARTIFACT_COLLECTIONS] + [FACTORY_KIND.value, CDC_KIND.value]

_ALLOWED_HOST = cloud.ARM_HOST
_RETRY_STATUSES = frozenset({429, 502, 503, 504})
_MAX_BACKOFF_SECONDS = 60
_FILENAME_UNSAFE = re.compile(r"[^A-Za-z0-9._ -]")


class SourceRetrievalError(Exception):
    """A hard stop: nothing usable can be retrieved. Message is user-facing + secret-free."""


class TokenUnavailable(Exception):
    """No ARM token could be obtained. Message is user-facing + secret-free."""


class AdfRequestError(Exception):
    """A non-successful ARM response (after retries)."""

    def __init__(self, status_code: Optional[int], url: str, code: Optional[str] = None,
                 detail: str = ""):
        self.status_code = status_code
        self.url_path = url.split("?", 1)[0]
        self.code = code
        self.detail = detail
        super().__init__(f"ARM GET returned HTTP {status_code} for {self.url_path}")


# --- ARM client --------------------------------------------------------------


class ArmFetcher:
    """GET-only ARM client: retries throttling/transient errors, follows paging."""

    def __init__(
        self,
        transport: Any,
        token: str,
        *,
        timeout: int = 30,
        max_retries: int = 5,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._transport = transport
        self._headers = {"Authorization": f"Bearer {token}"}
        self._timeout = timeout
        self._max_retries = max_retries
        self._sleep = sleep
        self.requests_made = 0

    def get(self, url: str) -> dict:
        self._check_host(url)
        attempt = 0
        while True:
            self.requests_made += 1
            try:
                resp = self._transport.get(url, headers=self._headers, timeout=self._timeout)
            except Exception as exc:  # noqa: BLE001 - network/transport failure
                if attempt >= self._max_retries:
                    raise AdfRequestError(
                        None, url, detail=f"network failure ({type(exc).__name__})"
                    ) from None
                self._sleep(self._backoff(attempt, None))
                attempt += 1
                continue

            status = getattr(resp, "status_code", None)
            if status == 200:
                try:
                    payload = resp.json()
                except Exception:  # noqa: BLE001
                    raise AdfRequestError(200, url, detail="response was not valid JSON") from None
                return payload if isinstance(payload, dict) else {}
            if status in _RETRY_STATUSES and attempt < self._max_retries:
                self._sleep(self._backoff(attempt, getattr(resp, "headers", None)))
                attempt += 1
                continue
            raise AdfRequestError(status, url, code=self._error_code(resp),
                                  detail=f"after {attempt} retr{'y' if attempt == 1 else 'ies'}")

    def list_all(self, url: str) -> list[dict]:
        """Every item of a collection, following ``nextLink`` to completion."""
        items: list[dict] = []
        seen: set[str] = set()
        next_url: Optional[str] = url
        while next_url:
            if next_url in seen:  # never loop on a repeated link
                break
            seen.add(next_url)
            payload = self.get(next_url)
            items.extend(x for x in payload.get("value", []) or [] if isinstance(x, dict))
            nxt = payload.get("nextLink")
            next_url = nxt if isinstance(nxt, str) and nxt else None
        return items

    @staticmethod
    def _check_host(url: str) -> None:
        # The bearer token must only ever be sent to ARM, even via a nextLink.
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc.lower() != _ALLOWED_HOST:
            raise AdfRequestError(None, url, detail="refused to send credentials to a non-ARM host")

    @staticmethod
    def _backoff(attempt: int, headers: Any) -> float:
        retry_after = None
        if headers is not None:
            try:
                retry_after = headers.get("Retry-After")
            except Exception:  # noqa: BLE001
                retry_after = None
        try:
            if retry_after is not None:
                return float(min(max(float(retry_after), 0.0), _MAX_BACKOFF_SECONDS))
        except (TypeError, ValueError):
            pass
        return float(min(2 ** attempt, _MAX_BACKOFF_SECONDS))

    @staticmethod
    def _error_code(resp: Any) -> Optional[str]:
        try:
            code = (resp.json() or {}).get("error", {}).get("code")
            return code if isinstance(code, str) else None
        except Exception:  # noqa: BLE001
            return None


# --- actionable messages -----------------------------------------------------


def describe_failure(exc: AdfRequestError, *, what: str, factory: str,
                     resource_group: str, subscription: str) -> tuple[str, str]:
    """(error, action) for a failed request. Contains no secret."""
    code = f" [{exc.code}]" if exc.code else ""
    s = exc.status_code
    if s == 401:
        return (
            f"Azure rejected the credentials while reading {what} (HTTP 401){code}.",
            "Run `az login` (or check the client-secret environment variables) and retry.",
        )
    if s == 403:
        return (
            f"Permission denied reading {what} (HTTP 403){code}.",
            f"Grant this identity at least the Reader role on factory '{factory}' "
            "(Data Factory Contributor also works), then retry.",
        )
    if s == 404:
        return (
            f"{what[0].upper() + what[1:]} not found (HTTP 404){code}.",
            f"Check that factory '{factory}' exists in resource group '{resource_group}' "
            f"of subscription '{subscription}', and that the active Azure CLI subscription "
            "is correct (`az account show`).",
        )
    if s == 429:
        return (
            f"Azure throttled the request for {what} (HTTP 429), {exc.detail}.",
            "Wait a few minutes and retry; reduce parallel use of this subscription.",
        )
    if s is not None and s >= 500:
        return (
            f"Azure returned a server error for {what} (HTTP {s}), {exc.detail}.",
            "Retry later; check Azure status if it persists.",
        )
    if s is None:
        return (
            f"Could not reach Azure Resource Manager while reading {what}: {exc.detail}.",
            "Check network/proxy settings and retry.",
        )
    return (
        f"Unexpected response reading {what} (HTTP {s}){code}: {exc.detail}.",
        "Retry; if it persists, report the status code.",
    )


# --- token + subscription (az CLI session reuse) ------------------------------


def _run_az(args: list[str]) -> str:
    az = shutil.which("az")
    if az is None:
        raise TokenUnavailable(
            "The Azure CLI ('az') was not found on PATH. Install it and run `az login`, "
            "or install the 'connectivity' extra and use another --auth mechanism."
        )
    proc = subprocess.run([az, *args], capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise TokenUnavailable(
            "The Azure CLI has no usable login session. Run `az login` and retry."
        )
    return proc.stdout.strip()


def default_token_provider(cred: AzureCredentialRef) -> tuple[str, str]:
    """``(token, secret)``; ``secret`` is returned only so callers can redact it."""
    try:
        credential, secret = build_credential(cred)
    except CredentialUnavailable as exc:
        if (
            exc.status == ConnectivityStatus.dependency_unavailable
            and (cred.auth or "").strip().lower() == "azure_cli"
        ):
            # No azure-identity: ask the Azure CLI directly for an ARM token.
            token = _run_az(
                ["account", "get-access-token", "--resource", cloud.ARM_RESOURCE,
                 *(["--subscription", cred.subscription_id] if cred.subscription_id else []),
                 "--query", "accessToken", "-o", "tsv"]
            )
            if not token:
                raise TokenUnavailable("The Azure CLI returned no token. Run `az login`.") from None
            return token, ""
        raise TokenUnavailable(f"{exc.detail}") from None
    try:
        return credential.get_token(arm.ARM_SCOPE).token, secret
    except Exception as exc:  # noqa: BLE001
        raise TokenUnavailable(
            "Could not acquire an Azure token: "
            f"{authentication.redact(str(exc), secret)}. Run `az login` and retry."
        ) from None


def default_subscription_resolver() -> str:
    sub = _run_az(["account", "show", "--query", "id", "-o", "tsv"])
    if not sub:
        raise TokenUnavailable("No active Azure CLI subscription. Pass --subscription.")
    return sub


# --- secret redaction ---------------------------------------------------------


def redact_artifact(node: Any, artifact_path: str, _field: str = "") -> tuple[Any, list[RedactionRecord]]:
    """Mask secret VALUES (never keys/structure); report each masking.

    Masks ``SecureString`` values and string values under credential-bearing key
    names. Key Vault references (dicts) are untouched: they hold no secret.
    """
    records: list[RedactionRecord] = []

    def walk(value: Any, path: str, depth: int) -> Any:
        if depth > 60:
            return value
        if isinstance(value, dict):
            out: dict = {}
            is_secure = value.get("type") == "SecureString"
            for k, v in value.items():
                sub = f"{path}.{k}" if path else str(k)
                if is_secure and k == "value" and isinstance(v, str) and not _is_mask(v):
                    out[k] = REDACTED
                    records.append(RedactionRecord(
                        artifact_path=artifact_path, field_path=sub, reason="secure_string"))
                elif isinstance(k, str) and k.lower() in _SECRET_KEY_HINTS \
                        and isinstance(v, str) and not _is_mask(v):
                    out[k] = REDACTED
                    records.append(RedactionRecord(
                        artifact_path=artifact_path, field_path=sub, reason="credential_field"))
                else:
                    out[k] = walk(v, sub, depth + 1)
            return out
        if isinstance(value, list):
            return [walk(v, f"{path}[{i}]", depth + 1) for i, v in enumerate(value)]
        return value

    return walk(node, _field, 0), records


def _is_mask(value: str) -> bool:
    return value == REDACTED or (bool(value) and set(value) <= {"*"})


# --- retrieval ----------------------------------------------------------------


@dataclass
class AzureRetrieval:
    factory: Optional[dict]
    items: dict[AdfArtifactKind, list[dict]] = field(default_factory=dict)
    failures: list[RetrievalFailure] = field(default_factory=list)
    requests_made: int = 0


def retrieve(
    fetcher: ArmFetcher,
    base_url: str,
    *,
    subscription: str,
    resource_group: str,
    factory_name: str,
    kinds: set[AdfArtifactKind],
) -> AzureRetrieval:
    """Read the factory and each requested collection. Raises on hard stops only."""

    def fail_hard(exc: AdfRequestError, what: str) -> SourceRetrievalError:
        err, action = describe_failure(
            exc, what=what, factory=factory_name,
            resource_group=resource_group, subscription=subscription)
        return SourceRetrievalError(f"{err} {action}")

    # The factory GET proves the target exists and carries the global parameters.
    try:
        factory_json = fetcher.get(f"{base_url}?api-version={arm.API_VERSION}")
    except AdfRequestError as exc:
        raise fail_hard(exc, f"factory '{factory_name}'") from None

    out = AzureRetrieval(factory=factory_json or None)
    for kind, segment in arm.ARTIFACT_COLLECTIONS:
        if kind not in kinds:
            continue
        url = f"{base_url}/{segment}?api-version={arm.API_VERSION}"
        try:
            items = fetcher.list_all(url)
        except AdfRequestError as exc:
            # Auth failure or no pipelines means nothing useful can follow.
            if exc.status_code == 401 or kind == AdfArtifactKind.pipeline:
                raise fail_hard(exc, f"the {segment} of factory '{factory_name}'") from None
            err, action = describe_failure(
                exc, what=f"the {segment} of factory '{factory_name}'", factory=factory_name,
                resource_group=resource_group, subscription=subscription)
            out.failures.append(RetrievalFailure(
                kind=kind.value, status_code=exc.status_code, error_code=exc.code,
                error=err, action=action))
            continue
        good: list[dict] = []
        for item in items:
            if isinstance(item.get("name"), str) and item["name"]:
                good.append(item)
            else:
                out.failures.append(RetrievalFailure(
                    kind=kind.value,
                    error=f"A {kind.value} returned by Azure has no name and cannot be stored.",
                    action="Report this to Azure support; re-run intake to confirm."))
        out.items[kind] = good
    if CDC_KIND in kinds:  # change data capture resources; a factory without the feature just has none
        url = f"{base_url}/{CDC_SEGMENT}?api-version={arm.API_VERSION}"
        try:
            items = fetcher.list_all(url)
        except AdfRequestError as exc:
            if exc.status_code == 401:
                raise fail_hard(exc, f"the {CDC_SEGMENT} of factory '{factory_name}'") from None
            if exc.status_code not in (400, 404, 405, 501):  # anything else is a real failure, reported
                err, action = describe_failure(
                    exc, what=f"the {CDC_SEGMENT} of factory '{factory_name}'", factory=factory_name,
                    resource_group=resource_group, subscription=subscription)
                out.failures.append(RetrievalFailure(
                    kind=CDC_KIND.value, status_code=exc.status_code, error_code=exc.code, error=err, action=action))
            items = []
        out.items[CDC_KIND] = [i for i in items if isinstance(i.get("name"), str) and i["name"]]
    out.requests_made = fetcher.requests_made
    return out


def _safe_filename(name: str, used: set[str]) -> str:
    base = _FILENAME_UNSAFE.sub("_", name).strip(" .") or "_"
    candidate, n = base, 1
    while candidate.lower() in used:
        n += 1
        candidate = f"{base}~{n}"
    used.add(candidate.lower())
    return f"{candidate}.json"


def write_export(staging: Path, retrieval: AzureRetrieval, kinds: set[AdfArtifactKind]
                 ) -> list[RedactionRecord]:
    """Write the ADF Git folder layout into ``staging``; return redaction records."""
    redactions: list[RedactionRecord] = []
    used_by_dir: dict[str, set[str]] = {}

    def write(kind: AdfArtifactKind, name: str, body: dict) -> None:
        folder = kind.value
        fname = _safe_filename(name, used_by_dir.setdefault(folder, set()))
        rel = f"{folder}/{fname}"
        clean, recs = redact_artifact(body, rel)
        redactions.extend(recs)
        target = staging / folder / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(clean, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    for kind, _seg in arm.ARTIFACT_COLLECTIONS:
        for item in sorted(retrieval.items.get(kind, []), key=lambda i: i["name"]):
            write(kind, item["name"], item)
    for item in sorted(retrieval.items.get(CDC_KIND, []), key=lambda i: i["name"]):
        write(CDC_KIND, item["name"], item)
    if FACTORY_KIND in kinds and retrieval.factory:
        write(FACTORY_KIND, str(retrieval.factory.get("name") or "factory"), retrieval.factory)
    return redactions


# --- orchestration ------------------------------------------------------------


class AzureSourceIntake:
    """Mode A orchestration, invoked by :class:`IntakeAgent`."""

    def __init__(
        self,
        store: ProjectStore,
        *,
        transport_factory: Callable[[], Any] = arm.default_transport,
        token_provider: Callable[[AzureCredentialRef], tuple[str, str]] = default_token_provider,
        subscription_resolver: Callable[[], str] = default_subscription_resolver,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 5,
    ):
        self.store = store
        self.transport_factory = transport_factory
        self.token_provider = token_provider
        self.subscription_resolver = subscription_resolver
        self.sleep = sleep
        self.max_retries = max_retries

    def run(self, request: AzureIntakeRequest) -> IntakeResult:
        try:
            return self._run(request)
        except (SourceRetrievalError, TokenUnavailable) as exc:
            return _failed(str(exc))  # already redacted at the raise site
        except Exception as exc:  # noqa: BLE001 - surface a clean failure, never a token
            return _failed(f"Azure intake failed unexpectedly: {type(exc).__name__}.")

    def _run(self, request: AzureIntakeRequest) -> IntakeResult:
        project_id = slugify(request.project_name)
        kinds = _resolve_kinds(request.artifact_kinds)

        credential_ref = request.credential
        if (request.subscription_id or "").strip() and not credential_ref.subscription_id:
            # Several accounts can be signed in: use the one that owns this subscription.
            credential_ref = credential_ref.model_copy(update={"subscription_id": request.subscription_id.strip()})
        token, secret = self.token_provider(credential_ref)
        try:
            subscription = (request.subscription_id or "").strip() or self.subscription_resolver()
            try:
                transport = self.transport_factory()
            except ImportError:
                raise SourceRetrievalError(
                    "The 'requests' package is not installed. Install the 'connectivity' extra."
                ) from None
            base_url = arm.factory_base_url(subscription, request.resource_group, request.factory_name)
            fetcher = ArmFetcher(transport, token, max_retries=self.max_retries, sleep=self.sleep)
            retrieval = retrieve(
                fetcher, base_url, subscription=subscription,
                resource_group=request.resource_group, factory_name=request.factory_name,
                kinds=kinds,
            )
        except (SourceRetrievalError, TokenUnavailable) as exc:
            raise type(exc)(authentication.redact(str(exc), token, secret)) from None
        finally:
            token = secret = ""  # drop promptly

        identifier = (
            f"azure://subscriptions/{subscription}/resourceGroups/{request.resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{request.factory_name}"
        )

        from adf2fabric.agent.intake.agent import _remove_tree  # lazy: avoids an import cycle

        project_dir = self.store.project_dir(project_id)
        staging = project_dir / "source.staging"
        project_dir.mkdir(parents=True, exist_ok=True)
        if staging.exists():
            _remove_tree(staging)
        staging.mkdir(parents=True)
        try:
            redactions = write_export(staging, retrieval, kinds)
            loaded = sv.load_source(staging)

            # Optional pipeline selection (+ everything it references).
            resolved_paths: list[str] = []
            scope = None
            if request.pipelines:
                closure = sv.resolve_selection(loaded, request.pipelines)
                if closure.missing_pipelines:
                    available = sorted(a.name for a in loaded.artifacts
                                       if a.kind == AdfArtifactKind.pipeline)
                    shown = ", ".join(available[:20]) + (" ..." if len(available) > 20 else "")
                    raise SourceRetrievalError(
                        f"Pipeline(s) not found in factory '{request.factory_name}': "
                        f"{', '.join(closure.missing_pipelines)}. "
                        f"Available pipelines: {shown or '(none)'}."
                    )
                keep = set(closure.paths) | {
                    a.path for a in loaded.artifacts if a.kind == FACTORY_KIND}
                for art in loaded.artifacts:
                    if art.path not in keep:
                        (staging / art.path).unlink()
                loaded = sv.load_source(staging)
                scope = closure.keys
                resolved_paths = sorted(a.path for a in loaded.artifacts)
                redactions = [r for r in redactions if r.artifact_path in keep]

            findings = sv.validate_source(loaded, scope=scope, expected_kinds=kinds)
            failures = retrieval.failures
            for f in failures:  # failed retrievals are also findings: never silent
                findings.append(SourceFinding(
                    severity=FindingSeverity.error, code="retrieval_failed",
                    message=f.error, action=f.action))

            if failures and not request.allow_partial:
                lines = "\n".join(f"  - {f.error} {f.action}" for f in failures)
                raise SourceRetrievalError(
                    f"{len(failures)} artifact retrieval(s) failed; nothing was stored.\n{lines}\n"
                    "Fix the cause, or pass --allow-partial to keep a partial source."
                )

            artifacts = sv.to_source_artifacts(loaded)
            git.init_repo(staging, EXPORT_BRANCH)
            retrieved_at = datetime.now(timezone.utc)
            commit_sha = git.commit_all(
                staging, f"ADF export of {request.factory_name} ({retrieved_at.isoformat()})")
        except BaseException:
            _remove_tree(staging)
            raise

        dest = self.store.source_dir(project_id)
        if dest.exists():
            _remove_tree(dest)
        staging.rename(dest)

        metadata = RepositoryMetadata(
            provider=PROVIDER_NAME, repository_url=identifier, branch=EXPORT_BRANCH,
            commit_sha=commit_sha, fetched_at=retrieved_at, local_source_path=str(dest))
        project = Project(id=project_id, name=request.project_name, repository=metadata)
        self.store.save(project)

        manifest = SourceManifest(
            project_id=project_id, source_type=SourceType.azure_adf,
            source_identifier=identifier, branch=EXPORT_BRANCH, version=commit_sha,
            content_sha256=sv.content_digest(artifacts), retrieved_at=retrieved_at,
            azure=AzureFactoryIdentity(
                subscription_id=subscription, resource_group=request.resource_group,
                factory_name=request.factory_name),
            selection=SourceSelection(
                pipelines=sorted(request.pipelines),
                artifact_kinds=sorted(k.value for k in kinds),
                resolved_paths=resolved_paths),
            artifacts=artifacts, failures=failures, redactions=redactions, findings=findings,
            complete=not failures and not any(
                f.severity == FindingSeverity.error for f in findings),
        )
        self.store.save_source_manifest(project_id, manifest)

        counts = ", ".join(
            f"{sum(1 for a in artifacts if a.kind == k.value)} {k.value}"
            for k in (AdfArtifactKind.pipeline, AdfArtifactKind.dataset,
                      AdfArtifactKind.linked_service, AdfArtifactKind.trigger))
        return IntakeResult(
            success=True, project=project, manifest=manifest,
            message=(f"Retrieved factory '{request.factory_name}' ({counts}) into {dest} "
                     f"as {commit_sha[:8]}."),
        )


def _failed(message: str) -> IntakeResult:
    return IntakeResult(success=False, error=message, message="Intake failed.")


def _resolve_kinds(requested: list[str]) -> set[AdfArtifactKind]:
    """Requested kinds (default: all). Pipelines are always included."""
    if not requested:
        return {k for k, _ in arm.ARTIFACT_COLLECTIONS} | {FACTORY_KIND, CDC_KIND}
    by_value = {k.value.lower(): k for k in AdfArtifactKind}
    kinds: set[AdfArtifactKind] = {AdfArtifactKind.pipeline}
    for raw in requested:
        kind = by_value.get(raw.strip().lower())
        valid = {k.lower() for k in KIND_CHOICES}
        if kind is None or kind.value.lower() not in valid:
            raise SourceRetrievalError(
                f"Unknown artifact kind '{raw}'. Choose from: {', '.join(KIND_CHOICES)}.")
        kinds.add(kind)
    return kinds
