"""Authenticated access to a Fusion Metadata Registry, on top of pysdmx.

pysdmx already ships the two FMR clients this module needs:
:class:`pysdmx.api.fmr.RegistryClient` for reads and
:class:`pysdmx.api.fmr.maintenance.RegistryMaintenanceClient` for writes.
What pysdmx does not do is authenticate reads at all, or acquire and refresh
the bearer token that an FMR behind single sign-on (OIDC, Azure Entra ID in
this organisation) expects on every request. Left to the caller, that means
hand-wiring ``azure-identity``, watching token expiry, and rebuilding pysdmx
clients whenever the token changes.

This module fills exactly that gap:

- :class:`TokenProvider` is the one extension point. Anything with a
  ``get_token()`` returning a :class:`BearerToken` can plug in — Azure today,
  another identity provider tomorrow.
- :class:`AzureTokenProvider` wraps any ``azure-core``-style credential;
  :class:`StaticTokenProvider` serves a token obtained elsewhere.
- :class:`FmrClient` owns one registry and one token cache, and hands out
  pysdmx clients that send a fresh token on every request, reads included.
  Its ``fetch_*`` methods read artefacts and schemas given as
  ``"AGENCY:ID(VERSION)"`` or as a URN, with or without a token.

``FmrClient`` is the package's first stateful object. Hold one per registry
and reuse it; the token cache lives on it.

Everything pysdmx offers stays available: ``FmrClient.registry`` *is* a
``RegistryClient`` and ``FmrClient.maintenance`` *is* a
``RegistryMaintenanceClient``. Where this module has to reach into pysdmx
internals to get a token onto the wire, it does so through guarded, tested
seams that are registered as temporary workarounds in
``docs/pysdmx-shortcomings.md`` (``PYSDMX-AUTH-nn``); the read-side gaps its
fetch methods guard against are registered there too (``PYSDMX-READ-nn``).
"""

import logging
import re
import threading
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib import import_module
from importlib.metadata import version as _installed_version
from typing import (
    Any,
    Final,
    Literal,
    Protocol,
    Self,
    TypeAlias,
    cast,
    overload,
    runtime_checkable,
)
from urllib.parse import urlsplit, urlunsplit

import httpx
from pysdmx.api.fmr import API_VERSION, RegistryClient
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient, StructureAction
from pysdmx.api.qb import ApiVersion, RefMetaFormat, RestService, SchemaFormat
from pysdmx.errors import Invalid, NotFound
from pysdmx.io.format import StructureFormat
from pysdmx.model import (
    Agency,
    Categorisation,
    CategoryScheme,
    Codelist,
    ConceptScheme,
    Dataflow,
    DataflowInfo,
    DataProvider,
    DataStructureDefinition,
    Hierarchy,
    ItemReference,
    Metadataflow,
    MetadataProvider,
    MetadataProvisionAgreement,
    MetadataReport,
    MetadataStructure,
    MultiRepresentationMap,
    ProvisionAgreement,
    RepresentationMap,
    Schema,
    StructureMap,
    TransformationScheme,
)
from pysdmx.model.__base import MaintainableArtefact
from pysdmx.util import parse_urn
from typeguard import typechecked

from .tidysdmx import parse_artefact_id

logger = logging.getLogger(__name__)

DEFAULT_REFRESH_MARGIN: Final[timedelta] = timedelta(minutes=5)
"""How long before its expiry a cached token is replaced.

Matches azure-identity's own proactive-refresh window, so asking the
credential inside this margin makes it mint a new token rather than hand the
cached one back.
"""

REGISTRY_API_PATH: Final[str] = "/sdmx/v2"
"""Path of the SDMX 2.x REST API below the registry root."""

ArtefactType: TypeAlias = Literal[
    "codelist",
    "hierarchy",
    "conceptscheme",
    "categoryscheme",
    "categorisation",
    "dataflow",
    "datastructure",
    "provisionagreement",
    "metadataflow",
    "metadatastructure",
    "metadataprovisionagreement",
    "structuremap",
    "representationmap",
    "transformationscheme",
]
"""The artefact types :meth:`FmrClient.fetch_artefact` accepts.

These are SDMX REST resource names, the values of pysdmx's ``StructureType``,
so they extend the ``context`` vocabulary of :meth:`FmrClient.fetch_schema`.
One per pysdmx ``RegistryClient`` getter that reads a single maintainable
artefact by agency, ID and version.
"""

RegistryArtefact: TypeAlias = (
    Codelist
    | Hierarchy
    | ConceptScheme
    | CategoryScheme
    | Categorisation
    | Dataflow
    | DataStructureDefinition
    | ProvisionAgreement
    | Metadataflow
    | MetadataStructure
    | MetadataProvisionAgreement
    | StructureMap
    | RepresentationMap
    | MultiRepresentationMap
    | TransformationScheme
)
"""What :meth:`FmrClient.fetch_artefact` returns, one class per artefact type.

A representation map with several sources or targets comes back as a
``MultiRepresentationMap``.
"""

_MAINTENANCE_PATHS: Final[tuple[str, ...]] = (
    "/ws/secure/sdmx/v2/metadata",
    "/ws/secure/sdmxapi/rest",
)
_SUPPORTED_FORMATS: Final[tuple[StructureFormat, ...]] = (
    StructureFormat.FUSION_JSON,
    StructureFormat.SDMX_JSON_2_0_0,
)
_SERVICE_ATTR: Final[str] = "_RegistryClient__service"
_BUILD_AUTH_ATTR: Final[str] = "_RegistryMaintenanceClient__build_auth"
_SEAM_DOC: Final[str] = "docs/pysdmx-shortcomings.md"
_BOOTSTRAP_VALUE: Final[str] = "<supplied per request>"
_SCOPE_EXAMPLE: Final[str] = "api://<fmr-app-id>/.default"
# What an agency, ID or version may hold. pysdmx puts all three into the URL
# path escaping only "[]:+*,", so anything else ("?", "#", "/", spaces) would
# silently query a different resource (PYSDMX-READ-02).
_REFERENCE_PART: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_@$.+~-]+")
# SDMX's nested agency ID grammar, e.g. "WB" or "WB.DEC".
_AGENCY_ID: Final[re.Pattern[str]] = re.compile(
    r"[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*"
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Tokens and providers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BearerToken:
    """A bearer token and, when known, the moment it expires.

    Attributes:
        token: The raw bearer token. Excluded from ``repr`` so it never lands
            in logs or tracebacks.
        expires_at: Timezone-aware expiry, or ``None`` when the provider does
            not know it. An unknown expiry makes :class:`FmrClient` ask the
            provider before every request, which is cheap for providers that
            cache internally.

    Raises:
        TypeError: If ``token`` is not a string or ``expires_at`` is not a
            datetime.
        ValueError: If ``token`` is blank or ``expires_at`` is naive.
    """

    token: str = field(repr=False)
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        # Tokens are built by user-written providers, so the fields are a
        # boundary: narrow them before calling anything on them.
        token: object = self.token
        expires_at: object = self.expires_at
        if not isinstance(token, str):
            raise TypeError(f"token must be a str; got {type(token).__name__}")
        if not token.strip():
            raise ValueError("token must be a non-empty string")
        if expires_at is not None and not isinstance(expires_at, datetime):
            raise TypeError(
                "expires_at must be a datetime or None; "
                f"got {type(expires_at).__name__}"
            )
        if expires_at is not None and expires_at.utcoffset() is None:
            raise ValueError(
                "expires_at must be timezone-aware, e.g. datetime.now(UTC); "
                f"got naive {expires_at!r}"
            )


@runtime_checkable
class TokenProvider(Protocol):
    """Anything that can hand out a bearer token that is valid right now.

    This is the single extension point for authentication backends: implement
    it to plug an identity provider other than Azure into :class:`FmrClient`.
    Implementations must define ``get_token(self) -> BearerToken``. When a
    provider is passed to :class:`FmrClient`, typeguard checks that the method
    exists and can be called without arguments; the returned value is checked
    on first use, so a provider that returns anything but a
    :class:`BearerToken` fails at the first request with a ``TypeError``.
    """

    def get_token(self) -> BearerToken:
        """Return a token valid now, acquiring or refreshing as needed."""
        ...


@typechecked
class StaticTokenProvider:
    """Serve one pre-acquired bearer token.

    For tokens obtained outside Python — ``az account get-access-token
    --resource api://<fmr-app-id>``, a CI secret, a pipeline parameter — and
    for tests. It never refreshes: once the token expires, build a new
    provider (and a new :class:`FmrClient`).

    Args:
        token: The bearer token.
        expires_at: Its timezone-aware expiry, if known. Without it the token
            is trusted until the registry rejects it.

    Raises:
        ValueError: If ``token`` is blank or ``expires_at`` is naive.
    """

    def __init__(self, token: str, expires_at: datetime | None = None) -> None:
        self._token = BearerToken(token, expires_at)

    def get_token(self) -> BearerToken:
        """Return the configured token."""
        return self._token

    def __repr__(self) -> str:
        return f"{type(self).__name__}(expires_at={self._token.expires_at!r})"


@typechecked
class AzureTokenProvider:
    """Bearer tokens from an Azure (Entra ID) credential.

    Works with any object shaped like ``azure.core.credentials.TokenCredential``
    — every credential in ``azure-identity`` (``DefaultAzureCredential``,
    ``InteractiveBrowserCredential``, ``DeviceCodeCredential``,
    ``ClientSecretCredential``, ...) — without importing the Azure SDK itself,
    so the core package stays free of it. The credential does the caching and
    the silent refresh; this class only asks it for the FMR scope and converts
    the answer.

    Args:
        credential: An object with a callable ``get_token(*scopes)`` returning
            something with ``token`` (str) and ``expires_on`` (Unix seconds)
            attributes, as every azure-identity credential does.
        scope: The FMR application's scope, e.g.
            ``api://<fmr-app-id>/.default``. Never a Microsoft Graph scope:
            the registry validates the token's audience.

    Raises:
        TypeError: If ``credential`` has no callable ``get_token``.
        ValueError: If ``scope`` is blank.
    """

    def __init__(self, credential: object, scope: str) -> None:
        get_token = getattr(credential, "get_token", None)
        if not callable(get_token):
            raise TypeError(
                "credential must expose a callable get_token(*scopes), like "
                "azure.core.credentials.TokenCredential; got "
                f"{type(credential).__name__}"
            )
        if not scope.strip():
            raise ValueError(
                f"scope must be a non-empty string such as {_SCOPE_EXAMPLE!r}"
            )
        self._credential = credential
        self._get_token: Callable[..., object] = get_token
        self._scope = scope.strip()

    @property
    def scope(self) -> str:
        """The scope requested from the credential."""
        return self._scope

    def get_token(self) -> BearerToken:
        """Acquire a token for the configured scope.

        Returns:
            The token and its expiry as an aware UTC datetime.

        Raises:
            TypeError: If the credential's result lacks a non-empty string
                ``token`` or a numeric ``expires_on``.
        """
        raw: object = self._get_token(self._scope)
        token = getattr(raw, "token", None)
        expires_on = getattr(raw, "expires_on", None)
        if not isinstance(token, str) or not token:
            raise TypeError(
                "the credential returned no string token; expected an "
                f"azure.core.credentials.AccessToken, got {type(raw).__name__}"
            )
        if isinstance(expires_on, bool) or not isinstance(expires_on, int | float):
            raise TypeError(
                "the credential returned no numeric expires_on (Unix seconds); "
                f"got {type(expires_on).__name__}"
            )
        return BearerToken(token, datetime.fromtimestamp(expires_on, tz=UTC))

    @classmethod
    def from_default_credential(cls, scope: str, **credential_kwargs: Any) -> Self:
        """Build a provider on ``azure.identity.DefaultAzureCredential``.

        ``DefaultAzureCredential`` tries, in order, environment variables
        (``AZURE_TENANT_ID`` / ``AZURE_CLIENT_ID`` / ``AZURE_CLIENT_SECRET``),
        a managed identity, the Azure CLI (``az login``) and the other
        developer tools, so the same line works unattended and on a laptop.
        Pass ``exclude_interactive_browser_credential=False`` to fall back to a
        browser sign-in. For a device-code flow, build
        ``azure.identity.DeviceCodeCredential`` yourself and pass it to
        :class:`AzureTokenProvider`.

        Requires the ``azure`` extra: ``pip install "tidysdmx[azure]"``.

        Args:
            scope: The FMR application's scope, e.g.
                ``api://<fmr-app-id>/.default``.
            **credential_kwargs: Passed through unchanged to
                ``DefaultAzureCredential``.

        Returns:
            A provider bound to ``scope``.

        Raises:
            ImportError: If ``azure-identity`` is not installed.
            ValueError: If ``scope`` is blank.
        """
        try:
            identity = import_module("azure.identity")
        except ImportError as err:
            raise ImportError(
                "azure-identity is not installed; install the extra with "
                'pip install "tidysdmx[azure]"'
            ) from err
        return cls(identity.DefaultAzureCredential(**credential_kwargs), scope)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(credential={type(self._credential).__name__}, "
            f"scope={self._scope!r})"
        )


class _BearerTokenCache:
    """Hand out a valid bearer token, going back to the provider on demand.

    A token is reused until it is within ``refresh_margin`` of its expiry;
    a token with unknown expiry is re-requested every time. The provider is
    user code, so its result is treated as untrusted input.
    """

    def __init__(
        self,
        provider: TokenProvider,
        *,
        refresh_margin: timedelta = DEFAULT_REFRESH_MARGIN,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        if refresh_margin < timedelta(0):
            raise ValueError(
                f"refresh_margin must not be negative; got {refresh_margin!r}"
            )
        self._provider = provider
        self._refresh_margin = refresh_margin
        self._clock = clock
        self._cached: BearerToken | None = None
        self._lock = threading.Lock()

    def token(self) -> str:
        with self._lock:
            now = self._clock()
            if self._cached is None or not self._is_fresh(self._cached, now):
                self._cached = self._acquire(now)
            return self._cached.token

    def _is_fresh(self, cached: BearerToken, now: datetime) -> bool:
        return cached.expires_at is not None and (
            cached.expires_at - now > self._refresh_margin
        )

    def _acquire(self, now: datetime) -> BearerToken:
        provider_name = type(self._provider).__name__
        fresh: object = self._provider.get_token()
        if not isinstance(fresh, BearerToken):
            raise TypeError(
                f"{provider_name}.get_token() must return a BearerToken; "
                f"got {type(fresh).__name__}"
            )
        if fresh.expires_at is not None and fresh.expires_at <= now:
            raise RuntimeError(
                f"{provider_name}.get_token() returned a token that expired at "
                f"{fresh.expires_at.isoformat()}"
            )
        logger.debug(
            "Acquired an FMR bearer token from %s (expires_at=%s)",
            provider_name,
            fresh.expires_at,
        )
        return fresh


# ---------------------------------------------------------------------------
# pysdmx seams — temporary workarounds, see docs/pysdmx-shortcomings.md
# ---------------------------------------------------------------------------


class _AuthenticatedRestService(RestService):
    """``RestService`` whose requests carry a bearer token evaluated per request.

    pysdmx's read clients have no authentication hook (PYSDMX-AUTH-01).
    ``RestService`` reads ``self._headers`` on every request, so overriding it
    as a property injects a fresh ``Authorization`` header at exactly that
    moment: references held for hours keep working across token rotation.
    """

    def __init__(
        self,
        token_supplier: Callable[[], str],
        api_endpoint: str,
        api_version: ApiVersion,
        *,
        structure_format: StructureFormat,
        schema_format: SchemaFormat,
        refmeta_format: RefMetaFormat,
        pem: str | None,
        timeout: float,
    ) -> None:
        # Set before super().__init__: the base class assigns _headers there,
        # which lands in the setter below.
        self._token_supplier = token_supplier
        self._base_headers: dict[str, str] = {}
        super().__init__(
            api_endpoint,
            api_version,
            structure_format=structure_format,
            schema_format=schema_format,
            refmeta_format=refmeta_format,
            pem=pem,
            timeout=timeout,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {
            **self._base_headers,
            "Authorization": f"Bearer {self._token_supplier()}",
        }

    @_headers.setter
    def _headers(self, value: dict[str, str]) -> None:
        self._base_headers = dict(value)


class _AuthenticatedRegistryClient(RegistryClient):
    """``RegistryClient`` whose GET requests carry a bearer token (PYSDMX-AUTH-01).

    Replaces the ``RestService`` the base class builds with an
    :class:`_AuthenticatedRestService`. The attribute is name-mangled and mypy
    does not model mangling, hence ``vars(self)``.
    """

    def __init__(
        self,
        api_endpoint: str,
        token_supplier: Callable[[], str],
        *,
        structure_format: StructureFormat,
        pem: str | None,
        timeout: float,
    ) -> None:
        super().__init__(
            api_endpoint, format=structure_format, pem=pem, timeout=timeout
        )
        if _SERVICE_ATTR not in vars(self):
            raise RuntimeError(
                f"pysdmx {_installed_version('pysdmx')}: RegistryClient no longer "
                f"stores its RestService as {_SERVICE_ATTR!r}; update the seam "
                f"described in {_SEAM_DOC} (PYSDMX-AUTH-01)."
            )
        # Mirrors RegistryClient.__init__'s format mapping.
        sdmx_json = structure_format == StructureFormat.SDMX_JSON_2_0_0
        vars(self)[_SERVICE_ATTR] = _AuthenticatedRestService(
            token_supplier,
            self.api_endpoint,
            API_VERSION,
            structure_format=structure_format,
            schema_format=(
                SchemaFormat.SDMX_JSON_2_0_0_STRUCTURE
                if sdmx_json
                else SchemaFormat.FUSION_JSON
            ),
            refmeta_format=(
                RefMetaFormat.SDMX_JSON_2_0_0
                if sdmx_json
                else RefMetaFormat.FUSION_JSON
            ),
            pem=pem,
            timeout=timeout,
        )


class _SupplierBearerAuth(httpx.Auth):
    """``httpx.Auth`` that asks for the token when the request goes out."""

    def __init__(self, token_supplier: Callable[[], str]) -> None:
        self._token_supplier = token_supplier

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._token_supplier()}"
        yield request


class _RefreshingMaintenanceClient(RegistryMaintenanceClient):
    """``RegistryMaintenanceClient`` whose token is evaluated per request.

    pysdmx accepts only a static ``access_token`` string (PYSDMX-AUTH-02), but
    it builds the ``httpx.Auth`` for every POST through one private hook,
    ``__build_auth``. Overriding that hook with a :class:`_SupplierBearerAuth`
    evaluates the token exactly once per request, at the moment the request is
    sent, so references held for hours keep working across token rotation. The
    base ``__init__`` still receives a placeholder token so its "no credentials"
    check passes without asking the provider for anything.
    """

    def __init__(
        self,
        api_endpoint: str,
        token_supplier: Callable[[], str],
        *,
        pem: str | None,
        timeout: float,
    ) -> None:
        if not callable(getattr(RegistryMaintenanceClient, _BUILD_AUTH_ATTR, None)):
            raise RuntimeError(
                f"pysdmx {_installed_version('pysdmx')}: RegistryMaintenanceClient "
                f"no longer builds its request auth through {_BUILD_AUTH_ATTR!r}; "
                f"update the seam described in {_SEAM_DOC} (PYSDMX-AUTH-02)."
            )
        self._auth = _SupplierBearerAuth(token_supplier)
        super().__init__(
            api_endpoint, access_token=_BOOTSTRAP_VALUE, pem=pem, timeout=timeout
        )

    # pysdmx's name-mangled hook; the spelling is what overrides it.
    def _RegistryMaintenanceClient__build_auth(self) -> httpx.Auth:  # noqa: N802
        return self._auth


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


def _normalise_root(base_url: str) -> str:
    """Return the registry root for ``base_url``, stripping known API suffixes.

    Only a *trailing* ``/sdmx/v2`` or secure upload path is removed. pysdmx's
    maintenance client removes those fragments wherever they occur in the URL
    (PYSDMX-AUTH-06), so a root that contains one mid-path is rejected rather
    than silently mangled.
    """
    parts = urlsplit(base_url.strip())
    if parts.username is not None or parts.password is not None:
        # Deliberately does not echo the URL: it contains the secret.
        raise ValueError(
            "base_url must not embed credentials (user:password@host); "
            "authenticate with token_provider= instead"
        )
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(
            "base_url must be an absolute http(s) URL such as "
            f"'https://fmr.example.org/FMR'; got {base_url!r}"
        )
    if parts.query or parts.fragment:
        raise ValueError(
            f"base_url must not carry a query string or fragment; got {base_url!r}"
        )
    path = parts.path.rstrip("/")
    for suffix in (*_MAINTENANCE_PATHS, REGISTRY_API_PATH):
        if path.endswith(suffix):
            path = path.removesuffix(suffix).rstrip("/")
            break
    for fragment in (*_MAINTENANCE_PATHS, REGISTRY_API_PATH):
        if fragment in path:
            raise ValueError(
                f"base_url must be the registry root; {fragment!r} may only appear "
                f"at the end of the path, got {base_url!r} (see {_SEAM_DOC}, "
                "PYSDMX-AUTH-06)"
            )
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _parse_reference(
    reference: str,
    urn_classes: frozenset[str],
    *,
    argument: str = "artefact_id",
    form: str = "AGENCY:ID(VERSION)",
) -> tuple[str, str, str]:
    """Split an artefact reference into agency, ID and version for a fetch.

    ``reference`` is ``form`` or the full or short URN of an artefact whose
    SDMX class is one of ``urn_classes``. SDMX identifiers never contain ``=``,
    which every URN does. ``parse_artefact_id`` alone would read a URN as
    agency ``"urn"``, and pysdmx's single-artefact readers keep the first of
    several matches without a word (PYSDMX-READ-01), so both are stopped
    before any request.
    """
    if "=" in reference and not urn_classes:
        raise ValueError(_malformed_message(reference, urn_classes, argument, form))
    if "=" in reference:
        parts = _split_urn(reference, urn_classes, argument=argument, form=form)
    else:
        try:
            parts = parse_artefact_id(reference)
        except ValueError as err:
            raise ValueError(
                _malformed_message(reference, urn_classes, argument, form)
            ) from err
    _check_single(parts, reference, argument=argument)
    return parts


def _split_urn(
    urn: str, urn_classes: frozenset[str], *, argument: str, form: str
) -> tuple[str, str, str]:
    """Return the agency, ID and version of the URN of one whole artefact."""
    try:
        ref = parse_urn(urn)
    except Invalid as err:
        raise ValueError(_malformed_message(urn, urn_classes, argument, form)) from err
    if not ref.sdmx_type:
        raise ValueError(_malformed_message(urn, urn_classes, argument, form))
    if isinstance(ref, ItemReference):
        raise ValueError(
            f"{argument} must reference a whole artefact, not the {ref.sdmx_type} "
            f"{ref.item_id!r} inside one; got {urn!r}"
        )
    if ref.sdmx_type not in urn_classes:
        raise ValueError(
            f"{argument} is a {ref.sdmx_type} URN, but {_classes_text(urn_classes)} "
            f"URN is expected here; got {urn!r}"
        )
    return ref.agency, ref.id, ref.version


def _check_single(
    parts: tuple[str, str, str], reference: str, *, argument: str
) -> None:
    """Refuse a reference that would not select exactly one artefact."""
    if any(char in part for part in parts for char in "*,"):
        raise ValueError(
            f"{argument} must identify a single artefact; wildcards ('*') and "
            f"lists (',') are not supported, got {reference!r}. For the latest "
            "version, use '~' (or '+' for the latest stable one)."
        )
    for part in parts:
        if not _REFERENCE_PART.fullmatch(part):
            raise ValueError(
                f"{argument} {reference!r} holds {part!r}, which is not a valid "
                "SDMX agency, ID or version"
            )


def _classes_text(urn_classes: frozenset[str]) -> str:
    """Name SDMX classes for a message: ``"a Codelist or ValueList"``."""
    return "a " + " or ".join(sorted(urn_classes))


def _malformed_message(
    reference: str, urn_classes: frozenset[str], argument: str, form: str
) -> str:
    urn = f" or the URN of {_classes_text(urn_classes)}" if urn_classes else ""
    return f"{argument} must be {form!r}{urn}; got {reference!r}"


def _check_agency(agency: str) -> str:
    """Return ``agency`` if it is one SDMX agency ID, else raise.

    pysdmx's organisation-scheme getters keep the first scheme's items when
    the agency matches several (PYSDMX-READ-01), so wildcards and lists are
    refused along with anything that is not an agency ID.
    """
    if not _AGENCY_ID.fullmatch(agency):
        raise ValueError(
            "agency must be one agency ID such as 'WB' or 'WB.DEC'; wildcards, "
            f"lists and artefact references are not supported, got {agency!r}"
        )
    return agency


def _exactly_one(
    found: Sequence[RegistryArtefact], artefact_type: str, artefact_id: str
) -> RegistryArtefact:
    """Return the one artefact a pysdmx list getter found for ``artefact_id``."""
    if len(found) == 1:
        return found[0]
    if not found:
        raise NotFound(
            "Not found",
            f"The registry returned no {artefact_type} for {artefact_id!r}.",
        )
    versions = ", ".join(sorted(artefact.version for artefact in found))
    raise ValueError(
        f"artefact_id {artefact_id!r} matches {len(found)} {artefact_type} "
        f"artefacts (versions {versions}); give an exact version"
    )


@dataclass(frozen=True)
class _ArtefactSpec:
    """How :meth:`FmrClient.fetch_artefact` reads one artefact type.

    Attributes:
        getter: The ``RegistryClient`` method that reads it, called with the
            agency, ID and version.
        urn_classes: The SDMX classes a URN of it names, exactly as URNs spell
            them (``"DataStructure="``, not the REST ``datastructure``).
        listed: Whether that getter returns a list, which ``fetch_artefact``
            narrows to the single artefact requested.
    """

    getter: str
    urn_classes: frozenset[str]
    listed: bool = False


_ARTEFACT_SPECS: Final[dict[ArtefactType, _ArtefactSpec]] = {
    "codelist": _ArtefactSpec("get_codes", frozenset({"Codelist", "ValueList"})),
    "hierarchy": _ArtefactSpec("get_hierarchy", frozenset({"Hierarchy"})),
    "conceptscheme": _ArtefactSpec("get_concepts", frozenset({"ConceptScheme"})),
    "categoryscheme": _ArtefactSpec("get_categories", frozenset({"CategoryScheme"})),
    "categorisation": _ArtefactSpec(
        "get_categorisation", frozenset({"Categorisation"})
    ),
    "dataflow": _ArtefactSpec("get_dataflows", frozenset({"Dataflow"}), listed=True),
    "datastructure": _ArtefactSpec(
        "get_data_structures", frozenset({"DataStructure"}), listed=True
    ),
    "provisionagreement": _ArtefactSpec(
        "get_provision_agreement", frozenset({"ProvisionAgreement"})
    ),
    "metadataflow": _ArtefactSpec(
        "get_metadataflows", frozenset({"Metadataflow"}), listed=True
    ),
    "metadatastructure": _ArtefactSpec(
        "get_metadata_structures", frozenset({"MetadataStructure"}), listed=True
    ),
    "metadataprovisionagreement": _ArtefactSpec(
        "get_metadata_provision_agreement", frozenset({"MetadataProvisionAgreement"})
    ),
    "structuremap": _ArtefactSpec("get_mapping", frozenset({"StructureMap"})),
    "representationmap": _ArtefactSpec(
        "get_code_map", frozenset({"RepresentationMap"})
    ),
    "transformationscheme": _ArtefactSpec(
        "get_vtl_transformation_scheme", frozenset({"TransformationScheme"})
    ),
}
"""The pysdmx getter behind each artefact type, for :meth:`FmrClient.fetch_artefact`."""


def _artefact_spec(artefact_type: str) -> _ArtefactSpec:
    """Return the spec for ``artefact_type``, or raise if it is not one.

    ``artefact_type`` often comes from a configuration file, so it is checked
    here rather than left to typeguard, which ``python -O`` switches off.
    """
    if artefact_type not in _ARTEFACT_SPECS:
        allowed = ", ".join(repr(name) for name in _ARTEFACT_SPECS)
        hint = (
            " A schema is not an artefact: use fetch_schema."
            if artefact_type == "schema"
            else ""
        )
        raise ValueError(
            f"artefact_type must be one of {allowed}; got {artefact_type!r}.{hint}"
        )
    return _ARTEFACT_SPECS[artefact_type]


@typechecked
class FmrClient:
    """One Fusion Metadata Registry, with authentication handled for you.

    ``FmrClient`` builds on the two pysdmx clients rather than replacing them:
    :attr:`registry` *is* a :class:`pysdmx.api.fmr.RegistryClient` and
    :attr:`maintenance` *is* a
    :class:`pysdmx.api.fmr.maintenance.RegistryMaintenanceClient`, so
    everything pysdmx can do against a registry is available unchanged. What
    the client adds is a bearer token that is acquired lazily from the
    :class:`TokenProvider`, cached, refreshed before it expires, and sent on
    every request — reads included.

    Reads take an ``"AGENCY:ID(VERSION)"`` string or the artefact's URN, full
    (``urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_X(1.0)``) or short
    (``Codelist=WB:CL_X(1.0)``), so the URNs pysdmx returns as references, such
    as ``Dataflow.structure``, can be passed straight back:
    :meth:`fetch_artefact` for any supported artefact type, one typed method per
    type (:meth:`fetch_codelist`, :meth:`fetch_hierarchy`,
    :meth:`fetch_dataflow`, ...), and :meth:`fetch_schema`,
    :meth:`fetch_dataflow_info` and :meth:`fetch_metadata_reports`, which take
    the artefact the same way. A URN must name the class being fetched.
    :meth:`fetch_agencies`, :meth:`fetch_data_providers` and
    :meth:`fetch_metadata_providers` take an agency ID, and
    :meth:`fetch_metadata_report` a metadata set as ``"PROVIDER:ID(VERSION)"``.
    Together they wrap every getter of pysdmx's ``RegistryClient``, and they
    behave the same whether or not a token provider is set.

    This is the package's first stateful object: hold one instance per
    registry and reuse it; the token cache lives on it. When a token provider
    is given, both pysdmx clients are built immediately, so a pysdmx release
    that no longer offers the hooks this module relies on fails here rather
    than on the first request of a pipeline. Nothing touches the network, or
    asks the provider for a token, until the first request.

    Args:
        base_url: The registry root, e.g. ``https://fmrqa.worldbank.org/FMR``.
            Surrounding whitespace, trailing slashes and a trailing
            ``/sdmx/v2`` (or secure upload path) are stripped, so a pasted
            endpoint works too.
        token_provider: Where bearer tokens come from. ``None`` means
            anonymous access: reads work against a registry whose reads are
            public, and :attr:`maintenance` raises.
        structure_format: Wire format for reads, passed to pysdmx as
            ``format``. Fusion JSON, the registry-native format, by default;
            SDMX-JSON 2.0.0 is the only alternative pysdmx supports.
        pem: Path to a CA bundle for a registry behind a private certificate
            authority. Passed through to both pysdmx clients.
        timeout: Seconds before a read times out (pysdmx's default).
        write_timeout: Seconds before an upload times out (pysdmx's default).
        refresh_margin: How long before its expiry a token is replaced.
        authenticate_reads: Send the bearer token on GET requests too. Turn
            it off for a registry whose reads are public when you would rather
            not trigger an interactive sign-in for a read.

    Raises:
        ValueError: If ``base_url`` is not an absolute http(s) URL, embeds
            credentials, carries a query string or fragment, or has an API
            path fragment before the root; or if ``structure_format`` is not
            one pysdmx's registry client supports.
        RuntimeError: If a token provider is given and the installed pysdmx no
            longer exposes the hooks this module relies on (PYSDMX-AUTH-01/02).
    """

    def __init__(
        self,
        base_url: str,
        *,
        token_provider: TokenProvider | None = None,
        structure_format: StructureFormat = StructureFormat.FUSION_JSON,
        pem: str | None = None,
        timeout: float = 10.0,
        write_timeout: float = 60.0,
        refresh_margin: timedelta = DEFAULT_REFRESH_MARGIN,
        authenticate_reads: bool = True,
    ) -> None:
        if structure_format not in _SUPPORTED_FORMATS:
            supported = ", ".join(f.name for f in _SUPPORTED_FORMATS)
            raise ValueError(
                f"structure_format must be one of {supported}; "
                f"got {structure_format.name}"
            )
        self._base_url = _normalise_root(base_url)
        self._structure_format = structure_format
        self._pem = pem
        self._timeout = timeout
        self._write_timeout = write_timeout
        self._authenticate_reads = authenticate_reads
        self._cache = (
            _BearerTokenCache(token_provider, refresh_margin=refresh_margin)
            if token_provider is not None
            else None
        )
        self._registry: RegistryClient | None = None
        self._maintenance: RegistryMaintenanceClient | None = None
        if self._cache is not None:
            # Fail fast on an incompatible pysdmx (docs/pysdmx-shortcomings.md):
            # neither constructor touches the network or asks for a token.
            self._registry = self._build_registry()
            self._maintenance = self._build_maintenance()

    @property
    def base_url(self) -> str:
        """The normalised registry root, e.g. ``https://fmr.example.org/FMR``."""
        return self._base_url

    @property
    def registry_endpoint(self) -> str:
        """The SDMX REST endpoint the read client targets: ``<base_url>/sdmx/v2``."""
        return f"{self._base_url}{REGISTRY_API_PATH}"

    @property
    def is_authenticated(self) -> bool:
        """Whether a token provider was configured."""
        return self._cache is not None

    @property
    def registry(self) -> RegistryClient:
        """The pysdmx read client, built on first access and reused.

        Authenticated when a token provider is set and ``authenticate_reads``
        is on; a plain ``RegistryClient`` otherwise. Every method pysdmx
        offers — ``get_schema``, ``get_codes``, ``get_dataflows``,
        ``get_mapping``, ... — is available on it.

        Raises:
            RuntimeError: If the installed pysdmx no longer exposes the seam
                the authenticated variant relies on (PYSDMX-AUTH-01).
        """
        if self._registry is None:
            self._registry = self._build_registry()
        return self._registry

    @property
    def maintenance(self) -> RegistryMaintenanceClient:
        """The pysdmx write client, built on first access and reused.

        No token is acquired until the first upload, so reading this property
        never triggers a sign-in by itself.

        Raises:
            ValueError: If the client was created without a token provider.
            RuntimeError: If the installed pysdmx no longer exposes the seam
                the refreshing variant relies on (PYSDMX-AUTH-02).
        """
        if self._cache is None:
            raise ValueError(
                "FMR uploads require authentication, but this FmrClient was "
                "created without a token_provider; pass token_provider=... "
                "(basic auth is not supported yet)"
            )
        if self._maintenance is None:
            self._maintenance = self._build_maintenance()
        return self._maintenance

    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["codelist"]
    ) -> Codelist: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["hierarchy"]
    ) -> Hierarchy: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["conceptscheme"]
    ) -> ConceptScheme: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["categoryscheme"]
    ) -> CategoryScheme: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["categorisation"]
    ) -> Categorisation: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["dataflow"]
    ) -> Dataflow: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["datastructure"]
    ) -> DataStructureDefinition: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["provisionagreement"]
    ) -> ProvisionAgreement: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["metadataflow"]
    ) -> Metadataflow: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["metadatastructure"]
    ) -> MetadataStructure: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["metadataprovisionagreement"]
    ) -> MetadataProvisionAgreement: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["structuremap"]
    ) -> StructureMap: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["representationmap"]
    ) -> RepresentationMap | MultiRepresentationMap: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: Literal["transformationscheme"]
    ) -> TransformationScheme: ...
    @overload
    def fetch_artefact(
        self, artefact_id: str, artefact_type: str
    ) -> RegistryArtefact: ...

    def fetch_artefact(self, artefact_id: str, artefact_type: str) -> RegistryArtefact:
        """Fetch one artefact of the given type from the registry.

        The workhorse behind every typed method (:meth:`fetch_codelist`,
        :meth:`fetch_dataflow`, ...): it checks the type, splits the
        reference, calls the matching pysdmx ``RegistryClient`` getter, and
        narrows the answer of a getter that only lists to the one artefact
        requested. Call it directly when the type is data, read from a
        configuration file for instance. When the type is known in code, the
        typed method reads better; with a literal ``artefact_type`` the return
        type of this method is exact too.

        Args:
            artefact_id: The artefact identifier or URN, ``"AGENCY:ID(VERSION)"``,
                e.g. ``"WB:CL_REF_AREA(1.0)"``, or its full or short URN. The
                version may be ``~`` for the latest or ``+`` for the latest
                stable one.
            artefact_type: The SDMX REST resource name of the artefact, one
                of ``"codelist"``, ``"hierarchy"``, ``"conceptscheme"``,
                ``"categoryscheme"``, ``"categorisation"``, ``"dataflow"``,
                ``"datastructure"``, ``"provisionagreement"``,
                ``"metadataflow"``, ``"metadatastructure"``,
                ``"metadataprovisionagreement"``, ``"structuremap"``,
                ``"representationmap"`` or ``"transformationscheme"`` (the
                values of :data:`ArtefactType`). A schema is not an artefact:
                use :meth:`fetch_schema`.

        Returns:
            The artefact, as the pysdmx class its typed method returns.

        Raises:
            ValueError: If ``artefact_type`` is not one of the values above;
                if ``artefact_id`` is neither ``AGENCY:ID(VERSION)`` nor the URN
                of a whole artefact of that type, holds a wildcard or a list, or
                holds a character no SDMX identifier has; or if a dataflow, data
                structure, metadataflow or metadata structure reference matches
                several versions.
            pysdmx.errors.NotFound: If the registry has no such artefact.
            pysdmx.errors.PysdmxError: Any other registry or connection
                failure: ``Invalid`` (any other 4xx, 401 and 403 included),
                ``InternalError`` or ``Unavailable``.
        """
        spec = _artefact_spec(artefact_type)
        agency, id_part, version = _parse_reference(artefact_id, spec.urn_classes)
        fetch = getattr(self.registry, spec.getter)
        if spec.listed:
            found: Sequence[RegistryArtefact] = fetch(agency, id_part, version)
            return _exactly_one(found, artefact_type, artefact_id)
        artefact: RegistryArtefact = fetch(agency, id_part, version)
        return artefact

    def fetch_codelist(self, artefact_id: str) -> Codelist:
        """Fetch a codelist by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The codelist identifier or URN, e.g.
                ``"WB:CL_REF_AREA(1.0)"``. The version may be ``~`` (latest) or
                ``+`` (latest stable).

        Returns:
            The codelist with its codes. A value list comes back as a
            ``Codelist`` too, with ``sdmx_type == "valuelist"``: pysdmx looks
            for a codelist first, then for a value list.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                codelist (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such codelist.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "codelist")

    def fetch_hierarchy(self, artefact_id: str) -> Hierarchy:
        """Fetch a hierarchy by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The hierarchy identifier or URN. The version may be
                ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The SDMX 3.0 hierarchy, each code's name and validity resolved from
            the codelist it belongs to.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                hierarchy (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such hierarchy.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "hierarchy")

    def fetch_concept_scheme(self, artefact_id: str) -> ConceptScheme:
        """Fetch a concept scheme by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The concept scheme identifier or URN. The version may
                be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The concept scheme with its concepts.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one concept
                scheme (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such concept scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "conceptscheme")

    def fetch_category_scheme(self, artefact_id: str) -> CategoryScheme:
        """Fetch a category scheme by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The category scheme identifier or URN. The version may
                be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The category scheme with its categories.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                category scheme (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such category scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "categoryscheme")

    def fetch_categorisation(self, artefact_id: str) -> Categorisation:
        """Fetch a categorisation by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The categorisation identifier or URN. The version may
                be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The categorisation. Its ``source`` is the URN of the categorised
            artefact and its ``target`` the URN of the category.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                categorisation (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such categorisation.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "categorisation")

    def fetch_dataflow(self, artefact_id: str) -> Dataflow:
        """Fetch a dataflow by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The dataflow identifier or URN, e.g.
                ``"WB:DF_IFPRI_ASTI(1.0)"``. The version may be ``~`` (latest)
                or ``+`` (latest stable).

        Returns:
            The dataflow. Its ``structure`` is the URN of its data structure
            definition; :meth:`fetch_schema` gives the resolved components.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                dataflow (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such dataflow.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "dataflow")

    def fetch_data_structure_definition(
        self, artefact_id: str
    ) -> DataStructureDefinition:
        """Fetch a data structure definition by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The data structure definition identifier or URN, e.g.
                ``"WB:IFPRI_ASTI(1.0)"``. The version may be ``~`` (latest) or
                ``+`` (latest stable).

        Returns:
            The data structure definition, with its components and the
            concepts and codelists they use.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one data
                structure definition (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such data structure
                definition.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "datastructure")

    def fetch_provision_agreement(self, artefact_id: str) -> ProvisionAgreement:
        """Fetch a provision agreement by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The provision agreement identifier or URN. The version
                may be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The provision agreement. Its ``dataflow`` and ``provider`` are URNs.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                provision agreement (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such provision
                agreement.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "provisionagreement")

    def fetch_metadataflow(self, artefact_id: str) -> Metadataflow:
        """Fetch a metadataflow by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The metadataflow identifier or URN. The version may be
                ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The metadataflow. Its ``structure`` is the URN of its metadata
            structure and its ``targets`` the URNs of what it describes.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                metadataflow (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such metadataflow.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "metadataflow")

    def fetch_metadata_structure(self, artefact_id: str) -> MetadataStructure:
        """Fetch a metadata structure definition by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The metadata structure definition identifier or URN.
                The version may be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The metadata structure definition, with its components and the
            concepts and codelists they use.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                metadata structure definition (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such metadata
                structure definition.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "metadatastructure")

    def fetch_metadata_provision_agreement(
        self, artefact_id: str
    ) -> MetadataProvisionAgreement:
        """Fetch a metadata provision agreement by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The metadata provision agreement identifier or URN. The
                version may be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The metadata provision agreement. Its ``metadataflow`` and
            ``metadata_provider`` are URNs.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                metadata provision agreement (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such metadata
                provision agreement.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "metadataprovisionagreement")

    def fetch_structure_map(self, artefact_id: str) -> StructureMap:
        """Fetch a structure map by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The structure map identifier or URN, e.g.
                ``"WB:SM_IFPRI_ASTI_TO_DATA360(~)"``. The version may be ``~``
                (latest) or ``+`` (latest stable).

        Returns:
            The structure map, with the representation maps it uses embedded,
            as :func:`tidysdmx.map_structures` needs; ``map_structures`` does
            not apply ``DatePatternMap`` rules and raises ``TypeError`` on one.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                structure map (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such structure map.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "structuremap")

    def fetch_representation_map(
        self, artefact_id: str
    ) -> RepresentationMap | MultiRepresentationMap:
        """Fetch a representation map by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The representation map identifier or URN. The version
                may be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The representation map. One with several sources or targets comes
            back as a ``MultiRepresentationMap``.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                representation map (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such representation
                map.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "representationmap")

    def fetch_transformation_scheme(self, artefact_id: str) -> TransformationScheme:
        """Fetch a VTL transformation scheme by ``"AGENCY:ID(VERSION)"`` or URN.

        Args:
            artefact_id: The VTL transformation scheme identifier or URN. The
                version may be ``~`` (latest) or ``+`` (latest stable).

        Returns:
            The VTL transformation scheme with its transformations.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one VTL
                transformation scheme (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such VTL
                transformation scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.fetch_artefact(artefact_id, "transformationscheme")

    def fetch_schema(
        self,
        artefact_id: str,
        context: Literal["dataflow", "datastructure", "provisionagreement"],
    ) -> Schema:
        """Fetch the schema of an artefact by ``"AGENCY:ID(VERSION)"`` or URN.

        Unlike the module-level :func:`tidysdmx.fetch_schema`, this goes
        through the client's registry root, token and settings.

        Args:
            artefact_id: The artefact identifier, e.g. ``"WB:WDI(1.0.0)"``, or
                its URN, which must name the class ``context`` gives.
            context: Whether the artefact is a dataflow, a data structure or a
                provision agreement.

        Returns:
            The resolved schema, with codelists and data types attached.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                artefact of the ``context`` type (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such artefact.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        agency, id_part, version = _parse_reference(
            artefact_id, _ARTEFACT_SPECS[context].urn_classes
        )
        return self.registry.get_schema(context, agency, id_part, version)

    def fetch_dataflow_info(
        self,
        artefact_id: str,
        detail: Literal["all", "core", "providers", "schema"] = "all",
    ) -> DataflowInfo:
        """Fetch what the registry knows about a dataflow, for discovery.

        Unlike :meth:`fetch_dataflow`, which returns the ``Dataflow`` artefact,
        this returns pysdmx's ``DataflowInfo`` summary: the dataflow's name and
        description with, as ``detail`` asks, the organisations providing data
        for it and its schema. ``"all"`` and ``"schema"`` cost two extra
        requests, for the schema.

        Args:
            artefact_id: The dataflow identifier, e.g.
                ``"WB:DF_IFPRI_ASTI(1.0)"``, or its URN. Prefer an exact
                version: pysdmx matches the dataflow in the registry's answer
                against the version string, so ``+`` misses a dataflow whose
                version is not ``X.Y.Z`` and a SemVer wildcard such as
                ``1.+.0`` never matches (PYSDMX-READ-04).
            detail: ``"core"`` for the dataflow only, ``"providers"`` to add
                its data providers, ``"schema"`` to add its schema, ``"all"``
                for both.

        Returns:
            The dataflow summary.

        Raises:
            ValueError: If ``artefact_id`` does not identify exactly one
                dataflow (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no such dataflow, or
                pysdmx finds none matching the requested version in the
                registry's answer.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        agency, id_part, version = _parse_reference(
            artefact_id, _ARTEFACT_SPECS["dataflow"].urn_classes
        )
        return self.registry.get_dataflow_details(agency, id_part, version, detail)

    def fetch_agencies(self, agency: str) -> Sequence[Agency]:
        """Fetch the sub-agencies an agency maintains.

        Args:
            agency: The ID of the agency whose agency scheme to read, e.g.
                ``"WB"``.

        Returns:
            The agencies in its scheme, their IDs qualified with ``agency``
            (``"WB.DECIS"``).

        Raises:
            ValueError: If ``agency`` is not one agency ID: a wildcard, a list
                or an artefact reference.
            pysdmx.errors.NotFound: If the agency maintains no agency scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.registry.get_agencies(_check_agency(agency))

    def fetch_data_providers(
        self, agency: str, *, with_flows: bool = False
    ) -> Sequence[DataProvider]:
        """Fetch the data providers an agency maintains.

        Args:
            agency: The ID of the agency whose data provider scheme to read,
                e.g. ``"WB"``.
            with_flows: Also fill each provider's ``dataflows`` with the
                dataflows it provides data for, read from its provision
                agreements.

        Returns:
            The data providers in its scheme.

        Raises:
            ValueError: If ``agency`` is not one agency ID: a wildcard, a list
                or an artefact reference.
            pysdmx.errors.NotFound: If the agency maintains no data provider
                scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        return self.registry.get_providers(_check_agency(agency), with_flows)

    def fetch_metadata_providers(
        self, agency: str, *, with_flows: bool = False
    ) -> Sequence[MetadataProvider]:
        """Fetch the metadata providers an agency maintains.

        Args:
            agency: The ID of the agency whose metadata provider scheme to
                read, e.g. ``"WB"``.
            with_flows: Also fill each provider's ``dataflows`` with references
                to the metadataflows it provides reports for, read from its
                metadata provision agreements.

        Returns:
            The metadata providers in its scheme.

        Raises:
            ValueError: If ``agency`` is not one agency ID: a wildcard, a list
                or an artefact reference.
            pysdmx.errors.NotFound: If the agency maintains no metadata
                provider scheme.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        providers = self.registry.get_metadata_providers(
            _check_agency(agency), with_flows
        )
        # pysdmx annotates Sequence[DataProvider] but returns MetadataProvider
        # objects (PYSDMX-READ-03); mypy reports this cast as redundant once
        # the annotation is fixed upstream.
        return cast("Sequence[MetadataProvider]", providers)

    def fetch_metadata_report(self, report_id: str) -> MetadataReport:
        """Fetch a reference metadata report given as ``"PROVIDER:ID(VERSION)"``.

        Args:
            report_id: The metadata provider's ID, the metadata set ID and its
                version, e.g. ``"DECIS:MDS_QUALITY(1.0)"``. The version may be
                ``~`` (latest) or ``+`` (latest stable). URNs are not accepted
                here.

        Returns:
            The metadata report.

        Raises:
            ValueError: If ``report_id`` is not ``PROVIDER:ID(VERSION)``, or
                holds a wildcard, a list or a character no SDMX identifier has.
            pysdmx.errors.NotFound: If the registry has no such report.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        provider, id_part, version = _parse_reference(
            report_id, frozenset(), argument="report_id", form="PROVIDER:ID(VERSION)"
        )
        return self.registry.get_report(provider, id_part, version)

    def fetch_metadata_reports(
        self, artefact_id: str, artefact_type: str
    ) -> Sequence[MetadataReport]:
        """Fetch the reference metadata reports attached to an artefact.

        Args:
            artefact_id: The artefact the reports describe, as
                ``"AGENCY:ID(VERSION)"`` or its URN, e.g.
                ``"WB:DF_IFPRI_ASTI(1.0)"``.
            artefact_type: The artefact's type, one of the values of
                :data:`ArtefactType`. ``"codelist"`` searches codelists only:
                unlike :meth:`fetch_codelist`, there is no fallback to a value
                list of the same ID.

        Returns:
            The reports attached to the artefact.

        Raises:
            ValueError: If ``artefact_type`` is not an :data:`ArtefactType`
                value, or ``artefact_id`` does not identify exactly one
                artefact of that type (see :meth:`fetch_artefact`).
            pysdmx.errors.NotFound: If the registry has no reports for it.
            pysdmx.errors.PysdmxError: Any other registry or connection failure.
        """
        spec = _artefact_spec(artefact_type)
        agency, id_part, version = _parse_reference(artefact_id, spec.urn_classes)
        return self.registry.get_reports(artefact_type, agency, id_part, version)

    def put_structures(
        self,
        artefacts: Sequence[MaintainableArtefact],
        *,
        action: StructureAction = StructureAction.Replace,
    ) -> None:
        """Upload maintainable artefacts to the registry.

        A thin delegation to ``RegistryMaintenanceClient.put_structures`` with
        the bearer token supplied automatically. Pairs with
        :func:`tidysdmx.prepare_structure_map_for_upload`.

        Args:
            artefacts: The artefacts to submit — codelists, concept schemes,
                data structures, structure maps, ...
            action: How the registry treats metadata that already exists:
                ``Append``, ``Merge`` or ``Replace`` (the default).

        Raises:
            ValueError: If the client was created without a token provider.
        """
        self.maintenance.put_structures(artefacts, action=action)

    def _build_maintenance(self) -> RegistryMaintenanceClient:
        if self._cache is None:
            raise ValueError(
                "FMR uploads require authentication, but this FmrClient was "
                "created without a token_provider"
            )
        return _RefreshingMaintenanceClient(
            self._base_url,
            self._cache.token,
            pem=self._pem,
            timeout=self._write_timeout,
        )

    def _build_registry(self) -> RegistryClient:
        if self._cache is not None and self._authenticate_reads:
            return _AuthenticatedRegistryClient(
                self.registry_endpoint,
                self._cache.token,
                structure_format=self._structure_format,
                pem=self._pem,
                timeout=self._timeout,
            )
        return RegistryClient(
            self.registry_endpoint,
            format=self._structure_format,
            pem=self._pem,
            timeout=self._timeout,
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(base_url={self._base_url!r}, "
            f"authenticated={self.is_authenticated})"
        )
