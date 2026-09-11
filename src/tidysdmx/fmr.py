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

``FmrClient`` is the package's first stateful object. Hold one per registry
and reuse it; the token cache lives on it.

Everything pysdmx offers stays available: ``FmrClient.registry`` *is* a
``RegistryClient`` and ``FmrClient.maintenance`` *is* a
``RegistryMaintenanceClient``. Where this module has to reach into pysdmx
internals to get a token onto the wire, it does so through guarded, tested
seams that are registered as temporary workarounds in
``docs/pysdmx-shortcomings.md`` (``PYSDMX-AUTH-nn``).
"""

import logging
import threading
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib import import_module
from importlib.metadata import version as _installed_version
from typing import Any, Final, Literal, Protocol, Self, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

import httpx
from pysdmx.api.fmr import API_VERSION, RegistryClient
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient, StructureAction
from pysdmx.api.qb import ApiVersion, RefMetaFormat, RestService, SchemaFormat
from pysdmx.io.format import StructureFormat
from pysdmx.model import Schema
from pysdmx.model.__base import MaintainableArtefact
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
        ValueError: If ``token`` is blank or ``expires_at`` is naive.
    """

    token: str = field(repr=False)
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.token.strip():
            raise ValueError("token must be a non-empty string")
        if self.expires_at is not None and self.expires_at.utcoffset() is None:
            raise ValueError(
                "expires_at must be timezone-aware, e.g. datetime.now(UTC); "
                f"got naive {self.expires_at!r}"
            )


@runtime_checkable
class TokenProvider(Protocol):
    """Anything that can hand out a bearer token that is valid right now.

    This is the single extension point for authentication backends: implement
    it to plug an identity provider other than Azure into :class:`FmrClient`.
    Implementations must define exactly ``get_token(self) -> BearerToken``;
    typeguard compares the signature structurally when a provider is passed
    to :class:`FmrClient`.
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
        self._scope = scope

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

    This is the package's first stateful object: hold one instance per
    registry and reuse it; the token cache lives on it. Nothing touches the
    network, or asks the provider for a token, until the first request.

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
        ValueError: If ``base_url`` is not an absolute http(s) URL, carries a
            query string or fragment, or has an API path fragment before the
            root; or if ``structure_format`` is not one pysdmx's registry
            client supports.
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
            self._maintenance = _RefreshingMaintenanceClient(
                self._base_url,
                self._cache.token,
                pem=self._pem,
                timeout=self._write_timeout,
            )
        return self._maintenance

    def get_schema(
        self,
        artefact_id: str,
        context: Literal["dataflow", "datastructure", "provisionagreement"],
    ) -> Schema:
        """Fetch the schema of an artefact given as ``"AGENCY:ID(VERSION)"``.

        The authenticated counterpart of :func:`tidysdmx.fetch_schema`.

        Args:
            artefact_id: The artefact identifier, e.g. ``"WB:WDI(1.0.0)"``.
            context: Whether the artefact is a dataflow, a data structure or a
                provision agreement.

        Returns:
            The resolved schema, with codelists and data types attached.

        Raises:
            ValueError: If ``artefact_id`` is not ``agency:id(version)``.
        """
        agency, id_part, version = parse_artefact_id(artefact_id)
        return self.registry.get_schema(context, agency, id_part, version)

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
