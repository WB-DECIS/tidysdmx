"""Unified read/write client facade for the FMR.

:class:`FmrClient` wraps pysdmx's read-only
:class:`pysdmx.api.fmr.RegistryClient` and experimental write client
:class:`pysdmx.api.fmr.maintenance.RegistryMaintenanceClient` behind a
single constructor with:

- credential/URL resolution from arguments or environment variables
  (``TIDYSDMX_FMR_URL``, ``TIDYSDMX_FMR_USER``, ``TIDYSDMX_FMR_PASSWORD``,
  ``TIDYSDMX_FMR_TOKEN``);
- registry-agnostic URL normalization (no hardcoded ``/FMR/`` path);
- a lazily-created write client, so read-only use needs no credentials;
- a generic :meth:`FmrClient.get_artefact` that dispatches a reference
  string to the right ``get_*`` method by artefact type.

All ``get_*`` methods of the underlying read client remain available
directly on the facade (attribute delegation), and the raw clients are
exposed via :attr:`FmrClient.reader` / :attr:`FmrClient.writer`.
"""

import logging
import os
import re
from collections.abc import Sequence
from typing import Any, Literal

from pysdmx.api.fmr import RegistryClient
from pysdmx.api.fmr.maintenance import (
    RegistryMaintenanceClient,
    StructureAction,
)
from pysdmx.errors import Invalid, NotFound, Unauthorized
from pysdmx.io.format import StructureFormat
from pysdmx.model import AgencyScheme, Reference
from pysdmx.model.message import Header
from pysdmx.util import parse_urn
from typeguard import typechecked

from tidysdmx.artefact_validation import raise_if_invalid

from ._compat import MaintainableArtefact
from ._compat import agency_id as _agency_id
from .versioning import _version_sort_key

logger = logging.getLogger(__name__)

ENV_FMR_URL = "TIDYSDMX_FMR_URL"
ENV_FMR_USER = "TIDYSDMX_FMR_USER"
ENV_FMR_PASSWORD = "TIDYSDMX_FMR_PASSWORD"
ENV_FMR_TOKEN = "TIDYSDMX_FMR_TOKEN"

ArtefactType = Literal[
    "Codelist",
    "ValueList",
    "ConceptScheme",
    "CategoryScheme",
    "AgencyScheme",
    "Hierarchy",
    "DataStructure",
    "DataStructureDefinition",
    "Dataflow",
    "StructureMap",
    "RepresentationMap",
    "MultiRepresentationMap",
    "ProvisionAgreement",
    "Categorisation",
]

#: Aliases mapping SDMX short-URN class names onto pysdmx class names.
_TYPE_ALIASES: dict[str, str] = {
    "DataStructure": "DataStructureDefinition",
    "ValueList": "Codelist",
    "Valuelist": "Codelist",
}

#: Canonical artefact type -> (RegistryClient method, result kind).
_GETTERS: dict[str, tuple[str, str]] = {
    "Codelist": ("get_codes", "single"),
    "ConceptScheme": ("get_concepts", "single"),
    "CategoryScheme": ("get_categories", "single"),
    "AgencyScheme": ("get_agencies", "agencies"),
    "Hierarchy": ("get_hierarchy", "single"),
    "DataStructureDefinition": ("get_data_structures", "sequence"),
    "Dataflow": ("get_dataflows", "sequence"),
    "StructureMap": ("get_mapping", "single"),
    "RepresentationMap": ("get_code_map", "single"),
    "MultiRepresentationMap": ("get_code_map", "single"),
    "ProvisionAgreement": ("get_provision_agreement", "single"),
    "Categorisation": ("get_categorisation", "single"),
}

_BARE_REF_RE = re.compile(r"^([^:(]+):([^:(]+)\((.+)\)$")

_SDMX_V2_SUFFIX = "/sdmx/v2"


def _resolve_env(value: str | None, env_key: str) -> str | None:
    return value if value is not None else os.environ.get(env_key)


def _normalize_read_url(base_url: str) -> str:
    """Append ``/sdmx/v2`` to the base URL unless already present."""
    url = base_url.rstrip("/")
    if url.endswith(_SDMX_V2_SUFFIX):
        return url
    return url + _SDMX_V2_SUFFIX


def _registry_root(base_url: str) -> str:
    """Strip a trailing ``/sdmx/v2`` to get the registry root URL."""
    url = base_url.rstrip("/")
    return url.removesuffix(_SDMX_V2_SUFFIX)


def _normalize_type(sdmx_type: str) -> str:
    return _TYPE_ALIASES.get(sdmx_type, sdmx_type)


def _parse_ref(ref: str | Reference, artefact_type: str | None) -> Reference:
    """Normalize a reference input to a pysdmx ``Reference``.

    Accepts a ``Reference``, a full or short URN, or a bare
    ``"agency:id(version)"`` string (which requires ``artefact_type``).
    """
    if isinstance(ref, Reference):
        parsed = ref
    else:
        try:
            parsed = parse_urn(ref)  # type: ignore[assignment]
        except Invalid:
            m = _BARE_REF_RE.match(ref)
            if not m:
                raise Invalid(
                    "Unparseable reference",
                    f"Could not parse {ref!r} as a URN, short URN, or "
                    "'agency:id(version)' reference.",
                ) from None
            if artefact_type is None:
                raise Invalid(
                    "Missing artefact type",
                    f"The reference {ref!r} carries no artefact type; "
                    "pass artefact_type explicitly (e.g. 'Codelist').",
                ) from None
            parsed = Reference(
                sdmx_type=artefact_type,
                agency=m.group(1),
                id=m.group(2),
                version=m.group(3),
            )
    if hasattr(parsed, "item_id"):
        raise Invalid(
            "Item reference not supported",
            f"{ref!r} points at an item; fetch its parent scheme instead.",
        )
    if artefact_type is not None:
        parsed = Reference(
            sdmx_type=artefact_type,
            agency=parsed.agency,
            id=parsed.id,
            version=parsed.version,
        )
    return parsed


class FmrClient:
    """Unified read/write FMR client with environment-based credentials.

    Reads are served by an eagerly-created
    :class:`pysdmx.api.fmr.RegistryClient`; writes by a
    :class:`RegistryMaintenanceClient` created lazily on first use, so
    a read-only ``FmrClient`` needs no credentials.

    Examples:
        >>> import os
        >>> os.environ["TIDYSDMX_FMR_URL"] = "https://registry.example.org"
        >>> client = FmrClient()  # read-only, URL from the environment
        >>> client.api_endpoint
        'https://registry.example.org/sdmx/v2'
    """

    @typechecked
    def __init__(
        self,
        base_url: str | None = None,
        user: str | None = None,
        password: str | None = None,
        access_token: str | None = None,
        structure_format: StructureFormat = StructureFormat.SDMX_JSON_2_0_0,
        pem: str | None = None,
        read_timeout: float = 10.0,
        write_timeout: float = 60.0,
    ) -> None:
        """Instantiate the client.

        Args:
            base_url: Registry base URL (e.g. ``https://host/FMR``). The
                ``/sdmx/v2`` read path is appended automatically unless
                already present. Falls back to ``TIDYSDMX_FMR_URL``.
            user: Username for HTTP Basic authentication (writes only).
                Falls back to ``TIDYSDMX_FMR_USER``.
            password: Password for HTTP Basic authentication. Falls back
                to ``TIDYSDMX_FMR_PASSWORD``.
            access_token: Bearer token for writes; takes precedence over
                ``user``/``password``. Falls back to
                ``TIDYSDMX_FMR_TOKEN``.
            structure_format: Format used by the read client. Only
                SDMX-JSON 2.0 (default) and Fusion-JSON are supported.
            pem: Optional CA bundle for self-signed registries.
            read_timeout: Timeout (seconds) for read requests.
            write_timeout: Timeout (seconds) for write requests.

        Raises:
            ValueError: If no base URL is given and ``TIDYSDMX_FMR_URL``
                is not set.
        """
        url = _resolve_env(base_url, ENV_FMR_URL)
        if not url:
            raise ValueError(
                "No FMR base URL provided: pass base_url or set the "
                f"{ENV_FMR_URL} environment variable."
            )
        self.api_endpoint = _normalize_read_url(url)
        self._root_url = _registry_root(self.api_endpoint)
        self._user = _resolve_env(user, ENV_FMR_USER)
        self._password = _resolve_env(password, ENV_FMR_PASSWORD)
        self._access_token = _resolve_env(access_token, ENV_FMR_TOKEN)
        self._pem = pem
        self._write_timeout = write_timeout
        self._reader = RegistryClient(
            self.api_endpoint,
            format=structure_format,
            pem=pem,
            timeout=read_timeout,
        )
        self._writer: RegistryMaintenanceClient | None = None

    @property
    def reader(self) -> RegistryClient:
        """The underlying pysdmx read client."""
        return self._reader

    @property
    def writer(self) -> RegistryMaintenanceClient:
        """The underlying pysdmx write client (created on first use).

        Raises:
            Unauthorized: If neither an access token nor both user and
                password are available.
        """
        if self._writer is None:
            has_creds = self._access_token or (self._user and self._password)
            if not has_creds:
                raise Unauthorized(
                    "Missing FMR credentials",
                    "Writing to the FMR requires either an access token "
                    "or a user and password. Pass them to FmrClient() "
                    f"or set {ENV_FMR_USER}/{ENV_FMR_PASSWORD} or "
                    f"{ENV_FMR_TOKEN} in the environment.",
                )
            self._writer = RegistryMaintenanceClient(
                self._root_url,
                user=self._user,
                password=self._password,
                access_token=self._access_token,
                pem=self._pem,
                timeout=self._write_timeout,
            )
        return self._writer

    def __getattr__(self, name: str) -> Any:
        """Delegate unknown ``get_*`` attributes to the read client."""
        if name.startswith("get_"):
            return getattr(self._reader, name)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    @typechecked
    def get_artefact(
        self,
        ref: str | Reference,
        artefact_type: ArtefactType | None = None,
    ) -> MaintainableArtefact:
        """Fetch any supported artefact by reference.

        Dispatches to the matching ``RegistryClient.get_*`` method based
        on the artefact type carried by the reference.

        Args:
            ref: A short URN (``"Codelist=WB:CL_FREQ(1.0)"``), a full
                URN, a pysdmx :class:`Reference`, or a bare
                ``"agency:id(version)"`` string (in which case
                ``artefact_type`` is required). Use version ``"~"`` for
                the latest version.
            artefact_type: Artefact type for bare references; overrides
                the type carried by ``ref`` when provided.

        Returns:
            The fetched artefact.

        Raises:
            Invalid: If the reference cannot be parsed, carries no type,
                or the type is not supported.
            NotFound: If the registry has no matching artefact.

        Examples:
            >>> client.get_artefact("Codelist=SDMX:CL_FREQ(2.1)")
            ... # doctest: +SKIP
        """
        parsed = _parse_ref(ref, artefact_type)
        canonical = _normalize_type(parsed.sdmx_type)
        getter = _GETTERS.get(canonical)
        if getter is None:
            supported = ", ".join(sorted(_GETTERS))
            raise Invalid(
                "Unsupported artefact type",
                f"Cannot fetch artefacts of type {parsed.sdmx_type!r}. "
                f"Supported types: {supported}.",
            )
        method, kind = getter
        if kind == "agencies":
            agencies = getattr(self._reader, method)(parsed.agency)
            return AgencyScheme(agency=parsed.agency, items=agencies)
        if kind == "sequence":
            result = getattr(self._reader, method)(
                parsed.agency, parsed.id, parsed.version
            )
            return self._narrow_sequence(result, parsed)
        return getattr(self._reader, method)(parsed.agency, parsed.id, parsed.version)

    @staticmethod
    def _narrow_sequence(
        result: Sequence[MaintainableArtefact], ref: Reference
    ) -> MaintainableArtefact:
        """Pick the artefact matching ``ref`` out of a sequence result."""
        matches = [
            a for a in result if a.id == ref.id and _agency_id(a.agency) == ref.agency
        ]
        # SDMX-REST 2.x version tokens: "~" (latest stable), "+" (latest),
        # "*" (all). "latest" (SDMX-REST 1.x) is kept for leniency.
        if ref.version not in ("~", "+", "*", "latest"):
            matches = [a for a in matches if a.version == ref.version]
        if not matches:
            raise NotFound(
                "Not found",
                f"No {ref.sdmx_type} matching "
                f"{ref.agency}:{ref.id}({ref.version}) was returned by "
                "the registry.",
            )
        if len(matches) > 1:
            matches.sort(key=lambda a: _version_sort_key(a.version))
        return matches[-1]

    @typechecked
    def get_existing(
        self,
        artefact: MaintainableArtefact,
        version: str = "~",
    ) -> MaintainableArtefact | None:
        """Fetch the registry counterpart of a local artefact.

        Args:
            artefact: The local artefact whose registry copy to fetch.
            version: Version to fetch; defaults to ``"~"`` (latest).

        Returns:
            The registry's artefact of the same type, agency, and id, or
            ``None`` if the registry does not have it.
        """
        ref = Reference(
            sdmx_type=type(artefact).__name__,
            agency=_agency_id(artefact.agency),
            id=artefact.id,
            version=version,
        )
        try:
            return self.get_artefact(ref)
        except NotFound:
            return None

    @typechecked
    def exists(
        self,
        ref: str | Reference,
        artefact_type: ArtefactType | None = None,
    ) -> bool:
        """Check whether an artefact exists in the registry.

        Args:
            ref: The reference, as accepted by :meth:`get_artefact`.
            artefact_type: Artefact type for bare references.

        Returns:
            ``True`` if the artefact exists, ``False`` otherwise.
        """
        try:
            self.get_artefact(ref, artefact_type)
        except NotFound:
            return False
        return True

    @typechecked
    def put_artefacts(
        self,
        artefacts: Sequence[MaintainableArtefact],
        action: StructureAction = StructureAction.Replace,
        header: Header | None = None,
        validate: bool = True,
    ) -> None:
        """Upload artefacts to the registry.

        Args:
            artefacts: The maintainable artefacts to upload.
            action: How the FMR applies the changes (Append, Merge, or
                Replace). Defaults to Replace.
            header: Optional SDMX message header.
            validate: Run tidysdmx publish-readiness validation before
                uploading. Defaults to ``True``.

        Raises:
            ValidationError: If ``validate`` is true and any artefact
                fails publish-readiness checks.
            Unauthorized: If no write credentials are available.
        """
        if validate:
            raise_if_invalid(list(artefacts))
        self.writer.put_structures(artefacts, header=header, action=action)
        logger.info(
            "Uploaded %d artefact(s) to %s with action=%s.",
            len(artefacts),
            self._root_url,
            action.value,
        )
