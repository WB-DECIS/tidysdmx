"""Tests for tidysdmx.fmr — all offline; HTTP is intercepted by respx."""

import dataclasses
import sys
import threading
import types
from datetime import UTC, datetime, timedelta
from typing import (
    Any,
    NamedTuple,
    Union,
    get_args,
    get_origin,
    get_overloads,
    get_type_hints,
)
from unittest.mock import call

import httpx
import pytest
from msgspec import structs
from pysdmx.api.fmr import RegistryClient
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient, StructureAction
from pysdmx.errors import Invalid, NotFound
from pysdmx.io.format import StructureFormat
from pysdmx.model import (
    Categorisation,
    CategoryScheme,
    Codelist,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    Hierarchy,
    Metadataflow,
    MetadataProvisionAgreement,
    MetadataStructure,
    MultiRepresentationMap,
    ProvisionAgreement,
    RepresentationMap,
    StructureMap,
    TransformationScheme,
)
from typeguard import TypeCheckError

from tests.fixtures.fxtr_fmr import (
    AGENCIES_FUSION_JSON,
    AGENCIES_URL,
    CODELIST_FUSION_JSON,
    CODELIST_URL,
    DSD_URN,
    FMR_ROOT,
    FMR_SCOPE,
    NOW,
    REGISTRY_ENDPOINT,
    STRUCTURES_UPLOAD_URL,
    CredentialWithoutGetToken,
    FailingTokenProvider,
    FakeAzureCredential,
    NotABearerTokenProvider,
    SequenceTokenProvider,
)
from tidysdmx import ArtefactType, RegistryArtefact
from tidysdmx.fmr import (
    _ARTEFACT_SPECS,
    DEFAULT_REFRESH_MARGIN,
    AzureTokenProvider,
    BearerToken,
    FmrClient,
    StaticTokenProvider,
    TokenProvider,
    _BearerTokenCache,
)

ONE_HOUR = timedelta(hours=1)
LIVE_FMR_ROOT = "https://fmrqa.worldbank.org/FMR"


class _FetchCase(NamedTuple):
    """One artefact type and how FmrClient fetches it.

    ``method`` is the typed FmrClient method, ``getter`` the pysdmx getter behind
    it, ``fixture`` the fixture holding what that getter returns, and
    ``result_type`` the class (or union) the typed method returns.
    """

    artefact_type: str
    method: str
    getter: str
    fixture: str
    result_type: Any
    listed: bool = False  # pysdmx returns a list for this type


_FETCH_CASES = [
    _FetchCase("codelist", "fetch_codelist", "get_codes", "codelist", Codelist),
    _FetchCase("hierarchy", "fetch_hierarchy", "get_hierarchy", "hierarchy", Hierarchy),
    _FetchCase(
        "conceptscheme",
        "fetch_concept_scheme",
        "get_concepts",
        "concept_scheme",
        ConceptScheme,
    ),
    _FetchCase(
        "categoryscheme",
        "fetch_category_scheme",
        "get_categories",
        "category_scheme",
        CategoryScheme,
    ),
    _FetchCase(
        "categorisation",
        "fetch_categorisation",
        "get_categorisation",
        "categorisation",
        Categorisation,
    ),
    _FetchCase(
        "dataflow", "fetch_dataflow", "get_dataflows", "dataflow", Dataflow, True
    ),
    _FetchCase(
        "datastructure",
        "fetch_data_structure_definition",
        "get_data_structures",
        "data_structure_definition",
        DataStructureDefinition,
        True,
    ),
    _FetchCase(
        "provisionagreement",
        "fetch_provision_agreement",
        "get_provision_agreement",
        "provision_agreement",
        ProvisionAgreement,
    ),
    _FetchCase(
        "metadataflow",
        "fetch_metadataflow",
        "get_metadataflows",
        "metadataflow",
        Metadataflow,
        True,
    ),
    _FetchCase(
        "metadatastructure",
        "fetch_metadata_structure",
        "get_metadata_structures",
        "metadata_structure",
        MetadataStructure,
        True,
    ),
    _FetchCase(
        "metadataprovisionagreement",
        "fetch_metadata_provision_agreement",
        "get_metadata_provision_agreement",
        "metadata_provision_agreement",
        MetadataProvisionAgreement,
    ),
    _FetchCase(
        "structuremap",
        "fetch_structure_map",
        "get_mapping",
        "structure_map",
        StructureMap,
    ),
    _FetchCase(
        "representationmap",
        "fetch_representation_map",
        "get_code_map",
        "representation_map",
        RepresentationMap | MultiRepresentationMap,
    ),
    _FetchCase(
        "transformationscheme",
        "fetch_transformation_scheme",
        "get_vtl_transformation_scheme",
        "transformation_scheme",
        TransformationScheme,
    ),
]
_FETCH_CASE_IDS = [case.artefact_type for case in _FETCH_CASES]
_LISTED_CASES = [case for case in _FETCH_CASES if case.listed]
_LISTED_CASE_IDS = [case.artefact_type for case in _LISTED_CASES]


def _classes(hint: Any) -> frozenset[Any]:
    """The classes a type hint admits: the members of a union, else the hint."""
    if get_origin(hint) in (Union, types.UnionType):
        return frozenset(get_args(hint))
    return frozenset([hint])


def _mock_agencies(respx_mock):
    return respx_mock.get(AGENCIES_URL).mock(
        return_value=httpx.Response(200, content=AGENCIES_FUSION_JSON)
    )


def _mock_upload(respx_mock, status_code: int = 200):
    return respx_mock.post(STRUCTURES_UPLOAD_URL).mock(
        return_value=httpx.Response(status_code)
    )


def _mock_codelist(respx_mock):
    return respx_mock.get(CODELIST_URL).mock(
        return_value=httpx.Response(200, content=CODELIST_FUSION_JSON)
    )


def _patch_getter(monkeypatch, client, getter, result):
    """Replace one pysdmx getter on ``client.registry``; return its calls."""
    calls = []

    def fake_getter(*args, **kwargs):
        calls.append(call(*args, **kwargs))
        return result

    monkeypatch.setattr(client.registry, getter, fake_getter)
    return calls


def _patch_case(monkeypatch, request, client, case):
    """Patch the getter behind ``case`` to return its fixture; return both."""
    artefact = request.getfixturevalue(case.fixture)
    result = [artefact] if case.listed else artefact
    return artefact, _patch_getter(monkeypatch, client, case.getter, result)


class TestBearerToken:
    def test_bearer_token_defaults_to_unknown_expiry(self):
        assert BearerToken("t").expires_at is None

    def test_bearer_token_rejects_empty_token(self):
        with pytest.raises(ValueError, match="non-empty"):
            BearerToken("   ")

    def test_bearer_token_rejects_naive_expires_at(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            BearerToken("t", expires_at=datetime(2026, 1, 1, 12, 0))

    def test_bearer_token_accepts_aware_expires_at(self):
        assert BearerToken("t", expires_at=NOW).expires_at == NOW

    def test_bearer_token_repr_hides_token(self):
        assert "very-secret" not in repr(BearerToken("very-secret"))

    def test_bearer_token_rejects_non_string_token(self):
        with pytest.raises(TypeError, match="token must be a str; got int"):
            BearerToken(123)

    def test_bearer_token_rejects_non_datetime_expires_at(self):
        with pytest.raises(TypeError, match="expires_at must be a datetime"):
            BearerToken("t", expires_at="2026-09-11")

    def test_bearer_token_is_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            BearerToken("t").token = "other"


class TestTokenProvider:
    def test_token_provider_isinstance_accepts_structural_implementation(self):
        assert isinstance(SequenceTokenProvider([]), TokenProvider)

    def test_typechecked_accepts_plain_class_with_get_token(self):
        class HomeGrownProvider:
            def get_token(self) -> BearerToken:
                return BearerToken("home-grown")

        client = FmrClient(FMR_ROOT, token_provider=HomeGrownProvider())

        assert client.is_authenticated

    def test_typechecked_rejects_object_without_get_token(self):
        with pytest.raises(TypeCheckError, match="TokenProvider"):
            FmrClient(FMR_ROOT, token_provider=object())

    def test_typechecked_rejects_get_token_requiring_arguments(self):
        class NeedsAScope:
            def get_token(self, scope: str) -> BearerToken:
                return BearerToken("never")

        with pytest.raises(TypeCheckError, match="TokenProvider"):
            FmrClient(FMR_ROOT, token_provider=NeedsAScope())


class TestStaticTokenProvider:
    def test_static_token_provider_returns_same_token(self):
        provider = StaticTokenProvider("fixed")

        assert provider.get_token() is provider.get_token()
        assert provider.get_token().token == "fixed"

    def test_static_token_provider_carries_expiry(self):
        assert StaticTokenProvider("fixed", NOW).get_token().expires_at == NOW

    def test_static_token_provider_rejects_empty_token(self):
        with pytest.raises(ValueError, match="non-empty"):
            StaticTokenProvider("")

    def test_static_token_provider_repr_hides_token(self):
        assert "fixed" not in repr(StaticTokenProvider("fixed"))


class TestAzureTokenProvider:
    def test_azure_token_provider_requests_configured_scope(self):
        credential = FakeAzureCredential()

        AzureTokenProvider(credential, FMR_SCOPE).get_token()

        assert credential.scopes == [(FMR_SCOPE,)]

    def test_azure_token_provider_converts_expires_on_to_aware_utc(self):
        credential = FakeAzureCredential(token="abc", expires_on=1_800_000_000)

        token = AzureTokenProvider(credential, FMR_SCOPE).get_token()

        assert token.token == "abc"
        assert token.expires_at == datetime.fromtimestamp(1_800_000_000, tz=UTC)

    def test_azure_token_provider_exposes_scope(self):
        assert AzureTokenProvider(FakeAzureCredential(), FMR_SCOPE).scope == FMR_SCOPE

    def test_azure_token_provider_rejects_credential_without_get_token(self):
        with pytest.raises(TypeError, match="CredentialWithoutGetToken"):
            AzureTokenProvider(CredentialWithoutGetToken(), FMR_SCOPE)

    def test_azure_token_provider_strips_scope(self):
        credential = FakeAzureCredential()

        provider = AzureTokenProvider(credential, f"  {FMR_SCOPE}  ")
        provider.get_token()

        assert provider.scope == FMR_SCOPE
        assert credential.scopes == [(FMR_SCOPE,)]

    def test_azure_token_provider_rejects_blank_scope(self):
        with pytest.raises(ValueError, match="scope must be a non-empty string"):
            AzureTokenProvider(FakeAzureCredential(), "  ")

    def test_azure_token_provider_rejects_non_string_token(self):
        credential = FakeAzureCredential(token="")

        with pytest.raises(TypeError, match="no string token"):
            AzureTokenProvider(credential, FMR_SCOPE).get_token()

    def test_azure_token_provider_rejects_non_numeric_expires_on(self):
        credential = FakeAzureCredential(expires_on="soon")

        with pytest.raises(TypeError, match="no numeric expires_on"):
            AzureTokenProvider(credential, FMR_SCOPE).get_token()

    def test_azure_token_provider_propagates_credential_errors(self):
        class BrokenCredential:
            def get_token(self, *scopes: str) -> None:
                raise PermissionError("consent required")

        with pytest.raises(PermissionError, match="consent required"):
            AzureTokenProvider(BrokenCredential(), FMR_SCOPE).get_token()

    def test_azure_token_provider_repr_names_credential_and_scope(self):
        text = repr(AzureTokenProvider(FakeAzureCredential(), FMR_SCOPE))

        assert "FakeAzureCredential" in text
        assert FMR_SCOPE in text

    def test_from_default_credential_raises_import_error_without_azure_identity(
        self, monkeypatch
    ):
        monkeypatch.setitem(sys.modules, "azure.identity", None)

        with pytest.raises(ImportError, match=r"tidysdmx\[azure\]"):
            AzureTokenProvider.from_default_credential(FMR_SCOPE)

    def test_from_default_credential_builds_default_credential(self, monkeypatch):
        created: list[dict[str, object]] = []

        class RecordingDefaultAzureCredential(FakeAzureCredential):
            def __init__(self, **kwargs: object) -> None:
                super().__init__()
                created.append(kwargs)

        fake_module = types.ModuleType("azure.identity")
        fake_module.DefaultAzureCredential = RecordingDefaultAzureCredential
        monkeypatch.setitem(sys.modules, "azure.identity", fake_module)

        provider = AzureTokenProvider.from_default_credential(
            FMR_SCOPE, exclude_interactive_browser_credential=False
        )

        assert isinstance(provider, AzureTokenProvider)
        assert provider.scope == FMR_SCOPE
        assert created == [{"exclude_interactive_browser_credential": False}]


class TestBearerTokenCache:
    def test_cache_returns_cached_token_while_far_from_expiry(self, fake_clock):
        provider = SequenceTokenProvider([BearerToken("t1", NOW + ONE_HOUR)])
        cache = _BearerTokenCache(provider, clock=fake_clock)

        tokens = [cache.token() for _ in range(3)]

        assert tokens == ["t1", "t1", "t1"]
        assert provider.calls == 1

    def test_cache_refreshes_inside_margin(self, fake_clock):
        provider = SequenceTokenProvider(
            [BearerToken("t1", NOW + ONE_HOUR), BearerToken("t2", NOW + 2 * ONE_HOUR)]
        )
        cache = _BearerTokenCache(provider, clock=fake_clock)
        cache.token()

        fake_clock.advance(ONE_HOUR - DEFAULT_REFRESH_MARGIN)

        assert cache.token() == "t2"
        assert provider.calls == 2

    def test_cache_keeps_token_just_outside_margin(self, fake_clock):
        provider = SequenceTokenProvider([BearerToken("t1", NOW + ONE_HOUR)])
        cache = _BearerTokenCache(provider, clock=fake_clock)
        cache.token()

        fake_clock.advance(ONE_HOUR - DEFAULT_REFRESH_MARGIN - timedelta(seconds=1))

        assert cache.token() == "t1"
        assert provider.calls == 1

    def test_cache_honours_custom_margin(self, fake_clock):
        provider = SequenceTokenProvider(
            [BearerToken("t1", NOW + ONE_HOUR), BearerToken("t2", NOW + 2 * ONE_HOUR)]
        )
        cache = _BearerTokenCache(
            provider, refresh_margin=timedelta(minutes=30), clock=fake_clock
        )
        cache.token()

        fake_clock.advance(timedelta(minutes=31))

        assert cache.token() == "t2"

    def test_cache_always_refreshes_when_expiry_unknown(self, fake_clock):
        provider = SequenceTokenProvider(
            [BearerToken("t1"), BearerToken("t2"), BearerToken("t3")]
        )
        cache = _BearerTokenCache(provider, clock=fake_clock)

        tokens = [cache.token() for _ in range(3)]

        assert tokens == ["t1", "t2", "t3"]

    def test_cache_rejects_expired_token_from_provider(self, fake_clock):
        provider = SequenceTokenProvider(
            [BearerToken("stale", NOW - timedelta(seconds=1))]
        )
        cache = _BearerTokenCache(provider, clock=fake_clock)

        with pytest.raises(RuntimeError, match="expired at"):
            cache.token()

    def test_cache_rejects_non_bearer_token_return(self, fake_clock):
        cache = _BearerTokenCache(NotABearerTokenProvider(), clock=fake_clock)

        with pytest.raises(TypeError, match="must return a BearerToken"):
            cache.token()

    def test_cache_propagates_provider_errors(self, fake_clock):
        cache = _BearerTokenCache(FailingTokenProvider(), clock=fake_clock)

        with pytest.raises(ConnectionError, match="identity provider unreachable"):
            cache.token()

    def test_cache_rejects_negative_margin(self, fake_clock):
        with pytest.raises(ValueError, match="must not be negative"):
            _BearerTokenCache(
                SequenceTokenProvider([]),
                refresh_margin=timedelta(seconds=-1),
                clock=fake_clock,
            )

    def test_cache_serialises_concurrent_first_acquisition(self):
        provider = SequenceTokenProvider(
            [BearerToken("t1", datetime.now(UTC) + ONE_HOUR)], delay=0.05
        )
        cache = _BearerTokenCache(provider)
        barrier = threading.Barrier(4)
        results: list[str] = []

        def worker() -> None:
            barrier.wait()
            results.append(cache.token())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert results == ["t1"] * 4
        assert provider.calls == 1


class TestFmrClientInit:
    @pytest.mark.parametrize(
        "base_url",
        [
            FMR_ROOT,
            f"{FMR_ROOT}/",
            f"{FMR_ROOT}/sdmx/v2",
            f"{FMR_ROOT}/sdmx/v2/",
            f"{FMR_ROOT}/ws/secure/sdmxapi/rest",
            f"{FMR_ROOT}/ws/secure/sdmx/v2/metadata/",
            f"  {FMR_ROOT}  ",
        ],
    )
    def test_fmr_client_normalises_base_url(self, base_url):
        client = FmrClient(base_url)

        assert client.base_url == FMR_ROOT
        assert client.registry_endpoint == REGISTRY_ENDPOINT

    def test_fmr_client_accepts_host_only_url(self):
        client = FmrClient("https://fmr.example.org/")

        assert client.base_url == "https://fmr.example.org"
        assert client.registry_endpoint == "https://fmr.example.org/sdmx/v2"

    @pytest.mark.parametrize(
        "base_url", ["ftp://fmr.example.org/FMR", "fmr.example.org/FMR", "", "https://"]
    )
    def test_fmr_client_rejects_non_http_url(self, base_url):
        with pytest.raises(ValueError, match=r"absolute http\(s\) URL"):
            FmrClient(base_url)

    @pytest.mark.parametrize("base_url", [f"{FMR_ROOT}?x=1", f"{FMR_ROOT}#top"])
    def test_fmr_client_rejects_query_or_fragment(self, base_url):
        with pytest.raises(ValueError, match="query string or fragment"):
            FmrClient(base_url)

    @pytest.mark.parametrize(
        "base_url",
        [
            "https://user:hunter2@fmr.example.org/FMR",
            "https://user@fmr.example.org/FMR",
        ],
    )
    def test_fmr_client_rejects_credentials_in_url(self, base_url):
        with pytest.raises(ValueError, match="must not embed credentials") as excinfo:
            FmrClient(base_url)

        assert "hunter2" not in str(excinfo.value)

    def test_fmr_client_rejects_api_path_before_root(self):
        with pytest.raises(ValueError, match="PYSDMX-AUTH-06"):
            FmrClient("https://fmr.example.org/sdmx/v2/FMR")

    def test_fmr_client_rejects_unsupported_structure_format(self):
        with pytest.raises(ValueError, match="structure_format must be one of"):
            FmrClient(FMR_ROOT, structure_format=StructureFormat.SDMX_ML_3_0)

    def test_fmr_client_rejects_non_string_base_url(self):
        with pytest.raises(TypeCheckError):
            FmrClient(123)

    def test_fmr_client_is_anonymous_without_provider(self):
        assert not FmrClient(FMR_ROOT).is_authenticated

    def test_fmr_client_is_authenticated_with_provider(self, fmr_client):
        assert fmr_client.is_authenticated

    def test_fmr_client_does_not_acquire_token_at_construction(self, rotating_provider):
        FmrClient(FMR_ROOT, token_provider=rotating_provider)

        assert rotating_provider.calls == 0

    def test_fmr_client_repr_has_no_secrets(self, static_provider):
        text = repr(FmrClient(FMR_ROOT, token_provider=static_provider))

        assert text == f"FmrClient(base_url={FMR_ROOT!r}, authenticated=True)"


class TestFmrClientRegistry:
    def test_registry_is_a_pysdmx_registry_client(self, fmr_client):
        assert isinstance(fmr_client.registry, RegistryClient)

    def test_registry_is_built_once(self, fmr_client):
        assert fmr_client.registry is fmr_client.registry

    def test_registry_sends_bearer_and_accept_headers(self, respx_mock, fmr_client):
        _mock_agencies(respx_mock)

        agencies = fmr_client.registry.get_agencies("WB")

        request = respx_mock.calls[0].request
        assert request.headers["Authorization"] == "Bearer t1"
        assert request.headers["Accept"] == StructureFormat.FUSION_JSON.value
        assert [agency.id for agency in agencies] == ["WB.DECIS"]

    def test_registry_sends_fresh_token_on_each_request(self, respx_mock, fmr_client):
        _mock_agencies(respx_mock)
        registry = fmr_client.registry  # held across the rotation

        registry.get_agencies("WB")
        registry.get_agencies("WB")

        sent = [call.request.headers["Authorization"] for call in respx_mock.calls]
        assert sent == ["Bearer t1", "Bearer t2"]

    def test_registry_is_anonymous_without_provider(self, respx_mock):
        _mock_agencies(respx_mock)

        FmrClient(FMR_ROOT).registry.get_agencies("WB")

        assert "authorization" not in respx_mock.calls[0].request.headers

    def test_registry_is_anonymous_when_authenticate_reads_is_off(
        self, respx_mock, rotating_provider
    ):
        _mock_agencies(respx_mock)
        client = FmrClient(
            FMR_ROOT, token_provider=rotating_provider, authenticate_reads=False
        )

        client.registry.get_agencies("WB")

        assert "authorization" not in respx_mock.calls[0].request.headers
        assert rotating_provider.calls == 0

    def test_registry_passes_timeout_to_requests(self, respx_mock, rotating_provider):
        _mock_agencies(respx_mock)
        client = FmrClient(FMR_ROOT, token_provider=rotating_provider, timeout=7.5)

        client.registry.get_agencies("WB")

        assert respx_mock.calls[0].request.extensions["timeout"]["read"] == 7.5

    def test_registry_uses_sdmx_json_when_asked(self, respx_mock, rotating_provider):
        respx_mock.get(AGENCIES_URL).mock(return_value=httpx.Response(404))
        client = FmrClient(
            FMR_ROOT,
            token_provider=rotating_provider,
            structure_format=StructureFormat.SDMX_JSON_2_0_0,
        )

        with pytest.raises(NotFound, match="Not found"):
            client.registry.get_agencies("WB")

        request = respx_mock.calls[0].request
        assert request.headers["Accept"] == StructureFormat.SDMX_JSON_2_0_0.value
        assert request.headers["Authorization"] == "Bearer t1"

    def test_registry_raises_at_construction_when_pysdmx_service_seam_is_missing(
        self, monkeypatch, rotating_provider
    ):
        def init_without_service(
            self, api_endpoint, format=None, pem=None, timeout=10.0
        ):
            self.api_endpoint = api_endpoint

        monkeypatch.setattr(RegistryClient, "__init__", init_without_service)

        # Fails fast: the authenticated client is built by FmrClient.__init__.
        with pytest.raises(RuntimeError, match="PYSDMX-AUTH-01"):
            FmrClient(FMR_ROOT, token_provider=rotating_provider)


class TestFmrClientMaintenance:
    def test_maintenance_requires_token_provider(self):
        with pytest.raises(ValueError, match="token_provider"):
            FmrClient(FMR_ROOT).maintenance  # noqa: B018 - the property raises

    def test_maintenance_is_a_pysdmx_maintenance_client(self, fmr_client):
        assert isinstance(fmr_client.maintenance, RegistryMaintenanceClient)

    def test_maintenance_is_built_once(self, fmr_client):
        assert fmr_client.maintenance is fmr_client.maintenance

    def test_maintenance_does_not_acquire_token_at_construction(
        self, rotating_provider
    ):
        FmrClient(FMR_ROOT, token_provider=rotating_provider).maintenance  # noqa: B018

        assert rotating_provider.calls == 0

    def test_maintenance_posts_bearer_and_action(
        self, respx_mock, fmr_client, codelist
    ):
        _mock_upload(respx_mock)

        fmr_client.maintenance.put_structures([codelist])

        request = respx_mock.calls[0].request
        assert request.url == STRUCTURES_UPLOAD_URL
        assert request.headers["Authorization"] == "Bearer t1"
        assert request.headers["Action"] == "Replace"

    def test_maintenance_sends_fresh_token_on_each_post(
        self, respx_mock, fmr_client, codelist
    ):
        _mock_upload(respx_mock)
        maintenance = fmr_client.maintenance  # held across the rotation

        maintenance.put_structures([codelist])
        maintenance.put_structures([codelist])

        sent = [call.request.headers["Authorization"] for call in respx_mock.calls]
        assert sent == ["Bearer t1", "Bearer t2"]

    def test_maintenance_passes_write_timeout_to_requests(
        self, respx_mock, rotating_provider, codelist
    ):
        _mock_upload(respx_mock)
        client = FmrClient(
            FMR_ROOT, token_provider=rotating_provider, write_timeout=12.5
        )

        client.maintenance.put_structures([codelist])

        assert respx_mock.calls[0].request.extensions["timeout"]["read"] == 12.5

    def test_maintenance_surfaces_401_as_pysdmx_invalid(
        self, respx_mock, fmr_client, codelist
    ):
        _mock_upload(respx_mock, status_code=401)

        # PYSDMX-AUTH-03: pysdmx maps 401 to Invalid, never Unauthorized.
        with pytest.raises(Invalid, match="Client error 401"):
            fmr_client.maintenance.put_structures([codelist])

    def test_maintenance_raises_at_construction_when_pysdmx_auth_seam_is_missing(
        self, monkeypatch, rotating_provider
    ):
        monkeypatch.delattr(
            RegistryMaintenanceClient, "_RegistryMaintenanceClient__build_auth"
        )

        # Fails fast: the maintenance client is built by FmrClient.__init__.
        with pytest.raises(RuntimeError, match="PYSDMX-AUTH-02"):
            FmrClient(FMR_ROOT, token_provider=rotating_provider)


class TestFmrClientArtefactTable:
    """The spec table, the overloads, the typed methods and pysdmx agree."""

    def test_specs_cover_every_artefact_type(self):
        assert set(_ARTEFACT_SPECS) == set(get_args(ArtefactType))

    def test_cases_cover_every_artefact_type(self):
        # Keeps the parametrised tests below honest when a type is added.
        assert set(_FETCH_CASE_IDS) == set(get_args(ArtefactType))

    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_spec_names_the_pysdmx_getter(self, case):
        spec = _ARTEFACT_SPECS[case.artefact_type]

        assert (spec.getter, spec.listed) == (case.getter, case.listed)

    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_pysdmx_getter_returns_the_case_type(self, case):
        hint = get_type_hints(getattr(RegistryClient, case.getter))["return"]
        if case.listed:
            (hint,) = get_args(hint)

        assert _classes(hint) == _classes(case.result_type)

    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_typed_method_returns_the_case_type(self, case):
        hint = get_type_hints(getattr(FmrClient, case.method))["return"]

        assert _classes(hint) == _classes(case.result_type)

    def test_overloads_match_the_cases(self):
        returns = {}
        for stub in get_overloads(FmrClient.fetch_artefact):
            hints = get_type_hints(stub)
            # A Literal overload names one type; the str catch-all has no args.
            for artefact_type in get_args(hints["artefact_type"]):
                returns[artefact_type] = _classes(hints["return"])

        assert returns == {
            case.artefact_type: _classes(case.result_type) for case in _FETCH_CASES
        }

    def test_registry_artefact_covers_every_result_type(self):
        result_classes = frozenset().union(
            *(_classes(case.result_type) for case in _FETCH_CASES)
        )

        assert result_classes == _classes(RegistryArtefact)


class TestFmrClientFetchArtefact:
    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_fetch_artefact_dispatches_by_type(
        self, monkeypatch, request, fmr_client, case
    ):
        artefact, _ = _patch_case(monkeypatch, request, fmr_client, case)

        fetched = fmr_client.fetch_artefact("WB:X_TEST(1.0)", case.artefact_type)

        assert fetched is artefact

    @pytest.mark.parametrize(
        "artefact_type", ["valuelist", "Codelist", "codelists", ""]
    )
    def test_fetch_artefact_rejects_unsupported_type(self, fmr_client, artefact_type):
        with pytest.raises(ValueError, match="artefact_type must be one of"):
            fmr_client.fetch_artefact("WB:CL_TEST(1.0)", artefact_type)

    def test_fetch_artefact_points_schema_to_fetch_schema(self, fmr_client):
        with pytest.raises(ValueError, match="use fetch_schema"):
            fmr_client.fetch_artefact("WB:WDI(1.0)", "schema")

    def test_fetch_artefact_rejects_non_string_type(self, fmr_client):
        with pytest.raises(TypeCheckError, match=r'argument "artefact_type"'):
            fmr_client.fetch_artefact("WB:CL_TEST(1.0)", None)

    @pytest.mark.parametrize(
        "artefact_id",
        [
            "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_TEST(1.0)",
            "Codelist=WB:CL_TEST(1.0)",
        ],
    )
    def test_fetch_artefact_rejects_urn(self, fmr_client, artefact_id):
        with pytest.raises(ValueError, match="not a URN"):
            fmr_client.fetch_artefact(artefact_id, "codelist")

    @pytest.mark.parametrize(
        "artefact_id",
        ["WB:CL_TEST(*)", "WB:*(1.0)", "*:CL_TEST(1.0)", "WB:CL_A,CL_B(1.0)"],
    )
    def test_fetch_artefact_rejects_wildcards_and_lists(self, fmr_client, artefact_id):
        with pytest.raises(ValueError, match="must identify a single artefact"):
            fmr_client.fetch_artefact(artefact_id, "codelist")

    @pytest.mark.parametrize("version", ["~", "+"])
    def test_fetch_artefact_passes_latest_version_wildcards(
        self, monkeypatch, fmr_client, codelist, version
    ):
        calls = _patch_getter(monkeypatch, fmr_client, "get_codes", codelist)

        fmr_client.fetch_artefact(f"WB:CL_TEST({version})", "codelist")

        assert calls == [call("WB", "CL_TEST", version)]

    @pytest.mark.parametrize("artefact_id", ["CL_TEST", "WB:CL_TEST", "WB:(1.0)"])
    def test_fetch_artefact_rejects_malformed_artefact_id(
        self, fmr_client, artefact_id
    ):
        with pytest.raises(ValueError, match=r"agency:id\(version\)"):
            fmr_client.fetch_artefact(artefact_id, "codelist")

    def test_fetch_artefact_propagates_pysdmx_not_found(self, monkeypatch, fmr_client):
        def missing(agency, id, version):
            raise NotFound("Not found", "no such codelist")

        monkeypatch.setattr(fmr_client.registry, "get_codes", missing)

        with pytest.raises(NotFound, match="no such codelist"):
            fmr_client.fetch_artefact("WB:CL_TEST(1.0)", "codelist")


class TestFmrClientTypedFetchers:
    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_typed_fetcher_returns_registry_artefact(
        self, monkeypatch, request, fmr_client, case
    ):
        artefact, _ = _patch_case(monkeypatch, request, fmr_client, case)

        fetched = getattr(fmr_client, case.method)("WB:X_TEST(1.0)")

        assert fetched is artefact

    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_typed_fetcher_passes_parsed_reference_to_pysdmx_getter(
        self, monkeypatch, request, fmr_client, case
    ):
        _, calls = _patch_case(monkeypatch, request, fmr_client, case)

        getattr(fmr_client, case.method)("WB.GGH:X_TEST(1.2.0)")

        assert calls == [call("WB.GGH", "X_TEST", "1.2.0")]

    def test_fetch_representation_map_returns_multi_representation_map(
        self, monkeypatch, fmr_client, multi_representation_map
    ):
        _patch_getter(monkeypatch, fmr_client, "get_code_map", multi_representation_map)

        fetched = fmr_client.fetch_representation_map("WB:MRM_TEST(1.0)")

        assert fetched is multi_representation_map

    @pytest.mark.parametrize("case", _FETCH_CASES, ids=_FETCH_CASE_IDS)
    def test_typed_fetcher_rejects_urn(self, fmr_client, case):
        with pytest.raises(ValueError, match="not a URN"):
            getattr(fmr_client, case.method)(DSD_URN)


class TestFmrClientFetchCodelist:
    def test_fetch_codelist_parses_registry_response(self, respx_mock, codelist):
        _mock_codelist(respx_mock)

        fetched = FmrClient(FMR_ROOT).fetch_codelist("WB:CL_TEST(1.0)")

        assert fetched == codelist

    def test_fetch_codelist_reads_anonymously_without_token_provider(self, respx_mock):
        _mock_codelist(respx_mock)

        FmrClient(FMR_ROOT).fetch_codelist("WB:CL_TEST(1.0)")

        assert "authorization" not in respx_mock.calls[0].request.headers

    def test_fetch_codelist_sends_bearer_with_token_provider(
        self, respx_mock, fmr_client
    ):
        _mock_codelist(respx_mock)

        fmr_client.fetch_codelist("WB:CL_TEST(1.0)")

        assert respx_mock.calls[0].request.headers["Authorization"] == "Bearer t1"


class TestFmrClientListedFetchers:
    """pysdmx only lists these types; FmrClient returns the one requested."""

    @pytest.mark.parametrize("case", _LISTED_CASES, ids=_LISTED_CASE_IDS)
    def test_listed_fetcher_raises_not_found_when_registry_returns_none(
        self, monkeypatch, fmr_client, case
    ):
        _patch_getter(monkeypatch, fmr_client, case.getter, [])

        with pytest.raises(NotFound, match=f"no {case.artefact_type} for 'WB:X_TEST"):
            getattr(fmr_client, case.method)("WB:X_TEST(1.0)")

    @pytest.mark.parametrize("case", _LISTED_CASES, ids=_LISTED_CASE_IDS)
    def test_listed_fetcher_rejects_several_matches(
        self, monkeypatch, request, fmr_client, case
    ):
        artefact = request.getfixturevalue(case.fixture)
        newer = structs.replace(artefact, version="2.0")
        _patch_getter(monkeypatch, fmr_client, case.getter, [artefact, newer])

        with pytest.raises(
            ValueError, match=rf"matches 2 {case.artefact_type} .*1\.0, 2\.0"
        ):
            getattr(fmr_client, case.method)("WB:X_TEST(1+.0)")


class TestFmrClientFetchSchema:
    def test_fetch_schema_parses_artefact_id_and_delegates(
        self, monkeypatch, fmr_client, sdmx_schema
    ):
        calls = []

        def fake_get_schema(context, agency, id, version):
            calls.append((context, agency, id, version))
            return sdmx_schema

        monkeypatch.setattr(fmr_client.registry, "get_schema", fake_get_schema)

        schema = fmr_client.fetch_schema("WB:WDI(1.0.0)", "dataflow")

        assert schema is sdmx_schema
        assert calls == [("dataflow", "WB", "WDI", "1.0.0")]

    def test_fetch_schema_rejects_malformed_artefact_id(self, fmr_client):
        with pytest.raises(ValueError, match=r"agency:id\(version\)"):
            fmr_client.fetch_schema("WDI", "dataflow")

    def test_fetch_schema_rejects_unknown_context(self, fmr_client):
        with pytest.raises(TypeCheckError, match=r'argument "context"'):
            fmr_client.fetch_schema("WB:WDI(1.0.0)", "codelist")

    def test_fetch_schema_rejects_urn(self, fmr_client):
        with pytest.raises(ValueError, match="not a URN"):
            fmr_client.fetch_schema(DSD_URN, "datastructure")


class TestFmrClientPutStructures:
    def test_put_structures_delegates_with_default_action(
        self, respx_mock, fmr_client, codelist
    ):
        _mock_upload(respx_mock)

        fmr_client.put_structures([codelist])

        request = respx_mock.calls[0].request
        assert request.url == STRUCTURES_UPLOAD_URL
        assert request.headers["Action"] == "Replace"
        assert request.headers["Authorization"] == "Bearer t1"

    def test_put_structures_passes_action(self, respx_mock, fmr_client, codelist):
        _mock_upload(respx_mock)

        fmr_client.put_structures([codelist], action=StructureAction.Append)

        assert respx_mock.calls[0].request.headers["Action"] == "Append"

    def test_put_structures_requires_token_provider(self, codelist):
        with pytest.raises(ValueError, match="token_provider"):
            FmrClient(FMR_ROOT).put_structures([codelist])


@pytest.mark.integration
class TestFmrClientFetchLive:
    """Live reads from the World Bank's QA registry.

    The artefacts are the ones the cassette fixtures already rely on. Run with
    ``-m integration``; these tests need FMR access.
    """

    def test_fetch_data_structure_definition_reads_live_registry(self):
        dsd = FmrClient(LIVE_FMR_ROOT).fetch_data_structure_definition(
            "WB:IFPRI_ASTI(1.0)"
        )

        assert dsd.short_urn == "DataStructure=WB:IFPRI_ASTI(1.0)"

    def test_fetch_dataflow_reads_live_registry(self):
        dataflow = FmrClient(LIVE_FMR_ROOT).fetch_dataflow("WB:DF_IFPRI_ASTI(1.0)")

        assert dataflow.short_urn == "Dataflow=WB:DF_IFPRI_ASTI(1.0)"

    def test_fetch_artefact_reads_live_structure_map(self):
        structure_map = FmrClient(LIVE_FMR_ROOT).fetch_artefact(
            "WB:SM_IFPRI_ASTI_TO_DATA360(~)", "structuremap"
        )

        assert structure_map.id == "SM_IFPRI_ASTI_TO_DATA360"
