"""Tests for tidysdmx.fmr — all offline; HTTP is intercepted by respx."""

import dataclasses
import sys
import threading
import types
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pysdmx.api.fmr import RegistryClient
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient, StructureAction
from pysdmx.errors import Invalid, NotFound
from pysdmx.io.format import StructureFormat
from typeguard import TypeCheckError

from tests.fixtures.fxtr_fmr import (
    AGENCIES_FUSION_JSON,
    AGENCIES_URL,
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
from tidysdmx.fmr import (
    DEFAULT_REFRESH_MARGIN,
    AzureTokenProvider,
    BearerToken,
    FmrClient,
    StaticTokenProvider,
    TokenProvider,
    _BearerTokenCache,
)

ONE_HOUR = timedelta(hours=1)


def _mock_agencies(respx_mock):
    return respx_mock.get(AGENCIES_URL).mock(
        return_value=httpx.Response(200, content=AGENCIES_FUSION_JSON)
    )


def _mock_upload(respx_mock, status_code: int = 200):
    return respx_mock.post(STRUCTURES_UPLOAD_URL).mock(
        return_value=httpx.Response(status_code)
    )


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


class TestFmrClientGetSchema:
    def test_get_schema_parses_artefact_id_and_delegates(
        self, monkeypatch, fmr_client, sdmx_schema
    ):
        calls = []

        def fake_get_schema(context, agency, id, version):
            calls.append((context, agency, id, version))
            return sdmx_schema

        monkeypatch.setattr(fmr_client.registry, "get_schema", fake_get_schema)

        schema = fmr_client.get_schema("WB:WDI(1.0.0)", "dataflow")

        assert schema is sdmx_schema
        assert calls == [("dataflow", "WB", "WDI", "1.0.0")]

    def test_get_schema_rejects_malformed_artefact_id(self, fmr_client):
        with pytest.raises(ValueError, match=r"agency:id\(version\)"):
            fmr_client.get_schema("WDI", "dataflow")

    def test_get_schema_rejects_unknown_context(self, fmr_client):
        with pytest.raises(TypeCheckError):
            fmr_client.get_schema("WB:WDI(1.0.0)", "codelist")


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
