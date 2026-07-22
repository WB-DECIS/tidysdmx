from unittest.mock import MagicMock

import pytest
from pysdmx.errors import Invalid, NotFound, Unauthorized
from pysdmx.model import AgencyScheme, Codelist, Dataflow, Reference

from tidysdmx.artefact_validation import ValidationError
from tidysdmx.fmr.client import (
    _GETTERS,
    ENV_FMR_PASSWORD,
    ENV_FMR_TOKEN,
    ENV_FMR_URL,
    ENV_FMR_USER,
    FmrClient,
)

BASE_URL = "https://registry.example.org"


def _artefact(id_="X", agency="WB", version="1.0"):
    return Codelist(id=id_, agency=agency, version=version, name=id_)


def _dataflow(id_="DF_X", agency="WB", version="1.0"):
    return Dataflow(id=id_, agency=agency, version=version, name=id_)


@pytest.fixture
def client(monkeypatch):
    """A read-only FmrClient with a mocked pysdmx reader."""
    monkeypatch.delenv(ENV_FMR_USER, raising=False)
    monkeypatch.delenv(ENV_FMR_PASSWORD, raising=False)
    monkeypatch.delenv(ENV_FMR_TOKEN, raising=False)
    fmr_client = FmrClient(BASE_URL)
    fmr_client._reader = MagicMock()
    return fmr_client


class TestFmrClientInit:
    def test_fmr_client_init_env_var_fallback(self, monkeypatch):
        """URL and credentials fall back to environment variables."""
        monkeypatch.setenv(ENV_FMR_URL, BASE_URL)
        monkeypatch.setenv(ENV_FMR_USER, "alice")
        monkeypatch.setenv(ENV_FMR_PASSWORD, "secret")
        fmr_client = FmrClient()
        assert fmr_client.api_endpoint == f"{BASE_URL}/sdmx/v2"
        assert fmr_client._user == "alice"
        assert fmr_client._password == "secret"

    def test_fmr_client_init_missing_url_raises(self, monkeypatch):
        """Without a URL anywhere, construction fails."""
        monkeypatch.delenv(ENV_FMR_URL, raising=False)
        with pytest.raises(ValueError, match="No FMR base URL"):
            FmrClient()

    def test_fmr_client_init_explicit_args_beat_env(self, monkeypatch):
        """Explicit arguments take precedence over environment values."""
        monkeypatch.setenv(ENV_FMR_URL, "https://other.example.org")
        fmr_client = FmrClient(BASE_URL)
        assert fmr_client.api_endpoint == f"{BASE_URL}/sdmx/v2"

    def test_fmr_client_read_url_appends_sdmx_v2(self):
        """The /sdmx/v2 read path is appended to plain base URLs."""
        fmr_client = FmrClient(f"{BASE_URL}/FMR/")
        assert fmr_client.api_endpoint == f"{BASE_URL}/FMR/sdmx/v2"

    def test_fmr_client_read_url_preserves_existing_path(self):
        """A base URL already ending in /sdmx/v2 is used verbatim."""
        fmr_client = FmrClient(f"{BASE_URL}/FMR/sdmx/v2")
        assert fmr_client.api_endpoint == f"{BASE_URL}/FMR/sdmx/v2"
        assert fmr_client._root_url == f"{BASE_URL}/FMR"


class TestFmrClientWriter:
    def test_fmr_client_writer_without_credentials_raises_unauthorized(self, client):
        """Accessing the writer without credentials fails clearly."""
        with pytest.raises(Unauthorized, match="TIDYSDMX_FMR_USER"):
            _ = client.writer

    def test_fmr_client_writer_lazy_instantiation(self, monkeypatch):
        """The write client is only created on first access."""
        monkeypatch.delenv(ENV_FMR_TOKEN, raising=False)
        fmr_client = FmrClient(BASE_URL, user="alice", password="secret")
        assert fmr_client._writer is None
        writer = fmr_client.writer
        assert fmr_client._writer is writer
        assert fmr_client.writer is writer

    def test_fmr_client_writer_accepts_access_token(self, monkeypatch):
        """A bearer token alone is sufficient for writes."""
        monkeypatch.delenv(ENV_FMR_USER, raising=False)
        monkeypatch.delenv(ENV_FMR_PASSWORD, raising=False)
        fmr_client = FmrClient(BASE_URL, access_token="tok")
        assert fmr_client.writer is not None


class TestGetArtefact:
    @pytest.mark.parametrize(
        ("artefact_type", "method"),
        [(t, m) for t, (m, k) in _GETTERS.items() if k == "single"],
    )
    def test_get_artefact_short_urn_dispatch(self, client, artefact_type, method):
        """Each supported type dispatches to its RegistryClient getter."""
        sentinel = _artefact()
        getattr(client._reader, method).return_value = sentinel
        result = client.get_artefact(f"{artefact_type}=WB:X(1.0)")
        getattr(client._reader, method).assert_called_once_with("WB", "X", "1.0")
        assert result is sentinel

    def test_get_artefact_full_urn_dispatch(self, client):
        """Full URNs are parsed and dispatched too."""
        sentinel = _artefact()
        client._reader.get_codes.return_value = sentinel
        urn = "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_TEST(1.0)"
        assert client.get_artefact(urn) is sentinel

    def test_get_artefact_datastructure_alias(self, client):
        """SDMX 'DataStructure' short URNs map to the DSD getter."""
        client._reader.get_data_structures.return_value = [_artefact("DSD_X")]
        result = client.get_artefact("DataStructure=WB:DSD_X(1.0)")
        assert result.id == "DSD_X"

    def test_get_artefact_bare_id_requires_type(self, client):
        """Bare agency:id(version) references need artefact_type."""
        with pytest.raises(Invalid, match="artefact_type"):
            client.get_artefact("WB:CL_TEST(1.0)")

    def test_get_artefact_bare_id_with_type_dispatches(self, client):
        """Bare references plus artefact_type dispatch normally."""
        sentinel = _artefact()
        client._reader.get_codes.return_value = sentinel
        result = client.get_artefact("WB:CL_TEST(1.0)", "Codelist")
        client._reader.get_codes.assert_called_once_with("WB", "CL_TEST", "1.0")
        assert result is sentinel

    def test_get_artefact_unknown_type_raises_invalid(self, client):
        """Unsupported artefact types are rejected with the type list."""
        ref = Reference(sdmx_type="Gibberish", agency="WB", id="X", version="1.0")
        with pytest.raises(Invalid, match="Supported types"):
            client.get_artefact(ref)

    def test_get_artefact_unparseable_reference_raises(self, client):
        """Garbage references are rejected."""
        with pytest.raises(Invalid, match="Unparseable reference"):
            client.get_artefact("not a reference")

    def test_get_artefact_item_reference_rejected(self, client):
        """Item references cannot be fetched; the scheme should be."""
        with pytest.raises(Invalid, match="parent scheme"):
            client.get_artefact("Codelist=WB:CL_TEST(1.0).RED")

    def test_get_artefact_sequence_result_narrowed(self, client):
        """Sequence-returning getters are narrowed to the exact match."""
        client._reader.get_dataflows.return_value = [
            _dataflow("DF_OTHER"),
            _dataflow("DF_X"),
        ]
        result = client.get_artefact("Dataflow=WB:DF_X(1.0)")
        assert result.id == "DF_X"

    def test_get_artefact_sequence_latest_picks_highest_version(self, client):
        """With version '~', the highest version wins."""
        client._reader.get_dataflows.return_value = [
            _dataflow("DF_X", version="1.0"),
            _dataflow("DF_X", version="2.0"),
        ]
        result = client.get_artefact("Dataflow=WB:DF_X(~)")
        assert result.version == "2.0"

    def test_get_artefact_sequence_plus_token_is_wildcard(self, client):
        """The SDMX-REST '+' (latest) token is not treated as a literal."""
        client._reader.get_dataflows.return_value = [
            _dataflow("DF_X", version="1.0"),
            _dataflow("DF_X", version="2.0"),
        ]
        ref = Reference(sdmx_type="Dataflow", agency="WB", id="DF_X", version="+")
        result = client.get_artefact(ref)
        assert result.version == "2.0"

    def test_get_artefact_sequence_no_match_raises_not_found(self, client):
        """An empty narrowing result raises NotFound."""
        client._reader.get_dataflows.return_value = []
        with pytest.raises(NotFound):
            client.get_artefact("Dataflow=WB:DF_X(1.0)")

    def test_get_artefact_agency_scheme_rebuilt(self, client):
        """get_agencies results are wrapped back into an AgencyScheme."""
        from pysdmx.model import Agency

        client._reader.get_agencies.return_value = [Agency(id="WB")]
        result = client.get_artefact("AgencyScheme=WB:AGENCIES(1.0)")
        assert isinstance(result, AgencyScheme)
        assert [a.id for a in result.items] == ["WB"]


class TestGetExistingAndExists:
    def test_get_existing_returns_none_on_not_found(self, client, codelist_base):
        """A missing registry counterpart yields None."""
        client._reader.get_codes.side_effect = NotFound("nf", "nope")
        assert client.get_existing(codelist_base) is None

    def test_get_existing_fetches_latest_by_default(self, client, codelist_base):
        """The registry counterpart is fetched at the latest version."""
        sentinel = _artefact("CL_COLOUR", agency="WB.TEST")
        client._reader.get_codes.return_value = sentinel
        assert client.get_existing(codelist_base) is sentinel
        client._reader.get_codes.assert_called_once_with("WB.TEST", "CL_COLOUR", "~")

    def test_exists_true_and_false(self, client):
        """Exists reflects NotFound from the registry."""
        client._reader.get_codes.return_value = _artefact()
        assert client.exists("Codelist=WB:CL_TEST(1.0)") is True
        client._reader.get_codes.side_effect = NotFound("nf", "nope")
        assert client.exists("Codelist=WB:CL_TEST(1.0)") is False


class TestPutArtefacts:
    def test_put_artefacts_validates_before_upload(self, client, codelist_base):
        """Invalid artefacts never reach the writer."""
        import msgspec

        invalid = msgspec.structs.replace(codelist_base, items=())
        client._writer = MagicMock()
        with pytest.raises(ValidationError):
            client.put_artefacts([invalid])
        client._writer.put_structures.assert_not_called()

    def test_put_artefacts_delegates_to_writer(self, client, codelist_base):
        """Valid artefacts are handed to put_structures."""
        from pysdmx.api.fmr.maintenance import StructureAction

        client._writer = MagicMock()
        client.put_artefacts([codelist_base])
        client._writer.put_structures.assert_called_once_with(
            [codelist_base], header=None, action=StructureAction.Replace
        )

    def test_put_artefacts_skips_validation_when_disabled(self, client, codelist_base):
        """validate=False bypasses publish-readiness checks."""
        import msgspec

        invalid = msgspec.structs.replace(codelist_base, items=())
        client._writer = MagicMock()
        client.put_artefacts([invalid], validate=False)
        client._writer.put_structures.assert_called_once()


class TestGettersContract:
    @pytest.mark.parametrize("method", sorted({m for m, _ in _GETTERS.values()}))
    def test_getter_exists_on_installed_registry_client(self, method):
        """Every dispatch-table method exists on pysdmx's RegistryClient.

        Guards against silent drift when pysdmx is upgraded and a getter
        is renamed or removed.
        """
        from pysdmx.api.fmr import RegistryClient

        assert callable(getattr(RegistryClient, method))


class TestAttributeDelegation:
    def test_getattr_delegates_get_methods_to_reader(self, client):
        """Unknown get_* attributes resolve on the pysdmx reader."""
        client._reader.get_schema.return_value = "schema"
        assert client.get_schema("dataflow", "WB", "DF", "1.0") == "schema"

    def test_getattr_rejects_non_getter_attributes(self, client):
        """Non-getter attributes raise AttributeError normally."""
        with pytest.raises(AttributeError, match="no attribute"):
            _ = client.does_not_exist
