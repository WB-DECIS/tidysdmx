# tests/fixtures/fxtr_fmr.py
"""Offline fixtures for the FMR client: fake providers, credentials and clocks.

Also one small, real pysdmx artefact per type the ``fetch_*`` methods return:
typeguard checks their return values, so a stand-in object would not do.

The test doubles are real classes with instance methods on purpose: typeguard
checks ``TokenProvider`` structurally (method presence *and* signature), which
a ``SimpleNamespace`` or a ``MagicMock`` does not satisfy. Nothing here touches
the network or imports the Azure SDK.
"""

import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import pytest
from pysdmx.model import (
    Categorisation,
    Category,
    CategoryScheme,
    Code,
    Codelist,
    Components,
    Concept,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    HierarchicalCode,
    Hierarchy,
    Metadataflow,
    MetadataProvisionAgreement,
    MetadataStructure,
    MultiRepresentationMap,
    ProvisionAgreement,
    RepresentationMap,
    StructureMap,
    Transformation,
    TransformationScheme,
)

from tidysdmx.fmr import BearerToken, FmrClient, StaticTokenProvider

FMR_ROOT = "https://fmr.example.org/FMR"
REGISTRY_ENDPOINT = f"{FMR_ROOT}/sdmx/v2"
AGENCIES_URL = f"{REGISTRY_ENDPOINT}/structure/agencyscheme/WB/"
STRUCTURES_UPLOAD_URL = f"{FMR_ROOT}/ws/secure/sdmxapi/rest"
FMR_SCOPE = "api://fmr-app/.default"

# The smallest Fusion-JSON agency scheme pysdmx's reader accepts: the scheme
# needs only ``agencyId``; each item needs ``id`` and ``names``.
AGENCIES_FUSION_JSON: bytes = (
    b'{"AgencyScheme":[{"agencyId":"WB","items":[{"id":"DECIS",'
    b'"names":[{"locale":"en","value":"Development Data Group"}]}]}]}'
)

# The smallest Fusion-JSON codelist pysdmx's reader accepts: ``id``, ``urn``,
# ``names`` and ``agencyId`` on the list, ``id`` on each code.
CODELIST_URL = f"{REGISTRY_ENDPOINT}/structure/codelist/WB/CL_TEST/1.0/"
CODELIST_FUSION_JSON: bytes = (
    b'{"Codelist":[{"id":"CL_TEST","agencyId":"WB","version":"1.0",'
    b'"urn":"urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_TEST(1.0)",'
    b'"names":[{"locale":"en","value":"Test codelist"}],'
    b'"items":[{"id":"A","names":[{"locale":"en","value":"Code A"}]}]}]}'
)

DSD_URN = "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=WB:DSD_TEST(1.0)"
DATAFLOW_URN = "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=WB:DF_TEST(1.0)"
SOURCE_DATAFLOW_URN = (
    "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=WB:DF_SOURCE(1.0)"
)
PROVIDER_URN = "urn:sdmx:org.sdmx.infomodel.base.DataProvider=WB:DATA_PROVIDERS(1.0).WB"
CATEGORY_URN = (
    "urn:sdmx:org.sdmx.infomodel.categoryscheme.Category=WB:CAT_TEST(1.0).ECO"
)
CODELIST_URN = "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_TEST(1.0)"
OTHER_CODELIST_URN = "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=WB:CL_OTHER(1.0)"
MSD_URN = (
    "urn:sdmx:org.sdmx.infomodel.metadatastructure.MetadataStructure=WB:MSD_TEST(1.0)"
)
METADATAFLOW_URN = (
    "urn:sdmx:org.sdmx.infomodel.metadatastructure.Metadataflow=WB:MDF_TEST(1.0)"
)
METADATA_PROVIDER_URN = (
    "urn:sdmx:org.sdmx.infomodel.base.MetadataProvider=WB:METADATA_PROVIDERS(1.0).DECIS"
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class FakeClock:
    """A clock the tests can move by hand."""

    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now = self.now + delta


class SequenceTokenProvider:
    """Hand out the given tokens in order and count the calls."""

    def __init__(self, tokens: Sequence[BearerToken], delay: float = 0.0) -> None:
        self._tokens = list(tokens)
        self._delay = delay
        self.calls = 0

    def get_token(self) -> BearerToken:
        if self.calls >= len(self._tokens):
            raise RuntimeError("SequenceTokenProvider has no tokens left")
        token = self._tokens[self.calls]
        self.calls += 1
        if self._delay:
            time.sleep(self._delay)
        return token


class FailingTokenProvider:
    """Raise from ``get_token`` to prove errors propagate unchanged."""

    def get_token(self) -> BearerToken:
        raise ConnectionError("identity provider unreachable")


class NotABearerTokenProvider:
    """Satisfy the protocol's signature but return the wrong type."""

    def get_token(self) -> BearerToken:
        return "just-a-string"  # type: ignore[return-value]  # deliberate: exercises the runtime guard


class FakeAccessToken(NamedTuple):
    """Shaped like ``azure.core.credentials.AccessToken``."""

    token: str
    expires_on: int | float


class FakeAzureCredential:
    """Shaped like ``azure.core.credentials.TokenCredential``; records scopes."""

    def __init__(
        self,
        token: str = "azure-access-token",
        expires_on: int | float = 1_800_000_000,
    ) -> None:
        self._token = token
        self._expires_on = expires_on
        self.scopes: list[tuple[str, ...]] = []

    def get_token(self, *scopes: str) -> FakeAccessToken:
        self.scopes.append(scopes)
        return FakeAccessToken(self._token, self._expires_on)


class CredentialWithoutGetToken:
    """Not a credential at all."""


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def rotating_provider() -> SequenceTokenProvider:
    """Tokens without expiry: re-acquired before every request, so t1 then t2."""
    return SequenceTokenProvider(
        [BearerToken("t1"), BearerToken("t2"), BearerToken("t3")]
    )


@pytest.fixture
def static_provider() -> StaticTokenProvider:
    return StaticTokenProvider("static-token")


@pytest.fixture
def fmr_client(rotating_provider: SequenceTokenProvider) -> FmrClient:
    return FmrClient(FMR_ROOT, token_provider=rotating_provider)


@pytest.fixture
def codelist() -> Codelist:
    return Codelist(
        id="CL_TEST",
        agency="WB",
        name="Test codelist",
        items=[Code(id="A", name="Code A")],
    )


@pytest.fixture
def hierarchy() -> Hierarchy:
    return Hierarchy(
        id="H_TEST",
        agency="WB",
        name="Test hierarchy",
        codes=[HierarchicalCode(id="A", name="Code A")],
    )


@pytest.fixture
def concept_scheme() -> ConceptScheme:
    return ConceptScheme(
        id="CS_TEST", agency="WB", name="Test concepts", items=[Concept(id="REF_AREA")]
    )


@pytest.fixture
def category_scheme() -> CategoryScheme:
    return CategoryScheme(
        id="CAT_TEST",
        agency="WB",
        name="Test categories",
        items=[Category(id="ECO", name="Economy")],
    )


@pytest.fixture
def dataflow() -> Dataflow:
    return Dataflow(id="DF_TEST", agency="WB", name="Test dataflow", structure=DSD_URN)


@pytest.fixture
def data_structure_definition() -> DataStructureDefinition:
    return DataStructureDefinition(
        id="DSD_TEST", agency="WB", name="Test DSD", components=Components([])
    )


@pytest.fixture
def provision_agreement() -> ProvisionAgreement:
    return ProvisionAgreement(
        id="PA_TEST",
        agency="WB",
        name="Test provision agreement",
        dataflow=DATAFLOW_URN,
        provider=PROVIDER_URN,
    )


@pytest.fixture
def structure_map() -> StructureMap:
    return StructureMap(
        id="SM_TEST",
        agency="WB",
        name="Test structure map",
        source=SOURCE_DATAFLOW_URN,
        target=DATAFLOW_URN,
        maps=[],
    )


@pytest.fixture
def categorisation() -> Categorisation:
    return Categorisation(
        id="CAT_DF_TEST",
        agency="WB",
        name="Test categorisation",
        source=DATAFLOW_URN,
        target=CATEGORY_URN,
    )


@pytest.fixture
def metadataflow() -> Metadataflow:
    return Metadataflow(
        id="MDF_TEST",
        agency="WB",
        name="Test metadataflow",
        structure=MSD_URN,
        targets=[DATAFLOW_URN],
    )


@pytest.fixture
def metadata_structure() -> MetadataStructure:
    return MetadataStructure(id="MSD_TEST", agency="WB", name="Test MSD")


@pytest.fixture
def metadata_provision_agreement() -> MetadataProvisionAgreement:
    return MetadataProvisionAgreement(
        id="MPA_TEST",
        agency="WB",
        name="Test metadata provision agreement",
        metadataflow=METADATAFLOW_URN,
        metadata_provider=METADATA_PROVIDER_URN,
    )


@pytest.fixture
def representation_map() -> RepresentationMap:
    return RepresentationMap(
        id="RM_TEST",
        agency="WB",
        name="Test representation map",
        source=CODELIST_URN,
        target=OTHER_CODELIST_URN,
        maps=[],
    )


@pytest.fixture
def multi_representation_map() -> MultiRepresentationMap:
    return MultiRepresentationMap(
        id="MRM_TEST",
        agency="WB",
        name="Test multi-representation map",
        source=[CODELIST_URN, OTHER_CODELIST_URN],
        target=[OTHER_CODELIST_URN],
        maps=[],
    )


@pytest.fixture
def transformation_scheme() -> TransformationScheme:
    return TransformationScheme(
        id="TS_TEST",
        agency="WB",
        name="Test transformation scheme",
        vtl_version="2.1",
        items=[Transformation(id="T1", expression="DS_1", result="DS_r")],
    )
