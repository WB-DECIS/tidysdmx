# tests/fixtures/fxtr_fmr.py
"""Offline fixtures for the FMR client: fake providers, credentials and clocks.

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
from pysdmx.model import Code, Codelist

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
