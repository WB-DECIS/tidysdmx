"""Tests for scripts/diagnose_fmr_auth.py — all offline; HTTP is intercepted by respx.

The script is a developer tool, not part of the package, so it is loaded from
its path rather than imported. Only the steps that need neither a sign-in nor
a registry are covered; the rest are exercised by running the script.
"""

import base64
import dataclasses
import importlib.util
import json
import logging
import sys
import types
from pathlib import Path

import httpx
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "diagnose_fmr_auth.py"
ROOT = "https://fmr.example.org/FMR"
CODELIST = "WB.TEST:CL_TEST(1.0)"
PUBLIC_URL = f"{ROOT}/sdmx/v2/structure/codelist/WB.TEST/CL_TEST/1.0"
SECURE_URL = f"{ROOT}/ws/secure/sdmxapi/rest/codelist/WB.TEST/CL_TEST/1.0"


def _load_script() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("diagnose_fmr_auth", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


diagnose = _load_script()


def _jwt(payload: object) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    return f"header.{encoded.rstrip('=')}.signature"


def _settings(**changes: object) -> object:
    base = diagnose.Settings(
        root=ROOT,
        scope="app-id/.default",
        client_id=None,
        tenant_id=None,
        codelist=CODELIST,
        credential="default",
        try_id_token=False,
    )
    return dataclasses.replace(base, **changes)


class _RecordingCredential:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


@pytest.fixture
def fake_identity(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """A stand-in for azure.identity whose credentials record their arguments."""
    module = types.ModuleType("azure.identity")
    for name in (
        "DefaultAzureCredential",
        "InteractiveBrowserCredential",
        "DeviceCodeCredential",
    ):
        setattr(module, name, type(name, (_RecordingCredential,), {}))
    monkeypatch.setitem(sys.modules, "azure.identity", module)
    # The default credential attaches a handler to this logger; keep it local.
    logger = logging.getLogger("azure.identity")
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logger.level)
    return module


class TestDecodeClaims:
    def test_decode_claims_reads_the_payload(self):
        assert diagnose.decode_claims(_jwt({"aud": "app-id"})) == {"aud": "app-id"}

    def test_decode_claims_rejects_a_token_without_three_parts(self):
        with pytest.raises(ValueError, match="three dot-separated parts"):
            diagnose.decode_claims("not-a-jwt")

    def test_decode_claims_rejects_a_payload_that_is_not_an_object(self):
        with pytest.raises(ValueError, match="not a JSON object"):
            diagnose.decode_claims(_jwt(["aud"]))


class TestCodelistProbes:
    def test_codelist_probes_targets_the_public_and_secure_apis(self):
        probes = diagnose.codelist_probes(ROOT, CODELIST)

        assert [probe.url for probe in probes] == [PUBLIC_URL, SECURE_URL]

    def test_codelist_probes_lets_only_the_public_api_decide(self):
        probes = diagnose.codelist_probes(ROOT, CODELIST)

        assert [probe.counts for probe in probes] == [True, False]

    def test_codelist_probes_rejects_a_malformed_reference(self):
        with pytest.raises(ValueError, match="not an AGENCY:ID"):
            diagnose.codelist_probes(ROOT, "CL_TEST")


class TestParseArgs:
    def test_parse_args_falls_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv("TIDYSDMX_FMR_ROOT", f"{ROOT}/")
        monkeypatch.setenv("TIDYSDMX_FMR_SCOPE", "app-id/.default")
        monkeypatch.setenv("TIDYSDMX_FMR_CLIENT_ID", "client-id")
        monkeypatch.setenv("TIDYSDMX_FMR_TENANT_ID", "tenant-id")

        settings = diagnose.parse_args([])

        assert settings == _settings(client_id="client-id", tenant_id="tenant-id")

    def test_parse_args_requires_a_scope(self, monkeypatch, capsys):
        monkeypatch.delenv("TIDYSDMX_FMR_SCOPE", raising=False)

        with pytest.raises(SystemExit, match=r"^2$"):
            diagnose.parse_args([])
        assert "pass --scope" in capsys.readouterr().err

    def test_parse_args_rejects_a_malformed_codelist(self, capsys):
        with pytest.raises(SystemExit, match=r"^2$"):
            diagnose.parse_args(["--scope", "app-id/.default", "--codelist", "CL"])
        assert "--codelist must be AGENCY:ID(VERSION)" in capsys.readouterr().err

    def test_parse_args_rejects_a_root_without_tls(self, capsys):
        with pytest.raises(SystemExit, match=r"^2$"):
            diagnose.parse_args(
                ["--scope", "app-id/.default", "--root", "http://fmr.example.org"]
            )
        assert "--root must be an https URL" in capsys.readouterr().err

    def test_parse_args_rejects_a_root_without_tls_from_the_environment(
        self, monkeypatch, capsys
    ):
        monkeypatch.setenv("TIDYSDMX_FMR_ROOT", "http://fmr.example.org")

        with pytest.raises(SystemExit, match=r"^2$"):
            diagnose.parse_args(["--scope", "app-id/.default"])
        assert "--root must be an https URL" in capsys.readouterr().err


class TestCheckAnonymousReads:
    @pytest.mark.parametrize("status", [200, 401, 403])
    def test_check_anonymous_reads_passes_when_the_registry_answers(
        self, respx_mock, status
    ):
        respx_mock.get(PUBLIC_URL).mock(return_value=httpx.Response(status))
        respx_mock.get(SECURE_URL).mock(return_value=httpx.Response(401))

        assert diagnose.check_anonymous_reads(diagnose.codelist_probes(ROOT, CODELIST))

    def test_check_anonymous_reads_fails_when_the_codelist_is_missing(self, respx_mock):
        respx_mock.get(PUBLIC_URL).mock(return_value=httpx.Response(404))
        respx_mock.get(SECURE_URL).mock(return_value=httpx.Response(404))

        assert not diagnose.check_anonymous_reads(
            diagnose.codelist_probes(ROOT, CODELIST)
        )

    def test_check_anonymous_reads_fails_when_the_registry_does_not_answer(
        self, respx_mock
    ):
        respx_mock.get(PUBLIC_URL).mock(side_effect=httpx.ConnectError("refused"))
        respx_mock.get(SECURE_URL).mock(side_effect=httpx.ConnectError("refused"))

        assert not diagnose.check_anonymous_reads(
            diagnose.codelist_probes(ROOT, CODELIST)
        )


class TestProbeWithToken:
    def test_probe_with_token_ignores_the_secure_api_answer(self, respx_mock):
        respx_mock.get(PUBLIC_URL).mock(return_value=httpx.Response(200))
        respx_mock.get(SECURE_URL).mock(return_value=httpx.Response(405))

        assert diagnose.probe_with_token(
            5, "offer", diagnose.codelist_probes(ROOT, CODELIST), "token"
        )

    def test_probe_with_token_fails_when_the_public_api_refuses(self, respx_mock):
        respx_mock.get(PUBLIC_URL).mock(return_value=httpx.Response(401))
        respx_mock.get(SECURE_URL).mock(return_value=httpx.Response(200))

        assert not diagnose.probe_with_token(
            5, "offer", diagnose.codelist_probes(ROOT, CODELIST), "token"
        )


class TestBuildCredential:
    def test_build_credential_returns_none_without_azure_identity(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "azure.identity", None)

        assert diagnose.build_credential(_settings()) is None

    def test_build_credential_defaults_to_the_default_chain(self, fake_identity):
        credential = diagnose.build_credential(_settings())

        assert isinstance(credential, fake_identity.DefaultAzureCredential)

    @pytest.mark.parametrize(
        ("changes", "expected_kwargs"),
        [
            ({"client_id": "client-id"}, {"client_id": "client-id"}),
            ({"tenant_id": "tenant-id"}, {"tenant_id": "tenant-id"}),
        ],
    )
    def test_build_credential_signs_in_through_the_browser_for_a_given_client(
        self, fake_identity, changes, expected_kwargs
    ):
        credential = diagnose.build_credential(_settings(**changes))

        assert (type(credential).__name__, credential.kwargs) == (
            "InteractiveBrowserCredential",
            expected_kwargs,
        )

    @pytest.mark.parametrize(
        ("choice", "expected"),
        [
            ("browser", "InteractiveBrowserCredential"),
            ("device-code", "DeviceCodeCredential"),
        ],
    )
    def test_build_credential_honours_an_explicit_choice(
        self, fake_identity, choice, expected
    ):
        credential = diagnose.build_credential(_settings(credential=choice))

        assert type(credential).__name__ == expected
