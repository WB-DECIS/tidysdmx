"""Walk through FMR authentication one step at a time and say where it breaks.

How a token reaches the registry:

1. An Entra ID app registration stands for the registry's API, the *resource*.
   A token is requested for its scope, ``<app-id>/.default`` (or
   ``api://<app-id>/.default`` when the app allows it), and the token's ``aud``
   claim names that app.
2. A *client* app signs the user in and asks Entra ID for the token.
   azure-identity signs in through the Azure CLI's public client unless told
   otherwise, and the resource app must authorise whichever client asks.
3. ``AzureTokenProvider`` gets the token from an azure-identity credential, and
   ``FmrClient`` sends it as ``Authorization: Bearer <token>`` on reads and on
   uploads, which go to the registry's secure API, ``/ws/secure/sdmxapi/rest``.
4. The registry validates the token against its OpenID Connect settings
   (signing keys, issuer, audience, expiry), then checks what the identity may
   do.

The script runs those steps in order, reports each, and names the likely cause
of a failure. It is read-only: it never uploads, and it never prints the token,
only the claims that describe its shape. Run it from the repository root::

    uv run python scripts/diagnose_fmr_auth.py --scope "<app-id>/.default"

and add ``--client-id`` and ``--tenant-id`` when the registry's app does not
authorise the Azure CLI's client; the sign-in then goes through the browser
with that client app. Every option falls back to an environment
variable: ``TIDYSDMX_FMR_ROOT``, ``TIDYSDMX_FMR_SCOPE``,
``TIDYSDMX_FMR_CLIENT_ID`` and ``TIDYSDMX_FMR_TENANT_ID``, the last three being
the ones the live upload test in ``tests/test_fmr.py`` reads. A browser window
opens for the sign-in. ``--try-id-token`` also signs in with MSAL directly and
offers the registry the ID token, for a registry that validates ID tokens
rather than access tokens.
"""

import argparse
import base64
import importlib
import json
import logging
import os
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import httpx
from pysdmx.errors import PysdmxError

from tidysdmx import AzureTokenProvider, FmrClient

DEFAULT_ROOT = "https://fmrqa.worldbank.org/FMR"
DEFAULT_CODELIST = "WB.TEST:CL_TEST(1.0)"
HTTP_TIMEOUT = 30.0
REFERENCE = re.compile(
    r"^(?P<agency>[\w.@$-]+):(?P<id>[\w@$-]+)\((?P<version>[\w.+~-]+)\)$"
)
CLAIMS_SHOWN = ("aud", "iss", "ver", "scp", "roles", "appid", "azp", "tid")
NEEDS_TOKEN = frozenset({httpx.codes.UNAUTHORIZED, httpx.codes.FORBIDDEN})

# The Entra ID errors met while signing in to a registry, and what each means
# for this setup. Any other code is looked up by URL.
AADSTS_HINTS: dict[str, str] = {
    "50011": (
        "The client app has no matching redirect URI: a browser sign-in needs "
        "http://localhost under 'Mobile and desktop applications'."
    ),
    "50194": "The client app is single-tenant: pass --tenant-id.",
    "65001": (
        "Nobody has consented to the client app using the registry's API: an "
        "administrator must grant consent."
    ),
    "70011": "The scope is malformed: it must end in /.default.",
    "90009": (
        "The client app is asking for a token for itself, which Entra ID allows "
        "only with the GUID scope: <app-id>/.default, not api://<app-id>/.default."
    ),
    "500011": (
        "No app with that scope exists in the tenant: check the app ID in --scope "
        "and --tenant-id."
    ),
    "501461": (
        "The registry's app sets acceptMappedClaims, so its tokens must name it by "
        "GUID: use <app-id>/.default, not api://<app-id>/.default."
    ),
    "650057": (
        "The registry's app does not authorise the client app that signed in (the "
        "Azure CLI's unless --client-id says otherwise): pass --client-id and "
        "--tenant-id of a client app it does authorise."
    ),
    "700016": (
        "The client app is not registered in that tenant: check --client-id and "
        "--tenant-id."
    ),
    "7000218": (
        "Entra ID expects a client secret, so the client app does not allow a user "
        "sign-in: enable 'Allow public client flows' on its registration."
    ),
}

# What the registry's answer to a read with a token means.
STATUS_HINTS: dict[int, str] = {
    401: (
        "The registry rejected the token itself. Compare the claims in step 4 "
        "with its OpenID Connect settings: the issuer (a v1.0 token comes from "
        "https://sts.windows.net/<tenant>/, a v2.0 token from "
        "https://login.microsoftonline.com/<tenant>/v2.0), the audience and the "
        "signing keys. The WWW-Authenticate header, when there is one, and the "
        "registry's log name the check that failed."
    ),
    403: "The token is valid, but this identity may not do that on the registry.",
}

# MSAL recommends form_post for its browser sign-in on every run; it is advice
# to MSAL's maintainers, not something a caller can act on.
warnings.filterwarnings(
    "ignore", message="response_mode='form_post' is recommended", category=UserWarning
)


@dataclass(frozen=True)
class Settings:
    """What to diagnose, from the command line and the environment."""

    root: str
    scope: str
    client_id: str | None
    tenant_id: str | None
    codelist: str
    credential: str
    try_id_token: bool


@dataclass(frozen=True)
class Probe:
    """One read of the codelist, against one of the registry's APIs.

    A probe that does not count is reported but never decides a verdict.
    """

    name: str
    url: str
    accept: str
    counts: bool = True


def parse_args(argv: list[str] | None = None) -> Settings:
    """Read the settings from the command line, falling back to the environment.

    Args:
        argv: The arguments, without the program name; ``sys.argv`` when None.

    Returns:
        The settings to diagnose with.
    """
    env = os.environ.get
    parser = argparse.ArgumentParser(
        description="Diagnose authentication against a Fusion Metadata Registry."
    )
    parser.add_argument(
        "--root",
        default=env("TIDYSDMX_FMR_ROOT", DEFAULT_ROOT),
        help=(
            "registry root, the part before /sdmx/v2; https only, since a "
            "bearer token is sent to it (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--scope",
        default=env("TIDYSDMX_FMR_SCOPE"),
        help="the registry app's scope, e.g. <app-id>/.default",
    )
    parser.add_argument(
        "--client-id",
        default=env("TIDYSDMX_FMR_CLIENT_ID"),
        help="client app to sign in through (default: the Azure CLI's)",
    )
    parser.add_argument(
        "--tenant-id",
        default=env("TIDYSDMX_FMR_TENANT_ID"),
        help="tenant of the client app (default: the user's home tenant)",
    )
    parser.add_argument(
        "--codelist",
        default=DEFAULT_CODELIST,
        help="codelist to read, as AGENCY:ID(VERSION) (default: %(default)s)",
    )
    parser.add_argument(
        "--credential",
        choices=("default", "browser", "device-code"),
        default="default",
        help=(
            "azure-identity credential: DefaultAzureCredential with its browser "
            "fallback, as AzureTokenProvider.from_default_credential builds it, "
            "or a browser or device-code sign-in alone. With --client-id or "
            "--tenant-id, default signs in through the browser, the only "
            "credential in DefaultAzureCredential's chain that would honour them "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--try-id-token",
        action="store_true",
        help="also sign in with MSAL and offer the registry the ID token",
    )
    args = parser.parse_args(argv)
    if not args.scope:
        parser.error("pass --scope or set TIDYSDMX_FMR_SCOPE, e.g. <app-id>/.default")
    if not REFERENCE.match(args.codelist):
        parser.error(f"--codelist must be AGENCY:ID(VERSION), got {args.codelist!r}")
    if urlsplit(args.root).scheme != "https":
        parser.error(
            "--root must be an https URL: the diagnostic sends a bearer token, "
            f"got {args.root!r}"
        )
    return Settings(
        root=args.root.rstrip("/"),
        scope=args.scope,
        client_id=args.client_id or None,
        tenant_id=args.tenant_id or None,
        codelist=args.codelist,
        credential=args.credential,
        try_id_token=args.try_id_token,
    )


def codelist_probes(root: str, reference: str) -> list[Probe]:
    """The reads of a codelist through the public API and the secure one.

    Args:
        root: The registry root, without a trailing slash.
        reference: The codelist as ``AGENCY:ID(VERSION)``.

    Returns:
        One probe per API: the public SDMX 3.0 one that reads use, and the
        secure SDMX 2.1 one that uploads go to. The secure one is shown for
        information only: uploads POST to its root, and FMR documents no GET
        of a structure there, so its answer cannot decide a verdict.

    Raises:
        ValueError: If ``reference`` is not ``AGENCY:ID(VERSION)``.
    """
    match = REFERENCE.match(reference)
    if match is None:
        raise ValueError(f"not an AGENCY:ID(VERSION) reference: {reference!r}")
    path = f"codelist/{match['agency']}/{match['id']}/{match['version']}"
    return [
        Probe(
            "public API, used by reads",
            f"{root}/sdmx/v2/structure/{path}",
            "application/vnd.sdmx.structure+json;version=2.0.0",
        ),
        Probe(
            "secure API, where uploads POST (a GET here is undocumented)",
            f"{root}/ws/secure/sdmxapi/rest/{path}",
            "application/vnd.sdmx.structure+xml;version=2.1",
            counts=False,
        ),
    ]


def decode_claims(token: str) -> dict[str, Any]:
    """The claims of a JWT, read without verifying its signature.

    Args:
        token: A JSON Web Token, ``header.payload.signature``.

    Returns:
        The payload's claims.

    Raises:
        ValueError: If ``token`` is not a JWT with a JSON object as payload.
    """
    parts = token.split(".")
    if len(parts) != len(("header", "payload", "signature")):
        raise ValueError("a JWT has three dot-separated parts")
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    if not isinstance(claims, dict):
        raise ValueError("the JWT payload is not a JSON object")
    return claims


def check_anonymous_reads(probes: list[Probe]) -> bool:
    """Step 1: rule out the network, the URL and the codelist before signing in.

    Args:
        probes: The reads to try, without a token.

    Returns:
        Whether the public API answered at all: 200, or 401 or 403 from a
        registry whose reads need a token, which the next steps then test.
    """
    _heading(1, "Read the codelist without a token")
    answers = {probe.name: _get(probe, token=None) for probe in probes}
    for name, response in answers.items():
        if response is not None:
            _detail(f"{name}: HTTP {response.status_code}")
    _detail(
        "The public API answers 200 when reads are public, or 401 or 403 when "
        "they need a token; the secure one may want a token either way."
    )
    public = answers[probes[0].name]
    if public is not None and public.status_code in NEEDS_TOKEN:
        _verdict(True, "the registry answers, and its reads need a token")
        return True
    if public is None or public.status_code != httpx.codes.OK:
        _verdict(False, "the registry or the codelist is unreachable; check --root")
        return False
    _verdict(True, "the registry answers and the codelist exists")
    return True


def build_credential(settings: Settings) -> Any | None:
    """Step 2: build the azure-identity credential that will sign in.

    Args:
        settings: Which credential, and the client app and tenant if given.

    Returns:
        The credential, or None when azure-identity is not installed.
    """
    _heading(2, f"Build the azure-identity credential ({settings.credential})")
    try:
        identity = importlib.import_module("azure.identity")
    except ImportError:
        _verdict(False, "azure-identity is missing: run uv sync --extra azure")
        return None
    client = {"client_id": settings.client_id, "tenant_id": settings.tenant_id}
    given = {key: value for key, value in client.items() if value}
    client_app = settings.client_id or "azure-identity's default"
    if settings.credential == "default" and given:
        # DefaultAzureCredential hands a client app only to its browser
        # fallback; the Azure CLI or any other credential earlier in its chain
        # would sign in through its own client and ignore the one asked for.
        _detail("--client-id or --tenant-id given: signing in through the browser")
        credential = identity.InteractiveBrowserCredential(**given)
    elif settings.credential == "browser":
        credential = identity.InteractiveBrowserCredential(**given)
    elif settings.credential == "device-code":
        credential = identity.DeviceCodeCredential(**given)
    else:
        # The chain logs which credential it tried and which one signed in.
        logger = logging.getLogger("azure.identity")
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("      azure.identity: %(message)s"))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        credential = identity.DefaultAzureCredential(
            exclude_interactive_browser_credential=False
        )
        client_app = "that of the credential the chain picks (see its log)"
    _verdict(True, f"{type(credential).__name__}, client app {client_app}")
    return credential


def acquire_token(credential: Any, scope: str) -> str | None:
    """Step 3: ask Entra ID for a token; a browser window opens to sign in.

    Args:
        credential: The azure-identity credential from step 2.
        scope: The registry app's scope.

    Returns:
        The access token, or None when Entra ID issued none.
    """
    _heading(3, f"Ask Entra ID for a token for {scope}")
    azure_error = importlib.import_module("azure.core.exceptions").AzureError
    try:
        token: str = credential.get_token(scope).token
    except azure_error as err:
        _verdict(False, "Entra ID issued no token")
        for line in str(err).splitlines():
            _detail(line)
        _explain_aadsts(str(err))
        return None
    _verdict(True, "Entra ID issued a token")
    return token


def describe_token(token: str, scope: str) -> bool:
    """Step 4: show the claims that decide whether a registry accepts a token.

    Args:
        token: The token from step 3; it is never printed.
        scope: The scope it was requested for.

    Returns:
        Whether the token is a JWT for the scope's app that has not expired.
    """
    _heading(4, "Read the token's claims (the token itself is never printed)")
    try:
        claims = decode_claims(token)
    except ValueError as err:
        _verdict(False, f"the token is not a readable JWT: {err}")
        return False
    for key in CLAIMS_SHOWN:
        if key in claims:
            _detail(f"{key}: {claims[key]}")
    resource = scope.removesuffix("/.default")
    audience_ok = claims.get("aud") in {resource, resource.removeprefix("api://")}
    if not audience_ok:
        _detail(f"NOTE: aud is not {resource}, the app the scope names")
    if claims.get("ver") == "1.0":
        _detail(
            "NOTE: a v1.0 token. A registry set up with the v2.0 issuer "
            "(https://login.microsoftonline.com/<tenant>/v2.0) rejects it; the "
            "registry app's manifest, accessTokenAcceptedVersion, decides which "
            "version Entra ID issues."
        )
    expires = datetime.fromtimestamp(claims.get("exp", 0), tz=UTC)
    minutes_left = int((expires - datetime.now(UTC)).total_seconds() // 60)
    _detail(f"expires: {expires:%Y-%m-%d %H:%M} UTC ({minutes_left} min left)")
    healthy = audience_ok and minutes_left > 0
    _verdict(healthy, "the claims match the scope" if healthy else "see the notes")
    return healthy


def probe_with_token(step: int, title: str, probes: list[Probe], token: str) -> bool:
    """Steps 5 and 7: offer a token to the registry's APIs over plain HTTP.

    tidysdmx and pysdmx stay out of the way here, so a failure is the
    registry's answer to the token and nothing else.

    Args:
        step: The step number to report under.
        title: What is being offered.
        probes: The reads to try.
        token: The bearer token; it is never printed.

    Returns:
        Whether every probe that counts answered 200.
    """
    _heading(step, title)
    statuses: set[int] = set()
    for probe in probes:
        response = _get(probe, token=token)
        status = 0 if response is None else response.status_code
        if probe.counts:
            statuses.add(status)
        if response is None:
            continue
        _detail(f"{probe.name}: HTTP {status}")
        if status != httpx.codes.OK:
            challenge = response.headers.get("WWW-Authenticate", "(none)")
            _detail(f"  WWW-Authenticate: {challenge}")
            body = " ".join(response.text.split())[:300]
            _detail(f"  body: {body or '(empty)'}")
            if not probe.counts:
                _detail(
                    "  For information only: the registry need not serve a GET "
                    "here, and this says nothing about whether uploads work."
                )
    accepted = statuses == {httpx.codes.OK}
    for status in sorted(statuses - {httpx.codes.OK}):
        if status in STATUS_HINTS:
            _detail(STATUS_HINTS[status])
    _verdict(
        accepted, "the registry accepts it" if accepted else "the registry refuses it"
    )
    return accepted


def check_tidysdmx(settings: Settings, credential: Any) -> bool:
    """Step 6: read the codelist the way tidysdmx does, with the same token.

    When step 5 passes and this fails, the fault is in tidysdmx or pysdmx, not
    in the token or the registry.

    Args:
        settings: The registry, scope and codelist.
        credential: The credential from step 2; its cached token is reused.

    Returns:
        Whether ``FmrClient.fetch_codelist`` succeeded.
    """
    _heading(6, "Read through tidysdmx: AzureTokenProvider and FmrClient")
    provider = AzureTokenProvider(credential, settings.scope)
    try:
        client = FmrClient(settings.root, token_provider=provider)
        codelist = client.fetch_codelist(settings.codelist)
    except (PysdmxError, ValueError) as err:
        _verdict(False, f"{type(err).__name__}: {err}")
        return False
    _verdict(True, f"fetched {codelist.short_urn}, {len(codelist)} codes")
    return True


def try_id_token(settings: Settings, probes: list[Probe]) -> None:
    """Step 7, optional: sign in with MSAL and offer the ID token instead.

    azure-identity only ever returns access tokens. A registry that validates
    the ID token of an OpenID Connect sign-in accepts this one where it refused
    the access token, which would mean ``AzureTokenProvider`` cannot work with
    it as configured.

    Args:
        settings: The client app, tenant and scope to sign in with.
        probes: The reads to try.
    """
    if not (settings.client_id and settings.tenant_id):
        _heading(7, "Offer the ID token instead")
        _detail("skipped: needs --client-id and --tenant-id")
        return
    msal = importlib.import_module("msal")
    app = msal.PublicClientApplication(
        settings.client_id,
        authority=f"https://login.microsoftonline.com/{settings.tenant_id}",
    )
    result = app.acquire_token_interactive(scopes=[settings.scope])
    id_token = result.get("id_token")
    if not id_token:
        _heading(7, "Offer the ID token instead")
        _verdict(False, f"MSAL returned no ID token: {result.get('error')}")
        _explain_aadsts(str(result.get("error_description", "")))
        return
    claims = decode_claims(id_token)
    shown = ", ".join(f"{key}={claims.get(key)}" for key in ("aud", "iss", "ver"))
    probe_with_token(7, f"Offer the ID token instead ({shown})", probes, id_token)


def main(argv: list[str] | None = None) -> int:
    """Run the steps in order, stopping when one leaves nothing more to test.

    Args:
        argv: The arguments, without the program name; ``sys.argv`` when None.

    Returns:
        0 when tidysdmx reads with the token, 1 otherwise.
    """
    settings = parse_args(argv)
    probes = codelist_probes(settings.root, settings.codelist)
    print(f"Registry {settings.root}, codelist {settings.codelist}")
    print(f"Scope {settings.scope}, client app {settings.client_id or 'default'}")
    if not check_anonymous_reads(probes):
        return 1
    credential = build_credential(settings)
    if credential is None:
        return 1
    token = acquire_token(credential, settings.scope)
    if token is None:
        return 1
    describe_token(token, settings.scope)
    probe_with_token(
        5, "Offer the token to the registry over plain HTTP", probes, token
    )
    healthy = check_tidysdmx(settings, credential)
    if settings.try_id_token:
        try_id_token(settings, probes)
    return 0 if healthy else 1


def _get(probe: Probe, *, token: str | None) -> httpx.Response | None:
    """GET a probe's URL, with the token when there is one."""
    headers = {"Accept": probe.accept}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    try:
        return httpx.get(probe.url, headers=headers, timeout=HTTP_TIMEOUT)
    except httpx.RequestError as err:
        _detail(f"{probe.name}: no answer ({type(err).__name__}: {err})")
        return None


def _explain_aadsts(message: str) -> None:
    """Explain each Entra ID error code the message holds."""
    for code in dict.fromkeys(re.findall(r"AADSTS(\d+)", message)):
        hint = AADSTS_HINTS.get(
            code, f"see https://login.microsoftonline.com/error?code={code}"
        )
        _detail(f"AADSTS{code}: {hint}")


def _heading(step: int, title: str) -> None:
    print(f"\n[{step}] {title}")


def _detail(text: str) -> None:
    print(f"      {text}")


def _verdict(ok: bool, text: str) -> None:
    print(f"    {'OK' if ok else 'FAIL'}: {text}")


if __name__ == "__main__":
    sys.exit(main())
