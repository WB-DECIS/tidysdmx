# pysdmx Shortcomings Register

**Purpose:** `tidysdmx` builds on pysdmx and does not reimplement it. Where
pysdmx has a gap that tidysdmx must work around, the workaround is recorded
here so it can be removed the moment upstream closes the gap. Every entry names
the pysdmx code involved (line numbers are for the **published 1.19.0 wheel**,
which is byte-identical to the `bis-med-it/pysdmx` `develop` head for these
files), the impact, the tidysdmx workaround, the proposed upstream change and
the trigger that lets us delete the workaround.

IDs are `PYSDMX-AUTH-nn` so they cannot collide with the `PYSDMX-nn` findings
in `docs/reviews/2026-06-architecture-review.md`. Code comments cite these IDs.

**Verified against:** pysdmx 1.19.0 (PyPI, 2026-08-14). Re-verify the seams
below on every pysdmx bump — `tests/test_fmr.py` does so on the wire, so a
Dependabot bump that moves a seam fails CI rather than sending unauthenticated
requests.

**Upstream status:** nothing filed yet. The fork `tonyfujs/pysdmx` is the
staging ground for a pull request against `bis-med-it/pysdmx` covering
AUTH-01 to AUTH-04; it is a follow-up, not part of the tidysdmx change that
introduced this register.

---

## PYSDMX-AUTH-01 — No authentication hook on the read clients

| | |
|---|---|
| **Symptom** | `RegistryClient` and `AsyncRegistryClient` cannot send an `Authorization` header. Their constructors take only `api_endpoint`, `format`, `pem` and `timeout`; there is no `auth=`, `headers=` or injectable `httpx.Client`. |
| **pysdmx location** | `api/fmr/__init__.py:332-369` (`RegistryClient.__init__` stores `self.__service = RestService(...)`); `api/qb/service.py:66-68` (`_CoreRestService.__init__` sets `self._headers`); `api/qb/service.py:177-192` and `:330-344` (`__fetch` builds a fresh `httpx.Client(verify=..., follow_redirects=True)` per request and copies `self._headers`). |
| **Impact** | An FMR whose read endpoints sit behind single sign-on is unreachable through pysdmx. Even where reads are public today, a client that authenticates writes but not reads is an inconsistency waiting to bite. |
| **tidysdmx workaround** | `tidysdmx.fmr._AuthenticatedRestService` overrides the protected `_headers` attribute with a property that adds `Authorization: Bearer <fresh token>` on every read; `_AuthenticatedRegistryClient` installs it into the name-mangled `_RegistryClient__service` slot after `RegistryClient.__init__` has run. A guard raises `RuntimeError` naming this entry if the slot is missing. `tests/test_fmr.py::TestFmrClientRegistry` asserts the header on the wire and that a held client reference rotates its token. |
| **Proposed upstream change** | Add `auth: httpx.Auth \| None = None` (and optionally `headers: Mapping[str, str] \| None = None`) to `_CoreRestService`, `RestService` and `AsyncRestService`, forward it to `httpx.Client(auth=...)`, and surface it on `RegistryClient` / `AsyncRegistryClient`. |
| **Remove when** | A released pysdmx accepts an auth object on `RegistryClient`. Then `FmrClient._build_registry` passes `auth=_SupplierBearerAuth(...)` and both private subclasses are deleted. |

## PYSDMX-AUTH-02 — Static bearer token, no refresh hook, on the maintenance client

| | |
|---|---|
| **Symptom** | `RegistryMaintenanceClient(access_token=...)` takes a string. Its docstring says so explicitly: *"The client does not obtain or refresh OIDC/OAuth2 tokens itself."* An Entra ID token lives 60–90 minutes, after which every upload fails. |
| **pysdmx location** | `api/fmr/maintenance.py:61-113` (constructor), `:115-128` (`__build_auth` wraps `self._access_token` in `BearerAuth`), `:130-157` (`__post` calls `__build_auth` on every POST). |
| **Impact** | Long-running pipelines must watch `expires_on` and rebuild the client; any reference held elsewhere goes stale silently. |
| **tidysdmx workaround** | `tidysdmx.fmr._RefreshingMaintenanceClient` overrides the private `__build_auth` hook (spelled `_RegistryMaintenanceClient__build_auth` in the subclass) to return `_SupplierBearerAuth`, an `httpx.Auth` that asks the token cache when the request is sent — exactly once per request. The constructor receives a placeholder `access_token` so pysdmx's "no credentials" check passes without a token being acquired. A guard raises `RuntimeError` naming this entry if the hook is gone. `tests/test_fmr.py::TestFmrClientMaintenance` asserts the bearer on the wire and the rotation across two POSTs on one held reference. |
| **Proposed upstream change** | Accept `access_token: str \| Callable[[], str]`, or add `auth: httpx.Auth \| None` to the constructor and use it in `__post`. |
| **Remove when** | A released pysdmx accepts a callable token or an auth object. Then `FmrClient.maintenance` builds a plain `RegistryMaintenanceClient` and the subclass is deleted. |

## PYSDMX-AUTH-03 — 401/403 (and 429) surface as `Invalid`, never `Unauthorized`

| | |
|---|---|
| **Symptom** | An expired or rejected token comes back as `pysdmx.errors.Invalid("Client error 401", ...)`. `pysdmx.errors.Unauthorized` is raised in exactly one place in the library — at `RegistryMaintenanceClient` construction when no credentials are given — never from an HTTP response. `429 Too Many Requests` is likewise `Invalid`, i.e. non-retriable. |
| **pysdmx location** | `util/_net_utils.py:8-40` (`map_httpx_errors`: 404 → `NotFound`, other 4xx → `Invalid`, 5xx → `InternalError`); `errors.py:92`; `api/fmr/maintenance.py:97-104`. |
| **Impact** | Callers cannot catch authentication failures by type; a refresh-and-retry-once policy has to string-match the title. |
| **tidysdmx workaround** | None in code. `FmrClient` refreshes tokens before they expire (`refresh_margin`), so 401s are rare; the user guide explains what `Client error 401` and `403` mean. `tests/test_fmr.py::TestFmrClientMaintenance::test_maintenance_surfaces_401_as_pysdmx_invalid` pins the current behaviour so a change upstream is noticed. |
| **Proposed upstream change** | Map 401 and 403 to `Unauthorized` and 429 to `Unavailable` (retriable) in `map_httpx_errors` and in the duplicated `_map_error` of the GDS service. |
| **Remove when** | Released. Then add "on `Unauthorized`, refresh once and retry" to `FmrClient` (follow-up). |

## PYSDMX-AUTH-04 — `BearerAuth` is private and `access_token` is undocumented

| | |
|---|---|
| **Symptom** | `BearerAuth` lives in `pysdmx.util._net_utils` and is absent from every `__all__`; `docs/howto/maintenance.rst` shows basic auth only. |
| **pysdmx location** | `util/_net_utils.py:42-72`; `util/__init__.py` (`__all__` exports `map_httpx_errors` but not `BearerAuth`); `docs/howto/maintenance.rst:23-32`. |
| **Impact** | Nothing reusable to build on; users discover `access_token` by reading the source. |
| **tidysdmx workaround** | `tidysdmx.fmr._SupplierBearerAuth` (ten lines, callable-backed rather than static); the user guide documents bearer authentication end to end. |
| **Proposed upstream change** | Export `BearerAuth` (ideally accepting a callable) and document `access_token` in the how-to. |
| **Remove when** | Exported with callable support — then `_SupplierBearerAuth` goes. |

## PYSDMX-AUTH-05 — The maintenance client is EXPERIMENTAL, and sync-only

| | |
|---|---|
| **Symptom** | `RegistryMaintenanceClient` is marked *EXPERIMENTAL*; pysdmx's changelog states experimental classes "may change, break, or be removed at any time without prior notice" and without a major version bump. There is no `AsyncRegistryMaintenanceClient`. |
| **pysdmx location** | `api/fmr/maintenance.py:45-59`; `CHANGELOG.rst`; `docs/api/fmr/maintenance.rst`. |
| **Impact** | Any minor release may change the constructor or the hook AUTH-02 relies on. No async write path for tidysdmx to build on. |
| **tidysdmx workaround** | `pyproject.toml` pins `pysdmx>=1.19.0,<2`. The upper bound is deliberately not the verified minor: pysdmx ships a minor roughly monthly and only the authenticated path touches these hooks, so a `<1.20` cap would force a tidysdmx release per pysdmx minor on every consumer. Instead, `FmrClient.__init__` builds both pysdmx clients as soon as a token provider is given, so an incompatible pysdmx fails at construction with the register entry named, and the wire tests in `tests/test_fmr.py` fail on any bump that changes the behaviour. Async support is deferred. |
| **Proposed upstream change** | Graduate the client from experimental; add an async twin. |
| **Remove when** | Graduated — the floor can then follow the normal cadence. |

## PYSDMX-AUTH-06 — Endpoint sanitisation uses `str.replace` anywhere in the URL

| | |
|---|---|
| **Symptom** | `RegistryMaintenanceClient.__sanitize_endpoint` removes `/sdmx/v2`, `/ws/secure/sdmxapi/rest` and `/ws/secure/sdmx/v2/metadata` wherever they occur, not only as a suffix. |
| **pysdmx location** | `api/fmr/maintenance.py:207-213`. |
| **Impact** | A registry deployed under a path containing one of those fragments (`https://host/sdmx/v2/FMR`) is silently mangled into a wrong URL. |
| **tidysdmx workaround** | `tidysdmx.fmr._normalise_root` strips only a *trailing* fragment and raises `ValueError` if one is left mid-path, so pysdmx never sees such a root. |
| **Proposed upstream change** | Use `str.removesuffix` (after stripping the trailing slash). |
| **Remove when** | Released — keep the normalisation, drop the mid-path guard. |

## PYSDMX-AUTH-07 — Uploads do not follow redirects, reads do

| | |
|---|---|
| **Symptom** | `__post` builds `httpx.Client(verify=...)` without `follow_redirects`, while every read client passes `follow_redirects=True`. `raise_for_status()` does not raise on 3xx, so a redirect on upload returns *silently as success* with the redirect body discarded. |
| **pysdmx location** | `api/fmr/maintenance.py:136` vs `api/qb/service.py:178-180`. |
| **Impact** | An authentication proxy that answers a POST with `302` makes uploads look successful when nothing was stored. |
| **tidysdmx workaround** | None beyond insisting on an absolute `http(s)` root in `FmrClient` (a scheme-less or `http` root behind an `https` redirect is the common cause). Documented in the user guide's troubleshooting section. |
| **Proposed upstream change** | Either `follow_redirects=True` on the POST client or treat 3xx as an error. |
| **Remove when** | Released. |

## PYSDMX-AUTH-08 — The two clients want two different URLs for one registry

| | |
|---|---|
| **Symptom** | `RegistryClient` must be given `https://host/FMR/sdmx/v2` (it strips only a trailing slash), whereas `RegistryMaintenanceClient` wants `https://host/FMR` (it strips `/sdmx/v2` itself). |
| **pysdmx location** | `api/fmr/__init__.py:84-86` vs `api/fmr/maintenance.py:207-213`. |
| **Impact** | Callers carry two URLs per registry; tidysdmx's `fetch_schema` papered over it by hard-coding `/FMR/sdmx/v2/` (review finding PYSDMX-04). |
| **tidysdmx workaround** | `FmrClient` takes the registry root once and derives `registry_endpoint` for reads and the root for writes. |
| **Proposed upstream change** | Accept the registry root on `RegistryClient` and append the API path, or document one convention for both clients. |
| **Remove when** | Released — the single-root convenience stays regardless. |
