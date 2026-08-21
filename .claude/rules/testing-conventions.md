# Testing Conventions

## Structure

- Test files mirror source modules: `src/tidysdmx/validation.py` →
  `tests/test_validation.py`.
- Group tests for one function in a `class Test<FunctionName>:` block.
- Reusable test data lives in `tests/fixtures/fxtr_*.py` and is imported by
  `tests/conftest.py`. Never build inline data when a fixture already provides it.

## Fixture registration — read this before adding a fixture module

Shared fixtures go **directly in `tests/conftest.py`**. Do **not** add a
`pytest_plugins` list to `tests/conftest.py`: pytest only permits
`pytest_plugins` in the *top-level* (rootdir) conftest, and putting it in
`tests/` depends on confcutdir resolution that changes between pytest versions.
If you genuinely need plugin-style registration, create a `conftest.py` at the
repository root and put it there.

- Function-scoped fixtures for anything mutable, no matter how cheap. Two tests
  sharing a mutable object is a bug waiting for a specific test ordering.
- Session-scoped fixtures only for genuinely expensive, immutable things.

## Markers

- `@pytest.mark.integration` — needs network or an external service. CI's default
  lane runs `-m "not integration"`, so anything unmarked must run offline.
- `@pytest.mark.unit` — available, but unmarked tests are treated as unit tests.
- `--strict-markers` is on: an unregistered marker is an error, not a typo that
  silently does nothing. Register new markers in `pyproject.toml`.

A marker-based CI lane only works if the markers are actually applied. If you add
a test that touches the network, mark it — otherwise CI will hit the network and
the "no network" guarantee is fiction.

## Test Design

- Names: `test_<function>_<scenario>` — e.g. `test_greet_rejects_blank_name`.
- One logical assertion per test. Prefer three focused tests over one that checks
  three things and reports only the first failure.
- Always cover: the happy path, edge cases (empty, `None`, boundaries, duplicates)
  **and** the error paths.
- `pytest.raises(SomeError, match="...")` — always with `match=`. Asserting the
  type alone passes even when the error is raised for the wrong reason.
- `@pytest.mark.parametrize` for the same assertion over several inputs.
- `filterwarnings = ["error"]` is set, so a warning fails the suite. If a warning
  is expected, assert it with `pytest.warns`; if a third party emits one you
  cannot fix, add a targeted ignore to `pyproject.toml` rather than removing the
  setting.

## Skips and xfails

- Never leave a bare `@pytest.mark.skip` behind. A skipped test is invisible debt
  that usually turns out to be hiding a real bug.
- If behaviour is known-broken, use `@pytest.mark.xfail(strict=True, reason=...)`
  so the suite tells you when it starts passing. `xfail_strict = true` is set.

## Coverage

- Every new public function needs at least one test.
- The gate is `fail_under = 85` in `pyproject.toml`, against ~87% actual. It is a
  floor, not a target —
  and once real coverage is well above it, raise it. A gate far below reality
  cannot catch a regression.
- Coverage is deliberately **not** in pytest's `addopts`, so `pytest -k one_test`
  does not trip the gate. Use `make cov` for a measured run.

## tidysdmx specifics

- **Cassettes.** `tests/fixtures/cassettes/*.pkl` are pickled pysdmx objects
  captured from FMR. Tests that depend on them, or that would otherwise reach
  the network, must carry `@pytest.mark.integration` so the CI lane stays
  offline and deterministic. Regenerate a cassette by running the fixture module
  directly (`uv run python -m tests.fixtures.fxtr_schemas`), which needs FMR
  access.
- **Warnings are errors.** `filterwarnings = ["error"]` is set. When a test
  exercises a deprecated function on purpose, scope the suppression to that test
  or class with `pytest.mark.filterwarnings`, next to the code it describes —
  never add a blanket ignore to `pyproject.toml`. A scoped suppression is deleted
  along with the deprecated function; a global one outlives it.
- **Known debt.** 54 `pytest.raises` calls still lack `match=`, so `PT011` is
  temporarily ignored for `tests/**` in `pyproject.toml`. New tests must pass
  `match=` anyway. Removing that ignore is backlog item TEST-15.
