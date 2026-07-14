# Testing Conventions

## Structure
- Test files mirror source modules: `src/tidysdmx/validation.py` → `tests/test_validation.py`.
- Use fixtures from `tests/fixtures/` — registered as pytest plugins in `conftest.py`.
- Never create inline pysdmx artefacts when a fixture already provides one.

## Markers
- `@pytest.mark.unit` — fast, isolated, no network. This is the default for most tests.
- `@pytest.mark.integration` — requires FMR or external services. Skipped in CI fast-path.

## Test Design
- Descriptive names: `test_<function>_<scenario>` (e.g., `test_validate_columns_missing_required`).
- One logical assertion per test — prefer multiple focused tests over one large test.
- Test happy path, edge cases (empty DataFrame, None values, missing columns), and expected errors.
- Use `pytest.raises` for expected exceptions with `match=` to verify the message.

## Coverage
- Every new public function must have at least one corresponding test.
- Target overall coverage: 70%+ (enforced in CI via pytest-cov).
- kedro.py is excluded from coverage: it is a thin, deprecated Kedro-node
  wrapper layer scheduled for removal, so it is not held to the coverage gate
  even though `tests/test_kedro.py` exercises its core behaviour.

## Fixtures
- Session-scoped fixtures for expensive objects (FMR schemas, pickled data).
- Function-scoped fixtures for mutable test data (DataFrames).
- Add new fixtures to the appropriate `tests/fixtures/fxtr_*.py` file and register in conftest.py if needed.
