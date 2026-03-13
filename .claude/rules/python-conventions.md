# Python Function Conventions

## Function Design
- Single responsibility: each function does one thing well.
- Explicit return types on all public functions.
- Use `@typechecked` from typeguard on all public functions.
- Prefer raising specific exceptions (ValueError, TypeError) over generic Exception.
- Keep function signatures narrow — avoid **kwargs unless wrapping an external API.

## Naming
- Functions: `snake_case` verbs — `fetch_schema`, `validate_dataset`, `build_value_map`.
- Private helpers: prefix with `_` — `_parse_date_pattern`.
- Constants: `UPPER_SNAKE_CASE`.
- Boolean functions: prefix with `is_` or `has_`.

## Documentation
- Google-style docstrings on all public functions (enforced by ruff D rules).
- Include: one-line summary, Args, Returns, Raises sections.
- Omit docstrings on trivial private helpers where the name is self-documenting.

## Error Handling
- Validate inputs at public API boundaries with clear error messages.
- Do not catch exceptions just to re-raise them unchanged.
- Let typeguard handle type validation — do not duplicate type checks manually.

## Data Conventions
- Use pandas DataFrames for tabular data, not lists of dicts.
- Column names: UPPER_SNAKE_CASE for SDMX dimensions (INDICATOR, TIME_PERIOD, OBS_VALUE).
- Return new DataFrames instead of mutating inputs in-place.

## Imports
- Group: stdlib → third-party → pysdmx → tidysdmx (enforced by ruff I rules).
- Use explicit imports, not `from module import *`.
