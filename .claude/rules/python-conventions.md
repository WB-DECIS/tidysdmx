# Python Conventions

## Function Design

- Single responsibility: each function does one thing well.
- Explicit return type annotations on everything, public and private. mypy is not
  yet in `--strict` mode here (see the burn-down list in `pyproject.toml`), but
  write as though it were: unannotated code is what is being burned down.
- Prefer specific exceptions (`ValueError`, `TypeError`) over generic `Exception`.
- Keep signatures narrow. Avoid `**kwargs` unless you are wrapping an external
  API whose parameters you must pass through.
- Validate inputs at public API boundaries with messages that say what was wrong
  and what was expected — not just "invalid input".

## Runtime type checking (deviation from pypackage-template)

This repository keeps typeguard's `@typechecked` on public functions, which the
template dropped. Use it on new public functions so the codebase stays
consistent.

The reason it is still here is sequencing, not disagreement: `TypeCheckError` is
currently part of the public contract (91 decorators, and tests asserting the
exception), so removing it is a breaking change that belongs in its own release.
mypy is being adopted first — see the burn-down list in `pyproject.toml`. Once
mypy reaches `strict = true`, typeguard goes, and this section with it.

Do not add manual `isinstance` checks that duplicate what `@typechecked` already
enforces.

## Typing

- Modern syntax only: `list[str]`, `dict[str, int]`, `X | None`. Never
  `typing.List` or `typing.Optional` — ruff's `UP` rules will flag them.
- Do not add `# type: ignore` to silence a real problem. If an ignore is
  genuinely necessary (an untyped third party), scope it narrowly and add a
  comment saying why.
- Prefer types from the libraries you depend on over re-declaring equivalents.
- The package ships `py.typed`, so annotations are part of the public contract. A
  wrong annotation breaks downstream users' type checking — treat it as a bug.

### Static checking vs runtime validation

They answer different questions, and one does not replace the other:

- mypy answers *"are our annotations consistent?"* — it verifies every call
  inside the package, and protects downstream callers who type-check. It knows
  nothing about the values that actually arrive at runtime.
- Runtime validation answers *"is this untrusted value what we assumed?"* — a
  parsed Excel workbook, a JSON mapping file, an FMR response. No type checker
  can see those.
- At a boundary, annotate what you actually hold — `object`, not the type you
  hope for — then narrow with `isinstance` and raise `TypeError` naming the
  expected and actual types.
- Never `isinstance`-guard a parameter that is already precisely annotated:
  once mypy runs with `warn_unreachable`, it flags the guard body as dead code,
  and it is right — such a guard distrusts your own annotation. Either the
  annotation is wrong (fix it) or the value is untrusted (annotate it `object`).

This is also the shape the typeguard removal should take. `@typechecked` today
covers both cases indiscriminately. When it goes, a `TypeCheckError` test at a
genuine boundary — `parse_mapping_template_wb` reading a workbook,
`read_mapping` reading JSON — becomes an explicit guard raising `TypeError`,
not a deleted test; only the tests covering already-annotated internal
parameters simply go. typeguard's pytest plugin is a compatible middle path if
runtime verification is still wanted at test time, without the production cost
or `TypeCheckError` in the public contract.

## Naming

- Functions: `snake_case` verbs — `fetch_schema`, `validate_dataset`.
- Private helpers: leading underscore, and absent from `__all__`.
- Constants: `UPPER_SNAKE_CASE`.
- Predicates: prefix `is_` or `has_`.

## Documentation

- Google-style docstrings on all public functions (ruff `D` rules enforce the
  shape; only you can enforce the accuracy).
- Include a one-line summary, then `Args:`, `Returns:`, `Raises:` as applicable.
  If the function raises, document it — callers cannot see it otherwise.
- Trivial private helpers whose name says everything may omit a docstring.
- When you change a signature, change its docstring in the same edit.

## Error Handling

- Do not catch an exception only to re-raise it unchanged.
- Do not swallow errors to keep going. If data is being dropped or coerced, the
  caller needs to know — raise, or log a warning unconditionally.
- Never use a bare `except:`.

## Data Conventions

- Return new objects rather than mutating arguments in place. If you take a
  mutable argument and need to change it, copy it first.
- Module-level constants for magic values, annotated with their type.

## Imports

- Grouped stdlib → third-party → first-party, enforced by ruff's `I` rules.
- Explicit imports only; never `from module import *`.
- Keep the public surface in `__init__.py`'s `__all__` sorted and accurate.

## Logging

- `logger = logging.getLogger(__name__)` at module scope.
- Lazy `%s` interpolation: `logger.info("Dropped %d rows", n)` — not f-strings.
- Never call `logging.basicConfig()` in library code; that is the application's
  decision.
- No `print()` in `src/` — ruff's `T20` rules will reject it.
