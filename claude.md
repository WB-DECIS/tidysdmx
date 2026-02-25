# Project: tidysdmx

## What This Project Is
`tidysdmx` is a thin, user-friendly Python wrapper around `pysdmx`, a low-level 
Python library implementing the SDMX standard. The goal of tidysdmx is to expose 
a clean, ergonomic API for common SDMX workflows, hiding pysdmx's complexity 
behind intuitive abstractions.

## Before Writing Any Code
Always read the following documents first — they define the domain and the 
upstream dependency your code wraps:

1. `docs/sdmx/information_model.md` — understand the SDMX artefacts involved
2. `docs/pysdmx/overview.md` — understand how pysdmx models those artefacts
3. `docs/architecture.md` — understand how tidysdmx maps onto pysdmx

## Python Environment
- Python 3.10+
- Install: `pip install -e ".[dev]"`
- pysdmx is the core upstream dependency — never reimplement what pysdmx already does

## Key Commands
- Run tests: `pytest`
- Lint + format: `ruff check . && ruff format .`
- Type check: `mypy src/`
- Full check: `ruff check . && mypy src/ && pytest`

## Architecture
See `docs/architecture.md` for full details. In brief:
- `src/tidysdmx/reader.py` — reading SDMX artefacts (wraps pysdmx readers)
- `src/tidysdmx/writer.py` — writing/serialising SDMX artefacts
- `src/tidysdmx/model.py` — any tidysdmx-specific model extensions (keep minimal)
- tidysdmx should delegate to pysdmx wherever possible; only add logic that 
  pysdmx doesn't provide or that needs to be simplified for end users

## Design Principles
- **Thin wrapper**: if pysdmx can do it, call pysdmx — don't reimplement
- **Pythonic API**: method names and signatures should feel natural to Python 
  developers unfamiliar with SDMX internals
- **SDMX correctness**: all data structures must conform to the SDMX IM; 
  refer to `docs/sdmx/` when in doubt
- **Type safety**: full type annotations required; pysdmx types should flow 
  through unless there's a strong reason to wrap them

## pysdmx Usage
- pysdmx source is available at: `<path or GitHub URL>`
- Key pysdmx classes used in this project: [list them, e.g. DataStructureDefinition, 
  Codelist, ConceptScheme, DataSet]
- pysdmx patterns to follow: see `docs/pysdmx/patterns.md`

## SDMX Domain Knowledge
- This project operates on SDMX 2.1 and 3.0 artefacts
- Key concepts: [e.g. DSD, Codelist, ConceptScheme, DataflowDefinition, ProvisionAgreement]
- When unsure about SDMX semantics, consult `docs/sdmx/information_model.md` 
  before making assumptions

## Testing
- Every wrapper function must have integration tests that use real pysdmx objects
- Tests live in `tests/` mirroring `src/tidysdmx/`
- Use fixtures in `tests/conftest.py` to construct common pysdmx artefacts

## What NOT to Do
- Do not duplicate pysdmx logic — call upstream instead
- Do not invent SDMX concepts not present in the IM
- Do not return raw pysdmx internal objects in the public API without 
  considering whether they need to be wrapped