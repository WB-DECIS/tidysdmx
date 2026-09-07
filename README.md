# tidysdmx

[![CI](https://github.com/WB-DECIS/tidysdmx/actions/workflows/ci.yml/badge.svg)](https://github.com/WB-DECIS/tidysdmx/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/tidysdmx.svg)](https://pypi.org/project/tidysdmx/)
[![Python versions](https://img.shields.io/pypi/pyversions/tidysdmx.svg)](https://pypi.org/project/tidysdmx/)
[![Docs](https://github.com/WB-DECIS/tidysdmx/actions/workflows/docs.yml/badge.svg)](https://wb-decis.github.io/tidysdmx/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A toolbox to work with SDMX data. tidysdmx wraps [pysdmx](https://py.sdmx.io)
and adds the higher-level pieces a statistical data pipeline needs: fetching
schemas from an FMR registry, building structure maps from Excel mapping
templates, applying them to DataFrames, validating datasets against codelists,
and preparing data for SDMX dissemination.

> [!WARNING]
> tidysdmx is under active development. Public APIs are stabilising but may
> still change between minor versions; pin a version in production and check the
> [changelog](https://github.com/WB-DECIS/tidysdmx/releases) before upgrading.

## Installation

```bash
pip install tidysdmx
```

Or with uv:

```bash
uv add tidysdmx
```

## Usage

The canonical flow is fetch → build a map → apply it → validate → standardise.

```python
import pandas as pd

from tidysdmx import (
    build_structure_map_from_template_wb,
    fetch_schema,
    map_structures,
    parse_mapping_template_wb,
    standardize_output,
    validate_dataset_local,
)

# 1. Fetch the target schema from an FMR registry.
schema = fetch_schema(
    base_url="https://fmr.example.org",
    artefact_id="WB:WDI(1.0.0)",
    context="dataflow",
)

# 2. Read an Excel mapping template and turn it into a pysdmx StructureMap.
sheets = parse_mapping_template_wb("mapping_template.xlsx")
structure_map = build_structure_map_from_template_wb(sheets)

# 3. Apply the map to raw data.
raw = pd.read_csv("raw_data.csv")
mapped = map_structures(raw, structure_map)

# 4. Validate against the schema's codelists. Returns a DataFrame of errors;
#    an empty frame means the dataset is clean.
errors = validate_dataset_local(mapped, schema=schema)
if not errors.empty:
    raise ValueError(errors["Error"].tolist())

# 5. Add the SDMX reference columns an SDMX-CSV message needs.
final = standardize_output(mapped, artefact_id="WB:WDI(1.0.0)", schema=schema)
```

Everything public is re-exported from the top-level package; `SKILL.md` lists
the functions by task.

## Documentation

Full documentation, including the API reference and a quick start, is at
<https://wb-decis.github.io/tidysdmx/>.

The site also publishes `llms.txt` and `llms-full.txt` for AI agents.

## Development

```bash
make install                # sync every dependency group and wire up the git hooks
make check                  # lint + typecheck + tests with coverage
```

Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/) —
they drive automated versioning and the changelog. A `commit-msg` hook enforces this.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow,
[RELEASING.md](RELEASING.md) for how releases are cut,
[SECURITY.md](SECURITY.md) for reporting a vulnerability, and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for the standards we hold each other to.

## License

`tidysdmx` was created by the DECGT team at the World Bank. MIT — see
[LICENSE](LICENSE).
