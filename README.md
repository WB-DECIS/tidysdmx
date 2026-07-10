<div align="center">
    <img src="https://img.shields.io/badge/status-work_in_progress-orange" alt="Work in progress" />
    <h1>⚠️ Work in progress</h1>
    <p><strong>This package is under active development. APIs, interfaces, and behavior may change without notice. Use with caution.</strong></p>
</div>

> ⚠️ **Work in progress:** This project is under active development. Expect breaking changes and incomplete features. Contributions and feedback are welcome.

# tidysdmx

**A tidy toolbox for working with SDMX data.** Fetch schemas from FMR, infer
schemas from tidy DataFrames, build structure maps from Excel templates, map data
to dissemination schemas, validate, and export SDMX-ML artefacts — all with a
Pythonic, pandas-friendly API built on top of [pysdmx](https://py.sdmx.io).

## Installation

```bash
pip install tidysdmx
```

## Quick start

```python
import pandas as pd
import pysdmx as px
from tidysdmx import (
    parse_mapping_template_wb,
    build_structure_map_from_template_wb,
    map_structures,
    standardize_output,
    validate_dataset_local,
)

# 1. Fetch the dissemination schema from FMR
client = px.api.fmr.RegistryClient("https://fmrqa.worldbank.org/FMR/sdmx/v2")
dis_schema = client.get_schema(
    "datastructure",
    agency="WB.GGH.HSP", id="DS_ASPIRE", version="1.0.0",
)

# 2. Build a structure map from an Excel mapping template
mappings = parse_mapping_template_wb("WB_ASPIRE_MAPPING.xlsx")
sm = build_structure_map_from_template_wb(
    mappings,
    target_structure_id="WB.GGH.HSP:DS_ASPIRE(1.0.0)",
    source_structure_id="WB.DP:DP_SCHEMA(1.0)",
)

# 3. Load your tidy data, map it, and validate the output
tidy_raw_df = pd.read_csv("my_tidy_data.csv")
mapped = map_structures(df=tidy_raw_df, structure_map=sm)
out = standardize_output(
    df=mapped, artefact_id="WB.GGH.HSP:DS_ASPIRE(1.0.0)",
    schema=dis_schema, action="I",
)
errors = validate_dataset_local(df=out, schema=dis_schema)
```

## What tidysdmx adds on top of pysdmx

- **Schema inference from tidy data** — `create_schema_from_table()` builds a DSD,
  ConceptScheme, and Codelists directly from a pandas DataFrame, so you can
  describe and validate your raw input without writing a line of XML.
- **Excel-driven structure maps** — `parse_mapping_template_wb()` +
  `build_structure_map_from_template_wb()` turn the World Bank mapping workbook
  format into a fully-formed pysdmx `StructureMap`.
- **Tidy mapping** — `map_structures()` applies a `StructureMap` to a DataFrame and
  returns a tidy DataFrame keyed by the target DSD's components.
- **Local validation** — `validate_dataset_local()` checks columns, codelist
  membership, duplicates, and required values against any pysdmx `Schema`.
- **Upload-ready outputs** — `standardize_output()` and
  `collect_structure_map_artifacts()` prepare both data and structure artefacts for
  FMR submission.

## Documentation

Full documentation, including the user guide and API reference, is published at the
[tidysdmx docs site](https://wb-decis.github.io/tidysdmx/).

## Contributing

Interested in contributing? Check out the [contributing guidelines](CONTRIBUTING.md).
Please note that this project is released with a Code of Conduct. By contributing to
this project, you agree to abide by its terms.

## License

`tidysdmx` was created by the DECGT team at the World Bank. It is licensed under the
terms of the MIT license.

## Credits

`tidysdmx` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).
