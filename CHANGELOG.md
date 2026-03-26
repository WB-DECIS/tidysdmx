# Changelog

<!--next-version-placeholder-->
## v0.8.0 (26/3/2026)
### Changed

- Modify artefact generation functions to ensure the artefacts are FMR compliant.
- Improved error reporting in validate_dataset_local: Refactored validation to return multiple granular errors instead of raising on the first failure. Capped by a new max_errors parameter (default 1000).
- Various clean-up tasks.

## v0.7.5 (26/1/2026)
### Fixed

- Fixed `validate_dataset_local` to support dataflows

## v0.7.4 (26/1/2026)
### Changed

- Changed how to organize dataframes based on `artefact_type`, e.g., `datastructure`, `dataflow` or `provisionagreement`.

## v0.7.3 (23/1/2026)
### Added

- Added functions to make more robust validations of the Excel template, and more helpful error messages.

## v0.7.2 (6/1/2026)
### Added

- Added a new parameter to `standardize_sdmx` and `standardize_data_for_upload` to support string in `OBS_VALUE`.

## v0.7.1 (23/12/2025)
### Added

- Added functions to the `structures` module

### Changed

- Refactored functions in the `structures` module


## v0.7.0 (18/12/2025)
### Added

- Added `parse_mapping_template_wb` function


## v0.6.0 (16/12/2025)
### Added

- Added more tests
- Added `standardize_output` function

### Changed

- Cleaned up some functions and tests, no impacts for the final user.


## v0.5.0 (8/12/2025)
### Added

- Added functions to the `structures` module
- Added functions to the `utils` module
- Added unit tests for these functions
- Added `openpyxl` package as dependency to manipulate excel files

## v0.4.0 (21/11/2025)
### Changed

- Split code into separate modules

### Added

- Added a first batch new standardized functions
- Added unit tests for these functions
- Added scaffholding to handle fixtures for unit-testing

## v0.3.0 (23/10/2025)
### Changed

- Updated function `transform_source_to_target` function and added tests to it.
- Modified functions that were in the package but not yet passed QA, to support some pipelines in Databricks.
- Users can now access import functions directly without referring to submodules.

## v0.2.0 (16/10/2025)
### Added

- Added function `fetch_schema` to fetch DSDs, Dataflows and Provision agreements from FMR.
- Added function `parse_artefact_id` to extract agency, id and version from an artefact.

### Deprecated

- `fetch_dsd_schema` function in favor of the new function `fetch_schema`.
- `parse_dsd_id` function in favor of the new function `parse_artefact_id`.


## v0.1.0 (14/03/2025)
- First release of `tidysdmx`!