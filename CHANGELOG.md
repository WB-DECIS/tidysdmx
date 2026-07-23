# Changelog

<!-- version list -->

## v0.10.0 (2026-07-23)

### Bug Fixes

- Adjust for compatibility with pysdmx 1,17
  ([`4ddb8e1`](https://github.com/WB-DECIS/tidysdmx/commit/4ddb8e1ce18ad780a81ea6e56abcd097bced2992))

- **validation**: Guard against stale cached valid dicts from <=0.9.0
  ([`fd220c6`](https://github.com/WB-DECIS/tidysdmx/commit/fd220c6680c722d0eb76e716a4f90ad3b69cc539))

- **validation**: Use standard SDMX-CSV reference columns for all contexts
  ([`7ce5589`](https://github.com/WB-DECIS/tidysdmx/commit/7ce558968b91fcd957f928bf02618984b521b8e4))


## v0.9.0 (2026-07-02)

### Bug Fixes

- Address review comments on multi-representation mapping (PR #225)
  ([`c782880`](https://github.com/WB-DECIS/tidysdmx/commit/c7828805080c42eb64ec039362387b897cc319ba))

- Tolerate i18n dict names and empty Dataflow structures in publish-readiness checks
  ([`fe5715a`](https://github.com/WB-DECIS/tidysdmx/commit/fe5715aa3f223e43794f69ca18b09cf1c5afb1fd))

- **docs**: Expose package name via [project] so great-docs builds the API reference
  ([`8a6b68e`](https://github.com/WB-DECIS/tidysdmx/commit/8a6b68eeea618b0889227241c5eed6ecc1b0883c))

- **docs**: Remove links to unwritten pages and close PR deployments
  ([`0edd95e`](https://github.com/WB-DECIS/tidysdmx/commit/0edd95e9ef8b2f3724dc29334c89ab9a130fa858))

- **mapping**: Keep str() regex semantics and cache stringified columns
  ([`787dacb`](https://github.com/WB-DECIS/tidysdmx/commit/787dacb8745938d113faa05eaa8e9b73666fe6e2))

- **mapping**: Replace print() with module logging
  ([`f06289e`](https://github.com/WB-DECIS/tidysdmx/commit/f06289ee252eb7682596fcb4d7428d565d496a7b))

- **structures**: Parse validity dates to datetime in build_value_map_list
  ([`cacc1dc`](https://github.com/WB-DECIS/tidysdmx/commit/cacc1dc9ef5800897f540750ab782119483a3faa))

- **tidysdmx**: Clear errors and precise annotations
  ([`4d9ba56`](https://github.com/WB-DECIS/tidysdmx/commit/4d9ba567ebe81770776daa1c671586f96a7b5d80))

- **utils**: Narrow broad exception handlers in Excel helpers
  ([`1c5d8ec`](https://github.com/WB-DECIS/tidysdmx/commit/1c5d8ecc42784ef26d947c38b353c2f0f5ee1896))

- **validation**: Infer SDMX reference columns from schema context
  ([`7261ec9`](https://github.com/WB-DECIS/tidysdmx/commit/7261ec97310e93350d143db79b290cd61f33b02d))

- **validation**: Tolerate legacy `valid` dicts and deprecate the argument
  ([`ca61078`](https://github.com/WB-DECIS/tidysdmx/commit/ca61078c8139b53858c73e09117d6afce03a793f))

### Features

- Add artefact publish-readiness validation and builders
  ([`79ed212`](https://github.com/WB-DECIS/tidysdmx/commit/79ed212289b619811fd307b02cddc15c2af66970))

- **structures**: Add DEFAULT_VALUE catch-all for component mappings
  ([`e1df804`](https://github.com/WB-DECIS/tidysdmx/commit/e1df8040bf6a68fbc6c45ceaf356c473c713943b))

- **structures**: Add N-to-1 multi-component mapping to the WB template
  ([`dcb0494`](https://github.com/WB-DECIS/tidysdmx/commit/dcb049436491c6927a7255860b87dee4b5aa8e54))

### Performance Improvements

- **mapping**: Vectorise apply_multi_component_map with np.select
  ([`e5ff780`](https://github.com/WB-DECIS/tidysdmx/commit/e5ff780c514aeefcb619bdf9c170e746ed795815))


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
