# tidysdmx Architecture & Quality Review — June 2026

**Scope:** full-codebase review of tidysdmx v0.7.5 covering architecture, pysdmx
leverage & interoperability, consistency, duplication, testing, documentation,
dependencies, and production readiness.
**Method:** baseline measurement, five parallel review passes (architecture;
pysdmx leverage; duplication & consistency; testing; production readiness),
synthesis, and a set of low-risk quick-win fixes applied on this branch.
Larger items are catalogued in the prioritized backlog (§7).

Severity rubric: **P0** correctness · **P1** API & interoperability ·
**P2** maintainability · **P3** style/polish.

---

## 1. Executive summary

tidysdmx is in good health at the function level — 545 tests, 88.9% coverage,
clean ruff/bandit, consistent docstrings, runtime type checking — but its
module topology and release/test plumbing have not kept pace with feature
growth. Top findings:

1. **The Excel template writer and reader are incompatible (ARCH-01, P1).**
   `write_excel_mapping_template` emits the legacy one-tab-per-component
   format; `build_structure_map_from_template_wb` requires the
   INFO/COMP_MAPPING/REP_MAPPING format and rejects the writer's output. The
   shipped write→parse round-trip is broken, and a stale duplicate doc still
   documents the dead format.
2. **`build_value_map_list` stored strings in `ValueMap.valid_from`
   (CONS-03, P0 — fixed in this branch).** pysdmx types the field
   `Optional[datetime]`; a pandas `Timestamp` became the non-ISO string
   `"2020-01-01 00:00:00"`. The sibling `build_multi_value_map_list` did the
   conversion correctly. Now unified via a shared `_parse_validity_date`.
3. **29 tests silently hit live FMR in CI (TEST-01/02, P1 — mitigated in this
   branch).** Cassettes `ifpri_asti_schema.pkl`/`ifpri_asti_sm.pkl` are missing
   from the repo, and the fixtures fall back to fmrqa.worldbank.org. CI's
   `-m "not integration"` filter excluded nothing because zero tests carried
   the marker. The affected tests are now marked `integration`; cassette
   regeneration is a backlog item.
4. **The pysdmx upgrade to 1.16.0 is verified zero-risk and unblocks deleting
   the XML workaround (PYSDMX-01/07, P1).** The full unit suite was run against
   pysdmx 1.16.0 in a scratch environment: 553 passed / 46 skipped — identical
   to the 1.13.0 baseline. The bug patched by `fix_sdmx_xml_datatype_tags` was
   fixed upstream in 1.14.0 (PR #556). The `"+"→"~"` default-version change is
   a no-op here (all calls pass explicit versions), and the PyArrow dtype
   change doesn't apply (the `data` extra is unused).
5. **`structures.py` (2,314 lines) and `tidysdmx.py` (775 lines) are
   god-modules (ARCH-02/03, P2)** spanning four and three unrelated domains
   respectively. A mechanical split behind unchanged `__init__.py` re-exports
   is proposed in §7 — no redesign needed.

Positive verdicts worth recording: **no pysdmx reimplementation violations**
(`gen_urn` exists because pysdmx has URN parsers but no builder — upstream
feature request drafted); dependencies are minimal and justified; bandit is
clean at all severities; no fixture cross-test mutation exists today; df-first
signatures and `max_errors` defaults are consistent throughout.

## 2. Baseline metrics (pre-quick-wins, this branch)

| Metric | Value |
|---|---|
| Tests | 553 passed, 46 skipped (offline), 0 failed |
| Coverage (`-m "not integration"`) | **88.9%** (gate: 70) |
| Weakest modules | tidysdmx.py 62%, utils.py 83%, mapping.py 84% |
| ruff check / format | clean / clean |
| pysdmx installed | 1.13.0 (constraint `^1.13.0`; latest upstream 1.16.0) |
| Source size | 5,489 lines / 12 modules; structures.py 2,314, tidysdmx.py 775 |
| mypy (trial, `--ignore-missing-imports`) | 45 errors in 5 files |
| bandit `-r src` | 0 findings (all severities) |
| pip-audit | 40 CVEs in 15 packages — all dev/Jupyter except **idna 3.11** (runtime, via httpx→pysdmx; fix: 3.15) |
| Docstring coverage (interrogate) | 89.1% — all misses are private helpers (permitted by conventions) |
| Skip breakdown | 29 cassette/network fallback, 17 decorator skips |

## 3. Findings

Consolidated and deduplicated across the five review passes. ✅ = fixed on this
branch (quick win); 📋 = backlog (§7).

### 3.1 Correctness (P0)

| ID | Status | Evidence | Finding |
|---|---|---|---|
| CONS-03 | ✅ | structures.py (was :305-308 vs :415-434) | `build_value_map_list` stored `str(row[...])` into `ValueMap.valid_from/valid_to` (typed `Optional[datetime]` in pysdmx; msgspec doesn't validate at construction). Timestamps became non-ISO strings. `build_multi_value_map_list` converted correctly. **Fixed:** shared `_parse_validity_date` helper used by both builders; the test asserting string behavior updated. |

### 3.2 API & interoperability (P1)

| ID | Status | Evidence | Finding |
|---|---|---|---|
| ARCH-01 | 📋 | utils.py:202-260 (writer: sheets `comp_mapping`, cols `source/target`) vs structures.py:1617-1622 (reader: `INFO/COMP_MAPPING/REP_MAPPING`, `SOURCE/TARGET`, `S:`/`T:` prefixes) | Excel template **writer output is rejected by the template reader** — write→parse round-trip broken in the public API. Rewrite the writer to emit the current format (or deprecate the writer trio) + add a round-trip test. |
| PYSDMX-01 | 📋 | utils.py:292-331; pysdmx 1.14.0 release notes PR #556; fix verified in 1.16.0 source (`__structure_aux_writer.py:980-1006`) | `fix_sdmx_xml_datatype_tags` patches an XML-writer bug fixed upstream in 1.14.0. Upgrade pysdmx (verified safe, see §5) then delete the function and exports. |
| PYSDMX-03 | 📋 | artefact_builder.py:20, artefact_validation.py:24, structure_map_writer.py:3, structures.py:22 | Four modules import from private `pysdmx.model.__base`. `Agency`/`ItemReference` are publicly re-exported from `pysdmx.model` — switch those; `MaintainableArtefact`/`ItemScheme` are not (isolate the private import in one place; request upstream re-export). |
| ARCH-05 / CONS-21 / TEST-04 | 📋 | tidysdmx.py:63,124,437,524; :201-206; great-docs.yml:124-162 | Half-finished deprecation: 4 `FutureWarning` functions still exported, documented in the published API reference, ~30 statements untested — and the **non-deprecated `standardize_sdmx` internally calls deprecated `standardize_data_for_upload`**, so the supported API emits FutureWarnings. Re-route internals, then execute a 0.8→1.0 removal timeline. |
| ARCH-04 / PYSDMX-06 | 📋 | tidysdmx.py:259,305; mapping.py | `vectorized_lookup_ordered_v1/_v2` leak an internal JSON-schema version into public names; the bespoke-JSON pipeline duplicates the SDMX-native `map_structures` path. Privatize behind `map_to_sdmx` pre-1.0; long-term, migrate Kedro consumers to StructureMap-based mapping. |
| CONS-20 | 📋 | structures.py:452,542 vs artefact_builder.py:329,378 | `build_representation_map`/`build_multi_representation_map` defined in **both** modules with incompatible signatures (DataFrame-driven vs value-driven); only the structures pair is exported. Rename the artefact_builder pair (e.g. `*_from_values`). |
| TEST-01 | 📋 | fxtr_schemas.py:54-64, fxtr_mapping.py:42-50 | Cassette fixtures silently fall back to **live FMR** when the .pkl is missing; both IFPRI cassettes are absent → 29 tests skip offline / hit the network in CI. Regenerate & commit cassettes (`python -m tests.fixtures.fxtr_schemas` / `fxtr_mapping` with FMR access), then make cassette-missing a hard failure when `CI` is set. |
| TEST-02 | ✅ | tests/ (0 `integration` markers existed) | CI's `-m "not integration"` deselected nothing. **Fixed:** cassette/FMR-dependent tests now carry `@pytest.mark.integration`, making the CI lane deterministic and offline. |
| ARCH-15 / CONS-16 | ✅ | tidysdmx.py:497-504 | `standardize_indicator_id` raised an opaque `KeyError: None` when neither `DATABASE_ID` nor `DATASET_ID` exists. **Fixed:** clear `ValueError` at the API boundary. |
| CONS-17/18 | ✅(partial) 📋 | kedro.py:20-171; tidysdmx.py:63,186,212,362,707 | Public functions with bare `dict` annotations and no `@typechecked` (worst: kedro.py — zero annotations on two functions). **Fixed on this branch:** precise return/param types on `read_mapping` & dict helpers. Backlog: `_types.py` with `MappingDict`/`ValidationInfo` TypedDicts + `SdmxContext`/`SdmxAction` aliases (CONS-19), kedro.py annotations. |
| PROD-04 | ✅ | src/tidysdmx/py.typed (absent) | Not PEP 561 compliant — downstream type checkers discarded all annotations. **Fixed:** marker added (poetry-core includes package files automatically; verified in wheel). |
| PROD-06 | ✅ | tidysdmx.py:9 `import numpy as np` | numpy imported but undeclared (transitive-only via pandas). **Fixed:** declared in pyproject. |
| PROD-01 | 📋 | no release workflow; CHANGELOG uses PSR-v7 placeholder format; PSR ^10 installed | semantic-release is declared but not wired: nothing runs it and the changelog format predates the installed major version. Add a release workflow or remove PSR and document the manual process. |
| PROD-05 | 📋 | pip-audit: idna 3.11 → CVE-2026-45409 (fix 3.15) | One runtime-reachable CVE via the lock. `poetry update idna`; add a main-group `pip-audit` step to CI. |

### 3.3 Maintainability (P2)

| ID | Status | Evidence | Finding |
|---|---|---|---|
| ARCH-02/03 | 📋 | structures.py (C901: `build_multi_value_map_list`=20, `build_structure_map_from_template_wb`=13/152 lines, `_extract_mapping_rule` 98 lines) | God-modules. Proposed split (§7, B1–B3): structures.py → `map_builders.py` / `schema_builder.py` / `excel_template.py` / `urn.py`; tidysdmx.py → `registry.py` / `standardize.py` / `json_mapping.py`; utils.py dissolves into `introspection.py` + `excel_template.py` + `pysdmx_workarounds.py`. Public API preserved via `__init__.py` re-exports. |
| CONS-01/02 | ✅(helper) 📋(policy) | structures.py :293-297, :388-395, :622-624, :736-740 | 4 copies of string-column validation with **contradictory NaN policies** — `build_single_component_map` documents "or NaN" then delegates to a NaN-rejecting path, so the allowance is unreachable. **Fixed:** one `_validate_string_columns(df, cols, allow_na=...)` helper (current per-site behavior preserved). Backlog: decide a single NaN policy (CONS-02). |
| CONS-04/05/07 | 📋 | structures.py:2208-2220 vs 2279-2291; :1833-1839 vs :1866-1873; artefact_validation.py:152-187 + structure_map_writer.py:61-75 | Remaining duplication: REP_MAPPING guard block (12 identical lines), unique-ID counter logic, and **three** implementations of rep-map publish-readiness checks. Extract `_require_rep_data`, `_unique_map_id`; unify on `artefact_validation` rules. |
| CONS-06 | ✅ | tidysdmx.py:146-154 vs :171-179 | Deprecated `parse_dsd_id` duplicated `parse_artefact_id`'s whole body. **Fixed:** now delegates. |
| CONS-09 | ✅ | validation.py :126-129, :191-195, :248-250, :277-281, :309-311 | Four different error-truncation styles, one of them **silent** (no "and N more" suffix). **Fixed:** one `_format_truncated` helper, canonical suffix everywhere. |
| CONS-14/15 | 📋 | structures.py:2022-2046; tidysdmx.py:233-255 | Swallowed fallbacks: malformed INFO sheet silently stamps default agency/version on artefacts (add `logger.warning`); `transform_source_to_target` rebrands row-level KeyErrors as a missing-`components`-key error. |
| ARCH-08/09/10, CONS-08 | ✅(ARCH-08) 📋 | structures.py:889 (`_concept_ref`), :1424 (`_extract_artefact_id`); tidysdmx.py:58 (`remove_extension`) | Dead code. **Fixed:** `_concept_ref` deleted (zero call sites, verified). Backlog: `_extract_artefact_id` is zombie code kept alive only by its own tests + 2 notebooks (uses a third context vocabulary, CONS-19); `remove_extension` alias has zero consumers; the other aliases at tidysdmx.py:56-60 are de-facto kedro API mislabelled "for tests". |
| ARCH-06/07, PYSDMX-05, TEST-03 | 📋 | `gen_urn`, `fetch_schema`, `map_to_sdmx`, `standardize_sdmx`, `read_mapping`, `fix_sdmx_xml_datatype_tags`: 0 tests | Six public exports untested (violates the repo's own rule). The untested set is exactly what kedro.py wraps — the whole Kedro data path is unverified. ~59 uncovered live statements in tidysdmx.py. |
| TEST-05 | 📋 | kedro.py; pyproject.toml:85 | kedro.py imports **no kedro APIs** — partitioned datasets are plain dicts of callables — so the "integration-only" coverage exemption is unfounded. Unit-test with plain fakes (~8 tests) and drop the `omit`. |
| TEST-06 | 📋 | test_utils.py:243-295; utils.py:106-111 | The 4 "Temporary skipping to generate a coverage report" tests actually **crash** on wrong pysdmx constructor signatures (`Component(id=...)` missing required args; `Schema(id_=...)`); the same broken example is in the `extract_component_ids` docstring. Rewrite with correct constructors / `sdmx_schema` fixture; fix the docstring. |
| TEST-08 | 📋 | test_tidysdmx.py:242,371; tidysdmx.py:280,330 | The two NaN skips document a real bug: `series.astype(str)` converts NaN to the string `"nan"`. Convert to `xfail(strict=True)` or fix (mask NaN before astype). |
| TEST-12 | 📋 | fxtr_dummy_data.py:14,28,127; fxtr_structures.py:13,37 | 5 session-scoped fixtures return mutable DataFrames, violating the repo's own conventions. No mutation found today (audited all consumers) — switch to function scope (they build in microseconds); delete the dead `to_csv` "caching" calls. |
| TEST-13/14, PROD-02 | 📋 | pyproject.toml:88; ci.yml | Coverage gate 70 vs actual 88.9 (raise to 85); CI matrix lacks 3.13 (all deps support it); lint job installs the Jupyter stack to run ruff and has a dead `setup-uv` step; no dependency caching. |
| PROD-07 | 📋 | pyproject dev group | Jupyter (4 pkgs) and Sphinx docs deps live in the one dev group CI installs — ~23 of the 40 pip-audit CVEs come from that stack. Split into optional `notebooks`/`docs` groups. |
| ARCH-12/13, PROD-13 | 📋 | docs/architecture.md vs docs/tidysdmx-architecture.md (stale duplicate, documents the dead template format); docs/overview.md vs docs/pysdmx-overview.md (byte-identical except a stale version); docs/conf.py + Makefile (Sphinx, never built by CI; toctree targets missing) vs .github/workflows/docs.yml (great-docs/Quarto, triggers on `dev`); .readthedocs.yml still builds the dead Sphinx tree | **Two docs toolchains and two stale near-duplicate doc pairs.** Delete docs/architecture.md + docs/overview.md; pick one toolchain (great-docs is the live one) and remove or fix the Sphinx/RTD remnants; align trigger branches. |
| ARCH-11 | ✅ | CLAUDE.md | Agent instructions pointed to nonexistent `.pysdmx-src/`, misspelled `docs/tidy-sdmx-architecture.md`, and omitted 2 of 12 modules. **Fixed.** |

### 3.4 Style & polish (P3)

| ID | Status | Finding |
|---|---|---|
| CONS-10 / PROD-09 | ✅ | mapping.py's 7 `print()` calls replaced with module logger. Data-loss warnings (skipped maps, unmapped values → NaN) now `logger.warning` **unconditionally** — previously `verbose=False` (the default) silently swallowed them. `[OK]` messages → `info` when verbose, else `debug`. |
| CONS-11/12/13 | ✅ | utils.py broad `except Exception` narrowed: `wb.save` → `OSError`; `create_sheet` → `ValueError`; `read_excel` → `(ValueError, OSError, BadZipFile)`. |
| TEST-07/10 | ✅ | Skips removed for tests that pass today: 2 ordering tests (longest-pattern-first sort works) and the `is` vs `==` datetime-identity assertion. |
| TEST-09/11/15 | 📋 | Remaining skip triage (whitespace validation, fixture-contradicting tests — implement/xfail/delete per table in review notes); 54 `pytest.raises` without `match=`; test_pipeline_integration.py shares state via class attributes (order-dependent). |
| CONS-23/24 | 📋 | Ruff adoption: `N`, `ERA` are free (0 violations); `T20`/`ANN` near-free after quick wins; add `C90` at max-complexity 14; adopt `PT` for tests (81 hits, mostly auto-fixable + the PT011 burn-down). |
| PROD-08 | 📋 | README is a stub. Outline drafted: quick-start lifted from `tests/test_pipeline_integration.py:36-45` (the canonical fetch→validate→map→standardize flow); add repo/docs URLs + classifiers to pyproject. |
| PROD-12, PYSDMX-04 | 📋 | Pre-push hook needs `pre-commit install --hook-type pre-push` documented; `fetch_schema` hardcodes the `/FMR/sdmx/v2/` deployment path (breaks non-/FMR registries). |
| mypy note | — | `StructureMap.__replace__` (structure_map_writer.py:93) is provided by pysdmx/msgspec and works on 3.11 — mypy's flag is a false positive; the other 44 errors are real annotation debt (see PROD-03 adoption snippet in review notes). |

## 4. pysdmx leverage verdicts

| Candidate | Verdict | Key evidence |
|---|---|---|
| `gen_urn` + `SDMX_PACKAGE_MAP` | **Keep — no pysdmx equivalent; file upstream FR** | pysdmx (1.13→1.16) has URN *parsers* and `short_urn` only; it hard-codes full-URN prefixes in ~15 of its own writer locations. Propose `pysdmx.util.build_urn(Reference) -> str` upstream. Also centralise the second hand-rolled item-URN at structures.py:1096. |
| `fix_sdmx_xml_datatype_tags` | **Replace on upgrade — delete at pysdmx ≥1.14** | Bug confirmed in 1.13.0 source; fixed upstream 1.14.0 (PR #556); fix verified present in 1.16.0 and is broader than the workaround. |
| `fetch_schema` / `parse_artefact_id` | **Keep — thin wrap** | Direct delegation to `RegistryClient.get_schema`; pysdmx's `parse_short_urn` requires a `Type=` prefix so there is no parsing overlap. (Path-hardcoding smell → PYSDMX-04.) |
| `read_mapping` JSON pipeline | **Keep — custom format** | Bespoke Kedro JSON; pysdmx reads only SDMX-standard mapping messages. Internal consolidation with `map_structures` is long-term debt (PYSDMX-06), not a pysdmx replacement. |
| `create_schema_from_table` / artefact builders | **Keep — thin wrap** | Construct genuine pysdmx objects; the added validation covers publish-readiness rules pysdmx does not enforce. Micro-fix: import `Agency`/`ItemReference` from public `pysdmx.model` (PYSDMX-03). |
| Excel template layer | **Keep — settled, do not relitigate** | Zero Excel-related code in the entire pysdmx tree (verified 1.13.0 and 1.16.0). |

**No CLAUDE.md "don't reimplement pysdmx" violations found.**

## 5. pysdmx 1.16.0 upgrade assessment

- **Changes 1.13.0 → 1.16.0:** 1.14.0 fixes the RepresentationMap XML
  writer (PR #556) and a Dataflow short_urn bug; 1.15.0 changes the FMR default
  version `"+"` → `"~"` (breaking) and moves the `data` extra to PyArrow dtypes;
  1.15.1 patches a lxml CVE; 1.16.0 adds DDR connectors, token auth,
  `parse_flow_urn`.
- **Exposure:** zero. No `"+"` version literals in src/tests (every
  `get_schema` call passes an explicit parsed version); the `data` extra is not
  used; all imported symbols (`fmr.RegistryClient`, `StructureFormat`,
  `pysdmx.model.*`) are unchanged.
- **Empirical gate (already executed):** scratch venv with pysdmx **1.16.0** +
  tidysdmx wheel → `pytest -m "not integration"`: **553 passed, 46 skipped —
  identical to baseline.** Pickled Schema fixtures load cleanly under 1.16.0.
- **Recommended procedure:** `poetry update pysdmx` (constraint already admits
  1.16.0), bump the floor to `^1.16`, delete `fix_sdmx_xml_datatype_tags` +
  exports in the same PR, run the full suite plus one integration smoke against
  FMR. Low risk; do it deliberately rather than letting the caret float.

## 6. Quick wins applied on this branch

Each is a separate conventional commit; the unit suite and ruff were verified
green after every change.

1. `fix(structures)`: validity dates parsed to `datetime` in
   `build_value_map_list` via shared `_parse_validity_date` (CONS-03, P0).
2. `refactor(structures)`: `_validate_string_columns` helper replaces 4
   divergent copies, behavior-preserving (CONS-01); dead `_concept_ref`
   deleted (ARCH-08).
3. `refactor(validation)`: `_format_truncated` helper; silent truncation now
   reports "… and N more" (CONS-09).
4. `fix(mapping)`: `print()` → module logger; data-loss warnings no longer
   gated by `verbose` (CONS-10/PROD-09).
5. `fix(utils)`: broad `except Exception` narrowed ×3 (CONS-11/12/13).
6. `fix(tidysdmx)`: clear `ValueError` in `standardize_indicator_id`
   (ARCH-15/CONS-16); `parse_dsd_id` delegates to `parse_artefact_id`
   (CONS-06); precise annotations on `read_mapping`/dict helpers (CONS-18
   partial).
7. `build`: `py.typed` marker (PROD-04); numpy declared (PROD-06).
8. `docs`: CLAUDE.md drift fixed (ARCH-11).
9. `test`: `integration` markers on FMR/cassette-dependent tests (TEST-02);
   passing-today skips removed (TEST-07/10).

## 7. Prioritized refactoring backlog

Ordered by severity, then effort-ascending × unblocking value. Each item is
independently shippable.

**A. Now (P0/P1, small–medium)**
- **A1** Upgrade pysdmx → 1.16.0 + delete `fix_sdmx_xml_datatype_tags`
  (PYSDMX-01/07). Gate: full suite + FMR smoke. *S*
- **A2** Regenerate & commit the two IFPRI cassettes; make cassette-missing a
  hard failure when `$CI` is set (TEST-01). Requires FMR network access. *S*
- **A3** Fix the Excel template writer↔reader mismatch; add a write→parse→build
  round-trip test (ARCH-01). Decide: fix writer or deprecate writer trio. *M*
- **A4** Untangle deprecation: re-route `standardize_sdmx` off
  `standardize_data_for_upload`; remove deprecated functions from great-docs;
  schedule removal (0.9: drop from `__all__`; 1.0: delete) (ARCH-05/CONS-21). *M*
- **A5** Switch `Agency`/`ItemReference` imports to public `pysdmx.model`;
  isolate the two unavoidable private imports; request upstream re-export
  (PYSDMX-03). *S*
- **A6** `poetry update idna`; add main-group `pip-audit` step to CI (PROD-05). *S*
- **A7** Wire or remove semantic-release (PROD-01). *M*

**B. Structural (P2, larger)**
- **B1** Split `structures.py` → `map_builders.py` / `schema_builder.py` /
  `excel_template.py` (absorbing utils.py's Excel writer) / `urn.py`
  (absorbing `parse_artefact_id`, killing the back-import). One extraction per
  PR, `__init__.py` re-exports keep the API stable (ARCH-02/03). *L*
- **B2** Split `tidysdmx.py` → `registry.py` / `standardize.py` /
  `json_mapping.py`; retire the package-shadowing module name. *M*
- **B3** Dissolve `utils.py` → `introspection.py` + (Excel→B1) +
  `pysdmx_workarounds.py` (deleted entirely after A1). *S*
- **B4** Unit-test kedro.py with plain fakes; remove the coverage `omit`
  (TEST-05). Add tests for `map_to_sdmx`, `read_mapping`, `standardize_sdmx`,
  `gen_urn` (TEST-03, ARCH-06/07). *M*
- **B5** `_types.py`: `MappingDict`/`ValidationInfo` TypedDicts,
  `SdmxContext`/`SdmxAction` aliases; annotate kedro.py; eliminate the
  6 redeclared context Literals and the third vocabulary (CONS-17/18/19). *M*
- **B6** Remaining dedup: `_require_rep_data`, `_unique_map_id`, unify the
  three rep-map validators (CONS-04/05/07); resolve the NaN policy (CONS-02);
  add INFO-sheet fallback warnings (CONS-14); fix
  `transform_source_to_target` error attribution (CONS-15). *M*
- **B7** Rename the colliding artefact_builder map builders to
  `*_from_values` (CONS-20). *S*
- **B8** Docs consolidation: delete docs/architecture.md + docs/overview.md;
  retire the dead Sphinx toolchain or the great-docs one (pick one); align RTD
  config and trigger branches; update pysdmx-overview.md's stale version
  (ARCH-12/13, PROD-13). *M*
- **B9** Fixture hygiene: function-scope the 5 mutable DataFrame fixtures,
  delete dead `to_csv` caching (TEST-12). *S*
- **B10** Skip-debt triage: rewrite the 4 crashing test_utils tests + fix the
  broken docstring example (TEST-06); xfail the NaN-bug tests or fix NaN
  passthrough (TEST-08); implement whitespace validation (TEST-09); resolve or
  delete the fixture-contradicting tests (TEST-11). *M*
- **B11** Mypy adoption: lenient config + per-module burn-down of the 45
  errors (PROD-03). *M*

**C. Polish (P3)**
- **C1** CI: matrix +3.13, slim lint job, dependency caching, drop dead uv step
  (PROD-02, TEST-14); raise coverage gate to 85 (TEST-13); split
  notebooks/docs dependency groups (PROD-07).
- **C2** Ruff: adopt `N, ERA, T20, ANN, PLC, PLW, PLR2004` + `C90`
  (max-complexity 14); adopt `PT` for tests and burn down the 54 bare
  `pytest.raises` (CONS-23/24, TEST-15).
- **C3** README rewrite per drafted outline + pyproject URLs/classifiers
  (PROD-08).
- **C4** Privatize `vectorized_lookup_ordered_v1/_v2`; export or privatize
  `apply_component_map` consistently (ARCH-04/14); delete zombie
  `_extract_artefact_id` + dead alias (ARCH-09/10).
- **C5** `fetch_schema` path flexibility (PYSDMX-04); document the pre-push
  hook installation (PROD-12); file the upstream `build_urn` feature request
  (PYSDMX-02).

## 8. Out of scope / deferred

- Performing the module splits (B1–B3) — backlogged with PR slicing above.
- The pysdmx upgrade itself (A1) — assessment done, execution deliberate.
- Implementing kedro/skipped tests, mypy burn-down, README rewrite.
- Notebook review and performance benchmarking.
- Filing upstream pysdmx issues (drafts: `build_urn` builder; re-export
  `MaintainableArtefact`/`ItemScheme` from `pysdmx.model`).
