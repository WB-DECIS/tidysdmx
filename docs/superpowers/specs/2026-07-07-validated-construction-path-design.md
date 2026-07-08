# One Validated Construction Path — Design (DUP-02/03/04)

**Status:** approved for planning · **Branch:** `structure-refactor` · **Date:** 2026-07-07

## Context

The July 2026 review (`docs/reviews/2026-07-code-review.md`) found three overlapping
duplication problems that together mean tidysdmx has **more than one way to construct the
same SDMX artefact, and only some of them validate**:

- **DUP-02** — `build_representation_map` / `build_multi_representation_map` exist in *both*
  `structures.py` (DataFrame-driven, **unvalidated**, and the ones exported) and
  `artefact_builder.py` (value-driven, **validated** via `raise_if_invalid`, currently
  shadowed).
- **DUP-03** — three validation dialects: `artefact_validation` (rule IDs +
  `ValidationIssue`/`ValidationError`, the keeper), `structure_map_writer._validate_rep_map_fields`
  (re-codes R001/R002/R003 as plain strings + `ValueError`), and ad-hoc `ValueError`s. There is
  **no `StructureMap` entry** in `_SPECIFIC`, so a StructureMap with empty `source`/`target`
  passes `prepare_structure_map_for_upload(validate=True)` and is later rejected by FMR.
- **DUP-04** — `create_schema_from_table` constructs `Codelist`/`ConceptScheme`/`DSD`
  directly and hand-assembles URN strings inline, so the DataFrame→schema path never sees the
  C001/CS001/D001/D002 publish-readiness rules.

This work makes `artefact_validation` the **single validation dialect** and routes every
construction path through the **validated builders**, so construction fails fast instead of
letting an invalid artefact escape into the FMR-upload path. It also positions the codebase
for the planned pysdmx-native validation swap (PX-04): once every path funnels through
`artefact_validation`, replacing it with `pysdmx.model.validate` becomes a one-line change.

## Goals

- Exactly one validated construction path per artefact type; every builder ends in
  `raise_if_invalid`.
- One validation dialect (`artefact_validation`); delete the re-implementations.
- Close the StructureMap `source`/`target` validation gap.

## Non-goals

- **SMP-02 (splitting `structures.py`)** is a *separate* follow-on effort. It will relocate
  the code consolidated here behind unchanged `__init__` re-exports. Not in this spec.
- No change to the pysdmx dependency or to `artefact_validation`'s eventual self-destruct plan.

## Target architecture

Acyclic dependency graph, with one **new** edge (`structures → artefact_builder`), verified
safe because `artefact_builder` and `artefact_validation` have no intra-package imports back to
`structures`:

```
artefact_validation      (leaf — rules, ValidationIssue/Error, _SPECIFIC dispatch)
   ▲              ▲
artefact_builder   structure_map_writer      ← both call raise_if_invalid
   ▲               (also imports structures.gen_urn)
structures          ← NEW: delegates DataFrame construction to artefact_builder
```

## Design

### Phase 1 — DUP-03: single validation dialect (do first; unblocks the rest)

`artefact_validation.py`:
- Collapse the byte-identical `_check_representation_map` (`:152`) and
  `_check_multi_representation_map` (`:170`) into one shared helper applying R001 (source),
  R002 (target), R003 (≥1 map). Both entries in `_SPECIFIC` point at it.
- Add **`_check_structure_map`** and register `StructureMap` in `_SPECIFIC` (`:230`):
  - **SM001** — `source` non-empty · **SM002** — `target` non-empty.
  - **SM003** — every `ComponentMap`/`MultiComponentMap` `values` is a resolved rep-map object,
    not a bare URN string (mirrors the existing `validate_structure_map_references` check).
  - Embedded rep-maps are validated via the shared rep-map helper (reuse, don't re-walk).

`structure_map_writer.py`:
- **Delete `_validate_rep_map_fields`**. Have `validate_structure_map_references` /
  `prepare_structure_map_for_upload` call `artefact_validation.raise_if_invalid(structure_map)`
  (which now runs `_check_structure_map`). Keep the existing "unresolved URN reference"
  behaviour by folding it into SM003. `structure_map_writer` gains a
  `from .artefact_validation import raise_if_invalid` import.

Net effect: a StructureMap with `source=""`/`target=""` (what
`build_structure_map_from_template_wb` produces with no structure ids) now fails validation
before upload.

### Phase 2 — DUP-02: builder consolidation

Chosen shape (clean rename + canonical value builder + module-level deprecation shim):

- **Canonical** `build_representation_map` / `build_multi_representation_map` = the **validated
  value builders** from `artefact_builder.py:329/378`. These become the top-level exports.
- **DataFrame adapters** in `structures.py`, renamed:
  - `build_representation_map_from_df(df, agency, id, name, source_cl, target_cl, version,
    description, source_col, target_col, valid_from_col, valid_to_col, generate_urn,
    default_value)` — builds the `maps` list from the frame (via `build_value_map_list`, which
    it already uses), then **delegates** to `artefact_builder.build_representation_map(...)`
    for construction + validation. Same for `build_multi_representation_map_from_df`.
  - The adapters keep exactly the frame-shaping params; the id/agency/name/source/target/maps
    flow through to the validated builder.
- **Deprecation shim:** `structures.build_representation_map` /
  `build_multi_representation_map` remain for one release as
  `@deprecated(replacement="build_representation_map_from_df")` aliases to the adapters, so
  direct `from tidysdmx.structures import build_representation_map` importers get a
  `FutureWarning` instead of a surprise. (Uses the existing `_deprecation.deprecated`.)
- `__init__.py __all__`: export the canonical value builders **and** the `*_from_df` adapters.
  The top-level `build_representation_map` now means the value builder; a caller passing a
  DataFrame positionally hits the id-first `@typechecked` signature and fails loudly (a
  `TypeCheckError`, not silent corruption) — documented in the CHANGELOG/migration note.
- **Internal callers switch to `*_from_df`:** `build_single_component_map`
  (`structures.py:~794`) and the Excel-template rule builder in
  `build_structure_map_from_template_wb` that call the old DataFrame `build_representation_map`.

### Phase 3 — DUP-04: schema-from-table through validated builders

`create_schema_from_table` (`structures.py:1157`) and its helpers
(`_create_dimension_component`, `_create_attribute_component`, and the inline
`ConceptScheme`/`DSD` construction at `:1268`/`:1277`):

- **Add an optional `urn: str | None = None` parameter** to `build_codelist`,
  `build_concept_scheme`, and `build_data_structure_definition` (passed straight through to the
  pysdmx constructor). This is the chosen URN approach — see below.
- Route `Codelist` → `build_codelist(id, agency, name, codes, version, urn=gen_urn(...))`;
  `ConceptScheme` → `build_concept_scheme(..., urn=gen_urn(...))`; `DSD` →
  `build_data_structure_definition(id, agency, name, components, version, urn=gen_urn(...))`.
  Each `gen_urn(...)` replaces one of the three inline `urn="urn:sdmx:..."` f-strings.
- Compute `_to_identifier(schema_id)` **once** and reuse (currently 3×).
- **Confirmed behaviour change (approved):** the validated builders `raise ValidationError`.
  The realistic trigger is an **all-NaN dimension column → empty codelist → C001**. Today
  `create_schema_from_table` builds an invalid schema silently; after this change it raises.
  Update the function's `Raises:` docstring accordingly.

**URN handling — decided: Option B (add `urn=` to the builders).** The decision is forced by
two facts: pysdmx artefacts are **frozen** (verified: setting `.urn` post-construction raises
`AttributeError: immutable type`), and the value builders currently neither set nor accept a
`urn`. So there is no "build then set" path. Rather than dropping the full URNs (which would
change `create_schema_from_table`'s output from full URN to `None`/`short_urn` and risk the
`schema.dsd.to_schema()` path the tests exercise at `test_structures.py:1234`), we add an
optional `urn=` to the three builders and pass `gen_urn(...)`. This keeps the output
byte-identical while gaining validation, and matches the review's "route through builders **and
gen_urn**." The `urn=` param is a natural, backward-compatible addition to the canonical value
builders.

> **Refinement (from planning):** `urn=` is added to **all five** value builders — the three
> schema builders **and** `build_representation_map` / `build_multi_representation_map` —
> because the DUP-02 `*_from_df` adapters (which today generate a URN via `gen_urn`) must pass
> that URN through the canonical value builders to preserve current behaviour. Same rationale
> (frozen artefacts), applied uniformly.

## Testing

- **DUP-03:** a StructureMap with `source=""`/`target=""` now raises via `raise_if_invalid`;
  `prepare_structure_map_for_upload(validate=True)` rejects it; the collapsed rep-map check
  still flags R001/R002/R003 for both `RepresentationMap` and `MultiRepresentationMap`;
  SM003 flags a StructureMap whose ComponentMap `values` is a bare URN string.
- **DUP-02:** `build_representation_map_from_df` produces a `RepresentationMap` equal to the one
  the value builder yields for the same rows; the adapter now raises `ValidationError` on an
  empty/NaN-only frame (previously silent); a `pytest.warns(FutureWarning)` test on the
  `structures.build_representation_map` shim; internal callers still pass.
- **DUP-04:** `create_schema_from_table` happy-path schema is unchanged (component ids, dtypes,
  codelist codes) and its artefact URNs are byte-identical to today (via `gen_urn`); an all-NaN
  dimension column now raises `ValidationError` (C001); `schema.dsd.to_schema()` still works.
  Add a test that `build_codelist`/`build_concept_scheme`/`build_data_structure_definition`
  accept `urn=` and set it on the returned artefact.
- Full suite stays green; coverage stays ≥ 85 (the raised gate).

**Existing tests that must change (measured — not just additions):**
- `tests/test_structures.py` — **~24 call sites** of the DataFrame builders (lines 772, 797,
  803, 809, 821, 831, 844, 851 for `build_representation_map(df, …)`; 998–1110 for
  `build_multi_representation_map(df, …)`) rename to `*_from_df`, plus the two import lines.
  Mostly mechanical, but a few change assertions: e.g. `build_representation_map(empty_df)`
  (`:797`) now raises `ValidationError` (⊂ `ValueError`, so `pytest.raises(ValueError)` holds;
  any `match=` string changes).
- `tests/test_structure_map_writer.py::TestValidateRepMapFields` (`:371`) — **6 tests** call the
  deleted `_validate_rep_map_fields` (`:374, :382, :399, :416, :431, :439`) + its import
  (`:18`). Migrate to `test_artefact_validation.py` (assert R001/R002/R003 via `validate()`),
  or drop as redundant with the rep-map tests already there.
- `tests/test_structure_map_writer.py::TestValidateStructureMapReferences` (`:637`) — **~5
  tests** keep raising (`ValidationError` ⊂ `ValueError`) but their `match="unresolved"/"invalid"`
  strings become SM003/R00x — update the `match=` strings only.
- `tests/test_artefact_builder.py` — **no change** (its `build_representation_map(id=…)` calls
  target the canonical value builders, which keep their names).

## Sequencing & commits

Three reviewable, independently-green commits, in dependency order:
1. `refactor(validation): one dialect — collapse rep-map checks, add _check_structure_map, route structure_map_writer through raise_if_invalid` (DUP-03).
2. `refactor(builders): canonical value builders + *_from_df DataFrame adapters` (DUP-02, incl. shim + internal-caller switch + `__all__`).
3. `refactor(structures): route create_schema_from_table through validated builders + gen_urn` (DUP-04).

## Migration / breaking changes

- Top-level `tidysdmx.build_representation_map` / `build_multi_representation_map` now mean the
  **value** builders (id-first). DataFrame callers migrate to the `*_from_df` names.
  `tidysdmx.structures.build_representation_map` keeps working for one release with a
  `FutureWarning`.
- `create_schema_from_table` now raises `ValidationError` on tables that yield invalid
  artefacts (e.g. empty codelists).
- CHANGELOG note required; version bump handled by semantic-release from the commit types.
