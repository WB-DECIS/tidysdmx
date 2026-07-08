# One Validated Construction Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `artefact_validation` the single validation dialect and route every artefact
construction path through the validated builders, so construction fails fast and no invalid
artefact escapes to FMR.

**Architecture:** Three phases in dependency order — (1) consolidate validation into
`artefact_validation` and route `structure_map_writer` through it; (2) make the value builders
in `artefact_builder` the canonical `build_(multi_)representation_map` and turn the DataFrame
builders in `structures` into `*_from_df` adapters that delegate to them; (3) route
`create_schema_from_table` through the validated builders + `gen_urn`. New one-way dependency
edge: `structures → artefact_builder → artefact_validation` (verified acyclic — neither
`artefact_builder` nor `artefact_validation` imports `structures`).

**Tech Stack:** Python 3.11+, pysdmx 1.17, pandas, typeguard `@typechecked`, pytest, ruff,
Poetry. Runner: `python -m pytest`, `python -m ruff` (Poetry shim is blocked in this env).

## Global Constraints

- Google-style docstrings on all public functions (ruff `D` rules, `convention=google`).
- `@typechecked` on all public functions.
- Every commit must leave `python -m pytest -m "not integration" -q` green, `python -m ruff
  check src tests` clean, and `python -m ruff format --check src tests` clean.
- Coverage gate is 85 (`pyproject.toml` `fail_under = 85`); do not drop below.
- pysdmx artefacts are **frozen** — never set an attribute after construction; pass it to the
  constructor.
- Conventional-commit messages; end each with the `Co-Authored-By` trailer used in this repo.
- Line length 88 (ruff default). Run `python -m ruff format <files>` before committing.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `src/tidysdmx/artefact_validation.py` | The single validation dialect (rules, `_SPECIFIC`, `validate`) | collapse rep-map checks; add `_check_structure_map`; register `StructureMap` |
| `src/tidysdmx/structure_map_writer.py` | Collect/validate StructureMaps for upload | delete `_validate_rep_map_fields`; route through `raise_if_invalid` |
| `src/tidysdmx/artefact_builder.py` | Validated value builders | add optional `urn=` to 5 builders |
| `src/tidysdmx/structures.py` | DataFrame-driven builders + schema-from-table | rename builders → `*_from_df` + delegate; deprecation shims; route `create_schema_from_table` |
| `src/tidysdmx/__init__.py` | Public API surface | export canonical value builders + `*_from_df` |
| `tests/test_artefact_validation.py` | validation tests | add StructureMap + collapse tests |
| `tests/test_structure_map_writer.py` | writer tests | drop `TestValidateRepMapFields`; fix `match=` strings |
| `tests/test_artefact_builder.py` | builder tests | add `urn=` tests |
| `tests/test_structures.py` | structures tests | rename call sites → `*_from_df`; add id/name; empty→ValidationError |

---

## Phase 1 — DUP-03: one validation dialect

### Task 1: Collapse the duplicate representation-map checks

**Files:**
- Modify: `src/tidysdmx/artefact_validation.py:152-187`
- Test: `tests/test_artefact_validation.py`

**Interfaces:**
- Produces: `_check_rep_map_fields(a: RepresentationMap | MultiRepresentationMap) -> list[ValidationIssue]` — the shared R001/R002/R003 check reused by Task 2.

- [ ] **Step 1: Confirm the existing rep-map tests pass (baseline for a pure refactor)**

Run: `python -m pytest tests/test_artefact_validation.py -k "Representation" -q`
Expected: PASS (they assert R001/R002/R003 for both map types).

- [ ] **Step 2: Replace the two byte-identical checks with a shared helper**

In `src/tidysdmx/artefact_validation.py`, replace the bodies of `_check_representation_map`
and `_check_multi_representation_map` (currently duplicated) with:

```python
def _check_rep_map_fields(
    a: RepresentationMap | MultiRepresentationMap,
) -> list[ValidationIssue]:
    """Shared R001/R002/R003 checks for (multi) representation maps."""
    issues: list[ValidationIssue] = []
    if not a.source:
        issues.append(_issue("R001", a, "source must be populated.", "source"))
    if not a.target:
        issues.append(_issue("R002", a, "target must be populated.", "target"))
    if not a.maps:
        issues.append(
            _issue(
                "R003",
                a,
                "maps must contain at least one value mapping.",
                "maps",
            )
        )
    return issues


def _check_representation_map(a: RepresentationMap) -> list[ValidationIssue]:
    return _check_rep_map_fields(a)


def _check_multi_representation_map(
    a: MultiRepresentationMap,
) -> list[ValidationIssue]:
    return _check_rep_map_fields(a)
```

- [ ] **Step 3: Run the rep-map tests + ruff**

Run: `python -m pytest tests/test_artefact_validation.py -q && python -m ruff check src/tidysdmx/artefact_validation.py`
Expected: PASS / clean.

- [ ] **Step 4: Commit**

```bash
git add src/tidysdmx/artefact_validation.py
git commit -m "refactor(validation): share the representation-map field checks

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Add `_check_structure_map` and register `StructureMap`

**Files:**
- Modify: `src/tidysdmx/artefact_validation.py` (imports at `:29`, `_SPECIFIC` at `:230`)
- Test: `tests/test_artefact_validation.py`

**Interfaces:**
- Consumes: `_check_rep_map_fields` (Task 1), `_issue`, `_SPECIFIC`.
- Produces: `StructureMap` validation via `validate()` — rules SM001 (source), SM002 (target),
  SM003 (unresolved ComponentMap/MultiComponentMap reference), plus R001/R002/R003 on embedded
  rep-maps.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_artefact_validation.py` (it already imports `StructureMap`, `ComponentMap`,
`RepresentationMap`, `ValueMap`, `MultiValueMap` from `pysdmx.model`):

```python
def _structure_map(**over):
    kw = {
        "id": "SM",
        "agency": "AGY",
        "name": "SM",
        "version": "1.0",
        "source": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=A:SRC(1.0)",
        "target": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=A:TGT(1.0)",
        "maps": (),
    }
    kw.update(over)
    return StructureMap(**kw)


class TestStructureMap:
    def test_empty_source_flagged(self):
        """A StructureMap with empty source triggers SM001."""
        issues = validate(_structure_map(source=""))
        assert any(i.rule_id == "SM001" for i in issues)

    def test_empty_target_flagged(self):
        """A StructureMap with empty target triggers SM002."""
        issues = validate(_structure_map(target=""))
        assert any(i.rule_id == "SM002" for i in issues)

    def test_unresolved_urn_reference_flagged(self):
        """A ComponentMap whose values is a bare URN string triggers SM003."""
        cm = ComponentMap(
            source="COUNTRY",
            target="GEO",
            values="urn:sdmx:org.sdmx.infomodel.structuremapping.RepresentationMap=A:RM(1.0)",
        )
        issues = validate(_structure_map(maps=(cm,)))
        assert any(i.rule_id == "SM003" for i in issues)

    def test_embedded_rep_map_empty_source_flagged(self):
        """An embedded RepresentationMap with empty source triggers R001."""
        rm = RepresentationMap(
            id="RM",
            agency="AGY",
            name="RM",
            source="",
            target="urn:t",
            maps=[ValueMap(source="A", target="B")],
        )
        cm = ComponentMap(source="COUNTRY", target="GEO", values=rm)
        issues = validate(_structure_map(maps=(cm,)))
        assert any(i.rule_id == "R001" for i in issues)

    def test_valid_structure_map_ok(self):
        """A StructureMap with non-empty source/target and no maps is valid."""
        assert validate(_structure_map()) == []
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_artefact_validation.py::TestStructureMap -q`
Expected: FAIL (no SM001/SM002/SM003 — `_SPECIFIC` has no `StructureMap` entry yet).

- [ ] **Step 3: Add imports**

In `src/tidysdmx/artefact_validation.py`, change the map import (`:29`) from:

```python
from pysdmx.model.map import MultiRepresentationMap, RepresentationMap
```

to:

```python
from pysdmx.model.map import (
    ComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    RepresentationMap,
    StructureMap,
)
```

- [ ] **Step 4: Add `_check_structure_map`**

Add near the other `_check_*` functions (before `_SPECIFIC`):

```python
def _check_structure_map(a: StructureMap) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not a.source:
        issues.append(_issue("SM001", a, "source must be populated.", "source"))
    if not a.target:
        issues.append(_issue("SM002", a, "target must be populated.", "target"))
    for i, rule in enumerate(a.maps):
        if not isinstance(rule, (ComponentMap, MultiComponentMap)):
            continue
        values = rule.values
        if isinstance(values, str):
            issues.append(
                _issue(
                    "SM003",
                    a,
                    f"map[{i}] references an unresolved RepresentationMap URN "
                    f"'{values}'; embed the object instead.",
                    "maps",
                )
            )
        elif isinstance(values, (RepresentationMap, MultiRepresentationMap)):
            issues.extend(_check_rep_map_fields(values))
    return issues
```

- [ ] **Step 5: Register it in `_SPECIFIC`**

In the `_SPECIFIC` dict (`:230`), add:

```python
    StructureMap: _check_structure_map,
```

- [ ] **Step 6: Run tests + ruff**

Run: `python -m pytest tests/test_artefact_validation.py -q && python -m ruff check src/tidysdmx/artefact_validation.py`
Expected: PASS / clean.

- [ ] **Step 7: Commit**

```bash
git add src/tidysdmx/artefact_validation.py tests/test_artefact_validation.py
git commit -m "feat(validation): validate StructureMap publish-readiness

Add _check_structure_map (SM001 source, SM002 target, SM003 unresolved
reference) and validate embedded rep-maps via the shared check. Closes
the gap where a StructureMap with empty source/target passed validation.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Route `structure_map_writer` through `raise_if_invalid`

**Files:**
- Modify: `src/tidysdmx/structure_map_writer.py` (delete `_validate_rep_map_fields:61-75`;
  rewrite `validate_structure_map_references:146-211`; add import)
- Test: `tests/test_structure_map_writer.py`

**Interfaces:**
- Consumes: `artefact_validation.raise_if_invalid`, `_check_structure_map` (Task 2).
- Produces: `validate_structure_map_references(sm)` now raises `ValidationError` (⊂ `ValueError`)
  carrying SM/R rule ids; `prepare_structure_map_for_upload` unchanged in signature.

- [ ] **Step 1: Update the existing writer tests to the new contract**

In `tests/test_structure_map_writer.py`:
- Remove the `_validate_rep_map_fields` import (`:18`) and delete the entire
  `TestValidateRepMapFields` class (`:371`, 6 tests) — its rule coverage now lives in
  `tests/test_artefact_validation.py::TestStructureMap` and `TestCodelist`/rep-map tests.
- In `TestValidateStructureMapReferences`, update the `match=` strings: the unresolved-reference
  test changes `match="unresolved"` → `match="SM003"`; the missing-source test changes
  `match="invalid"` → `match="R001"`; the empty-maps test changes `match="invalid"` →
  `match="R003"`. (The `pytest.raises(ValueError, …)` wrappers stay — `ValidationError` is a
  `ValueError`.)

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_structure_map_writer.py -q`
Expected: FAIL (old `_validate_rep_map_fields` still raises plain `ValueError` with old
messages; new `match=` strings don't match yet).

- [ ] **Step 3: Add the import and delete `_validate_rep_map_fields`**

In `src/tidysdmx/structure_map_writer.py`, add after the existing imports:

```python
from .artefact_validation import raise_if_invalid
```

Delete the whole `_validate_rep_map_fields` function (`:61-75`).

- [ ] **Step 4: Rewrite `validate_structure_map_references`**

Replace the body (`:168-211`) so it delegates:

```python
    raise_if_invalid(structure_map)
```

Keep the function's signature, `@typechecked`, and docstring (update the docstring's `Raises:`
to say `ValidationError` and remove the `unresolved`/`invalid_rep_maps` prose). The
`_get_embedded_rep_map`, `_replace_values_with_urn`, `_convert_to_urn_references`, and
`collect_structure_map_artifacts` functions are unchanged.

- [ ] **Step 5: Run tests + ruff**

Run: `python -m pytest tests/test_structure_map_writer.py -q && python -m ruff check src/tidysdmx/structure_map_writer.py`
Expected: PASS / clean. (If `validate_structure_map_references` no longer references
`ComponentMap`/`MultiComponentMap` locally, remove any now-unused imports ruff flags.)

- [ ] **Step 6: Commit**

```bash
git add src/tidysdmx/structure_map_writer.py tests/test_structure_map_writer.py
git commit -m "refactor(writer): validate StructureMaps via artefact_validation

Delete the home-grown _validate_rep_map_fields and route
validate_structure_map_references through raise_if_invalid, so the
writer uses the single validation dialect.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Phase 2 — DUP-02: builder consolidation

### Task 4: Add an optional `urn=` to the five value builders

**Files:**
- Modify: `src/tidysdmx/artefact_builder.py` (`build_codelist:43`, `build_concept_scheme:84`,
  `build_data_structure_definition:243`, `build_representation_map:329`,
  `build_multi_representation_map:378`)
- Test: `tests/test_artefact_builder.py`

**Interfaces:**
- Produces: each of the 5 builders gains a trailing `urn: str | None = None` parameter, passed
  straight to the pysdmx constructor. Used by Tasks 5 (adapters) and 6 (schema-from-table).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_artefact_builder.py`:

```python
def test_build_codelist_sets_urn():
    """build_codelist stores an explicitly supplied urn."""
    cl = build_codelist(
        id="CL",
        agency="AG",
        name="n",
        codes=[Code(id="A")],
        urn="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=AG:CL(1.0)",
    )
    assert cl.urn == "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=AG:CL(1.0)"
```

(`Code` is already imported in that test module; if not, add
`from pysdmx.model import Code`.)

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_artefact_builder.py::test_build_codelist_sets_urn -q`
Expected: FAIL with `unexpected keyword argument 'urn'`.

- [ ] **Step 3: Add `urn=` to all five builders**

For each of `build_codelist`, `build_concept_scheme`, `build_data_structure_definition`,
`build_representation_map`, `build_multi_representation_map`: add `urn: str | None = None,` as
the last parameter (after `description`), document it in the `Args:` block
(`urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.`), and pass
`urn=urn,` to the pysdmx constructor call. Example for `build_codelist`:

```python
    cl = Codelist(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        items=tuple(codes),
        sdmx_type=sdmx_type,
        urn=urn,
    )
    raise_if_invalid(cl)
    return cl
```

- [ ] **Step 4: Run test + full builder suite + ruff**

Run: `python -m pytest tests/test_artefact_builder.py -q && python -m ruff check src/tidysdmx/artefact_builder.py`
Expected: PASS / clean.

- [ ] **Step 5: Commit**

```bash
git add src/tidysdmx/artefact_builder.py tests/test_artefact_builder.py
git commit -m "feat(builders): accept an optional urn on the value builders

Lets the DataFrame adapters and schema-from-table pass a gen_urn full
URN through the validated builders (pysdmx artefacts are frozen, so the
URN must be set at construction).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Rename DataFrame builders → `*_from_df`, delegate, and shim

**Files:**
- Modify: `src/tidysdmx/structures.py` (`build_representation_map:502`,
  `build_multi_representation_map:596`, internal callers, imports)
- Modify: `src/tidysdmx/__init__.py` (`:7`, `:42-49`, `:114-116`)
- Test: `tests/test_structures.py`

**Interfaces:**
- Consumes: `artefact_builder.build_representation_map` / `build_multi_representation_map` with
  the new `urn=` (Task 4).
- Produces: `build_representation_map_from_df(...)`, `build_multi_representation_map_from_df(...)`
  (same DataFrame signatures as the old builders); top-level `build_representation_map` /
  `build_multi_representation_map` now mean the **value** builders; `structures.build_representation_map`
  / `build_multi_representation_map` remain as `@deprecated` shims.

- [ ] **Step 1: Write the failing adapter tests**

Add to `tests/test_structures.py` (imports for the new names are added in Step 6):

```python
class TestBuildRepresentationMapFromDf:
    def test_delegates_and_validates(self):
        """A well-formed frame yields a RepresentationMap via the value builder."""
        df = pd.DataFrame({"source": ["BE"], "target": ["BEL"]})
        rm = build_representation_map_from_df(df, agency="ECB", id="RM1", name="Ctry")
        assert isinstance(rm, RepresentationMap)
        assert rm.urn is not None and "RM1" in rm.urn

    def test_empty_frame_raises_validation_error(self):
        """An empty frame -> no maps -> R003 -> ValidationError."""
        from tidysdmx.artefact_validation import ValidationError

        with pytest.raises(ValidationError):
            build_representation_map_from_df(
                pd.DataFrame({"source": [], "target": []}),
                agency="ECB",
                id="RM1",
                name="Ctry",
            )

    def test_deprecated_shim_warns(self):
        """The old structures.build_representation_map name warns."""
        import tidysdmx.structures as s

        df = pd.DataFrame({"source": ["BE"], "target": ["BEL"]})
        with pytest.warns(FutureWarning, match="build_representation_map_from_df"):
            s.build_representation_map(df, agency="ECB", id="RM1", name="Ctry")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_structures.py::TestBuildRepresentationMapFromDf -q`
Expected: FAIL (`build_representation_map_from_df` undefined).

- [ ] **Step 3: Add the aliased artefact_builder import to `structures.py`**

Add to the intra-package import group in `src/tidysdmx/structures.py` (direct submodule import
— NOT `from . import artefact_builder`, which would re-enter the package `__init__`):

```python
from .artefact_builder import (
    build_multi_representation_map as _build_multi_representation_map,
    build_representation_map as _build_representation_map,
)
```

Also ensure `from ._deprecation import deprecated` is imported (add it if absent).

- [ ] **Step 4: Rename + delegate `build_representation_map` → `build_representation_map_from_df`**

Rename the function (`:502`) to `build_representation_map_from_df` and replace its
construction tail (the `urn = ...` + `return RepresentationMap(...)` block, `:577-592`) with a
delegation:

```python
    value_maps = build_value_map_list(
        df,
        source_col=source_col,
        target_col=target_col,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
        default_value=default_value,
    )
    urn = gen_urn("RepresentationMap", agency, id, version) if (generate_urn and id) else None
    return _build_representation_map(
        id=id,
        agency=agency,
        name=name,
        source=_resolve_representation_ref(source_cl),
        target=_resolve_representation_ref(target_cl),
        maps=value_maps,
        version=version,
        description=description,
        urn=urn,
    )
```

Do the same for `build_multi_representation_map` → `build_multi_representation_map_from_df`
(`:596`): keep its column-validation body, then replace the `urn = ...` + `return
MultiRepresentationMap(...)` tail (`:694-714`) with a call to `_build_multi_representation_map(
id=id, agency=agency, name=name, source=[...], target=[...], maps=multi_value_maps,
version=version, description=description, urn=urn)`, computing `source`/`target` with the same
`_resolve_representation_ref`/`DataType.STRING` expressions the old body used.

> Note: the value builders require `id: str` and `name: str`. The `*_from_df` contract is now
> stricter than the old lenient DataFrame builders — an id-less/name-less or empty-frame call
> now raises. This is intended (validation everywhere).

- [ ] **Step 5: Add the deprecation shims**

Immediately after each new `*_from_df` function in `structures.py`, add:

```python
build_representation_map = deprecated(
    replacement="build_representation_map_from_df"
)(build_representation_map_from_df)
build_multi_representation_map = deprecated(
    replacement="build_multi_representation_map_from_df"
)(build_multi_representation_map_from_df)
```

- [ ] **Step 6: Switch internal callers and update `tests/test_structures.py`**

- In `src/tidysdmx/structures.py`, update internal callers of the old DataFrame builders to the
  `*_from_df` names: `build_single_component_map` (`~:717`) and the representation-rule branch
  inside `build_structure_map_from_template_wb`. Search: `grep -n "build_representation_map\|build_multi_representation_map" src/tidysdmx/structures.py` and switch every *call* (not the
  shim assignments) to `*_from_df`.
- In `tests/test_structures.py`, update the import to bring in the new names
  (`build_representation_map_from_df`, `build_multi_representation_map_from_df`,
  `RepresentationMap`) and rename the ~24 call sites (lines 772–851, 998–1110). Command:
  `python - <<'PY'` isn't needed — use editor find/replace within that file: `build_representation_map(` → `build_representation_map_from_df(` and `build_multi_representation_map(` → `build_multi_representation_map_from_df(`.
- Fix the sites that omitted `id`/`name` (e.g. `:831`, `:844`, `:851` call with only `df=`): add
  `id="RM1", name="Ctry"` so the now-validated path passes M001/M003.
- Fix empty-frame assertions: sites asserting `pytest.raises(ValueError)` on an empty frame now
  raise `ValidationError` (still a `ValueError`, so the wrapper holds; remove any `match=` that
  referenced the old message).

- [ ] **Step 7: Update `src/tidysdmx/__init__.py` exports**

- In the `from .artefact_builder import (` block (`:7`), add `build_multi_representation_map,`
  and `build_representation_map,` (the canonical value builders).
- In the `from .structures import (` block (`:42`), rename the two imported names to
  `build_multi_representation_map_from_df` and `build_representation_map_from_df`.
- In `__all__`, add `"build_multi_representation_map_from_df",` and
  `"build_representation_map_from_df",` (keep `"build_representation_map"` /
  `"build_multi_representation_map"` — they now resolve to the value builders). Keep the block
  alphabetically sorted.

- [ ] **Step 8: Run the full suite + ruff**

Run: `python -m pytest -m "not integration" -q && python -m ruff check src tests && python -m ruff format --check src tests`
Expected: PASS / clean. Investigate any residual `build_representation_map(df, …)` call the
grep missed.

- [ ] **Step 9: Commit**

```bash
git add src/tidysdmx/structures.py src/tidysdmx/__init__.py tests/test_structures.py
git commit -m "refactor(builders): canonical value builders + *_from_df adapters

build_(multi_)representation_map now mean the validated value builders;
the DataFrame builders become build_(multi_)representation_map_from_df
that delegate to them (validated), with a @deprecated shim under the old
structures name for one release.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Phase 3 — DUP-04: schema-from-table through validated builders

### Task 6: Route `create_schema_from_table` through the validated builders + `gen_urn`

**Files:**
- Modify: `src/tidysdmx/structures.py` (`create_schema_from_table:1157` and its helpers
  `_create_dimension_component`, `_create_attribute_component`, and the inline
  `ConceptScheme`/`DSD` construction at `:1268`/`:1277`)
- Test: `tests/test_structures.py`

**Interfaces:**
- Consumes: `artefact_builder.build_codelist` / `build_concept_scheme` /
  `build_data_structure_definition` with `urn=` (Task 4), `gen_urn`, `_to_identifier`.
- Produces: `create_schema_from_table` output unchanged (same components, dtypes, codes, full
  URNs) but now raises `ValidationError` on degenerate input.

- [ ] **Step 1: Write the failing tests**

Add to the `create_schema_from_table` test class in `tests/test_structures.py`:

```python
    def test_all_nan_dimension_raises_validation_error(self):
        """A dimension column with no values -> empty codelist -> C001."""
        from tidysdmx.artefact_validation import ValidationError

        df = pd.DataFrame({"REF_AREA": [None], "TIME_PERIOD": ["2020"], "OBS": [1]})
        with pytest.raises(ValidationError):
            create_schema_from_table(
                df, dimensions=["REF_AREA"], time_dimension="TIME_PERIOD", measure="OBS"
            )

    def test_generated_dsd_urn_matches_gen_urn(self):
        """The generated DSD carries the full gen_urn URN."""
        df = pd.DataFrame({"FREQ": ["A"], "TIME_PERIOD": ["2020"], "OBS": [1]})
        sc = create_schema_from_table(
            df, dimensions=["FREQ"], time_dimension="TIME_PERIOD", measure="OBS",
            agency_id="WB.DP", schema_id="DP_SCHEMA",
        )
        assert sc.dsd.urn == gen_urn(
            "DataStructure", "WB.DP", "DP_SCHEMA", "1.0"
        )
```

(`gen_urn` is imported into the test module in Task 5's import update; add it if absent.)

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_structures.py -k "all_nan_dimension or generated_dsd_urn" -q`
Expected: FAIL (today it builds silently / sets an inline-string URN that may differ).

- [ ] **Step 3: Route the codelist construction through `build_codelist`**

Read `_create_dimension_component` and `_create_attribute_component` (between `:925` and
`:1156`). Where each constructs a `Codelist(...)` directly with an inline `urn=` f-string,
replace it with:

```python
codelist = build_codelist(
    id=codelist_id,
    agency=agency_id,
    name=codelist_name,
    codes=codes,
    version=version,
    urn=gen_urn("Codelist", agency_id, codelist_id, version),
)
```

(keep the surrounding logic that computes `codelist_id`, `codelist_name`, `codes`). Import
`build_codelist`, `build_concept_scheme`, `build_data_structure_definition` at the top of
`structures.py` alongside the Task-5 aliased imports:

```python
from .artefact_builder import (
    build_codelist,
    build_concept_scheme,
    build_data_structure_definition,
    build_multi_representation_map as _build_multi_representation_map,
    build_representation_map as _build_representation_map,
)
```

- [ ] **Step 4: Route the ConceptScheme and DSD through the validated builders**

Replace the inline `ConceptScheme(...)` (`:1268-1275`) and `DataStructureDefinition(...)`
(`:1277-1284`) blocks, and compute the identifier once:

```python
    dsd_id = _to_identifier(schema_id)

    concept_scheme = build_concept_scheme(
        id=scheme_id,
        agency=agency_id,
        name=f"{schema_id} generated concept scheme",
        concepts=concept_items,
        version=version,
        urn=gen_urn("ConceptScheme", agency_id, scheme_id, version),
    )

    dsd = build_data_structure_definition(
        id=dsd_id,
        agency=agency_id,
        name=f"{schema_id} generated DSD",
        components=Components(components),
        version=version,
        urn=gen_urn("DataStructure", agency_id, dsd_id, version),
    )

    return SchemaComponents(dsd=dsd, concept_scheme=concept_scheme, codelists=codelists)
```

Update the `create_schema_from_table` docstring `Raises:` to add
`ValidationError: If the generated artefacts are not publish-ready (e.g. an empty codelist).`

- [ ] **Step 5: Run the targeted + full suite + ruff**

Run: `python -m pytest tests/test_structures.py -k "create_schema" -q && python -m pytest -m "not integration" -q && python -m ruff check src tests && python -m ruff format --check src tests`
Expected: PASS / clean. Confirm the existing `test_create_schema_structure` /
`_time_dimension` tests (which call `schema.dsd.to_schema()`) still pass — proving the
full-URN output is preserved.

- [ ] **Step 6: Commit**

```bash
git add src/tidysdmx/structures.py tests/test_structures.py
git commit -m "refactor(structures): validate create_schema_from_table artefacts

Route the generated Codelist/ConceptScheme/DSD through the validated
builders with gen_urn URNs (compute the identifier once). The DataFrame
path now raises ValidationError on degenerate input (e.g. an all-NaN
dimension column) instead of silently building an invalid schema.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage:**
- DUP-03 (collapse checks, `_check_structure_map`, delete `_validate_rep_map_fields`, route
  writer) → Tasks 1–3. ✓
- DUP-02 (canonical value builders + `*_from_df` + shim + `__all__` + internal callers) →
  Tasks 4–5. ✓
- DUP-04 (route `create_schema_from_table` + `gen_urn` + raise-on-invalid) → Task 6. ✓
- URN handling = Option B (`urn=` on the builders) → Task 4, consumed by Tasks 5 & 6. ✓
- Test migration set (24 renames, 6 dropped, ~5 `match=` updates, `test_artefact_builder`
  unchanged) → Tasks 3, 5. ✓

**Placeholder scan:** No TBD/TODO; every code step shows code; the one "read the current helper"
step (Task 6 Step 3) still shows the exact replacement pattern.

**Type consistency:** `_check_rep_map_fields` (Task 1) is reused by name in Task 2;
`build_*_from_df` names match between Tasks 5 and the `__init__`/test updates; `urn=` param name
consistent across Tasks 4/5/6; `gen_urn` artefact-type strings (`"RepresentationMap"`,
`"Codelist"`, `"ConceptScheme"`, `"DataStructure"`) match `SDMX_PACKAGE_MAP` keys.

**Out of scope:** SMP-02 (splitting `structures.py`) — separate plan; it will relocate this
consolidated code behind unchanged `__init__` re-exports.
