# tidysdmx Code Review — July 2026

**Scope:** full-codebase review of the `dev` branch at v0.9.0 (`ac37aed`), with emphasis on
code simplicity, duplication, maintainability, consistency, documentation, potential bugs,
pysdmx complementarity, and engineering practice. Includes a follow-through audit of the
[June 2026 review](2026-06-architecture-review.md).
**Method:** baseline measurement (tests, coverage, lint), first-hand read of all 12 source
modules, and **runtime verification of every reported bug** — each correctness finding below
was reproduced with a small script against the installed package before being written up.
Findings that could not be confirmed were dropped. pysdmx claims were checked against the
installed 1.13.0 source and the upstream release notes.

Severity rubric (same as June): **P0** correctness / silent data corruption ·
**P1** API contract & interoperability · **P2** maintainability · **P3** style/polish.

> **Roadmap update (post-review).** Since this review was written the team adopted a retirement
> roadmap: `kedro.py` and the bespoke JSON-mapping pipeline (Engine A: `read_mapping`,
> `transform_source_to_target`, `map_to_sdmx`, `vectorized_lookup_ordered_*`, `standardize_sdmx`)
> plus the old Excel *writer* template are being **soft-deprecated** in favour of pysdmx
> `StructureMap`s (Engine B, `mapping.py`). §1–§12 record the findings **as measured on 0.9.0** —
> where `standardize_sdmx` was still the nominally-supported entry — and remain accurate as of
> that state. The forward plan lives in **§13**, which resolves bugs inside retiring code by
> deprecation rather than repair. Read §13's disposition list before actioning any §4–§9 finding.

---

## 1. Executive summary

Module-by-module, the codebase remains in good shape: 550 unit tests pass, coverage is 87%,
ruff is clean, docstrings are consistent, and the June quick-wins all landed. The problems
this review found live almost entirely **at the seams between modules — and precisely those
seams are untested**. The headline findings:

1. **The bespoke JSON mapping pipeline is silently broken end-to-end (BUG-01, P0).**
   `read_mapping` flattens the `representation` sub-keys to the top level of its result, but
   `map_to_sdmx` looks them up under a nested `mapping["representation"]` key. Feeding
   `read_mapping` output into `standardize_sdmx` (the documented composition, and the exact
   path `kd_read_mappings` → `kd_standardize_sdmx` takes) applies **zero** value mappings and
   raises no error. None of the three functions involved has a single test.
2. **`read_mapping` corrupts legitimate `"NA"` codes (BUG-02, P0).** A blanket
   `.replace("NA", pd.NA)` turns the ISO 3166 code for Namibia — or any other legitimate
   `"NA"` source/target value — into a missing value.
3. **`kedro.py` silently drops data and its safety check is dead (BUG-03, P1).**
   `kd_standardize_sdmx` calls `check_dict_keys(...)` and discards the returned error string;
   partitions without a matching mapping key are then dropped without a warning (verified:
   2 partitions in → 1 row out, no error). The module has no tests, no type checking, and is
   excluded from coverage on an unfounded rationale.
4. **Two parallel mapping engines with conflicting semantics (DUP-01, P1).** The
   `tidysdmx.py` engine re-sorts rules by pattern length (contradicting its own "first match
   wins" docstring), matches substrings, and stringifies NaN to `"nan"`; the pysdmx-native
   `mapping.py` engine is literal-first, full-match, NaN-safe. Same feature, two divergent
   behaviours — and `docs/pysdmx-overview.md` §11 explicitly says to use the latter.
5. **June's 0.9 deprecation milestone was missed (API-01, P1).** The supported
   `standardize_sdmx` still calls deprecated `standardize_data_for_upload` (FutureWarning on
   every call), all four deprecated functions are still exported, and the docs site still
   documents them as current API. Likewise pysdmx is still locked at 1.13.0 while upstream is
   at 1.17.0, keeping the `fix_sdmx_xml_datatype_tags` workaround alive (PX-01).

Positive verdicts worth recording: the June-review quick wins are all in and map 1:1 to
commits; release automation (PSR v10 + Trusted Publishing + RELEASING.md) is exemplary;
`apply_multi_component_map`'s vectorisation is careful and well-commented;
`artefact_builder`/`artefact_validation` is a well-designed pair with rule IDs and a clear
upstream migration plan; conventional commits are consistently used.

## 2. Baseline metrics (this branch, measured)

| Metric | Value |
|---|---|
| Tests (`-m "not integration"`) | 550 passed, 13 skipped, 42 deselected, 0 failed |
| Coverage | **86.71%** (gate: 70) — June measured 88.9%; drifting down |
| Weakest modules | mapping.py 57%, tidysdmx.py 62%, utils.py 84% (kedro.py omitted) |
| ruff check / format | clean / clean |
| pysdmx installed | 1.13.0 (constraint `^1.13.0`; upstream latest **1.17.0**, 2026-06-23) |
| Source size | 5,602 lines / 12 modules; structures.py 2,353, tidysdmx.py 773 |
| pip-audit | not re-run (unavailable in this environment); June finding stands: runtime `idna 3.11` CVE, fix in 3.15 |

The low coverage of `mapping.py`/`tidysdmx.py` in the unit lane is exactly where BUG-01,
BUG-05 and BUG-10 live: `map_to_sdmx` (385–426), `read_mapping` (733–770) and
`apply_multi_component_map` (291–372) have no unit coverage.

## 3. Follow-through on the June 2026 review

**Landed since June** (verified on this branch): PROD-01 semantic-release fully wired;
TEST-02 integration markers on all FMR-fixture tests; PROD-04 `py.typed`; PROD-06 numpy
declared; CONS-03 `_parse_validity_date`; print→logging in mapping.py; narrowed excepts in
utils.py; `parse_dsd_id` delegates to `parse_artefact_id`; canonical truncation notes;
`validate_dataset_local` schema-context inference (#218/#220).

**Still open** — the June backlog has not otherwise moved:

| June ID | Finding | Status on dev @ 0.9.0 |
|---|---|---|
| ARCH-01 / A3 (P1) | Excel writer ↔ reader incompatible | **Open** — re-verified by round-trip repro (BUG-06) |
| ARCH-05 (P1) | `standardize_sdmx` calls deprecated fn; deprecated names in `__all__` | **Open** — `tidysdmx.py:196`; the "drop at 0.9" plan slipped (API-01) |
| PYSDMX-01/07 / A1 (P1) | Bump pysdmx (≥1.14 fixes PR #556), delete `fix_sdmx_xml_datatype_tags` | **Open** — lock still 1.13.0; upstream now 1.17.0 (PX-01) |
| TEST-01 / A2 (P1) | Commit FMR cassettes; hard-fail in CI | **Open** — `ifpri_asti_*.pkl` still missing; live-FMR fallback remains (TST-03) |
| PROD-05 (P1) | Runtime `idna 3.11` CVE | **Open** — still locked at 3.11 (CI-04) |
| CONS-20 (P2) | Duplicate `build_(multi_)representation_map` in two modules | **Open** (DUP-02) |
| TEST-05/03, ARCH-06 | kedro.py untested & coverage-omitted; 6 untested exports | **Open** (TST-01/02) |
| TEST-06/08/12 | Crashing skipped tests; NaN→"nan"; mutable session fixtures | **Open** (TST-04/07, BUG-05) |
| ARCH-02/03 / B1-B3 (P2) | structures.py / tidysdmx.py god-modules | **Open** — structures.py grew to 2,353 lines (SMP-02) |
| ARCH-09, ARCH-14 | Zombie `_extract_artefact_id`; `apply_component_map` not exported | **Open** (SMP-03, API-02) |
| ARCH-12/13 / B8 | Dual doc toolchains; stale duplicate docs; broken RTD | **Open** (DOC-01/02) |
| TEST-13/14, PROD-02 / C1 | Coverage gate 70; no 3.13 in CI; dead `setup-uv` step | **Open** (CI-01/02/03) |

Given two consecutive reviews now flag the same P1s, §13 proposes tying them to the 0.10
milestone explicitly rather than re-backlogging.

## 4. Correctness findings (all runtime-verified)

### BUG-01 (P0) — `read_mapping` output disables all value mapping in `standardize_sdmx`
`read_mapping` stores each representation sub-key at the **top level** of its result
(`tidysdmx.py:760` — `result[sub_key] = ...`; the JSON's actual shape is nested, see
`notebooks/utils/master_mapping_wb_shp.json`). `map_to_sdmx` reads
`mapping.get("representation", {})` (`tidysdmx.py:387`) — the nested shape. Verified:

```
read_mapping keys = ['REF_AREA', 'components', 'dsd_id', 'schema_version']
map_to_sdmx(df, read_mapping(path))  -> 'France'  (unmapped, no warning)
map_to_sdmx(df, raw_json_dict)       -> 'FRA'
```

Every consumer that composes the two — `standardize_sdmx`, and the Kedro path
`kd_read_mappings` → `kd_standardize_sdmx` — silently skips representation mapping and
uploads unmapped values. **Fix:** keep the `representation` key nested in `read_mapping`'s
result (breaking for anyone relying on the flat shape, so do it deliberately), and add a
JSON → `standardize_sdmx` round-trip test. Longer term this whole path is DUP-01.

### BUG-02 (P0) — blanket `.replace("NA", pd.NA)` corrupts real codes
`tidysdmx.py:752,760`. `"NA"` is a legitimate SDMX code (ISO 3166 Namibia, among others); a
mapping that targets it comes back as `<NA>` (verified). The docstring even advertises the
behaviour. **Fix:** don't treat `"NA"` as missing — JSON already has `null` for that. If
legacy files rely on it, gate the replacement behind an explicit `na_token=` parameter.

### BUG-03 (P1) — `kd_standardize_sdmx`: dead key check, silent partition loss, wrong signature
`kedro.py:79` calls `check_dict_keys(data, mappings)` as a bare statement — the function
*returns* the error message, so the guard does nothing. Verified consequences: with partially
overlapping keys, non-matching partitions are **silently dropped** (2 partitions in → 1 row
out, no error); with fully mismatched keys the user gets pandas'
`ValueError: No objects to concatenate` instead of the intended key diff. Additionally
`data: dict` is wrong for CASE 1 (`standardize_sdmx` is `@typechecked` to require a
DataFrame, so `data` must be one there), and the `boolean: bool = True` parameter is never
used (its sibling `kd_validate_datasets_local` gates on it and returns `({}, {})` — a silent
no-op — when False). The docstring describes calling `transform_source_to_target`; the body
calls `standardize_sdmx`. `kd_validate_dataset_local` logs "JSON report will be exported to
working directory" (`kedro.py:121-124`) but never writes one. **Fix:** raise on key mismatch,
annotate honestly (`pd.DataFrame | dict`), drop or use `boolean`, fix the log/docstrings —
and add tests (TST-01), which would have caught all of this.

### BUG-04 (P1) — `standardize_indicator_id` double-prefixes and is not idempotent
`tidysdmx.py:512-516`: the prefix is applied to **all** rows unless **all** rows already have
it, and the check runs against the dotted `dataset_id` before dots are replaced. Verified:

```
['WB.DATA360_GDP', 'POP']  ->  ['WB_DATA360_WB_DATA360_GDP', 'WB_DATA360_POP']
second pass                ->  ['WB_DATA360_WB_DATA360_WB_DATA360_GDP', ...]
```

**Fix:** normalise `dataset_id` (dots→underscores, uppercase) first, then prefix only the
rows that lack the normalised prefix (per-row mask, not `.all()`).

### BUG-05 (P1) — `vectorized_lookup_ordered_v1/v2` violate their documented contract
`tidysdmx.py:279-282, 328-332`: both docstrings promise "ordered by priority… later rules are
skipped", but the code re-sorts rules by `SOURCE` string length descending, discarding caller
order (verified: with rules `["^A.*", "^AB.*"]`, `"ABC"` maps to the *second* rule). Three
more verified traps: matching is `str.contains` — substring, so SOURCE `"A"` matches `"CAT"`
(the `mapping.py` engine uses `fullmatch`); NaN becomes the literal string `"nan"`
(the skipped tests at `test_tidysdmx.py:227,353` are this bug — June TEST-08); and the return
dtype flips between original (empty mapping) and object-of-str (non-empty). **Fix:** honour
caller order, anchor or full-match patterns, mask missing values, return a consistent dtype —
or better, retire this engine entirely (DUP-01).

### BUG-06 (P1) — Excel template round-trip still broken (June ARCH-01)
Verified: `write_excel_mapping_template(...)` produces sheets `['comp_mapping', 'REF_AREA']`
(lowercase `source/target/mapping_rules` headers, `utils.py:231-247`);
`build_structure_map_from_template_wb(parse_mapping_template_wb(path))` rejects it with
"Missing required sheet: 'INFO' / 'COMP_MAPPING' / 'REP_MAPPING'". The public writer produces
a format only the *dead* documentation (`docs/architecture.md`) describes. **Fix:** rewrite
the writer to emit INFO/COMP_MAPPING/REP_MAPPING (with `S:`/`T:` prefixes) and add a
write→parse→build round-trip test.

### BUG-07 (P1) — `_match_column_name` fuzzy containment binds wrong columns
`structures.py:1550-1560` accepts containment in *either* direction and returns the first
hit. Verified: `_match_column_name("AGE", ["PERCENTAGE", "AGE_GROUP"])` → `"PERCENTAGE"`
(because `"age" in "percentage"`), even though `AGE_GROUP` is available. A template whose
REP_MAPPING headers don't exactly match component IDs can silently wire a component to an
unrelated column. **Fix:** keep exact + normalised-equality matching; for substring matches
require a *unique* candidate and raise listing candidates otherwise.

### BUG-08 (P2) — `contextlib.suppress(ValueError)` hides the real REP_MAPPING error
`structures.py:1836`. If REP_MAPPING exists but its `S:`/`T:` prefixes are wrong,
`_parse_rep_mapping_sheet`'s precise error ("No source columns (prefixed with 'S:') found")
is suppressed; the user later gets "Mapping rule requires 'REP_MAPPING' sheet with data, but
it was invalid or empty" (verified) — with no hint their prefixes are the problem. **Fix:**
catch the ValueError, keep `rep_data = {}`, but stash the message and include it (as
`from`-cause or appended text) in the eventual error raised at `structures.py:2256`.

### BUG-09 (P2) — `parse_artefact_id` accepts malformed identifiers
`tidysdmx.py:164-172`. Verified: `"WB:WDI(1.0"` (no closing paren) → version `"1.0"`;
`"WB:WDI(1.0)x"` → version `"1.0)x"`; `"WB:WDI(1.0)))"` → `"1.0"`. **Fix:** parse with
`^(?P<agency>[^:]+):(?P<id>[^(]+)\((?P<version>[^)]+)\)$` — or lean on pysdmx (PX-02).

### BUG-10 (P2) — `map_to_sdmx` fixed mapping silently uses the first TARGET row
`tidysdmx.py:407-408`: a "fixed" mapping with two conflicting TARGET rows applies row 0 with
no warning (verified). **Fix:** raise (or at least warn) when a fixed mapping has more than
one distinct target.

### BUG-11 (P2) — `sdmx_reference_cols_for` deviates from SDMX-CSV for non-DSD contexts
`utils.py:29-33` / `standardize_output`. The SDMX-CSV field guide defines the reference
columns as **STRUCTURE / STRUCTURE_ID / ACTION** for *every* context, with the artefact type
as the STRUCTURE *value*; there is no `DATAFLOW_ID` or `PROVISION_AGREEMENT_ID` column in the
standard. tidysdmx emits `DATAFLOW="dataflow"`, `DATAFLOW_ID="WB:X(1.0)"` for dataflow
schemas — the datastructure case is coincidentally spec-shaped, which is presumably why this
hasn't bitten. `kd_validate_datasets_local`'s own docstring cites the field guide as the
authority. **Fix:** confirm what the WB FMR ingestion actually accepts; unless it demands the
current names, emit STRUCTURE/STRUCTURE_ID for all contexts (one function to change, plus
`validation.py` defaults).

### BUG-12 (P2) — `_infer_sdmx_type` misclassifies nullable/category dtypes
`structures.py:933-944` uses case-sensitive substring checks on `str(dtype)`. Verified:
pandas `Int64` → STRING, `Float64` → STRING, `category` → STRING. A
`create_schema_from_table` over a nullable-int column produces a STRING measure and a
codelist over every distinct number. **Fix:** use `pd.api.types.is_integer_dtype` /
`is_float_dtype` / `is_bool_dtype` / `is_datetime64_any_dtype` and unwrap
`CategoricalDtype.categories.dtype`.

### BUG-13 (P2) — deprecated `add_sdmx_reference_cols` mutates its input
`tidysdmx.py:456-458` writes three columns into the caller's DataFrame (verified) — the only
input-mutating function in the codebase, and it sits on the supported path via
`standardize_data_for_upload` ← `standardize_sdmx`. **Fix:** one `df = df.copy()` line;
deprecation is not a reason to keep a side effect.

### BUG-14 (P3) — `artefact_validation` dispatch skips subclasses
`artefact_validation.py:261` uses `_SPECIFIC.get(type(artefact))` — exact type only.
Verified: a `Codelist` subclass with zero items yields no C001 issue (only common M-rules
run). **Fix:** walk the MRO (`next(fn for t in type(a).__mro__ if (fn := _SPECIFIC.get(t)))`)
or match with `isinstance`.

### BUG-15 (P3) — misleading error messages
- `transform_source_to_target` (`tidysdmx.py:243-248`) catches *any* KeyError from its body —
  a components table merely missing the TARGET column produces "The mapping file should
  contain 'components' key…" (verified). Narrow the `try` to the `mapping["components"]`
  access.
- `read_mapping` (`tidysdmx.py:739-768`) uses truthiness, so a present-but-empty
  `"components": []` raises "Missing 'components' key" (verified). Distinguish missing from
  empty.

### BUG-16 (P3) — `qa_coerce_numeric` silently drops pre-existing-NaN rows
`qa_utils.py:31-41`: rows whose value was *already* missing are dropped alongside coercion
failures (verified: 1 valid + 1 NaN + 1 `"abc"` → 1 row), and the log line — INFO level —
says they "cannot be coerced to numeric". On the supported path this runs inside
`standardize_sdmx` with no opt-out. **Fix:** log at WARNING with accurate wording; consider
reporting pre-existing-missing separately from coercion failures.

## 5. Parallel subsystems & duplication

### DUP-01 (P1) — two mapping engines, two behaviours
Engine A (`tidysdmx.py`): `read_mapping` + `transform_source_to_target` + `map_to_sdmx` +
`vectorized_lookup_ordered_v1/v2`, driven by a bespoke JSON format. Engine B (`mapping.py`):
`map_structures` + `apply_*`, driven by pysdmx `StructureMap` objects. They implement the
same feature with **conflicting semantics** — priority (length-sort vs literal→regex→catch-all
via `_value_map_rank`), matching (substring vs fullmatch), NaN (stringified `"nan"` vs
kept-missing). `docs/pysdmx-overview.md` §11 explicitly instructs: "StructureMap application →
use `map_structures()`". Engine A is also where BUG-01/02/05/10 live, and it is untested.
**Recommendation:** decide Engine B is the one engine. Either (a) deprecate Engine A wholesale,
or (b) keep `read_mapping`'s JSON as an input format and convert it into pysdmx maps (the
builders in `structures.py` already exist), so `standardize_sdmx` becomes: read JSON → build
`StructureMap` → `map_structures` → `standardize_output`. One semantics, one test surface.

### DUP-02 (P1) — same-name builders in two modules (June CONS-20)
`build_representation_map` and `build_multi_representation_map` exist in **both**
`structures.py:503,593` (DataFrame-driven, `(df, agency, id, name, source_cl, target_cl…)`,
no publish validation) and `artefact_builder.py:329,378` (value-driven,
`(id, agency, name, source, target, maps…)`, validated via `raise_if_invalid`). Same names,
swapped `id`/`agency` order, different vocabulary, opposite validation behaviour — and
`__init__.py` exports only the structures variants, shadowing the validated ones.
**Fix:** make the DataFrame variants thin adapters that build the `maps` list and delegate to
the validated value builders (aligning names/order), or rename the DataFrame variants
(`*_from_df`) and export both. Either way, one construction path should end in validation.

### DUP-03 (P2) — three validation dialects, and the StructureMap gap between them
1. `artefact_validation.py` — rule IDs, `ValidationIssue`/`ValidationError` (the keeper);
2. `structure_map_writer._validate_rep_map_fields` (`:61-75`) — re-codes exactly R001/R002/R003
   as strings + plain ValueError;
3. ad-hoc `ValueError`s throughout `structures.py`.
There is no `StructureMap` entry in `_SPECIFIC`, and `validate_structure_map_references`
never checks `source`/`target` — verified: a StructureMap with `source=""`/`target=""`
(what `build_structure_map_from_template_wb` produces when no structure ids are passed)
sails through `prepare_structure_map_for_upload(validate=True)` and would be rejected by FMR.
**Fix:** add `_check_structure_map` (non-empty source/target, resolved rep-map references,
rep-map field rules) to `artefact_validation`, delete `_validate_rep_map_fields`, and have
`structure_map_writer` call `raise_if_invalid`. Also collapse `_check_representation_map` /
`_check_multi_representation_map` (`:152-187`) — they are byte-identical.

> **Update (2026-07, partially upstream in 1.17.0).** pysdmx 1.17's
> `RepresentationMap`/`MultiRepresentationMap.__post_init__` now raises `Invalid` at
> **construction** when `source`/`target` is `None` while `maps` are set (and enforces
> multi-map entry-count consistency) — closing part of this gap upstream. But it still
> **accepts `source=""`/`target=""`** (verified), which is exactly what
> `build_structure_map_from_template_wb` emits with no structure ids. So the `_check_structure_map`
> non-empty check is still needed; only the "required/None" half is now redundant with upstream.

### DUP-04 (P2) — `create_schema_from_table` bypasses the validated builders and `gen_urn`
`structures.py:1029-1036, 1261-1277` construct `Codelist`/`ConceptScheme`/`DSD` directly and
hand-assemble URN strings inline at four sites (`:1025, :1135, :1266, :1275`) despite
`gen_urn` existing in the same module. The DataFrame path therefore never sees C001/CS001/
D001/D002. **Fix:** route through `build_codelist`/`build_concept_scheme`/
`build_data_structure_definition` and `gen_urn`; also reuse `_to_identifier(schema_id)`
instead of computing it three times (`:1189, :1271, :1275`).

### DUP-05 (P2) — `vectorized_lookup_ordered_v1` ⊂ `v2`
`tidysdmx.py:252-294` vs `297-351`: v1 is v2 with `IS_REGEX=True` everywhere. If the engine
survives DUP-01, implement v1 as a one-line call into v2; the version-suffixed public names
also leak the JSON schema version into the API (June ARCH-04).

### DUP-06 (P3) — deprecated `fetch_dsd_schema` re-implements `fetch_schema`
`tidysdmx.py:89-96` duplicates the client construction in `:116-121` and, by calling
deprecated `parse_dsd_id`, emits **two** FutureWarnings per call. Delegate to
`fetch_schema(fmr_params[env]["url"], dsd_id, "datastructure")`.

### DUP-07 (P3) — verbatim blocks worth extracting
Rep-map ID counter duplicated inside `build_structure_map_from_template_wb`
(`structures.py:1872-1878` ≡ `:1906-1912`); REP_MAPPING presence guard
(`:2247-2259` ≡ `:2318-2330`); mandatory-columns logic re-inlined in
`validate_dataset_local` (`validation.py:152-161`) next to `validate_mandatory_columns`
(`:237-241`); codelist-scan loop in `validation.py:66-76` vs `tidy_raw.py:32-38` (with
*different* NaN policy — see API-06); the `cols_to_move + [c for c in ...]` reorder idiom
(`tidysdmx.py:565-567` ≡ `:621-623`).

## 6. pysdmx complementarity

### PX-01 (P1) — pysdmx pinned three releases behind; workaround kept alive
Lock: 1.13.0. Upstream: **1.17.0** (2026-06-23). The June review already verified 1.16 as
zero-risk against the full suite, and upstream 1.14.0 fixed "RepresentationMap XML writer
using wrong element for data types" (PR #556) — the exact bug `fix_sdmx_xml_datatype_tags`
(`utils.py:296-334`) string-patches (verified still broken in installed 1.13:
`io/xml/__structure_aux_writer.py:992`). **Fix:** bump the lock to ≥1.16, re-run the suite,
deprecate `fix_sdmx_xml_datatype_tags` (and the related caveat in
`_resolve_representation_ref`'s docstring, `structures.py:59-65`), delete next minor.

> **Update (2026-07, actioned).** Bumped to **pysdmx 1.17.0** (`pyproject.toml` `^1.17.0`,
> lock regenerated). Verified the upstream fix: the writer now selects the element
> conditionally (`io/xml/__structure_aux_writer.py:1006-1010` — `Codelist` only when the value
> references one, else `DataType`), so `fix_sdmx_xml_datatype_tags` is obsolete and is now
> `@deprecated` (FutureWarning). **Correction to the "1.16 zero-risk" claim:** 1.17 is *not*
> drop-in — it added stricter `RepresentationMap`/`MultiRepresentationMap` construction
> validation that broke 8 `test_structure_map_writer.py` tests (invalid fixture data + tests
> that built `source=None` maps upstream now rejects). All 8 were fixed; suite is green
> (550 passed). The `_resolve_representation_ref` docstring caveat remains to be cleaned up.

### PX-02 (P2) — reference parsing: reuse pysdmx's
pysdmx ≥1.13 publicly ships `pysdmx.util.parse_urn` / `parse_short_urn` /
`parse_item_urn` (verified against installed source). tidysdmx's bare
`"agency:id(version)"` format isn't quite a short URN (`Type=agency:id(version)`), so
`parse_artefact_id` is not a pure duplicate — but it should be hardened (BUG-09) and, where
inputs may be URNs, accept them via pysdmx's parsers. Conversely `gen_urn` is a **legitimate
gap-filler**: pysdmx's `Reference` renders only short URNs and has no full-URN builder
(June's upstream feature request remains the right move).

### PX-03 (P2) — private `pysdmx.model.__base` imports, one now avoidable
`structures.py:22` imports `ItemReference` from `pysdmx.model.__base` — it is publicly
exported from `pysdmx.model` (verified). `MaintainableArtefact` (`structure_map_writer.py:3`)
and `ItemScheme` (`artefact_validation.py:24`) are *not* yet public. **Fix:** switch
`ItemReference` to the public path now; funnel the remaining private imports through one
internal shim module and raise the exposure need upstream.

> **Update (2026-07, actioned).** Under 1.17.0, `Agency` (`artefact_builder.py:20`) is **also**
> public now — the original finding missed it. Both `ItemReference` (`structures.py`) and
> `Agency` were switched to `from pysdmx.model import …`. `ItemScheme` and `MaintainableArtefact`
> remain private (still imported from `pysdmx.model.__base` in `artefact_validation.py` /
> `structure_map_writer.py`) — the shim-module + upstream-exposure recommendation stands for
> those two.

### PX-04 (P2) — keep the `artefact_validation` migration plan honest
The module docstring commits to deleting itself once `pysdmx.model.validate` ships; as of
1.17.0 it has not (release notes checked). Fine — but consolidating the other two dialects
onto it now (DUP-03) is what makes the eventual swap a one-line re-export, as the docstring
promises. Separately, `structure_map_writer`'s URN-flattening
(`_replace_values_with_urn` reconstructs `type(map_rule)(source=…, target=…, values=urn)`)
is registry-upload plumbing that unpacks pysdmx objects field-by-field — against the
architecture doc's "opaque handles" principle — and is a good candidate to propose upstream
as a `write_sdmx(..., externalize_references=True)` option.

## 7. API & consistency

### API-01 (P1) — the deprecation story stalled at its own milestone
`standardize_sdmx` (supported) → `standardize_data_for_upload` (deprecated) at
`tidysdmx.py:196`: every supported-path and Kedro call warns. All four deprecated functions
(`fetch_dsd_schema`, `parse_dsd_id`, `add_sdmx_reference_cols`,
`standardize_data_for_upload`) remain in `__all__` (`__init__.py:96,124,138,146`) at 0.9.0 —
the version June scheduled for their removal — and remain documented as current in
`great-docs.yml:124,126,159,162`. **Fix:** extract the shared internals (qa steps + indicator
fix + reference cols) into private helpers used by both old and new paths; drop the four from
`__all__` and the docs nav in 0.10; delete in 0.11.

### API-02 (P2) — `apply_component_map` is the only unexported sibling
`mapping.py:181` is public-named, has a docstring, and is imported directly by tests
(`test_mapping.py`), while `apply_fixed_value_maps`/`apply_implicit_component_maps`/
`apply_multi_component_map` are all in `__all__`. Export it (June ARCH-14).

### API-03 (P2) — `kedro.py` is exempt from every project convention
No `@typechecked` on any of the four public functions; `schema=None`, `valid=None`,
`boolean` untyped or unused; bare `dict` annotations; docstring drift (see BUG-03). The
project's own rules ("use `@typechecked` on all public functions", "every new public function
must have a test") carve out no exception for this module.

### API-04 (P2) — `allow_na=True` contradicted downstream
`build_single_component_map` validates with `allow_na=True` (`structures.py:794-799`), then
delegates to `build_value_map_list`, which re-validates the same columns with the default
`allow_na=False` (`:347-351`) — verified: a NaN-bearing frame passes the first check and
raises TypeError in the delegate. Same pattern in `build_multi_representation_map`
(`:673-679`) → `build_multi_value_map_list` (`:446-457`). Pick one policy and thread the flag
through (the double validation itself is redundant — validate once, in the innermost
builder).

### API-05 (P2) — error-type conventions drift
`KeyError` for missing columns in `mapping.py:208,299` and (re-raised, misleadingly) in
`transform_source_to_target`; `TypeError`+`ValueError` mixed in `standardize_output`;
structured `ValidationError` only on the artefact path. Also `validate` is a very generic
top-level export name (`from tidysdmx import validate`). Convention suggestion: `ValueError`
for bad data/arguments, `TypeError` only for wrong types, `ValidationError` for artefact
publish-readiness, and messages that distinguish *missing* from *empty*.

### API-06 (P3) — NaN policy differs between the two codelist checks
Verified: `filter_rows` keeps NaN rows (`tidy_raw.py:37` masks with `.notna()`), while
`validate_dataset_local`/`_get_codelist_violations` reports the same cell as a violation
literally named `'nan'` (`validation.py:69-71`). Decide once: missing values are the
missing-values check's job; exclude them from codelist violations (and stop stringifying
them into `'nan'`/`'None'` report entries).

## 8. Simplicity & maintainability

### SMP-01 (P2) — god-functions to decompose
- `build_structure_map_from_template_wb` (`structures.py:1755-1967`, ~212 lines): workbook
  validation + INFO parsing + 5-way rule dispatch + ID dedup + URN generation + assembly.
  Extract a per-rule builder and an `_unique_map_id(counter, base)` helper (kills DUP-07).
- `_extract_mapping_rule` (`:2100-2219`): six branches returning near-identical dicts with
  *inconsistent keys* (`default_value` present on only two branches; docstring omits
  `multi_representation`). Return a small frozen dataclass with all fields.
- `create_schema_from_table` (`:1149-1279`) — shrinks naturally under DUP-04.
- `validate_dataset_local` (`validation.py:80-184`): mixes append-records with
  raise-then-catch over its own sibling validators. Have the `_get_*` helpers return
  records for all five checks; let the `validate_*` wrappers do the raising.
- `apply_multi_component_map` (`mapping.py:261-372`) is long but earns it (vectorisation +
  excellent comments) — leave it.

### SMP-02 (P2) — `structures.py` remains a 2,353-line god-module (June ARCH-02/B1)
Four domains under one roof: map builders (127–919), schema-from-table (922–1279), Excel
template parsing (1282–2353), URN utilities (1708–1752). The June mechanical split behind
unchanged `__init__` re-exports is still the right move and is prerequisite to keeping
review/merge conflicts manageable — the file *grew* 39 lines since June.

### SMP-03 (P3) — zombie & orphaned code
`_extract_artefact_id` (`structures.py:1463-1525`): zero src callers (verified), kept alive
by 8 direct test references — delete both (June ARCH-09). `scripts/debug-config-checks.py`:
self-described "remove once debugging is stable", referenced nowhere. Back-compat aliases
`check_dict_keys` etc. (`tidysdmx.py:58-61`) exist for kedro + tests only — fold into the
private names. `tests/fixtures/cassettes/dsd_schema.pkl`: only reference is a commented-out
line in `conftest.py`.

### SMP-04 (P3) — micro-inefficiencies (fix opportunistically)
`apply_component_map` runs `removeprefix` + `re.fullmatch` per cell per regex rule
(`mapping.py:227-232`) — hoist `re.compile` per rule, as the multi-map variant's
vectorisation comment (`:323-328`) already implies. `_get_codelist_violations` uses
`continue` instead of `break` once the cap is hit (`validation.py:67`).
`validate_duplicates` computes the deduplicated key frame twice (`validation.py:296-297`).

## 9. Testing

### TST-01 (P1) — `kedro.py`: zero tests, zero type checking, excluded from coverage
No test file, zero `kd_*` references anywhere in `tests/` (verified), and
`pyproject.toml:130` omits it from coverage. The stated rationale ("integration-only") is
unfounded: the module imports nothing from Kedro — partitioned datasets are plain
dicts-of-callables, trivially faked. BUG-01's composition break and all of BUG-03 live here
undetected. **Fix:** add `tests/test_kedro.py` (fakes: `{"a.csv": lambda: df}`), remove the
coverage omit.

### TST-02 (P1) — six exported functions with no tests
`standardize_sdmx`, `map_to_sdmx`, `read_mapping`, `fetch_schema`, `gen_urn`,
`fix_sdmx_xml_datatype_tags` (plus the four `kd_*`). This is exactly the surface where the
P0s were found; "every new public function must have at least one test"
(testing-conventions) is the project's own rule.

### TST-03 (P1) — missing cassettes still fall back to live FMR (June TEST-01/A2)
`ifpri_asti_schema.pkl` (`fxtr_schemas.py:46`), `ifpri_asti_sm.pkl` (`fxtr_mapping.py:34`),
`multi_value_map_df.pkl` (`fxtr_structures.py:51`) are referenced but not committed; the
fixtures silently fetch from `fmrqa.worldbank.org`. Integration markers now keep CI clean,
but any full local run depends on a QA server and network. Also note the pickle trade-offs:
unpickling executes arbitrary code, and pysdmx-object pickles break silently across
pysdmx/Python upgrades — worth moving to serialized SDMX (Fusion-JSON via pysdmx I/O)
instead of pickles when regenerating.

### TST-04 (P2) — skipped tests that crash, and the same broken code in a docstring
`test_utils.py:246-306`: four tests skipped "temporarily" call pysdmx constructors wrongly
(`Component(id="FREQ")`, `Schema(id_=...)`) and would crash if unskipped; the identical
broken snippet is published in the `extract_component_ids` docstring
(`utils.py:107-114`). The correct construction exists 40 lines away in `fxtr_schemas.py`.
Note doctests never run (no `--doctest-modules`), which is how this and the garbled
`build_representation_map` example (`structures.py:557-562` — positional args land on the
wrong parameters) survive. Fix the four tests, fix both docstrings, and consider running
doctests in CI.

### TST-05 (P2) — order-dependent pipeline test, unmarked as integration
`test_pipeline_integration.py` threads state through class attributes
(`TestPipelineWorkflow.<attr>` set in one test, read in later ones, with `hasattr` guards) —
breaks under randomisation/xdist and cascades on first failure. Despite the name and an FMR
URL it carries no `integration` marker; it stays green in CI only because its cassette *is*
committed plus a runtime skip. Convert the chain into fixtures; mark appropriately.

### TST-06 (P2) — `pytest.raises` without `match=` is now the norm
114 of 168 (68%) — up from 54 flagged in June; the convention
(".claude/rules/testing-conventions.md": *use `match=`*) is losing. A bare
`pytest.raises(ValueError)` passes on the *wrong* ValueError — e.g. it would not have
distinguished BUG-03's "No objects to concatenate" from the intended key-mismatch message.

### TST-07 (P3) — fixture hygiene
Duplicate `multi_value_map_df`: the session-scoped, pkl-writing fixture
(`fxtr_structures.py:37`) is shadowed by an inline function-scoped one
(`test_structures.py:428`) with different data — the global one is dead weight that writes an
untracked file. Session-scoped fixtures return mutable DataFrames (against the conventions
file). `@pytest.mark.unit` is applied in exactly one file — either enforce it or drop it from
the rules. The NaN-comparison skips (`test_tidysdmx.py:227,353`) should become
`xfail(strict=True)` pinned to BUG-05.

## 10. Documentation

### DOC-01 (P1) — one live doc toolchain, one dead one that still claims RTD
Live: Quarto/great-docs (`great-docs.yml` + `index.qmd` + `user_guide/`, deployed by
`docs.yml`). Dead: the Sphinx tree — `.readthedocs.yml` still builds `docs/conf.py`, whose
toctree references `example.ipynb` which does not exist (verified), so the RTD build is
broken. Retire the Sphinx tree, `docs/Makefile`, and `.readthedocs.yml` (or repoint RTD at
the Quarto output). (June ARCH-12/B8.)

### DOC-02 (P1) — stale duplicate reference docs actively mislead
`docs/architecture.md` documents the **dead** Excel format (`comp_mapping`, lowercase
headers) — a user following it hits BUG-06 from the other side; `docs/tidysdmx-architecture.md`
is the current one. `docs/overview.md` and `docs/pysdmx-overview.md` are identical except the
pysdmx version line — and `CLAUDE.md` directs contributors to the *staler* copy
(`pysdmx-overview.md`, "pysdmx >= 1.8.1"). Delete `docs/architecture.md` and one of the
overview twins; update `CLAUDE.md` pointers.

### DOC-03 (P2) — deprecated functions documented as current API
`great-docs.yml:124,126,159,162,187` lists the four deprecated functions and
`fix_sdmx_xml_datatype_tags` in the public reference with no deprecation note. Remove or
annotate.

### DOC-04 (P2) — README is still a stub
"Usage — TODO" + work-in-progress banner, while `index.qmd:16-47` already contains a polished
quick start. README is the PyPI/GitHub landing page; port the quick start. (June PROD-08.)

### DOC-05 (P2) — docs promise a `Schema` that the code doesn't return
`docs/tidysdmx-architecture.md` §6 and `docs/pysdmx-overview.md` describe
`create_schema_from_table` as returning "a pysdmx `Schema`… structurally identical to one
fetched from a registry"; it returns a `SchemaComponents(dsd, concept_scheme, codelists)`
namedtuple (`structures.py:1004-1006`), which the schema-consuming functions
(`validate_dataset_local`, `standardize_output`, `filter_tidy_raw`) do not accept. Fix the
docs — or better, add the missing `to_schema()` bridge, which would make the "generate a
schema from data, then validate the data against it" story real. Also: `pysdmx-overview.md`
§9 imports a non-existent `build_structure_map`; the `build_structure_map_from_template_wb`
doctest shows a REP_MAPPING without the required `S:`/`T:` prefixes (it only "works" because
the example rule is `fixed:`).

## 11. CI & packaging

- **CI-01 (P2)** `ci.yml:18-19` installs `uv` and never uses it (June PROD-02); the lint job
  runs `poetry install --only dev`, pulling the Jupyter/Sphinx stack to execute ruff — use
  `pipx run ruff` (or a dedicated `lint` dependency group) and cache poetry.
- **CI-02 (P2)** Coverage gate 70 vs 86.7% actual (and 88.9% in June — the gate is not
  catching the slow drift). Raise `fail_under` to 85.
- **CI-03 (P2)** Matrix is 3.11/3.12; `requires-python = ">=3.11.9"` is open-ended and June
  verified all deps support 3.13. Add 3.13; extend the trove classifiers (currently 3.11
  only) or rely fully on Poetry's auto-enrichment.
- **CI-04 (P2)** Runtime `idna 3.11` CVE (June PROD-05, fix 3.15) still locked; there is no
  dependency-audit step. Add `pip-audit` (fail on runtime-dependency CVEs only) or enable
  Dependabot/Renovate.
  > **Update (2026-07, actioned).** Added an explicit `idna = ">=3.15"` security floor to
  > `pyproject.toml` and re-locked → **idna 3.18** (CVE closed). The durable process fix
  > (pip-audit step / Dependabot) is still outstanding — the floor guards this one CVE, not the
  > next.
- **CI-05 (P3)** `pr-review.yml:34` pins the long-superseded `claude-sonnet-4-20250514`.
  The pre-push pytest hook requires `pre-commit install --hook-type pre-push`, which
  CONTRIBUTING doesn't mention.

## 12. What's working well

- **Runtime quality bar:** clean ruff (D/E/F/I/UP/B/SIM/RUF, Google docstrings), 550 fast
  unit tests in ~4s, `@typechecked` on essentially every public function outside kedro.py,
  `py.typed` shipped.
- **`mapping.py` engine:** the np.select vectorisation of `apply_multi_component_map` with
  its rank-based, storage-order-independent rule priority and deliberate NaN semantics is the
  best-engineered code in the repo — comments explain *why*, not *what*.
- **`artefact_builder`/`artefact_validation`:** clear separation of construction and
  publish-readiness, stable rule IDs, `ValidationError` subclassing `ValueError`, and an
  explicit self-destruct plan for when pysdmx ships validation.
- **Release engineering:** PSR v10 config (zero-version handling, changelog insertion-flag
  mode, exclude patterns), Trusted Publishing, master-only dispatch guard, dry-run gating,
  and an accurate RELEASING.md.
- **Process:** conventional commits throughout; the June review's quick wins all landed with
  1:1 commits; `__init__.py` is internally consistent (69 imports == 69 `__all__` entries,
  sorted).

## 13. Prioritized action plan

> **Retirement decision (supersedes parts of §4–§9).** This plan is revised against a roadmap
> the original findings pre-dated: **`kedro.py` is being retired**, and the **bespoke
> JSON-mapping pipeline ("Engine A") plus the old JSON/Excel *writer* mapping template are
> being superseded by pysdmx `StructureMap`s ("Engine B", `mapping.py`)**. A pivotal fact this
> reframes: `standardize_sdmx` (`tidysdmx.py:176`) is not the supported path — it is Engine A's
> orchestrator (`transform_source_to_target` → `map_to_sdmx` → deprecated
> `standardize_data_for_upload`), so it retires with the engine. Consequently, **bugs inside
> retiring code are resolved by deprecation, not repair.** Dispositions:
>
> - **Retire (soft-deprecate, do not fix):** BUG-01, BUG-02, BUG-05, BUG-10, BUG-15 (Engine A);
>   BUG-04 `standardize_indicator_id`, BUG-16 `qa_coerce_numeric` (indicator/QA utils reachable
>   only via the deprecated path — carried out with Engine A); BUG-06 (old Excel *writer* trio);
>   BUG-13 (already deprecated).
> - **Drop entirely (kedro retiring):** BUG-03, TST-01, API-03, and the `kd_*` slice of TST-02.
> - **Dissolved by the above:** DUP-01, DUP-05, DUP-06 (Engine A refactors — no longer worth
>   doing); DUP-07's Engine-A-only idioms.
> - **Still fix (staying, engine-agnostic):** BUG-07, BUG-08 (the Excel→`StructureMap`
>   *builder* stays — distinct from the retired writer), BUG-09, BUG-11, BUG-12, BUG-14, and all
>   of DUP-02/03/04, PX-01/02/03/04, SMP-01/02/03/04, DOC-01/02/04/05, CI-01..05,
>   TST-03/04/05/06/07, API-01/02/04/05/06. TST-02 shrinks to two staying exports
>   (`fetch_schema`, `gen_urn`); DOC-03 expands to cover the newly-deprecated names.

**Workstream 0 — Retire the legacy pipeline (do first; it unblocks and dissolves the rest):**
Deprecate the whole legacy surface in one pass rather than fixing its bugs. Net effect:
dissolves BUG-01/02/04/05/06/10/13/15/16, DUP-01/05/06, TST-01, API-03, and part of
TST-02/DOC-03.
1. **Add a shared `@deprecated` decorator** — the codebase currently hand-writes an inline
   `warnings.warn(…, FutureWarning, stacklevel=2)` + a `.. deprecated::` docstring directive per
   function (`tidysdmx.py:82-87`); with ~14 functions to deprecate that boilerplate should be
   factored into `@deprecated(replacement="map_structures / standardize_output", removal="0.11")`
   (in `utils.py` or a new `_deprecation.py`), then retrofit the four existing deprecations onto
   it. Messages point to the Engine-B replacements (`map_structures`, `standardize_output`,
   `build_structure_map_from_template_wb`).
2. **Apply it to the retiring surface** (all currently in `__all__`): Engine A —
   `read_mapping`, `transform_source_to_target`, `map_to_sdmx`, `vectorized_lookup_ordered_v1`,
   `vectorized_lookup_ordered_v2`, `standardize_sdmx`; old Excel writer —
   `write_excel_mapping_template`, `build_excel_workbook`, `create_mapping_rules`;
   indicator/QA utils — `standardize_indicator_id`, `qa_coerce_numeric`; kedro —
   `kd_read_mappings`, `kd_standardize_sdmx`, `kd_validate_dataset_local`,
   `kd_validate_datasets_local` (plus the `check_dict_keys`/`remove_extension`/`modify_dict_keys`/
   `create_keys_dict` aliases that exist only for kedro+tests, SMP-03). Keep them exported but
   warning; annotate them deprecated in `great-docs.yml` (DOC-03); schedule removal for 0.11.

**Quick wins (staying, small PRs, hours):**
1. BUG-09 (regex `parse_artefact_id`), BUG-08 (surface the suppressed REP_MAPPING cause),
   BUG-14 (MRO/`isinstance` dispatch), API-02 (export `apply_component_map`).
2. CI-01 (delete `setup-uv`), CI-02 (gate → 85), CI-03 (3.13 + classifiers).
3. Delete: `scripts/debug-config-checks.py`, `docs/architecture.md` (documents the dead Excel
   format), one overview twin, `dsd_schema.pkl`, dead `multi_value_map_df` session fixture.
4. TST-04 (fix the four crashing skips + the two wrong docstring examples).

**0.10 milestone (staying P1s — the June carry-overs, now twice-flagged):**
1. BUG-07 (unique-candidate column match in the Excel→`StructureMap` builder), BUG-12
   (`pd.api.types` dtype checks in `create_schema_from_table`).
2. API-01 simplified: since the old path is being *deleted* (Workstream 0), no shared-internal
   extraction is needed — just drop the deprecated names from `__all__` and the docs nav in 0.10,
   delete in 0.11.
3. ~~PX-01: bump pysdmx lock ≥1.16, deprecate `fix_sdmx_xml_datatype_tags`; PX-03 public
   `ItemReference` import; CI-04 idna~~ — **done 2026-07** (pysdmx 1.17.0, +Agency public,
   `fix_sdmx_xml_datatype_tags` deprecated, idna floored to 3.18, 8 tests fixed). Still open:
   CI-04 pip-audit/Dependabot step; delete `fix_sdmx_xml_datatype_tags` next minor.
4. TST-03: commit cassettes (prefer serialized SDMX over pickle) and hard-fail fixture fallback
   when `CI` is set; TST-02 reduced to adding tests for `fetch_schema` and `gen_urn`.
5. DOC-01: retire the dead Sphinx/RTD toolchain (or repoint at Quarto).

**Strategic (staying design work — decisions, then mechanical):**
1. DUP-02/03/04: one validated construction path (DataFrame builders delegate to value builders;
   `_check_structure_map` moves into `artefact_validation`, deleting `_validate_rep_map_fields`;
   `create_schema_from_table` uses the validated builders + `gen_urn`) — positions the codebase
   for the planned pysdmx-validation swap (PX-04).
2. SMP-02: mechanical split of `structures.py` behind unchanged re-exports.
3. BUG-11: confirm FMR's accepted SDMX-CSV reference columns and align `sdmx_reference_cols_for`;
   DOC-05: decide whether `create_schema_from_table` should grow a `to_schema()` bridge.
