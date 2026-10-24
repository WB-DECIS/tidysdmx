# Performance Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure `map_structures` time and peak memory across four dataset shapes and four sizes, re-measuring the target against earlier releases on one machine. Publish the results with every release as a GitHub Release asset and a docs page, and post them on relevant pull requests as a sticky comment.

**Architecture:**
- A top-level `benchmarks/` package, which is not shipped.
  - `scenarios.py` builds deterministic inputs.
  - `case.py` measures one case inside one version's virtual environment.
  - `run.py` (stdlib only) installs each version from its git ref into its own venv and runs the grid interleaved.
  - `report.py` renders the results JSON as a docs page, a job summary or a PR comment.
- Three workflows:
  - `benchmarks.yml` measures and is read-only.
  - `benchmarks-pr.yml` posts the PR comment.
  - `release.yml` attaches the JSON to the Release.
- `docs.yml` fetches the newest JSON before `make docs`.

**Tech Stack:** Python 3.11, numpy, pandas (2 or 3), pysdmx map model, stdlib `subprocess` / `tarfile` / `tracemalloc`, matplotlib (charts), uv, GitHub Actions, great-docs + Quarto.

**Spec:** `docs/superpowers/specs/2026-10-23-performance-profile-design.md`

## Global Constraints

- Work on branch `ci/performance-profile` (created off `origin/dev`, no upstream). Never push to `dev` directly.
- Run every tool through `uv run` (`uv run python -m pytest ...`); `make` targets where they exist.
- Commit messages: Conventional Commits, ending with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Use `chore(bench)`, `docs(performance)`, `ci(bench)`, `docs`. **Never** `feat`, `fix` or `perf`: nothing here changes the package.
- `benchmarks/` is never imported by `src/tidysdmx/` and is not shipped in the wheel or sdist.
- `benchmarks/records.py`, `benchmarks/run.py` and `benchmarks/probe.py` import the standard library only (plus `benchmarks.records`). `run.py` runs under `uv run --no-project`.
- `benchmarks/report.py` imports matplotlib only inside the chart function.
- Scenario inputs use pysdmx constructors only, never tidysdmx builders: `build_multi_component_map` does not exist at v0.8.0.
- Target column names never equal source column names.
- A case's result travels through a file (`--result`), never stdout: tidysdmx 0.8.0 calls `print()`.
- Sizes: 1,000 / 10,000 / 100,000 / 1,000,000 rows. Scenarios: `typical`, `high_cardinality`, `regex_heavy`, `wide`.
- Constants:
  - `BUDGET_S = 60.0`: a larger size is skipped after a median above it.
  - `SLOW_CALL_S = 30.0`: timing stops after a call slower than this.
  - `MEMORY_SKIP_S = 60.0`: memory is not traced above this median.
  - `CASE_TIMEOUT_S = 900.0`.
  - Python `3.11` for every benchmark venv.
- The "same" band is ±10% (`SAME_BAND = 0.10`).
- Tests follow `.claude/rules/testing-conventions.md`:
  - `class Test<Function>`, `test_<function>_<scenario>` names, one logical assertion each.
  - `pytest.raises(..., match=...)` always.
  - Warnings are errors.
- Docstrings: Google style on every public function (ruff `D`). Annotations on everything (ruff `ANN`).
- Line length is 88, and ruff's `E501` applies everywhere, tests included. `ruff format` wraps code but not long strings, docstrings or comments, so wrap those by hand when `ruff check` flags them. The code in this plan is close to that limit, not guaranteed under it.
- If a step's code has a lint or type error the plan didn't foresee, fix the code. Don't add `# noqa` or `# type: ignore` unless a comment beside it explains a genuine exemption, as `CLAUDE.md` requires.
- Python source holds no literal non-ASCII characters. Use `\u` escapes (`"—"` em dash, `"≈"` ≈, `"×"` ×, `"Δ"` Δ, `"±"` ±), so ruff's `RUF001`–`RUF003` stay quiet.
- Actions are pinned exactly as the existing workflows pin them:
  - `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`
  - `astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d # v10.0.1`
  - `actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1`
  - `actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1`
- Reusable workflows are called as `uses: $/.github/workflows/<file>.yml`, the syntax `release.yml` already uses.

---

## File Structure

| Path | Status | Responsibility |
|---|---|---|
| `benchmarks/__init__.py` | create | Package docstring only |
| `benchmarks/records.py` | create | Results schema (TypedDicts), `SCENARIOS`, `WORKTREE`, `SCHEMA_VERSION`, `release_key()`, `empty_record()` |
| `benchmarks/scenarios.py` | create | `build(scenario, rows, seed)` → `(DataFrame, StructureMap)` |
| `benchmarks/case.py` | create | `repeats_for`, `run_case`, `main`: measure one case and write one record |
| `benchmarks/probe.py` | create | `installed()`: the Python and dependency versions in an env |
| `benchmarks/run.py` | create | Version selection, env creation, the interleaved grid, results JSON |
| `benchmarks/report.py` | create | Formatting, tables, charts, page/summary/PR-comment rendering, CLI |
| `benchmarks/page.qmd` | create | Page template with `<!-- RESULTS -->` |
| `tests/test_benchmarks.py` | create | All unit tests for `benchmarks/` |
| `tests/fixtures/fxtr_benchmarks.py` | create | `make_results()` and the `benchmark_results` fixture |
| `tests/conftest.py` | modify | Import `benchmark_results` |
| `pyproject.toml` | modify | mypy `files`; ruff per-file ignore for `run.py`; `matplotlib` in `dev` |
| `uv.lock` | modify | Via `uv add` |
| `.gitignore` | modify | `.bench/`, `benchmarks/results/`, `performance/` |
| `Makefile` | modify | `bench`, `performance-page`; `docs`/`docs-preview` depend on the latter |
| `great-docs.yml` | modify | `sections:` entry and navbar icon |
| `.github/workflows/benchmarks.yml` | create | Measure (workflow_call + workflow_dispatch) |
| `.github/workflows/benchmarks-pr.yml` | create | Decide → benchmark → comment |
| `.github/workflows/release.yml` | modify | `benchmark`, `attach-benchmark`; `deploy-docs` needs/if |
| `.github/workflows/docs.yml` | modify | Fetch the newest results before `make docs` |
| `CLAUDE.md`, `CONTRIBUTING.md`, `RELEASING.md` | modify | Document the commands, workflows and release behaviour |

---

### Task 1: Results schema and scenario inputs

**Files:**
- Create: `benchmarks/__init__.py`, `benchmarks/records.py`, `benchmarks/scenarios.py`
- Create: `tests/test_benchmarks.py`
- Modify: `pyproject.toml` (`[tool.mypy] files`)

**Interfaces:**
- Produces in `benchmarks.records`:
  - `SCHEMA_VERSION: int`, `SCENARIOS: tuple[str, ...]`, `WORKTREE: str`.
  - `Status`, `Role` (Literals).
  - `Record`, `VersionInfo`, `Machine`, `Results` (TypedDicts).
  - `release_key(tag: str) -> tuple[int, int, int] | None`.
  - `empty_record(scenario: str, rows: int, status: Status, reason: str) -> Record`.
- Produces in `benchmarks.scenarios`: `build(scenario: str, rows: int, seed: int = 0) -> tuple[pd.DataFrame, StructureMap]`, and `CATCH_ALL: str`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_benchmarks.py`:

```python
"""Tests for the benchmarks/ harness (not part of the tidysdmx package)."""

import pandas as pd
import pytest
from pysdmx.model.map import ComponentMap, FixedValueMap, MultiComponentMap

from benchmarks.records import SCENARIOS, empty_record, release_key
from benchmarks.scenarios import build
from tidysdmx import map_structures


def _sources_and_targets(structure_map):
    sources, targets = set(), set()
    for component in structure_map.maps:
        if isinstance(component, FixedValueMap):
            targets.add(component.target)
        elif isinstance(component, MultiComponentMap):
            sources.update(component.source)
            targets.update(component.target)
        else:
            sources.add(component.source)
            targets.add(component.target)
    return sources, targets


class TestReleaseKey:
    @pytest.mark.parametrize(
        ("tag", "expected"),
        [("v0.10.0", (0, 10, 0)), ("v1.2.3", (1, 2, 3))],
    )
    def test_release_key_parses_release_tags(self, tag, expected):
        assert release_key(tag) == expected

    @pytest.mark.parametrize("tag", ["v1.2.3-rc.1", "dev", "0.10.0", "v1.2"])
    def test_release_key_rejects_other_refs(self, tag):
        assert release_key(tag) is None


class TestEmptyRecord:
    def test_empty_record_carries_status_and_reason(self):
        record = empty_record("typical", 10, "skipped", "too slow")
        assert (record["status"], record["reason"]) == ("skipped", "too slow")

    def test_empty_record_has_no_measurements(self):
        record = empty_record("typical", 10, "error", "boom")
        assert record["median_s"] is None and record["samples_s"] == []


class TestBuild:
    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_build_returns_requested_rows(self, scenario):
        df, _ = build(scenario, 1_000)
        assert len(df) == 1_000

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_build_is_deterministic(self, scenario):
        first, _ = build(scenario, 500, seed=3)
        second, _ = build(scenario, 500, seed=3)
        pd.testing.assert_frame_equal(first, second)

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_build_targets_never_shadow_sources(self, scenario):
        _, structure_map = build(scenario, 100)
        sources, targets = _sources_and_targets(structure_map)
        assert sources.isdisjoint(targets)

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_build_sources_are_dataframe_columns(self, scenario):
        df, structure_map = build(scenario, 100)
        sources, _ = _sources_and_targets(structure_map)
        assert sources <= set(df.columns)

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_build_output_maps_with_installed_map_structures(self, scenario):
        df, structure_map = build(scenario, 1_000)
        _, targets = _sources_and_targets(structure_map)
        assert targets <= set(map_structures(df, structure_map).columns)

    def test_build_typical_uses_every_map_type(self):
        _, structure_map = build("typical", 100)
        kinds = sorted(type(component).__name__ for component in structure_map.maps)
        assert kinds == [
            *["ComponentMap"] * 3,
            *["FixedValueMap"] * 2,
            *["ImplicitComponentMap"] * 2,
            "MultiComponentMap",
        ]

    def test_build_high_cardinality_has_a_tenth_distinct_values(self):
        df, _ = build("high_cardinality", 10_000)
        # 10,000 draws from 1,000 codes leave almost none out.
        assert 900 < df["entity"].nunique() <= 1_000

    def test_build_regex_heavy_sends_half_the_values_past_the_literals(self):
        df, _ = build("regex_heavy", 10_000)
        literals = {f"K{i:06d}" for i in range(250)}
        assert 0.45 < (~df["code_a"].isin(literals)).mean() < 0.65

    def test_build_wide_maps_twenty_columns(self):
        _, structure_map = build("wide", 100)
        assert sum(isinstance(c, ComponentMap) for c in structure_map.maps) == 20

    def test_build_rejects_unknown_scenario(self):
        with pytest.raises(ValueError, match="Unknown scenario 'tall'"):
            build("tall", 10)

    def test_build_rejects_rows_below_one(self):
        with pytest.raises(ValueError, match="rows must be at least 1, got 0"):
            build("typical", 0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'benchmarks'`.

- [ ] **Step 3: Write `benchmarks/__init__.py`**

```python
"""Performance benchmarks for tidysdmx's ``map_structures``.

Not part of the installed package. ``run`` measures several tidysdmx versions
side by side, ``case`` measures one case inside one version's environment, and
``report`` renders the results. Design:
docs/superpowers/specs/2026-10-23-performance-profile-design.md.
"""
```

- [ ] **Step 4: Write `benchmarks/records.py`**

```python
"""The benchmark results schema, shared by every module in ``benchmarks``.

Standard library only: ``benchmarks.run`` imports it under
``uv run --no-project``, where neither pandas nor tidysdmx is installed.
"""

import re
from typing import Literal, NotRequired, TypedDict

SCHEMA_VERSION: int = 1
SCENARIOS: tuple[str, ...] = ("typical", "high_cardinality", "regex_heavy", "wide")
WORKTREE: str = "WORKTREE"

Status = Literal["ok", "skipped", "error"]
Role = Literal["target", "compare", "baseline"]

_RELEASE_TAG: re.Pattern[str] = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


class Record(TypedDict):
    """One case (version, scenario, size): its measurements, or why there are none."""

    scenario: str
    rows: int
    status: Status
    reason: str | None
    input_bytes: int | None
    samples_s: list[float]
    median_s: float | None
    min_s: float | None
    peak_alloc_bytes: int | None
    output_shape: list[int] | None
    version: NotRequired[str]


class VersionInfo(TypedDict):
    """A measured version: where it came from and what it ran with."""

    label: str
    ref: str
    commit: str
    role: Role
    aliases: list[str]
    deps: dict[str, str | None]


class Machine(TypedDict):
    """The machine a benchmark ran on."""

    platform: str
    cpu: str
    cpu_count: int | None
    memory_bytes: int | None
    runner_image: str | None


class Results(TypedDict):
    """A whole benchmark run, as written by ``benchmarks.run``."""

    schema_version: int
    generated_at: str
    machine: Machine
    sizes: list[int]
    scenarios: list[str]
    versions: list[VersionInfo]
    results: list[Record]


def release_key(tag: str) -> tuple[int, int, int] | None:
    """Return a release tag's version as a sortable tuple.

    Args:
        tag: A git tag such as ``"v0.10.0"``.

    Returns:
        ``(major, minor, patch)``, or ``None`` if ``tag`` is not a final
        ``vX.Y.Z`` release tag.
    """
    match = _RELEASE_TAG.match(tag)
    if match is None:
        return None
    return int(match[1]), int(match[2]), int(match[3])


def empty_record(scenario: str, rows: int, status: Status, reason: str) -> Record:
    """Return the record of a case that produced no measurement.

    Args:
        scenario: The case's scenario.
        rows: The case's row count.
        status: ``"skipped"`` or ``"error"``.
        reason: Why nothing was measured.

    Returns:
        A record whose measurements are all empty.
    """
    return {
        "scenario": scenario,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": None,
        "samples_s": [],
        "median_s": None,
        "min_s": None,
        "peak_alloc_bytes": None,
        "output_shape": None,
    }
```

- [ ] **Step 5: Write `benchmarks/scenarios.py`**

```python
"""Deterministic synthetic inputs for benchmarking ``map_structures``.

Every scenario returns a DataFrame and a StructureMap built from pysdmx
constructors only, so the same inputs run unchanged against every tidysdmx
release since 0.8.0, whose builders differ from today's. Source columns use raw
names and targets SDMX ids, so no target overwrites a source: releases up to
0.10.0 feed each map the previous map's output, later ones read every source
from the input, and distinct names make both produce the same columns.
"""

from collections.abc import Callable, Sequence

import numpy as np
import pandas as pd
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)

from benchmarks.records import SCENARIOS

CATCH_ALL: str = "regex:.*"
_AGENCY: str = "BENCH"
_HIGH_CARDINALITY_MAP_SIZE: int = 10_000
_WIDE_COLUMNS: int = 20
_WIDE_CODES: int = 50

Inputs = tuple[pd.DataFrame, StructureMap]
_Map = ComponentMap | FixedValueMap | ImplicitComponentMap | MultiComponentMap


def build(scenario: str, rows: int, seed: int = 0) -> Inputs:
    """Build the DataFrame and StructureMap for one benchmark case.

    Args:
        scenario: One of ``benchmarks.records.SCENARIOS``.
        rows: Number of rows in the DataFrame; at least 1.
        seed: Seed for the random draws, so a case is reproducible.

    Returns:
        The DataFrame to map and the StructureMap to map it with.

    Raises:
        ValueError: If ``scenario`` is unknown or ``rows`` is below 1.
    """
    builder = _BUILDERS.get(scenario)
    if builder is None:
        raise ValueError(
            f"Unknown scenario {scenario!r}; expected one of: {', '.join(SCENARIOS)}"
        )
    if rows < 1:
        raise ValueError(f"rows must be at least 1, got {rows}")
    return builder(rows, np.random.default_rng(seed))


def _codes(prefix: str, count: int) -> list[str]:
    return [f"{prefix}{i:06d}" for i in range(count)]


def _draw(rng: np.random.Generator, codes: Sequence[str], rows: int) -> np.ndarray:
    # An object array of Python str, as a CSV reader produces: pandas then
    # applies its default string dtype (object under pandas 2, str under 3).
    return rng.choice(np.asarray(codes, dtype=object), size=rows)


def _code_map(codes: Sequence[str], target_prefix: str) -> list[ValueMap]:
    return [
        ValueMap(source=code, target=f"{target_prefix}{i:06d}")
        for i, code in enumerate(codes)
    ]


def _component_map(
    source: str, target: str, value_maps: Sequence[ValueMap]
) -> ComponentMap:
    return ComponentMap(
        source=source,
        target=target,
        values=RepresentationMap(
            id=f"RM_{target}",
            agency=_AGENCY,
            source="String",
            target="String",
            maps=list(value_maps),
        ),
    )


def _structure_map(maps: Sequence[_Map]) -> StructureMap:
    return StructureMap(
        id="SM_BENCH", name="Benchmark", agency=_AGENCY, maps=list(maps)
    )


def _typical(rows: int, rng: np.random.Generator) -> Inputs:
    countries = _codes("C", 200)
    series = _codes("S", 300)
    sexes = ["F", "M", "_T"]
    ages = _codes("A", 10)
    units = _codes("U", 5)
    years = [str(year) for year in range(1995, 2025)]
    df = pd.DataFrame(
        {
            "country": _draw(rng, countries, rows),
            "series": _draw(rng, series, rows),
            "sex": _draw(rng, sexes, rows),
            "age": _draw(rng, ages, rows),
            "unit": _draw(rng, units, rows),
            "year": _draw(rng, years, rows),
            "value": rng.random(rows),
        }
    )
    # 40 of the 50 age x unit combinations are mapped and the rest are left
    # unmapped, as in real data where a multi-map covers what occurs.
    combinations = [(age, unit) for age in ages for unit in units][:40]
    unit_measure = MultiComponentMap(
        source=["age", "unit"],
        target=["UNIT_MEASURE"],
        values=MultiRepresentationMap(
            id="MRM_UNIT_MEASURE",
            agency=_AGENCY,
            source=["String", "String"],
            target=["String"],
            maps=[
                MultiValueMap(source=[age, unit], target=[f"UM{i:02d}"])
                for i, (age, unit) in enumerate(combinations)
            ],
        ),
    )
    maps: list[_Map] = [
        _component_map("country", "REF_AREA", _code_map(countries, "RA")),
        _component_map("series", "INDICATOR", _code_map(series, "IN")),
        _component_map("sex", "SEX", _code_map(sexes, "SX")),
        ImplicitComponentMap(source="year", target="TIME_PERIOD"),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
        FixedValueMap(target="FREQ", value="A"),
        FixedValueMap(target="UNIT_MULT", value="0"),
        unit_measure,
    ]
    return df, _structure_map(maps)


def _high_cardinality(rows: int, rng: np.random.Generator) -> Inputs:
    entities = _codes("E", max(1, rows // 10))
    df = pd.DataFrame({"entity": _draw(rng, entities, rows), "value": rng.random(rows)})
    # A fixed-size literal map whatever the row count: at 1M rows it covers a
    # tenth of the 100,000 distinct entities and the catch-all takes the rest.
    value_maps = _code_map(_codes("E", _HIGH_CARDINALITY_MAP_SIZE), "EN")
    value_maps.append(ValueMap(source=CATCH_ALL, target="_O"))
    maps: list[_Map] = [
        _component_map("entity", "ENTITY", value_maps),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
    ]
    return df, _structure_map(maps)


def _regex_heavy(rows: int, rng: np.random.Generator) -> Inputs:
    known = _codes("K", 500)
    noise = _codes("N", 50)

    def column() -> np.ndarray:
        # 90% known codes, 10% codes that only the catch-all matches.
        return np.where(
            rng.random(rows) < 0.9, _draw(rng, known, rows), _draw(rng, noise, rows)
        )

    df = pd.DataFrame(
        {"code_a": column(), "code_b": column(), "value": rng.random(rows)}
    )
    # Half the known codes are literals. Ten rules split the other half by
    # final digit; ten more never match, as dead rules accumulate in real
    # templates; the catch-all takes the noise codes after trying all twenty.
    value_maps = _code_map(known[:250], "KL")
    value_maps += [
        ValueMap(source=f"regex:K\\d{{5}}{digit}", target=f"KR{digit}")
        for digit in range(10)
    ]
    value_maps += [
        ValueMap(source=f"regex:Q{rule:02d}\\d+", target=f"Q{rule:02d}")
        for rule in range(10)
    ]
    value_maps.append(ValueMap(source=CATCH_ALL, target="_O"))
    maps: list[_Map] = [
        _component_map("code_a", "CODE_A", value_maps),
        _component_map("code_b", "CODE_B", value_maps),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
    ]
    return df, _structure_map(maps)


def _wide(rows: int, rng: np.random.Generator) -> Inputs:
    data: dict[str, np.ndarray] = {}
    maps: list[_Map] = []
    for column in range(_WIDE_COLUMNS):
        codes = _codes(f"D{column:02d}_", _WIDE_CODES)
        source = f"dim_{column:02d}"
        data[source] = _draw(rng, codes, rows)
        maps.append(
            _component_map(
                source, f"DIM_{column:02d}", _code_map(codes, f"W{column:02d}_")
            )
        )
    data["value"] = rng.random(rows)
    maps.append(ImplicitComponentMap(source="value", target="OBS_VALUE"))
    return pd.DataFrame(data), _structure_map(maps)


_BUILDERS: dict[str, Callable[[int, np.random.Generator], Inputs]] = {
    "typical": _typical,
    "high_cardinality": _high_cardinality,
    "regex_heavy": _regex_heavy,
    "wide": _wide,
}
```

- [ ] **Step 6: Add `benchmarks` to mypy's files**

In `pyproject.toml`, `[tool.mypy]`, change `files = ["src", "tests"]` to:

```toml
files = ["src", "tests", "benchmarks"]
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: all pass. If any test fails with a pandas `FutureWarning` raised as an error, that warning comes from `src/tidysdmx/mapping.py` on these inputs. Stop and report it; don't suppress it.

- [ ] **Step 8: Lint and type-check**

Run: `uv run python -m ruff check benchmarks tests/test_benchmarks.py && uv run python -m ruff format benchmarks tests/test_benchmarks.py && uv run python -m mypy`
Expected: no errors. Fix what is reported rather than adding ignores.

- [ ] **Step 9: Commit**

```bash
git add benchmarks/__init__.py benchmarks/records.py benchmarks/scenarios.py tests/test_benchmarks.py pyproject.toml
git commit -F - <<'EOF'
chore(bench): add benchmark scenarios and the results schema

Four deterministic map_structures inputs (typical, high cardinality,
regex-heavy, wide) built from pysdmx constructors only, so they run
against every release since 0.8.0.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: Single-case runner and dependency probe

**Files:**
- Create: `benchmarks/case.py`, `benchmarks/probe.py`
- Modify: `tests/test_benchmarks.py` (append classes and imports)

**Interfaces:**
- Consumes: `build` from Task 1; `SCENARIOS`, `Record`, `empty_record` from `benchmarks.records`.
- Produces:
  - `benchmarks.case`:
    - `WARMUP_ROWS`, `SLOW_CALL_S`, `MEMORY_SKIP_S`;
    - `repeats_for(rows: int) -> int`;
    - `run_case(scenario: str, rows: int, seed: int = 0) -> Record`;
    - `main(argv: Sequence[str] | None = None) -> int`.

    CLI: `python -m benchmarks.case --scenario S --rows N [--seed K] --result PATH`.
  - `benchmarks.probe`: `PACKAGES`, `installed() -> dict[str, str | None]`. CLI: `python -m benchmarks.probe` prints JSON.

- [ ] **Step 1: Write the failing tests**

Add to the imports at the top of `tests/test_benchmarks.py`:

```python
import json
import platform
import sys

from benchmarks import case, probe
from benchmarks.case import repeats_for, run_case
```

Append:

```python
class TestRepeatsFor:
    @pytest.mark.parametrize(
        ("rows", "expected"),
        [(1_000, 7), (10_000, 7), (100_000, 5), (1_000_000, 3)],
    )
    def test_repeats_for_shrinks_with_size(self, rows, expected):
        assert repeats_for(rows) == expected


class TestRunCase:
    def test_run_case_reports_ok(self):
        assert run_case("typical", 1_000)["status"] == "ok"

    def test_run_case_records_every_sample(self):
        assert len(run_case("wide", 1_000)["samples_s"]) == 7

    def test_run_case_measures_peak_allocation(self):
        assert run_case("typical", 1_000)["peak_alloc_bytes"] > 0

    def test_run_case_records_output_rows(self):
        assert run_case("high_cardinality", 1_000)["output_shape"][0] == 1_000

    def test_run_case_records_input_size(self):
        assert run_case("regex_heavy", 1_000)["input_bytes"] > 0

    def test_run_case_stops_after_a_slow_call(self, monkeypatch):
        monkeypatch.setattr(case, "SLOW_CALL_S", 0.0)
        assert len(run_case("typical", 1_000)["samples_s"]) == 1

    def test_run_case_skips_memory_when_too_slow(self, monkeypatch):
        monkeypatch.setattr(case, "MEMORY_SKIP_S", 0.0)
        assert run_case("typical", 1_000)["peak_alloc_bytes"] is None


class TestCaseMain:
    def _main(self, tmp_path):
        result = tmp_path / "case.json"
        code = case.main(
            ["--scenario", "typical", "--rows", "100", "--result", str(result)]
        )
        return code, json.loads(result.read_text(encoding="utf-8"))

    def test_main_refuses_tidysdmx_from_outside_the_environment(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(sys, "prefix", str(tmp_path / "elsewhere"))
        _, record = self._main(tmp_path)
        assert record["reason"].startswith("ImportError: tidysdmx was imported from")

    def test_main_returns_one_on_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "prefix", str(tmp_path / "elsewhere"))
        code, _ = self._main(tmp_path)
        assert code == 1

    def test_main_writes_an_ok_record(self, tmp_path, monkeypatch):
        monkeypatch.setattr(case, "_assert_installed", lambda: None)
        _, record = self._main(tmp_path)
        assert record["status"] == "ok"


class TestInstalled:
    def test_installed_reports_python(self):
        assert probe.installed()["python"] == platform.python_version()

    def test_installed_reports_pandas(self):
        assert probe.installed()["pandas"] == pd.__version__

    def test_installed_reports_missing_package_as_none(self, monkeypatch):
        monkeypatch.setattr(probe, "PACKAGES", ("no-such-package-xyz",))
        assert probe.installed()["no-such-package-xyz"] is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: collection error, `ImportError: cannot import name 'case' from 'benchmarks'`.

- [ ] **Step 3: Write `benchmarks/case.py`**

```python
"""Measure one benchmark case: ``python -m benchmarks.case``.

``benchmarks.run`` starts this module once per case, inside the virtual
environment of the tidysdmx version under test and with the harness root as
the working directory. The record goes to the file named by ``--result``,
never to stdout, because tidysdmx 0.8.0 reports progress with ``print()``.
"""

import argparse
import gc
import json
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable, Sequence
from pathlib import Path

import pandas as pd
from pysdmx.model.map import StructureMap

from benchmarks.records import SCENARIOS, Record, empty_record
from benchmarks.scenarios import build

WARMUP_ROWS: int = 1_000
SLOW_CALL_S: float = 30.0
MEMORY_SKIP_S: float = 60.0

MapStructures = Callable[[pd.DataFrame, StructureMap], pd.DataFrame]


def repeats_for(rows: int) -> int:
    """Return how many timed calls a case of ``rows`` rows gets.

    Args:
        rows: The case's row count.

    Returns:
        7 up to 10,000 rows, 5 up to 100,000, and 3 above.
    """
    if rows <= 10_000:
        return 7
    if rows <= 100_000:
        return 5
    return 3


def run_case(scenario: str, rows: int, seed: int = 0) -> Record:
    """Time ``map_structures`` on one scenario and measure its peak allocation.

    A warm-up call on the first ``WARMUP_ROWS`` rows absorbs typeguard's
    instrumentation, imports and regex compilation. Timing stops early after a
    call slower than ``SLOW_CALL_S``. Memory is not traced when the median
    exceeds ``MEMORY_SKIP_S``, because tracemalloc slows Python-level code.

    Args:
        scenario: One of ``benchmarks.records.SCENARIOS``.
        rows: Number of input rows.
        seed: Seed passed to ``benchmarks.scenarios.build``.

    Returns:
        A record with status ``"ok"``.
    """
    # Imported here rather than at module level, so that a release that fails
    # to import becomes an error record in main() instead of a crash.
    from tidysdmx import map_structures

    df, structure_map = build(scenario, rows, seed)
    map_structures(df.head(WARMUP_ROWS), structure_map)

    samples: list[float] = []
    output_shape: list[int] = []
    for _ in range(repeats_for(rows)):
        start = time.perf_counter()
        result = map_structures(df, structure_map)
        samples.append(time.perf_counter() - start)
        output_shape = [int(n) for n in result.shape]
        # Free this output before the next call, so calls don't overlap in memory.
        del result
        if samples[-1] > SLOW_CALL_S:
            break

    median = statistics.median(samples)
    peak = (
        None
        if median > MEMORY_SKIP_S
        else _peak_allocation(map_structures, df, structure_map)
    )
    return {
        "scenario": scenario,
        "rows": rows,
        "status": "ok",
        "reason": None,
        "input_bytes": int(df.memory_usage(deep=True).sum()),
        "samples_s": samples,
        "median_s": median,
        "min_s": min(samples),
        "peak_alloc_bytes": peak,
        "output_shape": output_shape,
    }


def _peak_allocation(
    map_structures: MapStructures, df: pd.DataFrame, structure_map: StructureMap
) -> int:
    """Return the most memory one call allocates beyond its inputs, output included."""
    gc.collect()
    tracemalloc.start()
    try:
        map_structures(df, structure_map)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak


def _assert_installed() -> None:
    """Refuse to measure a tidysdmx that is not installed in this environment.

    Raises:
        ImportError: If tidysdmx was imported from outside ``sys.prefix``, such
            as a source tree on ``sys.path``: the benchmark would measure the
            wrong code.
    """
    import tidysdmx

    location = Path(tidysdmx.__file__).resolve()
    prefix = Path(sys.prefix).resolve()
    if not location.is_relative_to(prefix):
        raise ImportError(
            f"tidysdmx was imported from {location}, outside this environment "
            f"({prefix}); the benchmark would measure the wrong code"
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Measure one case and write its record to ``--result``.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 if the case was measured, 1 if it errored. The record is written
        either way.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.case",
        description="Measure one map_structures benchmark case.",
    )
    parser.add_argument("--scenario", required=True, choices=SCENARIOS)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--result", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        _assert_installed()
        record = run_case(args.scenario, args.rows, args.seed)
    except Exception as exc:
        # Not swallowed: the error becomes this case's record, which the run
        # reports. Any exception counts, since an old release can fail anywhere.
        record = empty_record(
            args.scenario, args.rows, "error", f"{type(exc).__name__}: {exc}"
        )
    args.result.write_text(json.dumps(record), encoding="utf-8")
    return 0 if record["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Write `benchmarks/probe.py`**

```python
"""Print the versions installed in this environment: ``python -m benchmarks.probe``.

``benchmarks.run`` runs it inside each version's environment and records the
output, so the results say what every version ran with. Standard library only.
"""

import importlib.metadata
import json
import platform
import sys

PACKAGES: tuple[str, ...] = ("tidysdmx", "pandas", "numpy", "pysdmx", "typeguard")


def installed() -> dict[str, str | None]:
    """Return the Python version and each package's installed version.

    Returns:
        ``{"python": ..., <package>: <version>}``, with ``None`` for a
        package that is not installed.
    """
    versions: dict[str, str | None] = {"python": platform.python_version()}
    for package in PACKAGES:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


if __name__ == "__main__":
    sys.stdout.write(json.dumps(installed()) + "\n")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: all pass.

- [ ] **Step 6: Lint and type-check**

Run: `uv run python -m ruff check benchmarks tests/test_benchmarks.py && uv run python -m ruff format benchmarks tests/test_benchmarks.py && uv run python -m mypy`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/case.py benchmarks/probe.py tests/test_benchmarks.py
git commit -F - <<'EOF'
chore(bench): measure one benchmark case in a version's environment

Times map_structures after a warm-up, traces its peak allocation in a
separate call, and writes the record to a file because tidysdmx 0.8.0
prints to stdout. Refuses a tidysdmx imported from outside the venv.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Orchestrator

**Files:**
- Create: `benchmarks/run.py`
- Modify: `tests/test_benchmarks.py`, `pyproject.toml` (ruff per-file ignore), `.gitignore`

**Interfaces:**
- Consumes: `benchmarks.records` (all names); `python -m benchmarks.case` and `python -m benchmarks.probe` as subprocess CLIs.
- Produces in `benchmarks.run`:
  - Constants: `DEFAULT_SIZES`, `BUDGET_S`, `CASE_TIMEOUT_S`, `PYTHON`.
  - `Version(label, ref, role, commit, aliases=())`, a frozen dataclass.
  - Pure functions:
    - `select_baselines(tags, target, count) -> list[str]`;
    - `parse_named_ref(text) -> tuple[str, str]`;
    - `merge_same_commit(versions) -> list[Version]`;
    - `plan_versions(target, target_label, compare, baselines, tags, resolve) -> list[Version]`;
    - `rotated(items, shift) -> list`;
    - `exceeds_budget(record) -> bool`;
    - `run_grid(versions, scenarios, sizes, run_case) -> list[Record]`.
  - `measure(python: Path, harness: Path, work: Path, scenario: str, rows: int) -> Record`.
  - `main(argv) -> int`. CLI: `python -m benchmarks.run --target REF [--target-label L] [--compare [LABEL=]REF ...] [--baselines N] [--sizes N ...] [--scenarios S ...] [--work-dir DIR] --out PATH`.

- [ ] **Step 1: Write the failing tests**

Add to the imports at the top of `tests/test_benchmarks.py`:

```python
from pathlib import Path

from benchmarks import run
from benchmarks.run import (
    Version,
    exceeds_budget,
    measure,
    merge_same_commit,
    parse_named_ref,
    plan_versions,
    rotated,
    run_grid,
    select_baselines,
)

ROOT = Path(__file__).resolve().parents[1]
TAGS = ["v0.8.0", "v0.10.0", "v0.9.0", "v0.7.5", "not-a-release", "v1.0.0-rc.1"]
```

Append:

```python
def _version(label, commit=None, role="baseline"):
    return Version(label=label, ref=label, role=role, commit=commit or f"sha-{label}")


def _ok(median):
    record = empty_record("typical", 1, "error", "")
    record.update(status="ok", reason=None, median_s=median, samples_s=[median])
    return record


class TestSelectBaselines:
    def test_select_baselines_takes_releases_below_a_tag(self):
        assert select_baselines(TAGS, "v0.10.0", 2) == ["v0.9.0", "v0.8.0"]

    def test_select_baselines_takes_newest_releases_for_a_branch(self):
        assert select_baselines(TAGS, "origin/dev", 3) == [
            "v0.10.0",
            "v0.9.0",
            "v0.8.0",
        ]

    def test_select_baselines_returns_nothing_for_zero(self):
        assert select_baselines(TAGS, "origin/dev", 0) == []


class TestParseNamedRef:
    def test_parse_named_ref_splits_label(self):
        assert parse_named_ref("base=origin/dev") == ("base", "origin/dev")

    def test_parse_named_ref_uses_bare_ref_as_label(self):
        assert parse_named_ref("origin/dev") == ("origin/dev", "origin/dev")

    @pytest.mark.parametrize("text", ["=origin/dev", "base="])
    def test_parse_named_ref_rejects_empty_side(self, text):
        with pytest.raises(ValueError, match="Expected LABEL=REF or REF"):
            parse_named_ref(text)


class TestMergeSameCommit:
    def test_merge_same_commit_keeps_the_first_label(self):
        merged = merge_same_commit(
            [_version("base", "abc"), _version("v0.10.0", "abc")]
        )
        assert [v.label for v in merged] == ["base"]

    def test_merge_same_commit_records_aliases(self):
        merged = merge_same_commit(
            [_version("base", "abc"), _version("v0.10.0", "abc")]
        )
        assert merged[0].aliases == ("v0.10.0",)

    def test_merge_same_commit_keeps_distinct_commits(self):
        merged = merge_same_commit([_version("a", "1"), _version("b", "2")])
        assert len(merged) == 2


class TestPlanVersions:
    def _plan(self, target, compare=(), baselines=2, label=None):
        commits = {"origin/dev": "sha-dev", "WORKTREE": "WORKTREE"}
        return plan_versions(
            target,
            label,
            list(compare),
            baselines,
            TAGS,
            lambda ref: commits.get(ref, f"sha-{ref}"),
        )

    def test_plan_versions_labels_worktree(self):
        assert self._plan("WORKTREE")[0].label == "worktree"

    def test_plan_versions_orders_target_compare_baselines(self):
        planned = self._plan("head", compare=["base=origin/dev"], baselines=1)
        assert [(v.label, v.role) for v in planned] == [
            ("head", "target"),
            ("base", "compare"),
            ("v0.10.0", "baseline"),
        ]

    def test_plan_versions_uses_given_target_label(self):
        assert self._plan("abc123", label="head")[0].label == "head"


class TestRotated:
    def test_rotated_shifts_left(self):
        assert rotated(["a", "b", "c"], 1) == ["b", "c", "a"]

    def test_rotated_wraps_around(self):
        assert rotated(["a", "b", "c"], 4) == ["b", "c", "a"]

    def test_rotated_handles_empty(self):
        assert rotated([], 3) == []


class TestExceedsBudget:
    def test_exceeds_budget_flags_slow_case(self):
        assert exceeds_budget(_ok(run.BUDGET_S + 1))

    def test_exceeds_budget_ignores_errors(self):
        assert not exceeds_budget(empty_record("typical", 1, "error", "boom"))


class TestRunGrid:
    def test_run_grid_rotates_version_order_per_case(self):
        calls = []

        def fake(version, scenario, rows):
            calls.append(version.label)
            return _ok(0.1)

        run_grid([_version("a"), _version("b")], ["typical"], [1, 2], fake)
        assert calls == ["a", "b", "b", "a"]

    def test_run_grid_skips_larger_sizes_after_budget(self):
        def fake(version, scenario, rows):
            return _ok(run.BUDGET_S + 1 if version.label == "slow" else 0.1)

        records = run_grid(
            [_version("slow"), _version("fast")], ["typical"], [1, 2], fake
        )
        statuses = {(r["version"], r["rows"]): r["status"] for r in records}
        assert statuses[("slow", 2)] == "skipped"

    def test_run_grid_keeps_measuring_other_scenarios(self):
        def fake(version, scenario, rows):
            return _ok(run.BUDGET_S + 1 if scenario == "typical" else 0.1)

        records = run_grid([_version("a")], ["typical", "wide"], [1, 2], fake)
        assert [r["status"] for r in records if r["scenario"] == "wide"] == ["ok", "ok"]

    def test_run_grid_labels_every_record(self):
        records = run_grid([_version("a")], ["typical"], [1], lambda *_: _ok(0.1))
        assert records[0]["version"] == "a"


class TestMeasure:
    def test_measure_returns_the_record_the_case_wrote(self, tmp_path):
        record = measure(Path(sys.executable), ROOT, tmp_path, "typical", 100)
        assert (record["scenario"], record["rows"]) == ("typical", 100)

    def test_measure_reports_a_case_that_wrote_nothing(self, tmp_path):
        # From tmp_path, `python -m benchmarks.case` cannot even be imported.
        record = measure(Path(sys.executable), tmp_path, tmp_path, "typical", 100)
        assert record["reason"].startswith("exited 1 without a result")

    def test_measure_reports_a_timeout(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run, "CASE_TIMEOUT_S", 0.001)
        record = measure(Path(sys.executable), ROOT, tmp_path, "typical", 100)
        assert record["reason"].startswith("timed out")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: collection error, `ImportError: cannot import name 'run' from 'benchmarks'`.

- [ ] **Step 3: Write `benchmarks/run.py`**

```python
"""Benchmark ``map_structures`` across tidysdmx versions: ``python -m benchmarks.run``.

Each version is installed from its git ref into a virtual environment of its
own, with the dependencies it declares, so a release that requires pandas 2
and one that requires pandas 3 are each measured as users install them. Every
case runs in a fresh subprocess, and the cases are interleaved across
versions, so slow drift on the machine spreads over every version instead of
landing on one.

Standard library only: it runs under ``uv run --no-project``.
"""

import argparse
import io
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar, cast

from benchmarks.records import (
    SCENARIOS,
    SCHEMA_VERSION,
    WORKTREE,
    Machine,
    Record,
    Results,
    Role,
    VersionInfo,
    empty_record,
    release_key,
)

DEFAULT_SIZES: tuple[int, ...] = (1_000, 10_000, 100_000, 1_000_000)
BUDGET_S: float = 60.0
CASE_TIMEOUT_S: float = 900.0
PYTHON: str = "3.11"

# Not __name__: under `python -m` that is "__main__".
logger = logging.getLogger("benchmarks.run")
T = TypeVar("T")


@dataclass(frozen=True)
class Version:
    """One tidysdmx version to measure.

    Attributes:
        label: The name the results use for it.
        ref: The git ref it is built from, or ``WORKTREE``.
        role: ``"target"``, ``"compare"`` or ``"baseline"``.
        commit: The commit ``ref`` resolved to, or ``WORKTREE``.
        aliases: Labels of other requested versions at the same commit.
    """

    label: str
    ref: str
    role: Role
    commit: str
    aliases: tuple[str, ...] = ()


CaseRunner = Callable[[Version, str, int], Record]


def select_baselines(tags: Iterable[str], target: str, count: int) -> list[str]:
    """Return the ``count`` newest release tags older than ``target``.

    Args:
        tags: The repository's tags; non-release tags are ignored.
        target: The profiled ref. When it is not a release tag (a branch, a
            commit, the working tree), every release counts as older.
        count: How many to return.

    Returns:
        Release tags, newest first.
    """
    releases = sorted(
        (key, tag) for tag in tags if (key := release_key(tag)) is not None
    )
    ceiling = release_key(target)
    older = [tag for key, tag in releases if ceiling is None or key < ceiling]
    return older[::-1][:count]


def parse_named_ref(text: str) -> tuple[str, str]:
    """Split ``LABEL=REF`` into its label and ref.

    Args:
        text: ``LABEL=REF``, or a bare ``REF`` that is its own label.

    Returns:
        ``(label, ref)``.

    Raises:
        ValueError: If either side of the ``=`` is empty.
    """
    label, separator, ref = text.partition("=")
    if not separator:
        return text, text
    if not label or not ref:
        raise ValueError(f"Expected LABEL=REF or REF, got {text!r}")
    return label, ref


def merge_same_commit(versions: Sequence[Version]) -> list[Version]:
    """Measure each commit once.

    A version at the same commit as an earlier one is dropped, and its label
    becomes one of the earlier version's aliases.

    Args:
        versions: Versions in priority order: target, compares, baselines.

    Returns:
        The versions, with duplicates folded into the first of their commit.
    """
    merged: list[Version] = []
    for version in versions:
        for position, kept in enumerate(merged):
            if kept.commit == version.commit:
                merged[position] = replace(kept, aliases=(*kept.aliases, version.label))
                break
        else:
            merged.append(version)
    return merged


def plan_versions(
    target: str,
    target_label: str | None,
    compare: Sequence[str],
    baselines: int,
    tags: Sequence[str],
    resolve: Callable[[str], str],
) -> list[Version]:
    """Return the versions to measure, target first.

    Args:
        target: The git ref to profile, or ``WORKTREE``.
        target_label: Its label; by default the ref, or ``"worktree"``.
        compare: Extra versions, each ``LABEL=REF`` or ``REF``.
        baselines: How many earlier releases to add.
        tags: The repository's tags.
        resolve: Maps a ref to its commit.

    Returns:
        Target, then compares, then baselines newest first; each commit once.
    """
    label = target_label or ("worktree" if target == WORKTREE else target)
    planned = [Version(label, target, "target", resolve(target))]
    for text in compare:
        name, ref = parse_named_ref(text)
        planned.append(Version(name, ref, "compare", resolve(ref)))
    for tag in select_baselines(tags, target, baselines):
        planned.append(Version(tag, tag, "baseline", resolve(tag)))
    return merge_same_commit(planned)


def rotated(items: Sequence[T], shift: int) -> list[T]:
    """Return ``items`` rotated left by ``shift`` places, modulo its length."""
    if not items:
        return []
    shift %= len(items)
    return [*items[shift:], *items[:shift]]


def exceeds_budget(record: Record) -> bool:
    """Return whether a measured case's median exceeded ``BUDGET_S``."""
    median = record["median_s"]
    return record["status"] == "ok" and median is not None and median > BUDGET_S


def run_grid(
    versions: Sequence[Version],
    scenarios: Sequence[str],
    sizes: Sequence[int],
    run_case: CaseRunner,
) -> list[Record]:
    """Measure every (scenario, size) case for every version.

    Sizes run smallest first. Within a case the version order rotates, so no
    version always runs first. Once a version's median for a scenario exceeds
    ``BUDGET_S``, its larger sizes of that scenario are recorded as skipped.

    Args:
        versions: The versions to measure.
        scenarios: The scenarios to run.
        sizes: Row counts.
        run_case: Measures one version on one case.

    Returns:
        One record per version and case, labelled with the version.
    """
    over_budget: set[tuple[str, str]] = set()
    records: list[Record] = []
    cases = [(scenario, rows) for scenario in scenarios for rows in sorted(sizes)]
    for number, (scenario, rows) in enumerate(cases):
        for version in rotated(versions, number):
            if (version.label, scenario) in over_budget:
                record = empty_record(
                    scenario,
                    rows,
                    "skipped",
                    f"a smaller size took over {BUDGET_S:.0f} s",
                )
            else:
                record = run_case(version, scenario, rows)
                if exceeds_budget(record):
                    over_budget.add((version.label, scenario))
            record["version"] = version.label
            records.append(record)
    return records


def _run(command: Sequence[str], cwd: Path | None = None) -> str:
    """Run ``command`` and return its stripped stdout.

    Raises:
        RuntimeError: If it exits non-zero; the message carries its stderr.
    """
    completed = subprocess.run(
        list(command), cwd=cwd, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed:\n{completed.stderr.strip()}")
    return completed.stdout.strip()


def _slug(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", label)


def resolve_commit(root: Path, ref: str) -> str:
    """Return the commit ``ref`` names, or ``WORKTREE`` for the working tree."""
    if ref == WORKTREE:
        return WORKTREE
    return _run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=root)


def export_source(root: Path, version: Version, work: Path) -> Path:
    """Return a directory holding ``version``'s source tree.

    Raises:
        RuntimeError: If ``git archive`` fails.
    """
    if version.commit == WORKTREE:
        return root
    destination = work / "src" / _slug(version.label)
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True)
    archive = subprocess.run(
        ["git", "archive", "--format=tar", version.commit],
        cwd=root,
        capture_output=True,
        check=False,
    )
    if archive.returncode != 0:
        stderr = archive.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git archive {version.commit} failed:\n{stderr}")
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(destination, filter="data")
    return destination


def create_env(version: Version, source: Path, work: Path) -> Path:
    """Install ``source`` into a fresh virtual environment of its own.

    Returns:
        The environment's Python interpreter.

    Raises:
        RuntimeError: If uv cannot create the environment or install into it.
    """
    venv = work / "venv" / _slug(version.label)
    shutil.rmtree(venv, ignore_errors=True)
    _run(["uv", "venv", "--quiet", "--python", PYTHON, str(venv)])
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    # --reinstall-package: uv keys a local directory's cached build on
    # pyproject.toml alone, so an edited working tree would otherwise be
    # benchmarked as it was when first built.
    _run(
        [
            "uv",
            "pip",
            "install",
            "--quiet",
            "--python",
            str(python),
            "--reinstall-package",
            "tidysdmx",
            str(source),
        ]
    )
    return python


def installed_versions(python: Path, harness: Path) -> dict[str, str | None]:
    """Return what ``python``'s environment has installed (see ``benchmarks.probe``)."""
    output = _run([str(python), "-m", "benchmarks.probe"], cwd=harness)
    return cast(dict[str, str | None], json.loads(output))


def measure(
    python: Path, harness: Path, work: Path, scenario: str, rows: int
) -> Record:
    """Run one case in ``python``'s environment and return its record.

    Args:
        python: The environment's interpreter.
        harness: The directory holding ``benchmarks/``; the working directory.
        work: Scratch directory for the result file.
        scenario: The scenario to run.
        rows: Its row count.

    Returns:
        The record the case wrote, or an error record if it timed out or
        exited without writing one.
    """
    result = work / "case.json"
    result.parent.mkdir(parents=True, exist_ok=True)
    result.unlink(missing_ok=True)
    command = [
        str(python),
        "-m",
        "benchmarks.case",
        "--scenario",
        scenario,
        "--rows",
        str(rows),
        "--result",
        str(result),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=harness,
            capture_output=True,
            text=True,
            timeout=CASE_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return empty_record(
            scenario, rows, "error", f"timed out after {CASE_TIMEOUT_S:.0f} s"
        )
    if result.exists():
        return cast(Record, json.loads(result.read_text(encoding="utf-8")))
    tail = " | ".join(completed.stderr.strip().splitlines()[-5:])
    return empty_record(
        scenario,
        rows,
        "error",
        f"exited {completed.returncode} without a result: {tail}",
    )


def machine_info() -> Machine:
    """Describe the machine this runs on."""
    return {
        "platform": platform.platform(),
        "cpu": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": _memory_bytes(),
        "runner_image": _runner_image(),
    }


def _cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.partition(":")[2].strip()
    return platform.processor() or "unknown CPU"


def _memory_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def _runner_image() -> str | None:
    image, version = os.environ.get("ImageOS"), os.environ.get("ImageVersion")
    return f"{image} {version}" if image and version else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.run",
        description="Benchmark map_structures across tidysdmx versions.",
    )
    parser.add_argument(
        "--target",
        required=True,
        help=f"git ref to profile, or {WORKTREE} for the working tree",
    )
    parser.add_argument("--target-label", help="the target's label (default: the ref)")
    parser.add_argument(
        "--compare",
        nargs="*",
        default=[],
        metavar="[LABEL=]REF",
        help="other refs to measure alongside the target",
    )
    parser.add_argument(
        "--baselines",
        type=int,
        default=3,
        help="how many earlier releases to measure (default: 3)",
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument(
        "--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS)
    )
    parser.add_argument("--work-dir", type=Path, default=Path(".bench"))
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Benchmark the requested versions and write the results JSON.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 if the target produced at least one measurement, 1 otherwise.

    Raises:
        RuntimeError: If the target version cannot be installed.
    """
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    harness = Path(_run(["git", "rev-parse", "--show-toplevel"]))
    work = args.work_dir.resolve()
    tags = _run(["git", "tag", "--list", "v*"], cwd=harness).splitlines()
    versions = plan_versions(
        args.target,
        args.target_label,
        args.compare,
        args.baselines,
        tags,
        lambda ref: resolve_commit(harness, ref),
    )

    pythons: dict[str, Path] = {}
    infos: list[VersionInfo] = []
    for version in versions:
        logger.info("Installing %s (%s)", version.label, version.ref)
        try:
            python = create_env(version, export_source(harness, version, work), work)
        except RuntimeError:
            if version.role == "target":
                raise
            logger.exception("Skipping %s: it could not be installed", version.label)
            continue
        pythons[version.label] = python
        infos.append(
            {
                "label": version.label,
                "ref": version.ref,
                "commit": version.commit,
                "role": version.role,
                "aliases": list(version.aliases),
                "deps": installed_versions(python, harness),
            }
        )

    def _run_case(version: Version, scenario: str, rows: int) -> Record:
        record = measure(pythons[version.label], harness, work, scenario, rows)
        median = record["median_s"]
        outcome = f"{median:.4f} s" if median is not None else record["reason"]
        logger.info("%-14s %-17s %9d rows  %s", version.label, scenario, rows, outcome)
        return record

    measured = [version for version in versions if version.label in pythons]
    records = run_grid(measured, args.scenarios, args.sizes, _run_case)
    results: Results = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine_info(),
        "sizes": sorted(args.sizes),
        "scenarios": list(args.scenarios),
        "versions": infos,
        "results": records,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    logger.info("Wrote %s", args.out)
    target = versions[0].label
    measured_target = any(
        record["status"] == "ok" and record.get("version") == target
        for record in records
    )
    return 0 if measured_target else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Allow the orchestrator's subprocess calls in ruff**

In `pyproject.toml`, `[tool.ruff.lint.per-file-ignores]`, after the `"tests/fixtures/fxtr_fmr.py"` entry, add:

```toml
# The orchestrator's job is to run git, uv and each benchmark environment's
# interpreter. Every call passes an argv list (never a shell string) built from
# refs and paths the caller chose; "git"/"uv" are resolved from PATH on purpose,
# as everywhere else in this repository's tooling.
"benchmarks/run.py" = ["S603", "S607"]
```

- [ ] **Step 5: Ignore the benchmark work directories**

In `.gitignore`, under `# tidysdmx-specific`, add:

```gitignore
# Benchmark environments and results (`make bench`, benchmarks/run.py)
.bench/
benchmarks/results/
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: all pass.

- [ ] **Step 7: Lint and type-check**

Run: `uv run python -m ruff check benchmarks tests/test_benchmarks.py && uv run python -m ruff format benchmarks tests/test_benchmarks.py && uv run python -m mypy`
Expected: no errors. If ruff reports `S202` on `extractall` despite `filter="data"`, add `"S202"` to the `benchmarks/run.py` entry, with a line in its comment saying `filter="data"` is the mitigation.

- [ ] **Step 8: Smoke-run the orchestrator end to end**

Run: `uv run --no-project python -m benchmarks.run --target WORKTREE --baselines 2 --sizes 1000 10000 --out benchmarks/results/latest.json`

Expected:
- the log installs `worktree`, `v0.10.0` and `v0.9.0`, then prints 24 case lines;
- it exits 0;
- `benchmarks/results/latest.json` exists.

Then check what each version ran with:

Run: `uv run --no-project python -c "import json; d=json.load(open('benchmarks/results/latest.json')); print([(v['label'], v['deps']['pandas']) for v in d['versions']]); print({r['status'] for r in d['results']})"`

Expected:
- pandas `2.x` for every version on this branch (the `dev` lock is still pandas 2);
- statuses `{'ok'}`.

If a version errors, read its `reason` in the JSON before going on.

- [ ] **Step 9: Commit**

```bash
git add benchmarks/run.py tests/test_benchmarks.py pyproject.toml .gitignore
git commit -F - <<'EOF'
chore(bench): benchmark several tidysdmx versions side by side

Installs each version from its git ref into its own venv with the
dependencies it declares, runs every case in a fresh subprocess
interleaved across versions, skips sizes past a 60 s budget, and
writes one results JSON.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: Text reports (job summary and PR comment)

**Files:**
- Create: `benchmarks/report.py`, `tests/fixtures/fxtr_benchmarks.py`
- Modify: `tests/conftest.py`, `tests/test_benchmarks.py`

**Interfaces:**
- Consumes: `Results`, `Record`, `VersionInfo`, `SCHEMA_VERSION`, `WORKTREE`, `release_key` from `benchmarks.records`.
- Produces in `benchmarks.report`:
  - Constants: `COMMENT_MARKER`, `RESULTS_MARKER`, `MISSING`, `SAME`, `TIMES`, `DELTA`, `SAME_BAND`, `REPO_URL`, `HEADLINE_SCENARIO`.
  - Types: `Metric = Literal["time", "memory"]`.
  - Functions:
    - `load_results(path) -> Results | None`;
    - `format_seconds(float) -> str`, `format_bytes(float) -> str`;
    - `relative_change(new, old) -> float | None`, `format_change(change) -> str`;
    - `metric_value(record, metric) -> float | None`, `format_metric(value, metric) -> str`;
    - `index_records(results) -> dict[tuple[str, str, int], Record]`;
    - `display_label(info) -> str`, `reference_versions(results) -> list[VersionInfo]`;
    - `markdown_table(header, body) -> str`;
    - `comparison_table(results, rows, metric) -> str`;
    - `machine_description(results) -> str`, `environment_table(results) -> str`;
    - `render_comparison(results, run_url=None) -> str`, `render_pr_comment(results, run_url=None) -> str`;
    - `main(argv) -> int`.
- Produces in `tests.fixtures.fxtr_benchmarks`: `make_results() -> dict` and the function-scoped fixture `benchmark_results`.

- [ ] **Step 1: Write the fixture module**

Create `tests/fixtures/fxtr_benchmarks.py`:

```python
"""Benchmark results for the report tests in tests/test_benchmarks.py."""

import pytest


def _record(version, scenario, rows, median, peak, status="ok", reason=None):
    measured = status == "ok"
    return {
        "version": version,
        "scenario": scenario,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": rows * 100 if measured else None,
        "samples_s": [median] if measured else [],
        "median_s": median,
        "min_s": median,
        "peak_alloc_bytes": peak,
        "output_shape": [rows, 9] if measured else None,
    }


def _deps(pandas):
    return {
        "python": "3.11.14",
        "tidysdmx": "0.10.0",
        "pandas": pandas,
        "numpy": "2.3.4",
        "pysdmx": "1.20.0",
        "typeguard": "4.4.4",
    }


def make_results():
    """Return a fresh results dict: a PR head, its base, and one release.

    At 1M rows the head is twice as fast and half as hungry as the base on the
    typical table, and within 10% of v0.10.0. On the wide table the base
    errored and v0.10.0 was skipped.
    """
    mib = 2**20
    return {
        "schema_version": 1,
        "generated_at": "2026-10-23T12:00:00+00:00",
        "machine": {
            "platform": "Linux-6.8.0-x86_64-with-glibc2.39",
            "cpu": "AMD EPYC 7763 64-Core Processor",
            "cpu_count": 4,
            "memory_bytes": 16 * 2**30,
            "runner_image": "ubuntu24 20261020.1",
        },
        "sizes": [1_000, 1_000_000],
        "scenarios": ["typical", "wide"],
        "versions": [
            {
                "label": "head",
                "ref": "abc1234def",
                "commit": "abc1234def5678",
                "role": "target",
                "aliases": [],
                "deps": _deps("3.0.6"),
            },
            {
                "label": "base",
                "ref": "origin/dev",
                "commit": "def5678abc1234",
                "role": "compare",
                "aliases": [],
                "deps": _deps("2.3.3"),
            },
            {
                "label": "v0.10.0",
                "ref": "v0.10.0",
                "commit": "0123456789abcdef",
                "role": "baseline",
                "aliases": [],
                "deps": _deps("2.3.3"),
            },
        ],
        "results": [
            _record("head", "typical", 1_000, 0.010, 2 * mib),
            _record("base", "typical", 1_000, 0.011, 2 * mib),
            _record("v0.10.0", "typical", 1_000, 0.020, 4 * mib),
            _record("head", "typical", 1_000_000, 1.0, 300 * mib),
            _record("base", "typical", 1_000_000, 2.0, 600 * mib),
            _record("v0.10.0", "typical", 1_000_000, 1.05, 310 * mib),
            _record("head", "wide", 1_000, 0.02, 3 * mib),
            _record("base", "wide", 1_000, 0.02, 3 * mib),
            _record("v0.10.0", "wide", 1_000, 0.03, 5 * mib),
            _record("head", "wide", 1_000_000, 4.0, 900 * mib),
            _record("base", "wide", 1_000_000, None, None, "error", "MemoryError: "),
            _record(
                "v0.10.0",
                "wide",
                1_000_000,
                None,
                None,
                "skipped",
                "a smaller size took over 60 s",
            ),
        ],
    }


@pytest.fixture
def benchmark_results():
    """Return a fresh results dict. Function-scoped: tests modify it."""
    return make_results()
```

- [ ] **Step 2: Register the fixture**

In `tests/conftest.py`:
- add `from tests.fixtures.fxtr_benchmarks import benchmark_results` to the imports, keeping them sorted (it goes before `from tests.fixtures.fxtr_dummy_data import ...`);
- add `"benchmark_results",` to `__all__` in sorted position.

- [ ] **Step 3: Write the failing tests**

Add to the imports at the top of `tests/test_benchmarks.py`:

```python
from benchmarks import report
from benchmarks.report import (
    COMMENT_MARKER,
    MISSING,
    SAME,
    format_bytes,
    format_change,
    format_seconds,
    load_results,
    render_comparison,
    render_pr_comment,
)
```

Append:

```python
class TestFormatSeconds:
    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (0.000412, "0.412 ms"),
            (0.35, "350 ms"),
            (0.9996, "1 s"),
            (2.314, "2.31 s"),
            (123.4, "123 s"),
        ],
    )
    def test_format_seconds_keeps_three_figures(self, seconds, expected):
        assert format_seconds(seconds) == expected


class TestFormatBytes:
    @pytest.mark.parametrize(
        ("count", "expected"),
        [
            (512, "512 B"),
            (1_000, "0.977 KiB"),
            (2 * 2**20, "2 MiB"),
            (300 * 2**20, "300 MiB"),
            (1.5 * 2**30, "1.5 GiB"),
        ],
    )
    def test_format_bytes_picks_the_unit(self, count, expected):
        assert format_bytes(count) == expected


class TestFormatChange:
    @pytest.mark.parametrize(
        ("change", "expected"),
        [
            (None, MISSING),
            (0.05, SAME),
            (-0.0999, SAME),
            (-0.5, "-50%"),
            (0.25, "+25%"),
        ],
    )
    def test_format_change_folds_small_changes(self, change, expected):
        assert format_change(change) == expected


class TestLoadResults:
    def test_load_results_returns_none_for_missing_file(self, tmp_path):
        assert load_results(tmp_path / "absent.json") is None

    def test_load_results_rejects_other_schemas(self, tmp_path):
        path = tmp_path / "old.json"
        path.write_text('{"schema_version": 0}', encoding="utf-8")
        with pytest.raises(
            ValueError, match="is not a schema 1 benchmark results file"
        ):
            load_results(path)


class TestRenderComparison:
    def test_render_comparison_compares_head_with_base(self, benchmark_results):
        assert "| typical | 1 s | 2 s | 1.05 s | -50% | " in render_comparison(
            benchmark_results
        )

    def test_render_comparison_folds_small_change_from_release(self, benchmark_results):
        assert f"| -50% | {SAME} |" in render_comparison(benchmark_results)

    def test_render_comparison_marks_unmeasured_cells(self, benchmark_results):
        row = f"| wide | 4 s | {MISSING} | {MISSING} | {MISSING} | {MISSING} |"
        assert row in render_comparison(benchmark_results)

    def test_render_comparison_folds_all_sizes_away(self, benchmark_results):
        text = render_comparison(benchmark_results)
        assert text.index("<details>") < text.index("**1,000 rows**")

    def test_render_comparison_links_the_run(self, benchmark_results):
        text = render_comparison(benchmark_results, "https://example.test/run/1")
        assert "[this run](https://example.test/run/1)" in text

    def test_render_comparison_shows_aliases(self, benchmark_results):
        benchmark_results["versions"].pop()
        benchmark_results["versions"][1]["aliases"] = ["v0.10.0"]
        assert "base (= v0.10.0)" in render_comparison(benchmark_results)

    def test_render_comparison_drops_change_from_a_merged_release(
        self, benchmark_results
    ):
        benchmark_results["versions"].pop()
        benchmark_results["versions"][1]["aliases"] = ["v0.10.0"]
        assert "vs v0.10.0" not in render_comparison(benchmark_results)

    def test_render_comparison_lists_dependency_versions(self, benchmark_results):
        assert "| head | target | `abc1234` | 3.11.14 | 3.0.6 |" in render_comparison(
            benchmark_results
        )


class TestRenderPrComment:
    def test_render_pr_comment_starts_with_marker(self, benchmark_results):
        assert render_pr_comment(benchmark_results).startswith(COMMENT_MARKER + "\n")


class TestReportMain:
    def test_main_writes_pr_comment(self, tmp_path, benchmark_results):
        source = tmp_path / "results.json"
        source.write_text(json.dumps(benchmark_results), encoding="utf-8")
        output = tmp_path / "comment.md"
        report.main(
            ["--format", "pr-comment", "--input", str(source), "--output", str(output)]
        )
        assert output.read_text(encoding="utf-8").startswith(COMMENT_MARKER)

    def test_main_rejects_summary_without_results(self, tmp_path):
        with pytest.raises(SystemExit):
            report.main(
                ["--format", "summary", "--input", str(tmp_path / "absent.json")]
            )
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: collection error, `ImportError: cannot import name 'report' from 'benchmarks'`.

- [ ] **Step 5: Write `benchmarks/report.py`**

```python
"""Render benchmark results: ``python -m benchmarks.report``.

One results file (``benchmarks.records.Results``) feeds three formats:

- ``page``: the docs site's Performance page, filled into ``page.qmd``, or a
  placeholder when no results exist yet;
- ``summary``: a GitHub Actions job summary;
- ``pr-comment``: the sticky pull-request comment, ``summary`` behind a marker.

Standard library only, except the page's charts, which import matplotlib when
they are drawn: the text formats run under ``uv run --no-project``.
"""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, cast

from benchmarks.records import (
    SCHEMA_VERSION,
    WORKTREE,
    Record,
    Results,
    VersionInfo,
    release_key,
)

COMMENT_MARKER: str = "<!-- tidysdmx-benchmark -->"
RESULTS_MARKER: str = "<!-- RESULTS -->"
HEADLINE_SCENARIO: str = "typical"
SAME_BAND: float = 0.10
REPO_URL: str = "https://github.com/WB-DECIS/tidysdmx"
TEMPLATE: Path = Path(__file__).with_name("page.qmd")

MISSING: str = "—"  # em dash
SAME: str = "≈ same"
TIMES: str = "×"
DELTA: str = "Δ"
PLUS_MINUS: str = "±"

Metric = Literal["time", "memory"]
Key = tuple[str, str, int]

_METRIC_TITLES: dict[str, str] = {"time": "Median time", "memory": "Peak extra memory"}
_BYTE_UNITS: tuple[str, ...] = ("B", "KiB", "MiB", "GiB")


def load_results(path: Path) -> Results | None:
    """Read a results file written by ``benchmarks.run``.

    Args:
        path: The file.

    Returns:
        The results, or ``None`` if the file does not exist.

    Raises:
        ValueError: If the file is not a schema ``SCHEMA_VERSION`` results file.
    """
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"{path} is not a schema {SCHEMA_VERSION} benchmark results file"
        )
    return cast(Results, data)


def format_seconds(seconds: float) -> str:
    """Format a duration to three significant figures, e.g. ``"350 ms"``."""
    if seconds < 0.9995:
        return f"{seconds * 1000:.3g} ms"
    if seconds < 99.95:
        return f"{seconds:.3g} s"
    return f"{seconds:.0f} s"


def format_bytes(count: float) -> str:
    """Format a byte count in binary units to three figures, e.g. ``"300 MiB"``."""
    unit = 0
    while count >= 999.5 and unit < len(_BYTE_UNITS) - 1:
        count /= 1024
        unit += 1
    precision = ".3g" if count < 99.95 else ".0f"
    return f"{count:{precision}} {_BYTE_UNITS[unit]}"


def relative_change(new: float | None, old: float | None) -> float | None:
    """Return ``new`` relative to ``old`` (``-0.5`` is half), if both exist."""
    if new is None or old is None or old <= 0:
        return None
    return new / old - 1


def format_change(change: float | None) -> str:
    """Format a relative change; within ``SAME_BAND`` it reads ``SAME``."""
    if change is None:
        return MISSING
    if abs(change) < SAME_BAND:
        return SAME
    return f"{change:+.0%}"


def metric_value(record: Record | None, metric: Metric) -> float | None:
    """Return a record's median time or peak allocation, if it was measured."""
    if record is None or record["status"] != "ok":
        return None
    value = record["median_s"] if metric == "time" else record["peak_alloc_bytes"]
    return None if value is None else float(value)


def format_metric(value: float | None, metric: Metric) -> str:
    """Format a time or memory value, or ``MISSING``."""
    if value is None:
        return MISSING
    return format_seconds(value) if metric == "time" else format_bytes(value)


def index_records(results: Results) -> dict[Key, Record]:
    """Map ``(version label, scenario, rows)`` to its record."""
    return {
        (record.get("version", ""), record["scenario"], record["rows"]): record
        for record in results["results"]
    }


def display_label(info: VersionInfo) -> str:
    """Return a version's label with its aliases, e.g. ``"base (= v0.10.0)"``."""
    if not info["aliases"]:
        return info["label"]
    return f"{info['label']} (= {', '.join(info['aliases'])})"


def reference_versions(results: Results) -> list[VersionInfo]:
    """Return what the target is compared with: the first compare, then the first baseline."""
    references: list[VersionInfo] = []
    for role in ("compare", "baseline"):
        found = next((v for v in results["versions"] if v["role"] == role), None)
        if found is not None:
            references.append(found)
    return references


def markdown_table(header: Sequence[str], body: Sequence[Sequence[str]]) -> str:
    """Return a Markdown table, first column left-aligned and the rest right-aligned."""
    separator = ["---", *(["---:"] * (len(header) - 1))]
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(separator) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in body]
    return "\n".join(lines)


def comparison_table(results: Results, rows: int, metric: Metric) -> str:
    """Compare every version on every scenario at one size.

    Args:
        results: The benchmark results.
        rows: The size to compare at.
        metric: ``"time"`` or ``"memory"``.

    Returns:
        A table with one column per version, then one change column per
        reference version (see ``reference_versions``), taken from the target.
    """
    index = index_records(results)
    versions = results["versions"]
    target = versions[0]["label"]
    references = reference_versions(results)
    header = [
        _METRIC_TITLES[metric],
        *(display_label(v) for v in versions),
        *(f"{DELTA} vs {r['label']}" for r in references),
    ]
    body = []
    for scenario in results["scenarios"]:
        values = {
            v["label"]: metric_value(index.get((v["label"], scenario, rows)), metric)
            for v in versions
        }
        body.append(
            [
                scenario,
                *(format_metric(values[v["label"]], metric) for v in versions),
                *(
                    format_change(relative_change(values[target], values[r["label"]]))
                    for r in references
                ),
            ]
        )
    return markdown_table(header, body)


def machine_description(results: Results) -> str:
    """Describe the benchmark machine, e.g. ``"a GitHub-hosted runner (CPU, 4 CPUs, 16 GiB RAM)"``."""
    machine = results["machine"]
    cpu_count, memory_bytes = machine["cpu_count"], machine["memory_bytes"]
    parts = [machine["cpu"]]
    if cpu_count:
        parts.append(f"{cpu_count} CPUs")
    if memory_bytes:
        parts.append(f"{format_bytes(memory_bytes)} RAM")
    description = ", ".join(parts)
    if machine["runner_image"]:
        return f"a GitHub-hosted runner ({description})"
    return description


def _short_commit(commit: str) -> str:
    return "working tree" if commit == WORKTREE else f"`{commit[:7]}`"


def environment_table(results: Results) -> str:
    """List what every version ran with."""
    header = ["Version", "Role", "Commit", "Python", "pandas", "numpy", "pysdmx"]
    body = [
        [
            display_label(info),
            info["role"],
            _short_commit(info["commit"]),
            *(
                info["deps"].get(name) or MISSING
                for name in ("python", "pandas", "numpy", "pysdmx")
            ),
        ]
        for info in results["versions"]
    ]
    return markdown_table(header, body)


def render_comparison(results: Results, run_url: str | None = None) -> str:
    """Return the job summary: every version compared at the largest size.

    Every other size follows inside a collapsed ``<details>`` block.

    Args:
        results: The benchmark results.
        run_url: The workflow run to link, if any.

    Returns:
        GitHub-flavoured Markdown.
    """
    target = results["versions"][0]
    largest = max(results["sizes"])
    source = f" in [this run]({run_url})" if run_url else ""
    by_size = [
        f"**{rows:,} rows**\n\n{comparison_table(results, rows, 'time')}"
        f"\n\n{comparison_table(results, rows, 'memory')}"
        for rows in results["sizes"]
    ]
    return "\n\n".join(
        [
            "### `map_structures` benchmark",
            f"{display_label(target)} at {_short_commit(target['commit'])}, measured "
            f"{results['generated_at']}{source} on {machine_description(results)}. "
            f"{DELTA} columns give the change from the named version: negative is "
            f"faster or smaller, and changes within {PLUS_MINUS}10% read "
            f'"{SAME}". {MISSING} marks a case that was not measured; the '
            "run's JSON says why.",
            f"**{largest:,} rows**",
            comparison_table(results, largest, "time"),
            comparison_table(results, largest, "memory"),
            "<details><summary>All sizes</summary>",
            *by_size,
            "</details>",
            environment_table(results),
        ]
    )


def render_pr_comment(results: Results, run_url: str | None = None) -> str:
    """Return the sticky PR comment: ``render_comparison`` behind ``COMMENT_MARKER``."""
    return f"{COMMENT_MARKER}\n{render_comparison(results, run_url)}"


def main(argv: Sequence[str] | None = None) -> int:
    """Render a results file in the requested format.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0. A missing results file ends the program through ``argparse``.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.report", description="Render benchmark results."
    )
    parser.add_argument("--format", required=True, choices=("summary", "pr-comment"))
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="file to write (default: stdout)")
    parser.add_argument("--run-url", help="the workflow run to link")
    args = parser.parse_args(argv)
    results = load_results(args.input)
    if results is None:
        parser.error(f"no results at {args.input}")
    if args.format == "summary":
        text = render_comparison(results, args.run_url)
    else:
        text = render_pr_comment(results, args.run_url)
    _write(text, args.output)
    return 0


def _write(text: str, output: Path | None) -> None:
    if output is None:
        # Bytes, not text: a Windows console's code page cannot encode the
        # em dash and approximately-equal signs.
        sys.stdout.buffer.write((text + "\n").encode("utf-8"))
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: all pass.

- [ ] **Step 7: Lint and type-check**

Run: `uv run python -m ruff check benchmarks tests && uv run python -m ruff format benchmarks tests && uv run python -m mypy`
Expected: no errors.

- [ ] **Step 8: Eyeball the summary from the smoke run**

Run: `uv run --no-project python -m benchmarks.report --format summary --input benchmarks/results/latest.json --output benchmarks/results/summary.md`
Expected: the file opens in a Markdown preview with readable tables:
- worktree, v0.10.0 and v0.9.0 columns;
- `Δ vs v0.10.0` only, since this run has no compare version.

- [ ] **Step 9: Commit**

```bash
git add benchmarks/report.py tests/fixtures/fxtr_benchmarks.py tests/conftest.py tests/test_benchmarks.py
git commit -F - <<'EOF'
chore(bench): render benchmark results as a job summary and PR comment

Compares every version at the largest size, with the change from the
base branch and the latest release, and folds all sizes into a
details block. The PR comment carries a marker so it can be edited
in place.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 5: Charts and the Performance page

**Files:**
- Create: `benchmarks/page.qmd`
- Modify: `benchmarks/report.py`, `tests/test_benchmarks.py`, `pyproject.toml` + `uv.lock` (matplotlib)

**Interfaces:**
- Consumes: everything `benchmarks.report` produced in Task 4.
- Produces in `benchmarks.report`:
  - `sizing_table(results, metric) -> str`, `headline(results) -> str`, `raw_data_link(results, repo_url=REPO_URL) -> str | None`.
  - `render_charts(results) -> str`.
  - `render_page(results: Results | None, template: str, charts: Callable[[Results], str] | None = None) -> str`.
  - `main` gains `--format page` and `--template`.

- [ ] **Step 1: Add matplotlib to the dev group**

Run: `uv add --group dev "matplotlib>=3.9"`
Then, in `pyproject.toml`, put this comment above the new `"matplotlib>=3.9",` line in `dev`:

```toml
    # Draws the Performance page's charts (benchmarks/report.py). In `dev`, not
    # `docs`, so CI's test job renders them too; docs builds sync `dev` anyway.
```

- [ ] **Step 2: Write the failing tests**

Add to the report import block at the top of `tests/test_benchmarks.py`: `RESULTS_MARKER, render_charts, render_page`. Also add:

```python
from tests.fixtures.fxtr_benchmarks import make_results

TEMPLATE = "---\ntitle: Performance\n---\n\n<!-- RESULTS -->\n\n## How it is measured\n"
```

Append:

```python
class TestRenderPage:
    def _page(self, results):
        return render_page(results, TEMPLATE, charts=lambda _: "CHARTS")

    def test_render_page_shows_placeholder_without_results(self):
        assert "No performance profile has been published yet" in render_page(
            None, TEMPLATE
        )

    def test_render_page_replaces_the_marker(self, benchmark_results):
        assert RESULTS_MARKER not in self._page(benchmark_results)

    def test_render_page_rejects_template_without_marker(self, benchmark_results):
        with pytest.raises(ValueError, match="has no <!-- RESULTS --> marker"):
            render_page(benchmark_results, "# Performance\n")

    def test_render_page_headline_gives_time_and_memory(self, benchmark_results):
        assert "in **1 s**, allocating **300 MiB**" in self._page(benchmark_results)

    def test_render_page_sizing_relates_memory_to_input(self, benchmark_results):
        assert "300 MiB (3.1× input)" in self._page(benchmark_results)

    def test_render_page_includes_charts(self, benchmark_results):
        assert "CHARTS" in self._page(benchmark_results)

    def test_render_page_links_raw_data_for_a_release(self, benchmark_results):
        benchmark_results["versions"][0]["label"] = "v0.11.0"
        assert "releases/download/v0.11.0/benchmarks-v0.11.0.json" in self._page(
            benchmark_results
        )

    def test_render_page_omits_raw_data_link_otherwise(self, benchmark_results):
        assert "releases/download" not in self._page(benchmark_results)

    def test_render_page_skips_comparison_for_a_single_version(self, benchmark_results):
        benchmark_results["versions"] = benchmark_results["versions"][:1]
        assert "## Compared with earlier versions" not in self._page(benchmark_results)


@pytest.fixture(scope="module")
def rendered_charts():
    # Module-scoped: drawing eight SVGs takes seconds, and the string is immutable.
    return render_charts(make_results())


class TestRenderCharts:
    def test_render_charts_draws_two_metrics_in_two_themes(self, rendered_charts):
        assert rendered_charts.count("<svg") == 4

    def test_render_charts_wraps_dark_variants(self, rendered_charts):
        assert rendered_charts.count("{.perf-chart-dark}") == 2

    def test_render_charts_scales_to_the_page_width(self, rendered_charts):
        assert rendered_charts.count('width="100%"') == 4

    def test_render_charts_drops_xml_prolog(self, rendered_charts):
        assert "<?xml" not in rendered_charts


class TestReportMainPage:
    def test_main_writes_placeholder_page_without_results(self, tmp_path):
        output = tmp_path / "index.qmd"
        report.main(
            [
                "--format",
                "page",
                "--input",
                str(tmp_path / "absent.json"),
                "--output",
                str(output),
            ]
        )
        assert "No performance profile has been published yet" in output.read_text(
            encoding="utf-8"
        )
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: collection error, `ImportError: cannot import name 'render_charts'`.

- [ ] **Step 4: Write `benchmarks/page.qmd`**

````markdown
---
title: "Performance"
description: "How long map_structures() takes and how much memory it needs, by dataset shape and size, compared with earlier releases."
---

```{=html}
<style>
/* One chart variant per theme. great-docs' toggle sets data-bs-theme on <html>;
   it does not use Quarto's .light-content / .dark-content classes. */
.perf-chart-dark { display: none; }
html[data-bs-theme="dark"] .perf-chart-light { display: none; }
html[data-bs-theme="dark"] .perf-chart-dark { display: block; }
</style>
```

How long does `map_structures()` take, and how much memory does it need, for a
dataset of a given shape and size? This page answers that for the latest
release. The benchmark ran on a GitHub-hosted runner when the release was cut,
and re-measured earlier releases in the same run on the same machine, so the
comparison is like for like.

<!-- RESULTS -->

## How it is measured

- **Four dataset shapes**, each generated with a fixed seed at 1,000 to
  1,000,000 rows (`benchmarks/scenarios.py` in the repository):
  - *typical*: an indicator table with six low-cardinality dimensions, mapped by
    three value maps, two implicit maps, two fixed values and one two-to-one
    multi-component map;
  - *high cardinality*: one column whose distinct values number a tenth of the
    rows, mapped through a 10,000-entry value map with a catch-all;
  - *regex heavy*: two columns where half the values miss the literal lookup and
    fall through to twenty regular expressions and a catch-all;
  - *wide*: twenty mapped columns.
- **Time** is the median of 3 to 7 calls, measured with `time.perf_counter`
  after a warm-up call.
- **Peak extra memory** is the most memory one call allocated beyond its
  inputs, output included, measured with `tracemalloc` in a separate call.
  - It counts NumPy buffers, pandas hash tables and Python objects.
  - It misses memory allocated outside Python's allocator hooks.
  - A job's peak is roughly the input plus this figure.
- **Each version is installed as users install it**, with the dependencies it
  declares, resolved when the benchmark ran.
  - Versions that require different pandas releases are measured with different
    pandas releases, so part of a difference between them can come from pandas.
  - The Environment table lists what each version ran with.
- **Noise.** Runs on shared hardware vary by a few percent, so changes within
  ±10% read "≈ same". Versions whose mapping code is unchanged differ by noise
  alone.
- **Cells marked "—" were not measured**, for one of three reasons:
  - a smaller size of the same shape took over a minute;
  - the call failed;
  - the call was too slow to trace its memory.
````

- [ ] **Step 5: Add the page and chart functions to `benchmarks/report.py`**

Add to the imports: `import io`, `import math`, `import re`, `from collections.abc import Callable, Sequence` (replacing the existing `Sequence` import) and `from dataclasses import dataclass`.

Add after `render_pr_comment`:

```python
_PLACEHOLDER: str = (
    "::: {.callout-note}\n"
    "No performance profile has been published yet. The first one arrives with\n"
    "the next release; `make bench` produces one locally.\n"
    ":::"
)


def sizing_table(results: Results, metric: Metric) -> str:
    """Show the target's time or memory for every scenario and size.

    Memory cells add the peak's multiple of the input size, which is what
    sizing a job needs: its memory is roughly the input plus the peak.
    """
    index = index_records(results)
    target = results["versions"][0]["label"]
    header = [_METRIC_TITLES[metric], *(f"{rows:,} rows" for rows in results["sizes"])]
    body = []
    for scenario in results["scenarios"]:
        cells = [scenario]
        for rows in results["sizes"]:
            record = index.get((target, scenario, rows))
            value = metric_value(record, metric)
            input_bytes = record["input_bytes"] if record is not None else None
            cell = format_metric(value, metric)
            if metric == "memory" and value is not None and input_bytes:
                cell += f" ({value / input_bytes:.1f}{TIMES} input)"
            cells.append(cell)
        body.append(cells)
    return markdown_table(header, body)


def headline(results: Results) -> str:
    """Return the page's opening line: the target on the headline scenario at the largest size."""
    target = results["versions"][0]
    rows = max(results["sizes"])
    record = index_records(results).get((target["label"], HEADLINE_SCENARIO, rows))
    measured = (
        f"Measured {results['generated_at'][:10]} on {machine_description(results)}."
    )
    seconds = metric_value(record, "time")
    peak = metric_value(record, "memory")
    input_bytes = record["input_bytes"] if record is not None else None
    if seconds is None or peak is None or not input_bytes:
        return f"**{display_label(target)}**. {measured}"
    return (
        f"**{display_label(target)}** maps the {HEADLINE_SCENARIO} indicator table "
        f"of {rows:,} rows in **{format_seconds(seconds)}**, allocating "
        f"**{format_bytes(peak)}** beyond its input "
        f"({peak / input_bytes:.1f}{TIMES} the input's size). {measured}"
    )


def raw_data_link(results: Results, repo_url: str = REPO_URL) -> str | None:
    """Link the results file attached to the target's Release, if the target is a release."""
    tag = results["versions"][0]["label"]
    if release_key(tag) is None:
        return None
    url = f"{repo_url}/releases/download/{tag}/benchmarks-{tag}.json"
    return f"The raw measurements are attached to the release: [benchmarks-{tag}.json]({url})."


def render_page(
    results: Results | None,
    template: str,
    charts: Callable[[Results], str] | None = None,
) -> str:
    """Return the Performance page: ``template`` with its results marker filled in.

    Args:
        results: The results to show, or ``None`` for the placeholder.
        template: The page template's text; must contain ``RESULTS_MARKER``.
        charts: Draws the charts; ``render_charts`` by default.

    Returns:
        The page as Quarto Markdown.

    Raises:
        ValueError: If ``template`` has no ``RESULTS_MARKER``.
    """
    if RESULTS_MARKER not in template:
        raise ValueError(f"The page template has no {RESULTS_MARKER} marker")
    if results is None:
        return template.replace(RESULTS_MARKER, _PLACEHOLDER)
    draw = render_charts if charts is None else charts
    largest = max(results["sizes"])
    sections = [
        headline(results),
        "## Sizing",
        "Median time per call, by dataset shape and size:",
        sizing_table(results, "time"),
        "Peak memory allocated per call beyond the input, and its multiple of the "
        "input's size:",
        sizing_table(results, "memory"),
        "## Scaling",
        draw(results),
    ]
    if reference_versions(results):
        sections += [
            "## Compared with earlier versions",
            f"At {largest:,} rows. {DELTA} columns give the change from the named "
            f"version: negative is faster or smaller, and changes within "
            f'{PLUS_MINUS}10% read "{SAME}".',
            comparison_table(results, largest, "time"),
            comparison_table(results, largest, "memory"),
        ]
    sections += [
        "## Environment",
        f"Measured on {machine_description(results)}.",
        environment_table(results),
    ]
    link = raw_data_link(results)
    if link is not None:
        sections.append(link)
    return template.replace(RESULTS_MARKER, "\n\n".join(sections))


@dataclass(frozen=True)
class _Theme:
    surface: str
    text: str
    muted: str
    grid: str
    accent: str
    context: tuple[str, ...]


# Surfaces, text and the accent come from the dataviz reference palette. Older
# versions are neutral context lines, darker for newer releases. Identity never
# rests on colour alone: every line also has its own marker and dash style, and
# the tables beside the charts carry the exact values.
_THEMES: dict[str, _Theme] = {
    "light": _Theme(
        surface="#fcfcfb",
        text="#0b0b0b",
        muted="#52514e",
        grid="#e4e3df",
        accent="#2a78d6",
        context=("#52514e", "#8a8984", "#b5b4ae"),
    ),
    "dark": _Theme(
        surface="#1a1a19",
        text="#f0efec",
        muted="#c3c2b7",
        grid="#3a3936",
        accent="#3987e5",
        context=("#c3c2b7", "#8f8e87", "#64635e"),
    ),
}
_MARKERS: tuple[str, ...] = ("o", "s", "^", "D", "v", "P")
_DASHES: tuple[str, ...] = ("-", "--", ":", "-.")
_METRICS: tuple[Metric, ...] = ("time", "memory")


def render_charts(results: Results) -> str:
    """Return the scaling charts, time then memory, each in a light and a dark variant.

    Each variant sits in a ``perf-chart-light`` or ``perf-chart-dark`` div; the
    page template's style block shows the one matching the site's theme.
    """
    blocks = []
    for metric in _METRICS:
        blocks.append(f"**{_METRIC_TITLES[metric]} per call**, against rows:")
        for mode, theme in _THEMES.items():
            svg = _chart_svg(results, metric, theme)
            blocks.append(f"::: {{.perf-chart-{mode}}}\n```{{=html}}\n{svg}\n```\n:::")
    return "\n\n".join(blocks)


@dataclass(frozen=True)
class _Style:
    color: str
    linewidth: float
    marker: str
    linestyle: str
    zorder: int


def _series_style(theme: _Theme, position: int) -> _Style:
    """Return the target's accent style for position 0, a context style after it."""
    if position == 0:
        return _Style(theme.accent, 2.5, "o", "-", 3)
    return _Style(
        theme.context[min(position, len(theme.context)) - 1],
        1.5,
        _MARKERS[position % len(_MARKERS)],
        _DASHES[position % len(_DASHES)],
        2,
    )


def _plot_panel(
    ax: "Axes", results: Results, scenario: str, metric: Metric, theme: _Theme
) -> None:
    """Draw one scenario's panel: one line per version, log-log."""
    index = index_records(results)
    sizes = sorted(results["sizes"])
    scale = 1.0 if metric == "time" else 1 / 2**20
    for position, info in enumerate(results["versions"]):
        values = [
            metric_value(index.get((info["label"], scenario, rows)), metric)
            for rows in sizes
        ]
        style = _series_style(theme, position)
        ax.plot(
            sizes,
            [math.nan if v is None else v * scale for v in values],
            label=display_label(info),
            color=style.color,
            linewidth=style.linewidth,
            marker=style.marker,
            markersize=6,
            linestyle=style.linestyle,
            zorder=style.zorder,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_facecolor(theme.surface)
    ax.set_title(scenario.replace("_", " "), loc="left", color=theme.text, fontsize=11)
    ax.grid(visible=True, which="major", color=theme.grid, linewidth=0.8)
    ax.tick_params(colors=theme.muted, labelsize=9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(theme.grid)


def _chart_svg(results: Results, metric: Metric, theme: _Theme) -> str:
    """Draw one metric as small multiples, one panel per scenario, as inline SVG."""
    from matplotlib import rc_context
    from matplotlib.figure import Figure

    scenarios = results["scenarios"]
    nrows = math.ceil(len(scenarios) / 2)
    figure = Figure(
        figsize=(8, 3.2 * nrows + 0.6), layout="constrained", facecolor=theme.surface
    )
    axes = figure.subplots(nrows, 2, sharex=True, sharey=True, squeeze=False)
    for ax, scenario in zip(axes.flat, scenarios, strict=False):
        _plot_panel(ax, results, scenario, metric, theme)
    for ax in axes.flat[len(scenarios) :]:
        ax.set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel("Rows", color=theme.muted)
    for ax in axes[:, 0]:
        ax.set_ylabel("Seconds" if metric == "time" else "MiB", color=theme.muted)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    figure.legend(
        handles,
        labels,
        loc="outside upper center",
        ncols=len(labels),
        frameon=False,
        labelcolor=theme.text,
    )
    buffer = io.StringIO()
    # Text as text, not glyph paths: smaller, selectable, and in the page's font.
    with rc_context({"svg.fonttype": "none", "font.family": "sans-serif"}):
        figure.savefig(
            buffer, format="svg", facecolor=theme.surface, metadata={"Date": None}
        )
    svg = buffer.getvalue()
    # Drop the XML declaration and DOCTYPE, which are invalid inside HTML, and
    # let the viewBox scale the chart to the column instead of a fixed pt size.
    svg = svg[svg.index("<svg") :]
    return re.sub(r'width="[\d.]+pt" height="[\d.]+pt"', 'width="100%"', svg, count=1)
```

For the `"Axes"` annotation, add this to the imports at the top of `report.py`. matplotlib itself is still imported only when a chart is drawn:

```python
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from matplotlib.axes import Axes
```

Replace `main` with:

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Render a results file in the requested format.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0. A missing results file is fine for ``page`` (it renders the
        placeholder) and ends the program through ``argparse`` otherwise.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.report", description="Render benchmark results."
    )
    parser.add_argument(
        "--format", required=True, choices=("page", "summary", "pr-comment")
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="file to write (default: stdout)")
    parser.add_argument("--template", type=Path, default=TEMPLATE)
    parser.add_argument("--run-url", help="the workflow run to link")
    args = parser.parse_args(argv)
    results = load_results(args.input)
    if args.format == "page":
        text = render_page(results, args.template.read_text(encoding="utf-8"))
    elif results is None:
        parser.error(f"no results at {args.input}")
    elif args.format == "summary":
        text = render_comparison(results, args.run_url)
    else:
        text = render_pr_comment(results, args.run_url)
    _write(text, args.output)
    return 0
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run python -m pytest tests/test_benchmarks.py -q`
Expected: all pass. A matplotlib `UserWarning` raised as an error usually means a panel had no positive data. The fixture gives every panel data, so treat it as a bug in `_plot_panel`.

- [ ] **Step 7: Lint and type-check**

Run: `uv run python -m ruff check benchmarks tests && uv run python -m ruff format benchmarks tests && uv run python -m mypy`
Expected: no errors (see the note in Step 5 about the `Axes` narrowing).

- [ ] **Step 8: Look at the charts**

Run: `uv run python -m benchmarks.report --format page --input benchmarks/results/latest.json --output benchmarks/results/page.qmd`

Open the file and copy one light SVG block into `benchmarks/results/chart.html` inside `<html><body>…</body></html>`. Open it in a browser, then check the dark one the same way on a dark page background. Check for:
- legend overlap;
- labels clipping;
- a readable accent line;
- context lines told apart by marker and dash.

Fix what you see before committing.

- [ ] **Step 9: Commit**

```bash
git add benchmarks/page.qmd benchmarks/report.py tests/test_benchmarks.py pyproject.toml uv.lock
git commit -F - <<'EOF'
docs(performance): render the Performance page with scaling charts

Sizing tables, a comparison with earlier versions, and log-log small
multiples drawn in a light and a dark variant that follow the
great-docs theme toggle. Without results the page shows a placeholder.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 6: Wire the page into the docs build

**Files:**
- Modify: `Makefile`, `great-docs.yml`, `.gitignore`

**Interfaces:**
- Consumes: `python -m benchmarks.run` (Task 3) and `python -m benchmarks.report --format page` (Task 5).
- Produces:
  - `make bench`, `make performance-page`.
  - `make docs` and `make docs-preview` render the page first, from `PERF_RESULTS` (default `benchmarks/results/latest.json`).

- [ ] **Step 1: Update the Makefile**

Add `bench performance-page` to `.PHONY`. Replace the `docs:` and `docs-preview:` recipes with:

```make
# The results the Performance page renders from: what `make bench` writes
# locally, and what docs.yml downloads from the newest release that has one.
PERF_RESULTS ?= benchmarks/results/latest.json

performance-page: ## Render the Performance page from PERF_RESULTS (a placeholder if absent)
	uv run python -m benchmarks.report --format page --input $(PERF_RESULTS) \
		--output performance/index.qmd

docs: performance-page ## Build the documentation site
	uv run --group docs great-docs build

docs-preview: performance-page ## Serve the documentation site locally with live reload
	uv run --group docs great-docs preview
```

After the `release-dry` target, add:

```make
# Extra arguments for `make bench`, e.g. BENCH_ARGS="--sizes 1000 10000 --baselines 1".
BENCH_ARGS ?=

# --no-project: the orchestrator is standard library only, and each version
# under test gets a venv of its own under .bench/.
bench: ## Benchmark map_structures: the working tree against the last 3 releases
	uv run --no-project python -m benchmarks.run --target WORKTREE --baselines 3 \
		--out $(PERF_RESULTS) $(BENCH_ARGS)
```

- [ ] **Step 2: Add the section to `great-docs.yml`**

In `nav_icons.navbar`, add `Performance: gauge` after `Reference: code`. After the `changelog:` block, add:

```yaml
# Performance profile. performance/index.qmd is generated (and gitignored):
# `make docs` renders it from benchmarks/page.qmd and the newest benchmark
# results — see benchmarks/report.py and docs.yml's fetch step.
sections:
  - title: Performance
    dir: performance
    navbar_after: Reference
```

- [ ] **Step 3: Ignore the generated page**

In `.gitignore`, under the benchmark lines added in Task 3, add:

```gitignore
# Generated by `make docs` from benchmarks/page.qmd
performance/
```

- [ ] **Step 4: Build the docs and look at the page**

Run (Git Bash; on Windows great-docs needs UTF-8 mode): `PYTHONUTF8=1 make docs`

Expected:
- the build log lists `📂 Performance: 1 post(s) from performance/`;
- `great-docs/_site/performance/index.html` exists.

Then run `PYTHONUTF8=1 make docs-preview`, open the Performance page, and check:
- the navbar shows Performance after Reference;
- the tables render;
- the dark-mode toggle swaps the chart variants;
- at phone width the charts shrink with the column and the wide tables scroll inside their own box, not the page.

- [ ] **Step 5: Check the placeholder path**

Run: `PYTHONUTF8=1 make performance-page PERF_RESULTS=benchmarks/results/none.json && grep -c "No performance profile" performance/index.qmd`
Expected: `1`.

- [ ] **Step 6: Commit**

```bash
git add Makefile great-docs.yml .gitignore
git commit -F - <<'EOF'
docs(performance): add the Performance section to the docs site

make docs renders performance/index.qmd from the newest benchmark
results before building; make bench produces them locally.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 7: CI workflows

**Files:**
- Create: `.github/workflows/benchmarks.yml`, `.github/workflows/benchmarks-pr.yml`
- Modify: `.github/workflows/release.yml`, `.github/workflows/docs.yml`

**Interfaces:**
- Consumes: `python -m benchmarks.run` and `python -m benchmarks.report` CLIs; `make docs` (Task 6).
- Produces:
  - Artifact `benchmark-results` (`benchmarks-results.json`, `pr-comment.md`) from every `benchmarks.yml` run.
  - Release asset `benchmarks-<tag>.json`.
  - PR comment marked `<!-- tidysdmx-benchmark -->`.

- [ ] **Step 1: Create `.github/workflows/benchmarks.yml`**

```yaml
name: Benchmarks

# Measures map_structures for one ref against named refs and earlier releases,
# and stores the results as the `benchmark-results` artifact. It only reads:
# release.yml attaches the JSON to a Release and benchmarks-pr.yml posts the PR
# comment, each with the one write permission it needs. They live in the
# callers because a called workflow's jobs cannot ask for more than the caller
# grants, even when they would be skipped.
on:
  workflow_call:
    inputs:
      ref:
        description: Git ref (or commit) to profile
        type: string
        required: true
      target-label:
        description: Label for the ref in the results (default the ref)
        type: string
        default: ""
      compare:
        description: Another ref to measure alongside, as LABEL=REF or REF
        type: string
        default: ""
      baselines:
        description: How many earlier releases to measure
        type: number
        default: 3
      sizes:
        description: Row counts, space-separated
        type: string
        default: "1000 10000 100000 1000000"
  workflow_dispatch:
    inputs:
      ref:
        description: Git ref (or commit) to profile
        type: string
        required: true
      target-label:
        description: Label for the ref in the results (default the ref)
        type: string
        default: ""
      compare:
        description: Another ref to measure alongside, as LABEL=REF or REF
        type: string
        default: ""
      baselines:
        description: How many earlier releases to measure
        type: number
        default: 3
      sizes:
        description: Row counts, space-separated
        type: string
        default: "1000 10000 100000 1000000"

permissions:
  contents: read

jobs:
  run:
    name: Run benchmarks
    runs-on: ubuntu-latest
    # A full grid over four versions; v0.8.0's row-wise multi-map path alone
    # can take minutes at 1M rows.
    timeout-minutes: 75
    steps:
      # The harness comes from this workflow's own commit; the versions under
      # test are exported from git by ref, so every tag and branch must be here.
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Install uv
        uses: astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d # v10.0.1
        with:
          enable-cache: true
          cache-dependency-glob: uv.lock
          python-version: "3.11"
      - name: Benchmark
        env:
          REF: ${{ inputs.ref }}
          TARGET_LABEL: ${{ inputs.target-label }}
          COMPARE: ${{ inputs.compare }}
          BASELINES: ${{ inputs.baselines }}
          SIZES: ${{ inputs.sizes }}
        run: |
          args=(--target "$REF" --baselines "$BASELINES" --out benchmarks-results.json)
          if [ -n "$TARGET_LABEL" ]; then args+=(--target-label "$TARGET_LABEL"); fi
          if [ -n "$COMPARE" ]; then args+=(--compare "$COMPARE"); fi
          read -r -a sizes <<< "$SIZES"
          uv run --no-project python -m benchmarks.run "${args[@]}" --sizes "${sizes[@]}"
      - name: Report
        if: ${{ !cancelled() && hashFiles('benchmarks-results.json') != '' }}
        env:
          RUN_URL: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
        run: |
          uv run --no-project python -m benchmarks.report --format summary \
            --input benchmarks-results.json --run-url "$RUN_URL" >> "$GITHUB_STEP_SUMMARY"
          uv run --no-project python -m benchmarks.report --format pr-comment \
            --input benchmarks-results.json --run-url "$RUN_URL" --output pr-comment.md
      - name: Store results
        if: ${{ !cancelled() && hashFiles('benchmarks-results.json') != '' }}
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: benchmark-results
          path: |
            benchmarks-results.json
            pr-comment.md
          if-no-files-found: error
```

- [ ] **Step 2: Create `.github/workflows/benchmarks-pr.yml`**

```yaml
name: Benchmarks (PR)

# Benchmarks a pull request's head against its base branch and the latest
# release, and keeps one comment on the PR up to date with the result. Runs
# when the PR touches what map_structures' speed depends on, or carries the
# `benchmark` label.
on:
  pull_request:
    branches: [main, dev]
    types: [opened, synchronize, reopened, ready_for_review, labeled]

permissions:
  contents: read

concurrency:
  # One benchmark per PR: a new push cancels the run on the superseded head.
  # Adding any other label must not cancel a run in flight, so such an event
  # gets a group of its own (and then decides not to run).
  group: >-
    benchmarks-pr-${{ github.event.pull_request.number }}-${{
    github.event.action == 'labeled' && github.event.label.name != 'benchmark'
    && github.run_id || 'run' }}
  cancel-in-progress: true

jobs:
  decide:
    name: Decide whether to benchmark
    # Same trust model as pr-review.yml: fork PRs and drafts are skipped.
    if: >-
      github.event.pull_request.draft == false
      && github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    timeout-minutes: 5
    outputs:
      run: ${{ steps.decide.outputs.run }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0
          persist-credentials: false
      # A job, not an `on.pull_request.paths` filter: that filter would also
      # drop the `labeled` event, so the label could never force a run.
      - name: Check the label and the changed paths
        id: decide
        env:
          ACTION: ${{ github.event.action }}
          ADDED_LABEL: ${{ github.event.label.name }}
          HAS_LABEL: ${{ contains(github.event.pull_request.labels.*.name, 'benchmark') }}
          BASE_REF: ${{ github.event.pull_request.base.ref }}
        run: |
          if [ "$ACTION" = "labeled" ]; then
            if [ "$ADDED_LABEL" = "benchmark" ]; then run=true; else run=false; fi
          elif [ "$HAS_LABEL" = "true" ]; then
            run=true
          elif git diff --quiet "origin/$BASE_REF...HEAD" -- \
              src/tidysdmx/mapping.py benchmarks/ uv.lock; then
            run=false
          else
            run=true
          fi
          echo "run=$run" >> "$GITHUB_OUTPUT"
          echo "Benchmark this PR: $run"

  benchmark:
    name: Benchmark
    needs: decide
    if: needs.decide.outputs.run == 'true'
    uses: $/.github/workflows/benchmarks.yml
    with:
      ref: ${{ github.event.pull_request.head.sha }}
      target-label: head
      compare: base=origin/${{ github.event.pull_request.base.ref }}
      baselines: 1
    permissions:
      contents: read

  comment:
    name: Post the benchmark comment
    needs: benchmark
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      pull-requests: write # create or edit the PR comment
    # No checkout: this job holds a write token, so it never runs PR code. It
    # only posts the Markdown the read-only benchmark job rendered.
    steps:
      - name: Download results
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        with:
          name: benchmark-results
          path: results
      - name: Create or update the comment
        env:
          GH_TOKEN: ${{ github.token }}
          GH_REPO: ${{ github.repository }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
        run: |
          marker='<!-- tidysdmx-benchmark -->'
          id=$(gh api --paginate "repos/$GH_REPO/issues/$PR_NUMBER/comments" \
            --jq ".[] | select(.body | startswith(\"$marker\")) | .id" | head -n 1)
          if [ -n "$id" ]; then
            gh api --method PATCH "repos/$GH_REPO/issues/comments/$id" \
              -F body=@results/pr-comment.md > /dev/null
          else
            gh api --method POST "repos/$GH_REPO/issues/$PR_NUMBER/comments" \
              -F body=@results/pr-comment.md > /dev/null
          fi
```

- [ ] **Step 3: Add the benchmark to `release.yml`**

Insert these two jobs between `publish-pypi` and the comment block above `deploy-docs`:

```yaml
  # Benchmark the release against the previous three on one runner, and attach
  # the results to the GitHub Release; the docs redeploy below renders them.
  # Not on the critical path: deploy-docs runs whether or not these succeed.
  benchmark:
    name: Benchmark the release
    needs: [release, build]
    if: needs.release.outputs.released == 'true' && inputs.noop != true
    uses: $/.github/workflows/benchmarks.yml
    with:
      ref: ${{ needs.release.outputs.tag }}
      baselines: 3
    permissions:
      contents: read

  attach-benchmark:
    name: Attach benchmark results to the release
    needs: [release, benchmark]
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: write # upload the results to the GitHub Release
    steps:
      - name: Download results
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        with:
          name: benchmark-results
          path: results
      - name: Upload to the release
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          GH_REPO: ${{ github.repository }}
          TAG: ${{ needs.release.outputs.tag }}
        run: |
          cp results/benchmarks-results.json "benchmarks-$TAG.json"
          gh release upload "$TAG" "benchmarks-$TAG.json" --clobber
```

In `deploy-docs`, change `needs:` and `if:` to:

```yaml
    needs: [release, build, publish-pypi, attach-benchmark]
    # !cancelled(): wait for the benchmark, so the site carries the new
    # release's performance profile, but never depend on it. If the benchmark
    # failed, the Performance page shows the newest profile a release has.
    if: >-
      !cancelled()
      && needs.publish-pypi.result == 'success'
      && needs.release.outputs.released == 'true'
      && inputs.noop != true
```

At the end of the comment block above `deploy-docs`, add:

```yaml
  #
  # The benchmark jobs it waits for are not in the docs concurrency group, so
  # they do not widen that window; they only delay the redeploy.
```

- [ ] **Step 4: Fetch the newest results in `docs.yml`**

In `docs.yml`'s `build` job, insert between `Show Quarto version` and `Build docs`:

```yaml
      # The Performance page renders from the newest release that carries
      # benchmark results (release.yml attaches them). For a release redeploy
      # that is the new release, unless its benchmark failed; without any, the
      # page shows a placeholder and the build goes on.
      - name: Fetch the latest performance profile
        env:
          GH_TOKEN: ${{ github.token }}
          GH_REPO: ${{ github.repository }}
        run: |
          mkdir -p benchmarks/results
          for tag in $(gh release list --limit 20 --json tagName --jq '.[].tagName'); do
            if gh release download "$tag" --pattern 'benchmarks-*.json' \
                --output benchmarks/results/latest.json --clobber 2>/dev/null; then
              echo "Performance profile: $tag"
              exit 0
            fi
          done
          echo "::notice::No release carries benchmark results yet; the Performance page shows a placeholder."
```

- [ ] **Step 5: Lint the workflows**

Run: `uvx zizmor .github/workflows/`

Expected: no new findings in the four files. If a security policy blocks `uvx`, run `uv run --with zizmor python -m zizmor .github/workflows/`. If neither runs, note it in the PR: `security.yml` runs zizmor on the PR. Also check YAML syntax with `uv run python -c "import yaml,sys; [yaml.safe_load(open(f)) for f in sys.argv[1:]]" .github/workflows/benchmarks.yml .github/workflows/benchmarks-pr.yml .github/workflows/release.yml .github/workflows/docs.yml`. If PyYAML is not installed, add `--with pyyaml`.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/benchmarks.yml .github/workflows/benchmarks-pr.yml .github/workflows/release.yml .github/workflows/docs.yml
git commit -F - <<'EOF'
ci(bench): benchmark releases and pull requests

benchmarks.yml measures read-only. release.yml attaches the results to
the Release before the docs redeploy, which never depends on them.
benchmarks-pr.yml keeps one comment on a PR that touches the mapping
code, uv.lock or the harness, or carries the `benchmark` label.
docs.yml renders the Performance page from the newest release's results.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 8: Contributor documentation

**Files:**
- Modify: `CLAUDE.md`, `CONTRIBUTING.md`, `RELEASING.md`

- [ ] **Step 1: `CLAUDE.md`**

- **Key Commands:** after the `make release-dry` line, add:

  ```markdown
  - `make bench` — benchmark `map_structures`: working tree vs the last 3 releases
    (`BENCH_ARGS="--sizes 1000 10000 --baselines 1"` for a quick run)
  ```

- **Repository Layout:** after the `tests/` block, add:

  ```
  benchmarks/                 — map_structures benchmark harness, not shipped: scenarios,
                                per-case runner, multi-version orchestrator, report renderer
  performance/                — generated by `make docs` from benchmarks/page.qmd (gitignored)
  ```

- **CI/CD:** add two bullets after `pr-review.yml`:

  ```markdown
  - **`benchmarks.yml`** — measures `map_structures` for a ref against earlier releases,
    read-only, and stores the results as an artifact. Called by `release.yml` and
    `benchmarks-pr.yml`, or run by hand (`workflow_dispatch`).
  - **`benchmarks-pr.yml`** — on PRs touching `src/tidysdmx/mapping.py`, `benchmarks/` or
    `uv.lock` (or labelled `benchmark`), benchmarks head vs base vs the latest release and
    keeps one PR comment up to date.
  ```

  Extend the `release.yml` bullet with: "It also benchmarks the release against the previous three and attaches `benchmarks-vX.Y.Z.json` to the Release; the docs redeploy renders it on the Performance page and does not depend on it."

- [ ] **Step 2: `CONTRIBUTING.md`**

- **Everyday commands table:** after the `make docs` row, add:

  ```markdown
  | `make bench` | benchmark `map_structures`: the working tree vs the last 3 releases |
  ```

- **Pull requests:** at the end of the section, before `## Code of Conduct`, add:

  ```markdown
  ### Benchmarks on pull requests

  A PR that touches `src/tidysdmx/mapping.py`, `benchmarks/` or `uv.lock` gets a
  benchmark comment comparing its head with the base branch and the latest
  release on the same runner. Each push updates it. To benchmark a PR outside
  those paths, add the `benchmark` label. The full grid takes a while, and the
  check is not required for merging.

  To measure locally, `make bench` compares your working tree with the last
  three releases and `make docs-preview` shows the result on the Performance
  page. Laptop numbers describe the laptop; compare them only with each other.
  ```

- [ ] **Step 3: `RELEASING.md`**

- **How it works**, step 3: after the PyPI bullet, add:

  ```markdown
     - benchmarks `map_structures` against the previous three releases and attaches
       `benchmarks-vX.Y.Z.json` to the GitHub Release (non-blocking);
     - redeploys the docs, whose Performance page renders those results.
  ```

- **Cutting a release**, the verify list in step 4: add:

  ```markdown
     - `benchmarks-vX.Y.Z.json` is attached and the Performance page shows the new
       version. If the *Benchmark the release* job failed, re-run it from the
       Actions page; re-running also re-runs the attach and docs jobs after it.
  ```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md CONTRIBUTING.md RELEASING.md
git commit -F - <<'EOF'
docs: document the benchmarks, their workflows and the release step

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 9: Verify, measure the full grid, open the PR

- [ ] **Step 1: Full check**

Run: `make check`
Expected: lint, mypy and the gated tests pass. The coverage gate measures `src/` only, so the new code doesn't move it.

- [ ] **Step 2: Full-grid local run to measure duration**

Run: `time make bench`
Expected:
- it exits 0;
- the JSON has 4 versions × 4 scenarios × 4 sizes = 64 records.

Note the wall time. A laptop is not the CI runner, but over ~50 minutes here is a warning sign for the 75-minute CI timeout. In that case, reduce the 1M-row repeats in `repeats_for` (3 → 2) or `CASE_TIMEOUT_S`, and say so in the PR.

- [ ] **Step 3: Docs build from real results**

Run: `PYTHONUTF8=1 make docs`, then `make docs-preview`. Read the Performance page as a user would: does the headline answer "how long and how much memory for N rows"?

- [ ] **Step 4: Ask the user before anything outward-facing**

Ask for confirmation, then do only what they approve:
1. `git push -u origin ci/performance-profile`.
2. Create the label: `gh label create benchmark --description "Run the map_structures benchmark on this PR" --color 1D76DB`.
3. Open the PR to `dev`, titled `ci(bench): publish a performance profile with each release`. The body ends with the attribution line from the session.

- [ ] **Step 5: Verify on the PR**

Check that:
- `Benchmarks (PR)` runs, because the PR touches `benchmarks/`;
- the comment appears with `head`, `base` and `v0.10.0` columns;
- a second push edits the same comment instead of adding one;
- the `comment` job's log shows no checkout step;
- `security.yml`'s zizmor job is green;
- `docs.yml`'s build log shows the placeholder notice, since no release has results yet.

- [ ] **Step 6: Hand back**

Report:
- the measured local duration;
- the CI run duration from the PR;
- anything that needed adjusting.

Remind the user that the first published profile arrives with the next `dev` → `main` release. After this merges, merging `dev` into `perf/map-structures-large-data` gives that PR its own before/after comment.
