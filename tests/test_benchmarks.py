"""Tests for the benchmarks/ harness (not part of the tidysdmx package)."""

import json
import platform
import sys
from pathlib import Path

import pandas as pd
import pytest
from pysdmx.model.map import ComponentMap, FixedValueMap, MultiComponentMap

from benchmarks import case, probe, run
from benchmarks.case import repeats_for, run_case
from benchmarks.records import SCENARIOS, empty_record, release_key
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
from benchmarks.scenarios import build
from tidysdmx import map_structures

ROOT = Path(__file__).resolve().parents[1]
TAGS = ["v0.8.0", "v0.10.0", "v0.9.0", "v0.7.5", "not-a-release", "v1.0.0-rc.1"]


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
        assert (record["median_s"], record["samples_s"]) == (None, [])


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


def _version(label, commit=None, role="baseline"):
    return Version(label=label, ref=label, role=role, commit=commit or f"sha-{label}")


def _ok(median, scenario="typical", rows=1):
    record = empty_record(scenario, rows, "error", "")
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
            return _ok(run.BUDGET_S + 1 if scenario == "typical" else 0.1, scenario)

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
