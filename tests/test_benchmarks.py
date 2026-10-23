"""Tests for the benchmarks/ harness (not part of the tidysdmx package)."""

import json
import platform
import sys

import pandas as pd
import pytest
from pysdmx.model.map import ComponentMap, FixedValueMap, MultiComponentMap

from benchmarks import case, probe
from benchmarks.case import repeats_for, run_case
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
