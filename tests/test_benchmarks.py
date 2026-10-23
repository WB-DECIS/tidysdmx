"""Tests for the benchmarks/ harness (not part of the tidysdmx package)."""

import json
import logging
import platform
import sys
import tarfile
from pathlib import Path

import pandas as pd
import pytest
from pysdmx.model.map import ComponentMap, FixedValueMap, MultiComponentMap

from benchmarks import case, probe, report, run
from benchmarks.case import repeats_for, run_case
from benchmarks.records import SCENARIOS, empty_record, release_key
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
from benchmarks.run import (
    Version,
    exceeds_budget,
    install_versions,
    measure,
    merge_same_commit,
    parse_named_ref,
    plan_versions,
    rotated,
    run_grid,
    select_baselines,
    target_measured,
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


STUB_CASE = """\
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--scenario", required=True)
parser.add_argument("--rows", type=int, required=True)
parser.add_argument("--result", type=Path, required=True)
args = parser.parse_args()
record = {
    "scenario": args.scenario,
    "rows": args.rows,
    "status": "ok",
    "reason": None,
    "median_s": 0.5,
    "samples_s": [0.5],
}
args.result.write_text(json.dumps(record), encoding="utf-8")
"""


@pytest.fixture
def stub_harness(tmp_path):
    """A directory whose `benchmarks.case` writes a known record, stdlib only."""
    root = tmp_path / "stub"
    package = root / "benchmarks"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "case.py").write_text(STUB_CASE, encoding="utf-8")
    return root


class TestMeasure:
    def test_measure_returns_the_record_the_case_wrote(self, stub_harness, tmp_path):
        record = measure(Path(sys.executable), stub_harness, tmp_path, "typical", 100)
        assert record["median_s"] == 0.5

    def test_measure_passes_the_scenario_and_rows_to_the_case(
        self, stub_harness, tmp_path
    ):
        record = measure(Path(sys.executable), stub_harness, tmp_path, "wide", 7)
        assert (record["scenario"], record["rows"]) == ("wide", 7)

    def test_measure_ignores_a_result_left_by_an_earlier_case(self, tmp_path):
        (tmp_path / "case.json").write_text('{"median_s": 9.9}', encoding="utf-8")
        # tmp_path has no benchmarks package, so this case writes nothing.
        record = measure(Path(sys.executable), tmp_path, tmp_path, "typical", 100)
        assert record["median_s"] is None

    def test_measure_reports_a_case_that_wrote_nothing(self, tmp_path):
        # From tmp_path, `python -m benchmarks.case` cannot even be imported.
        record = measure(Path(sys.executable), tmp_path, tmp_path, "typical", 100)
        assert record["reason"].startswith("exited 1 without a result")

    def test_measure_reports_a_timeout(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run, "CASE_TIMEOUT_S", 0.001)
        record = measure(Path(sys.executable), ROOT, tmp_path, "typical", 100)
        assert record["reason"].startswith("timed out")


def _installer(failing=None):
    """Return an install function that fails for the labels in ``failing``."""
    failing = failing or {}

    def install(version):
        if version.label in failing:
            raise failing[version.label]
        return Path("py") / version.label, {"pandas": "2.3.3"}

    return install


class TestInstallVersions:
    def test_install_versions_maps_each_label_to_its_python(self):
        pythons, _ = install_versions(
            [_version("head", role="target"), _version("v0.9.0")], _installer()
        )
        assert pythons == {"head": Path("py/head"), "v0.9.0": Path("py/v0.9.0")}

    def test_install_versions_describes_each_version(self):
        version = Version("head", "origin/dev", "target", "abc123", aliases=("tip",))
        _, infos = install_versions([version], _installer())
        assert infos == [
            {
                "label": "head",
                "ref": "origin/dev",
                "commit": "abc123",
                "role": "target",
                "aliases": ["tip"],
                "deps": {"pandas": "2.3.3"},
            }
        ]

    def test_install_versions_raises_when_the_target_fails(self):
        versions = [_version("head", role="target"), _version("v0.9.0")]
        failing = {"head": RuntimeError("uv exploded")}
        with pytest.raises(RuntimeError, match="uv exploded"):
            install_versions(versions, _installer(failing))

    def test_install_versions_raises_when_the_target_tool_is_missing(self):
        failing = {"head": FileNotFoundError("uv")}
        with pytest.raises(FileNotFoundError, match="uv"):
            install_versions([_version("head", role="target")], _installer(failing))

    @pytest.mark.parametrize(
        "error",
        [RuntimeError("boom"), OSError("boom"), tarfile.TarError("boom")],
        ids=["RuntimeError", "OSError", "TarError"],
    )
    def test_install_versions_skips_a_baseline_that_fails(self, error):
        versions = [
            _version("head", role="target"),
            _version("v0.10.0"),
            _version("v0.9.0"),
        ]
        pythons, _ = install_versions(versions, _installer({"v0.10.0": error}))
        assert list(pythons) == ["head", "v0.9.0"]

    def test_install_versions_leaves_a_failed_baseline_out_of_the_infos(self):
        versions = [_version("head", role="target"), _version("v0.9.0")]
        failing = {"v0.9.0": RuntimeError("no wheel")}
        _, infos = install_versions(versions, _installer(failing))
        assert [info["label"] for info in infos] == ["head"]

    def test_install_versions_logs_the_baseline_it_skips(self, caplog):
        versions = [_version("head", role="target"), _version("v0.9.0")]
        failing = {"v0.9.0": RuntimeError("no wheel")}
        with caplog.at_level(logging.ERROR, logger="benchmarks.run"):
            install_versions(versions, _installer(failing))
        assert "Skipping v0.9.0" in caplog.text

    def test_install_versions_lets_a_programming_error_through(self):
        versions = [_version("head", role="target"), _version("v0.9.0")]
        with pytest.raises(KeyError, match="oops"):
            install_versions(versions, _installer({"v0.9.0": KeyError("oops")}))


class TestTargetMeasured:
    @staticmethod
    def _record(version, status):
        record = empty_record("typical", 1, status, "")
        record["version"] = version
        return record

    def test_target_measured_is_true_with_an_ok_record(self):
        records = [self._record("head", "error"), self._record("head", "ok")]
        assert target_measured(records, "head")

    def test_target_measured_is_false_when_only_another_version_is_ok(self):
        assert not target_measured([self._record("v0.9.0", "ok")], "head")

    @pytest.mark.parametrize("status", ["error", "skipped"])
    def test_target_measured_is_false_without_an_ok_record(self, status):
        assert not target_measured([self._record("head", status)], "head")

    def test_target_measured_is_false_for_no_records(self):
        assert not target_measured([], "head")


class TestMain:
    """`main` with git, uv and the cases replaced: only the wiring is real."""

    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        versions = [
            _version("head", role="target"),
            _version("v0.10.0"),
            _version("v0.9.0"),
        ]
        monkeypatch.setattr(run, "_run", lambda command, cwd=None: str(tmp_path))
        monkeypatch.setattr(run, "plan_versions", lambda *args: versions)
        monkeypatch.setattr(run, "export_source", lambda root, v, work: tmp_path)
        monkeypatch.setattr(run, "create_env", lambda v, source, work: Path(v.label))
        monkeypatch.setattr(run, "installed_versions", lambda python, harness: {})
        monkeypatch.setattr(run, "measure", self._measure_ok)
        out = tmp_path / "out" / "results.json"
        argv = [
            "--target",
            "head",
            "--sizes",
            "10",
            "--scenarios",
            "typical",
            "--work-dir",
            str(tmp_path / "work"),
            "--out",
            str(out),
        ]
        return argv, out

    @staticmethod
    def _measure_ok(python, harness, work, scenario, rows):
        return _ok(0.1, scenario, rows)

    def test_main_returns_zero_when_the_target_is_measured(self, wired):
        argv, _ = wired
        assert run.main(argv) == 0

    def test_main_skips_a_baseline_whose_probe_fails(self, wired, monkeypatch):
        argv, out = wired

        def probe_fails_for_v0_9_0(python, harness):
            if python.name == "v0.9.0":
                raise RuntimeError("probe failed")
            return {}

        monkeypatch.setattr(run, "installed_versions", probe_fails_for_v0_9_0)
        run.main(argv)
        written = json.loads(out.read_text(encoding="utf-8"))
        assert [v["label"] for v in written["versions"]] == ["head", "v0.10.0"]

    def test_main_measures_only_the_versions_that_installed(self, wired, monkeypatch):
        argv, out = wired

        def create_env_fails_for_v0_10_0(version, source, work):
            if version.label == "v0.10.0":
                raise RuntimeError("no wheel")
            return Path(version.label)

        monkeypatch.setattr(run, "create_env", create_env_fails_for_v0_10_0)
        run.main(argv)
        written = json.loads(out.read_text(encoding="utf-8"))
        assert {r["version"] for r in written["results"]} == {"head", "v0.9.0"}

    def test_main_returns_one_when_the_target_has_no_measurement(
        self, wired, monkeypatch
    ):
        argv, _ = wired

        def measure_fails_for_head(python, harness, work, scenario, rows):
            if python.name == "head":
                return empty_record(scenario, rows, "error", "boom")
            return _ok(0.1, scenario, rows)

        monkeypatch.setattr(run, "measure", measure_fails_for_head)
        assert run.main(argv) == 1

    def test_main_raises_when_the_target_cannot_be_installed(self, wired, monkeypatch):
        argv, _ = wired

        def create_env_fails(version, source, work):
            raise RuntimeError("target would not build")

        monkeypatch.setattr(run, "create_env", create_env_fails)
        with pytest.raises(RuntimeError, match="target would not build"):
            run.main(argv)


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

    def test_main_rejects_summary_without_results(self, tmp_path, capsys):
        with pytest.raises(SystemExit, match=r"^2$"):
            report.main(
                ["--format", "summary", "--input", str(tmp_path / "absent.json")]
            )
        assert "no results at" in capsys.readouterr().err
