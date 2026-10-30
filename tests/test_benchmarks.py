"""Tests for the benchmarks/ harness (not part of the tidysdmx package)."""

import json
import logging
import math
import platform
import re
import sys
import tarfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from matplotlib.figure import Figure
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    MultiComponentMap,
    StructureMap,
)

from benchmarks import case, pipeline_case, probe, report, run
from benchmarks.case import output_fingerprint, repeats_for, run_case
from benchmarks.fingerprint import Fingerprint, digest_counts
from benchmarks.pipeline_case import pipeline_options
from benchmarks.records import (
    PIPELINE_STEPS,
    SCENARIOS,
    asset_name,
    empty_pipeline_record,
    empty_record,
    release_key,
    suite_of,
)
from benchmarks.report import (
    COMMENT_MARKER,
    MISSING,
    NOT_COMPARABLE,
    PIPELINE_MARKER,
    RESULTS_MARKER,
    SAME,
    format_bytes,
    format_change,
    format_seconds,
    is_comparable,
    load_results,
    pipeline_headline,
    pipeline_sizing_table,
    render_charts,
    render_comparison,
    render_page,
    render_pipeline_comparison,
    render_pr_comment,
)
from benchmarks.run import (
    Version,
    exceeds_budget,
    generate_inputs,
    install_versions,
    measure,
    measure_pipeline,
    merge_same_commit,
    over_memory_budget,
    over_time_budget,
    parse_named_ref,
    plan_versions,
    rotated,
    run_grid,
    select_baselines,
    target_measured,
)
from benchmarks.scenarios import build, targets
from benchmarks.wb_ids import SAMPLE_CSV, SAMPLE_ROWS
from tests.fixtures.fxtr_benchmarks import (
    CHUNKED_REASON,
    make_pipeline_results,
    make_results,
)
from tidysdmx import map_structures

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (
    "---\ntitle: Performance\n---\n\n<!-- RESULTS -->\n\n"
    "## WB template pipeline\n\n<!-- PIPELINE-RESULTS -->\n\n## How it is measured\n"
)
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

    def test_empty_record_has_no_output_fingerprint(self):
        assert "output_fingerprint" not in empty_record("typical", 10, "error", "boom")


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


class TestTargets:
    def test_targets_lists_every_typical_target(self):
        _, structure_map = build("typical", 100)
        assert targets(structure_map) == [
            "FREQ",
            "INDICATOR",
            "OBS_VALUE",
            "REF_AREA",
            "SEX",
            "TIME_PERIOD",
            "UNIT_MEASURE",
            "UNIT_MULT",
        ]

    def test_targets_lists_each_target_once_in_order(self):
        structure_map = StructureMap(
            id="SM",
            agency="BENCH",
            maps=[
                FixedValueMap(target="B", value="1"),
                FixedValueMap(target="A", value="1"),
                FixedValueMap(target="B", value="2"),
            ],
        )
        assert targets(structure_map) == ["A", "B"]

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_targets_are_columns_of_the_mapped_output(self, scenario):
        df, structure_map = build(scenario, 100)
        assert set(targets(structure_map)) <= set(map_structures(df, structure_map))


class TestRepeatsFor:
    @pytest.mark.parametrize(
        ("rows", "expected"),
        [(1_000, 7), (10_000, 7), (100_000, 5), (1_000_000, 3)],
    )
    def test_repeats_for_shrinks_with_size(self, rows, expected):
        assert repeats_for(rows) == expected


def _frame(**columns):
    return pd.DataFrame(columns)


class TestOutputFingerprint:
    def test_output_fingerprint_is_sixteen_hex_digits(self):
        fingerprint = output_fingerprint(_frame(A=["x", "y"]), ["A"])
        assert re.fullmatch(r"[0-9a-f]{16}", fingerprint)

    def test_output_fingerprint_is_the_same_for_equal_frames(self):
        assert output_fingerprint(_frame(A=["x", "y"]), ["A"]) == output_fingerprint(
            _frame(A=["x", "y"]), ["A"]
        )

    def test_output_fingerprint_ignores_the_dtype(self):
        as_object = _frame(A=pd.Series(["x", "y", "x"], dtype=object))
        as_string = _frame(A=pd.Series(["x", "y", "x"], dtype="string"))
        assert output_fingerprint(as_object, ["A"]) == output_fingerprint(
            as_string, ["A"]
        )

    def test_output_fingerprint_ignores_how_a_null_is_spelled(self):
        spellings = [None, np.nan, pd.NA]
        fingerprints = {
            output_fingerprint(_frame(A=pd.Series(["x", null], dtype=object)), ["A"])
            for null in spellings
        }
        assert len(fingerprints) == 1

    def test_output_fingerprint_ignores_null_in_a_string_dtype(self):
        as_object = _frame(A=pd.Series(["x", None], dtype=object))
        as_string = _frame(A=pd.Series(["x", pd.NA], dtype="string"))
        assert output_fingerprint(as_object, ["A"]) == output_fingerprint(
            as_string, ["A"]
        )

    def test_output_fingerprint_ignores_row_order(self):
        assert output_fingerprint(_frame(A=["x", "y"]), ["A"]) == output_fingerprint(
            _frame(A=["y", "x"]), ["A"]
        )

    def test_output_fingerprint_ignores_columns_it_is_not_asked_for(self):
        assert output_fingerprint(
            _frame(A=["x"], B=["1"]), ["A"]
        ) == output_fingerprint(_frame(A=["x"], B=["2"]), ["A"])

    def test_output_fingerprint_differs_when_a_value_differs(self):
        assert output_fingerprint(_frame(A=["x", "y"]), ["A"]) != output_fingerprint(
            _frame(A=["x", "z"]), ["A"]
        )

    def test_output_fingerprint_differs_when_a_count_differs(self):
        assert output_fingerprint(_frame(A=["x", "y"]), ["A"]) != output_fingerprint(
            _frame(A=["x", "x"]), ["A"]
        )

    def test_output_fingerprint_differs_when_the_null_count_differs(self):
        # The same non-null values on both sides: only the nulls differ.
        assert output_fingerprint(
            _frame(A=["x", "y", None]), ["A"]
        ) != output_fingerprint(_frame(A=["x", "y"]), ["A"])

    def test_output_fingerprint_differs_when_a_value_moves_between_columns(self):
        assert output_fingerprint(
            _frame(A=["x"], B=["y"]), ["A", "B"]
        ) != output_fingerprint(_frame(A=["y"], B=["x"]), ["A", "B"])

    def test_output_fingerprint_flags_a_missing_column_instead_of_failing(self):
        complete = _frame(A=["x"], B=["y"])
        assert output_fingerprint(complete, ["A", "B"]) != output_fingerprint(
            complete[["A"]], ["A", "B"]
        )

    def test_output_fingerprint_tells_a_mapped_column_from_an_unmapped_one(self):
        df, structure_map = build("regex_heavy", 1_000)
        mapped = map_structures(df, structure_map)
        unmapped = mapped.assign(CODE_A=np.nan)
        columns = targets(structure_map)
        assert output_fingerprint(mapped, columns) != output_fingerprint(
            unmapped, columns
        )


class TestFingerprint:
    """The chunk-mergeable digest both suites share."""

    def test_fingerprint_matches_output_fingerprint(self):
        df = _frame(A=["x", "y", "x", None], B=["1", "2", "3", "4"])
        fingerprint = Fingerprint([("A", False), ("B", False)])
        fingerprint.update(df)
        assert fingerprint.digest() == output_fingerprint(df, ["A", "B"])

    def test_fingerprint_accumulates_chunks_to_the_whole(self):
        df = _frame(A=["x", "y", "x", "z"], V=[1.0, 2.5, None, 4.0])
        whole = Fingerprint([("A", False), ("V", True)])
        whole.update(df)
        chunked = Fingerprint([("A", False), ("V", True)])
        chunked.update(df.iloc[:2])
        chunked.update(df.iloc[2:])
        assert chunked.digest() == whole.digest()

    def test_fingerprint_ignores_unused_categories(self):
        plain = Fingerprint([("A", False)])
        plain.update(_frame(A=["x", "y"]))
        categorical = Fingerprint([("A", False)])
        categorical.update(
            _frame(A=pd.Categorical(["x", "y"], categories=["x", "y", "unused"]))
        )
        assert categorical.digest() == plain.digest()

    def test_fingerprint_reads_numeric_text_as_numbers(self):
        as_text = Fingerprint([("V", True)])
        as_text.update(_frame(V=["1.5", "2", None]))
        as_float = Fingerprint([("V", True)])
        as_float.update(_frame(V=[1.5, 2.0, np.nan]))
        assert as_text.digest() == as_float.digest()

    def test_fingerprint_folds_negative_zero(self):
        negative = Fingerprint([("V", True)])
        negative.update(_frame(V=[-0.0, 1.0]))
        positive = Fingerprint([("V", True)])
        positive.update(_frame(V=[0.0, 1.0]))
        assert negative.digest() == positive.digest()

    def test_fingerprint_differs_when_a_number_changes(self):
        one = Fingerprint([("V", True)])
        one.update(_frame(V=[1.0, 2.0]))
        other = Fingerprint([("V", True)])
        other.update(_frame(V=[1.0, 2.5]))
        assert one.digest() != other.digest()

    def test_fingerprint_differs_when_a_numeric_null_count_changes(self):
        one = Fingerprint([("V", True)])
        one.update(_frame(V=[1.0, None]))
        other = Fingerprint([("V", True)])
        other.update(_frame(V=[1.0, 0.0]))
        assert one.digest() != other.digest()

    def test_fingerprint_flags_a_column_never_seen(self):
        seen = Fingerprint([("A", False), ("B", False)])
        seen.update(_frame(A=["x"], B=["y"]))
        unseen = Fingerprint([("A", False), ("B", False)])
        unseen.update(_frame(A=["x"]))
        assert unseen.digest() != seen.digest()

    def test_fingerprint_of_nothing_is_sixteen_hex_digits(self):
        assert re.fullmatch(r"[0-9a-f]{16}", Fingerprint([("A", False)]).digest())

    def test_digest_counts_ignores_key_order(self):
        assert digest_counts({"a": 1, "b": 2}) == digest_counts({"b": 2, "a": 1})

    def test_digest_counts_differs_when_a_count_differs(self):
        assert digest_counts({"a": 1}) != digest_counts({"a": 2})


class TestRunCase:
    def test_run_case_reports_ok(self):
        assert run_case("typical", 1_000)["status"] == "ok"

    def test_run_case_records_an_output_fingerprint(self):
        fingerprint = run_case("wide", 1_000)["output_fingerprint"]
        assert re.fullmatch(r"[0-9a-f]{16}", fingerprint)

    def test_run_case_fingerprints_the_same_output_every_time(self):
        first = run_case("typical", 1_000)["output_fingerprint"]
        assert run_case("typical", 1_000)["output_fingerprint"] == first

    def test_run_case_fingerprints_only_the_first_call(self, monkeypatch):
        calls = []
        real = case.output_fingerprint

        def counting(df, columns):
            calls.append(len(df))
            return real(df, columns)

        monkeypatch.setattr(case, "output_fingerprint", counting)
        run_case("typical", 1_000)
        assert calls == [1_000]

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
        monkeypatch.setattr(case, "assert_installed", lambda: None)
        _, record = self._main(tmp_path)
        assert record["status"] == "ok"


class TestPipelineRepeats:
    @pytest.mark.parametrize(
        ("rows", "expected"), [(4_000, 3), (1_000_000, 3), (1_000_001, 1)]
    )
    def test_repeats_for_drops_to_one_above_a_million_rows(self, rows, expected):
        assert pipeline_case.repeats_for(rows) == expected


class TestPipelineOptions:
    def test_pipeline_options_whole_is_the_default_run(self):
        assert pipeline_options("whole", 1_000_000) == {}

    def test_pipeline_options_chunked_splits_the_rows_in_five(self):
        assert pipeline_options("chunked", 1_000_000) == {
            "chunk_rows": 200_000,
            "skip_unused": True,
            "categories": True,
        }

    def test_pipeline_options_chunked_keeps_at_least_one_row_per_chunk(self):
        assert pipeline_options("chunked", 3)["chunk_rows"] == 1

    def test_pipeline_options_rejects_unknown_configuration(self):
        with pytest.raises(ValueError, match="Unknown configuration 'lean'"):
            pipeline_options("lean", 10)


@pytest.fixture(scope="module")
def pipeline_cases():
    """Both configurations measured on the committed 4,000-row sample."""
    return {
        config: pipeline_case.run_case(config, SAMPLE_ROWS, SAMPLE_CSV)
        for config in ("whole", "chunked")
    }


class TestPipelineRunCase:
    @pytest.mark.parametrize("config", ["whole", "chunked"])
    def test_run_case_reports_ok(self, pipeline_cases, config):
        record = pipeline_cases[config]
        assert (record["status"], record["scenario"], record["rows"]) == (
            "ok",
            config,
            SAMPLE_ROWS,
        )

    @pytest.mark.parametrize("config", ["whole", "chunked"])
    def test_run_case_measures_every_step_but_the_write(self, pipeline_cases, config):
        steps = [step["step"] for step in pipeline_cases[config]["steps"]]
        assert steps == [step for step in PIPELINE_STEPS if step != "write"]

    def test_run_case_repeats_the_run(self, pipeline_cases):
        record = pipeline_cases["whole"]
        assert len(record["samples_s"]) == pipeline_case.repeats_for(SAMPLE_ROWS)

    def test_run_case_takes_the_median_of_every_step(self, pipeline_cases):
        step = pipeline_cases["whole"]["steps"][0]
        assert len(step["samples_s"]) == 3
        assert step["median_s"] == sorted(step["samples_s"])[1]

    def test_run_case_samples_peak_rss(self, pipeline_cases):
        record = pipeline_cases["whole"]
        assert record["peak_rss_bytes"] > 0
        assert all(step["peak_rss_bytes"] > 0 for step in record["steps"])

    def test_run_case_traces_no_allocation(self, pipeline_cases):
        assert pipeline_cases["whole"]["peak_alloc_bytes"] is None

    def test_run_case_fingerprints_whole_and_chunked_alike(self, pipeline_cases):
        whole, chunked = pipeline_cases["whole"], pipeline_cases["chunked"]
        assert whole["output_fingerprint"] == chunked["output_fingerprint"]

    def test_run_case_finds_the_same_errors_whole_and_chunked(self, pipeline_cases):
        whole, chunked = pipeline_cases["whole"], pipeline_cases["chunked"]
        assert whole["validation"] == chunked["validation"]
        assert whole["validation_fingerprint"] == chunked["validation_fingerprint"]

    def test_run_case_counts_the_samples_codelist_gaps(self, pipeline_cases):
        assert set(pipeline_cases["whole"]["validation"]) == {"codelist_ids"}

    def test_run_case_records_the_chunk_size(self, pipeline_cases):
        assert pipeline_cases["whole"]["chunk_rows"] is None
        assert pipeline_cases["chunked"]["chunk_rows"] == SAMPLE_ROWS // 5

    def test_run_case_records_the_input_size(self, pipeline_cases):
        assert pipeline_cases["whole"]["input_bytes"] == SAMPLE_CSV.stat().st_size

    def test_run_case_records_the_output_shape(self, pipeline_cases):
        whole, chunked = pipeline_cases["whole"], pipeline_cases["chunked"]
        assert whole["output_shape"][0] > 0
        assert whole["output_shape"] == chunked["output_shape"]

    def test_run_case_skips_chunked_without_dataset_validator(self, monkeypatch):
        monkeypatch.setattr(pipeline_case, "has_dataset_validator", lambda: False)
        record = pipeline_case.run_case("chunked", SAMPLE_ROWS, SAMPLE_CSV)
        assert (record["status"], record["reason"]) == ("skipped", CHUNKED_REASON)

    def test_run_case_stops_after_a_slow_run(self, monkeypatch):
        monkeypatch.setattr(pipeline_case, "SLOW_RUN_S", 0.0)
        record = pipeline_case.run_case("whole", SAMPLE_ROWS, SAMPLE_CSV)
        assert len(record["samples_s"]) == 1


class TestPipelineCaseMain:
    def _main(self, tmp_path, config="whole"):
        result = tmp_path / "case.json"
        code = pipeline_case.main(
            [
                "--scenario",
                config,
                "--rows",
                str(SAMPLE_ROWS),
                "--csv",
                str(SAMPLE_CSV),
                "--result",
                str(result),
            ]
        )
        return code, json.loads(result.read_text(encoding="utf-8"))

    def test_main_refuses_tidysdmx_from_outside_the_environment(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(sys, "prefix", str(tmp_path / "elsewhere"))
        code, record = self._main(tmp_path)
        assert code == 1
        assert record["reason"].startswith("ImportError: tidysdmx was imported from")

    def test_main_writes_an_ok_record(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pipeline_case, "assert_installed", lambda: None)
        monkeypatch.setattr(pipeline_case, "repeats_for", lambda rows: 1)
        code, record = self._main(tmp_path)
        assert (code, record["status"]) == (0, "ok")

    def test_main_returns_one_for_a_skipped_case(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pipeline_case, "assert_installed", lambda: None)
        monkeypatch.setattr(pipeline_case, "has_dataset_validator", lambda: False)
        code, record = self._main(tmp_path, "chunked")
        assert (code, record["status"]) == (1, "skipped")

    def test_main_records_a_missing_csv_as_an_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pipeline_case, "assert_installed", lambda: None)
        result = tmp_path / "case.json"
        code = pipeline_case.main(
            [
                "--scenario",
                "whole",
                "--rows",
                "10",
                "--csv",
                str(tmp_path / "absent.csv"),
                "--result",
                str(result),
            ]
        )
        record = json.loads(result.read_text(encoding="utf-8"))
        assert code == 1
        assert record["status"] == "error"
        assert record["reason"].startswith("FileNotFoundError")

    def test_main_rejects_an_unknown_configuration(self, tmp_path):
        with pytest.raises(SystemExit, match=r"^2$"):
            self._main(tmp_path, "lean")


class TestInstalled:
    def test_installed_reports_python(self):
        assert probe.installed()["python"] == platform.python_version()

    def test_installed_reports_the_pipeline_suites_packages(self):
        assert {"pyarrow", "psutil"} <= set(probe.installed())

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


def _ok_pipeline(peak_rss, rows, config="whole"):
    record = empty_pipeline_record(config, rows, "error", "")
    record.update(
        status="ok", reason=None, median_s=1.0, samples_s=[1.0], peak_rss_bytes=peak_rss
    )
    return record


class TestSkipRules:
    def test_over_time_budget_runs_the_first_size(self):
        assert over_time_budget(None, 10) is None

    def test_over_time_budget_skips_after_a_slow_size(self):
        assert over_time_budget(_ok(run.BUDGET_S + 1), 10) == (
            "a smaller size took over 60 s"
        )

    def test_over_time_budget_runs_after_a_fast_size(self):
        assert over_time_budget(_ok(0.1), 10) is None

    def test_over_memory_budget_skips_a_size_that_would_not_fit(self):
        rule = over_memory_budget(16 * 2**30)
        reason = rule(_ok_pipeline(2 * 2**30, 1_000_000), 10_000_000)
        assert reason == (
            "a 1,000,000-row run peaked at 2.0 GiB; 10,000,000 rows would need "
            "about 20.0 GiB, over 60% of this machine's 16.0 GiB"
        )

    def test_over_memory_budget_runs_a_size_that_fits(self):
        rule = over_memory_budget(16 * 2**30)
        assert rule(_ok_pipeline(2**30, 1_000_000), 5_000_000) is None

    def test_over_memory_budget_takes_the_fraction(self):
        rule = over_memory_budget(16 * 2**30, fraction=0.25)
        assert rule(_ok_pipeline(2**30, 1_000_000), 5_000_000) is not None

    def test_over_memory_budget_runs_the_first_size(self):
        assert over_memory_budget(16 * 2**30)(None, 10) is None

    def test_over_memory_budget_ignores_an_unmeasured_size(self):
        previous = empty_pipeline_record("whole", 1, "error", "boom")
        assert over_memory_budget(16 * 2**30)(previous, 10) is None

    def test_over_memory_budget_ignores_a_record_without_rss(self):
        assert over_memory_budget(16 * 2**30)(_ok(0.1), 10_000_000) is None

    def test_over_memory_budget_is_off_without_a_memory_figure(self):
        rule = over_memory_budget(None)
        assert rule(_ok_pipeline(2 * 2**30, 1_000_000), 10_000_000) is None


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

    def test_run_grid_applies_every_rule(self):
        def no_more_than_one_row(previous, rows):
            return "too big" if rows > 1 else None

        records = run_grid(
            [_version("a")],
            ["typical"],
            [1, 2, 3],
            lambda *_: _ok(0.1),
            rules=(over_time_budget, no_more_than_one_row),
        )
        assert [(r["status"], r["reason"]) for r in records] == [
            ("ok", None),
            ("skipped", "too big"),
            ("skipped", "too big"),
        ]

    def test_run_grid_shows_each_rule_the_last_measured_record(self):
        seen = []

        def remember(previous, rows):
            seen.append(None if previous is None else previous["rows"])
            return None

        run_grid(
            [_version("a")],
            ["typical"],
            [1, 2, 3],
            lambda version, scenario, rows: _ok(0.1, rows=rows),
            rules=(remember,),
        )
        assert seen == [None, 1, 2]

    def test_run_grid_builds_skipped_records_with_the_given_shape(self):
        def never(previous, rows):
            return "not today"

        records = run_grid(
            [_version("a")],
            ["whole"],
            [1],
            lambda *_: _ok(0.1),
            rules=(never,),
            empty=empty_pipeline_record,
        )
        assert records[0]["steps"] == []


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


STUB_PIPELINE_CASE = """\
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--scenario", required=True)
parser.add_argument("--rows", type=int, required=True)
parser.add_argument("--csv", type=Path, required=True)
parser.add_argument("--result", type=Path, required=True)
args = parser.parse_args()
record = {
    "scenario": args.scenario,
    "rows": args.rows,
    "status": "ok",
    "reason": None,
    "median_s": 0.5,
    "samples_s": [0.5],
    "csv": str(args.csv),
}
args.result.write_text(json.dumps(record), encoding="utf-8")
"""

STUB_GENERATE = """\
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, required=True)
parser.add_argument("--seed", type=int, required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
if args.rows == 13:
    raise SystemExit("thirteen rows cannot be generated")
args.out.parent.mkdir(parents=True, exist_ok=True)
args.out.write_text(f"rows={args.rows} seed={args.seed}", encoding="utf-8")
"""


@pytest.fixture
def stub_pipeline_harness(tmp_path):
    """A directory whose pipeline case and generator are stdlib stubs."""
    root = tmp_path / "stub"
    package = root / "benchmarks"
    wb_ids = package / "wb_ids"
    wb_ids.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "pipeline_case.py").write_text(STUB_PIPELINE_CASE, encoding="utf-8")
    (wb_ids / "__init__.py").write_text("", encoding="utf-8")
    (wb_ids / "generate.py").write_text(STUB_GENERATE, encoding="utf-8")
    return root


class TestMeasurePipeline:
    def test_measure_pipeline_returns_the_record_the_case_wrote(
        self, stub_pipeline_harness, tmp_path
    ):
        record = measure_pipeline(
            Path(sys.executable),
            stub_pipeline_harness,
            tmp_path,
            "whole",
            100,
            Path("m"),
        )
        assert record["median_s"] == 0.5

    def test_measure_pipeline_passes_the_case_its_arguments(
        self, stub_pipeline_harness, tmp_path
    ):
        csv = tmp_path / "mock.csv"
        record = measure_pipeline(
            Path(sys.executable), stub_pipeline_harness, tmp_path, "chunked", 7, csv
        )
        assert (record["scenario"], record["rows"], record["csv"]) == (
            "chunked",
            7,
            str(csv),
        )

    def test_measure_pipeline_reports_a_case_that_wrote_nothing(self, tmp_path):
        # From tmp_path, `python -m benchmarks.pipeline_case` cannot be imported.
        record = measure_pipeline(
            Path(sys.executable), tmp_path, tmp_path, "whole", 100, Path("m")
        )
        assert record["reason"].startswith("exited 1 without a result")
        assert record["steps"] == []


class TestGenerateInputs:
    def test_generate_inputs_writes_one_file_per_size(
        self, stub_pipeline_harness, tmp_path
    ):
        work = tmp_path / "work"
        inputs = generate_inputs(
            Path(sys.executable), stub_pipeline_harness, work, [20, 10]
        )
        assert inputs == {
            10: work / "data" / "wb_ids_10.csv",
            20: work / "data" / "wb_ids_20.csv",
        }
        assert all(path.exists() for path in inputs.values())

    def test_generate_inputs_passes_the_rows_and_seed(
        self, stub_pipeline_harness, tmp_path
    ):
        inputs = generate_inputs(
            Path(sys.executable), stub_pipeline_harness, tmp_path, [10], seed=7
        )
        assert inputs[10].read_text(encoding="utf-8") == "rows=10 seed=7"

    def test_generate_inputs_generates_each_size_once(
        self, stub_pipeline_harness, tmp_path
    ):
        inputs = generate_inputs(
            Path(sys.executable), stub_pipeline_harness, tmp_path, [10, 10]
        )
        assert list(inputs) == [10]

    def test_generate_inputs_raises_when_the_generator_fails(
        self, stub_pipeline_harness, tmp_path
    ):
        with pytest.raises(RuntimeError, match="thirteen rows cannot be generated"):
            generate_inputs(Path(sys.executable), stub_pipeline_harness, tmp_path, [13])


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
        monkeypatch.setattr(
            run, "create_env", lambda v, source, work, packages=(): Path(v.label)
        )
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

        def create_env_fails_for_v0_10_0(version, source, work, packages=()):
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

        def create_env_fails(version, source, work, packages=()):
            raise RuntimeError("target would not build")

        monkeypatch.setattr(run, "create_env", create_env_fails)
        with pytest.raises(RuntimeError, match="target would not build"):
            run.main(argv)

    def test_main_names_the_mapping_suite(self, wired):
        argv, out = wired
        run.main(argv)
        assert json.loads(out.read_text(encoding="utf-8"))["suite"] == "mapping"

    def test_main_installs_the_mapping_suite_without_extras(self, wired, monkeypatch):
        argv, _ = wired
        extras = []

        def create_env(version, source, work, packages=()):
            extras.append(tuple(packages))
            return Path(version.label)

        monkeypatch.setattr(run, "create_env", create_env)
        run.main(argv)
        assert set(extras) == {()}

    def test_main_generates_no_inputs_for_the_mapping_suite(self, wired, monkeypatch):
        argv, _ = wired

        def no_inputs(*args, **kwargs):
            raise AssertionError("the mapping suite needs no generated inputs")

        monkeypatch.setattr(run, "generate_inputs", no_inputs)
        assert run.main(argv) == 0


class TestMainPipeline:
    """`main --suite pipeline` with git, uv, the generator and the cases replaced."""

    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        versions = [_version("head", role="target"), _version("v0.10.0")]
        monkeypatch.setattr(run, "_run", lambda command, cwd=None: str(tmp_path))
        monkeypatch.setattr(run, "plan_versions", lambda *args: versions)
        monkeypatch.setattr(run, "export_source", lambda root, v, work: tmp_path)
        monkeypatch.setattr(
            run, "create_env", lambda v, source, work, packages=(): Path(v.label)
        )
        monkeypatch.setattr(run, "installed_versions", lambda python, harness: {})
        monkeypatch.setattr(run, "generate_inputs", self._generate)
        monkeypatch.setattr(run, "measure_pipeline", self._measure_ok)
        out = tmp_path / "out" / "results.json"
        argv = [
            "--suite",
            "pipeline",
            "--target",
            "head",
            "--sizes",
            "10",
            "--work-dir",
            str(tmp_path / "work"),
            "--out",
            str(out),
        ]
        return argv, out

    @staticmethod
    def _generate(python, harness, work, sizes, seed=0):
        return {rows: Path(f"wb_ids_{rows}.csv") for rows in sizes}

    @staticmethod
    def _measure_ok(python, harness, work, config, rows, csv):
        record = _ok_pipeline(2**20, rows, config)
        record["reason"] = str(csv)
        return record

    def test_main_names_the_pipeline_suite(self, wired):
        argv, out = wired
        run.main(argv)
        assert json.loads(out.read_text(encoding="utf-8"))["suite"] == "pipeline"

    def test_main_runs_both_configurations_by_default(self, wired):
        argv, out = wired
        run.main(argv)
        written = json.loads(out.read_text(encoding="utf-8"))
        assert written["scenarios"] == ["whole", "chunked"]

    def test_main_defaults_to_the_suites_sizes(self, wired, monkeypatch):
        argv, out = wired
        generated = []

        def generate(python, harness, work, sizes, seed=0):
            generated.append(list(sizes))
            return self._generate(python, harness, work, sizes)

        monkeypatch.setattr(run, "generate_inputs", generate)
        run.main([arg for arg in argv if arg not in ("--sizes", "10")])
        assert generated == [[1_000_000]]
        assert json.loads(out.read_text(encoding="utf-8"))["sizes"] == [1_000_000]

    def test_main_installs_the_suites_packages(self, wired, monkeypatch):
        argv, _ = wired
        extras = []

        def create_env(version, source, work, packages=()):
            extras.append(tuple(packages))
            return Path(version.label)

        monkeypatch.setattr(run, "create_env", create_env)
        run.main(argv)
        assert set(extras) == {("pyarrow", "psutil")}

    def test_main_works_under_the_suites_own_directory(self, wired, monkeypatch):
        argv, _ = wired
        dirs = []

        def create_env(version, source, work, packages=()):
            dirs.append(work)
            return Path(version.label)

        monkeypatch.setattr(run, "create_env", create_env)
        run.main(argv)
        assert {work.name for work in dirs} == {"pipeline"}

    def test_main_generates_inputs_with_the_targets_environment(
        self, wired, monkeypatch
    ):
        argv, _ = wired
        pythons = []

        def generate(python, harness, work, sizes, seed=0):
            pythons.append(python)
            return self._generate(python, harness, work, sizes)

        monkeypatch.setattr(run, "generate_inputs", generate)
        run.main(argv)
        assert pythons == [Path("head")]

    def test_main_passes_each_case_its_sizes_input(self, wired):
        argv, out = wired
        run.main(argv)
        written = json.loads(out.read_text(encoding="utf-8"))
        assert {r["reason"] for r in written["results"]} == {"wb_ids_10.csv"}

    def test_main_rejects_a_mapping_scenario(self, wired, capsys):
        argv, _ = wired
        with pytest.raises(SystemExit, match=r"^2$"):
            run.main([*argv, "--scenarios", "typical"])
        assert "the pipeline suite has no scenario ['typical']" in (
            capsys.readouterr().err
        )

    def test_main_skips_a_size_the_machine_cannot_hold(self, wired, monkeypatch):
        argv, out = wired
        monkeypatch.setattr(
            run,
            "machine_info",
            lambda: {
                "platform": "test",
                "cpu": "test",
                "cpu_count": 1,
                "memory_bytes": 3 * 2**20,
                "runner_image": None,
            },
        )
        argv.insert(argv.index("10") + 1, "20")
        run.main(argv)
        written = json.loads(out.read_text(encoding="utf-8"))
        statuses = {(r["scenario"], r["rows"]): r["status"] for r in written["results"]}
        assert statuses[("whole", 10)] == "ok"
        assert statuses[("whole", 20)] == "skipped"

    def test_main_raises_when_the_inputs_cannot_be_generated(self, wired, monkeypatch):
        argv, _ = wired

        def generate_fails(python, harness, work, sizes, seed=0):
            raise RuntimeError("generator failed")

        monkeypatch.setattr(run, "generate_inputs", generate_fails)
        with pytest.raises(RuntimeError, match="generator failed"):
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

    def test_load_results_reads_a_file_without_a_suite_as_mapping(self, tmp_path):
        path = tmp_path / "before-suites.json"
        results = make_results()
        results.pop("suite", None)
        path.write_text(json.dumps(results), encoding="utf-8")
        assert suite_of(load_results(path)) == "mapping"

    def test_load_results_keeps_the_pipeline_suite(self, tmp_path):
        path = tmp_path / "pipeline.json"
        path.write_text(json.dumps(make_pipeline_results()), encoding="utf-8")
        assert suite_of(load_results(path)) == "pipeline"


class TestAssetName:
    def test_asset_name_keeps_the_mapping_suites_name(self):
        assert asset_name("mapping", "v0.11.0") == "benchmarks-v0.11.0.json"

    def test_asset_name_gives_the_pipeline_suite_its_own(self):
        assert asset_name("pipeline", "v0.11.0") == "pipeline-v0.11.0.json"


def _pipeline_record_of(results, version, config, rows):
    return _record_of(results, version, config, rows)


class TestRenderPipelineComparison:
    def test_render_pipeline_comparison_opens_with_its_heading(self, pipeline_results):
        text = render_pipeline_comparison(pipeline_results)
        assert text.startswith("### WB template pipeline benchmark\n")

    def test_render_comparison_dispatches_on_the_suite(self, pipeline_results):
        assert render_comparison(pipeline_results) == render_pipeline_comparison(
            pipeline_results
        )

    def test_render_pr_comment_carries_the_pipeline_tables(self, pipeline_results):
        text = render_pr_comment(pipeline_results)
        assert text.startswith(COMMENT_MARKER + "\n")
        assert "### WB template pipeline benchmark" in text

    def test_render_pipeline_comparison_compares_the_total(self, pipeline_results):
        row = f"| total | 3.17 s | 6.34 s | 3.33 s | -50% | {SAME} |"
        assert row in render_pipeline_comparison(pipeline_results)

    def test_render_pipeline_comparison_compares_peak_rss(self, pipeline_results):
        row = f"| peak RSS | 1 GiB | 2 GiB | 1.05 GiB | -50% | {SAME} |"
        assert row in render_pipeline_comparison(pipeline_results)

    def test_render_pipeline_comparison_has_a_row_per_step(self, pipeline_results):
        text = render_pipeline_comparison(pipeline_results)
        assert "| load | 800 ms | 1.6 s | 840 ms | -50% | " in text
        assert "| write |" not in text

    def test_render_pipeline_comparison_titles_each_configuration(
        self, pipeline_results
    ):
        text = render_pipeline_comparison(pipeline_results)
        assert "| whole frame, 1,000,000 rows | head | base | v0.10.0 |" in text
        assert "| chunked, lean load, 1,000,000 rows | head | base | v0.10.0 |" in text

    def test_render_pipeline_comparison_marks_a_skipped_case(self, pipeline_results):
        text = render_pipeline_comparison(pipeline_results)
        assert f"| total | 3.17 s | 6.34 s | {MISSING} | -50% | {MISSING} |" in text

    def test_render_pipeline_comparison_explains_a_skipped_case(self, pipeline_results):
        note = f"{MISSING} chunked, lean load, v0.10.0: {CHUNKED_REASON}."
        assert note in render_pipeline_comparison(pipeline_results)

    def test_render_pipeline_comparison_explains_an_error(self, pipeline_results):
        record = _pipeline_record_of(pipeline_results, "base", "whole", 1_000_000)
        record.update(status="error", reason="MemoryError: out of memory\nmore")
        note = f"{MISSING} whole frame, base: MemoryError: out of memory."
        assert note in render_pipeline_comparison(pipeline_results)

    def test_render_pipeline_comparison_marks_different_output_throughout(
        self, pipeline_results
    ):
        _pipeline_record_of(pipeline_results, "v0.10.0", "whole", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        text = render_pipeline_comparison(pipeline_results)
        assert (
            f"| load | 800 ms | 1.6 s | {NOT_COMPARABLE} | -50% | {MISSING} |" in text
        )
        assert (
            f"| total | 3.17 s | 6.34 s | {NOT_COMPARABLE} | -50% | {MISSING} |" in text
        )
        assert "that version produced different results from head" in text

    def test_render_pipeline_comparison_marks_different_errors_on_validate_only(
        self, pipeline_results
    ):
        _pipeline_record_of(pipeline_results, "v0.10.0", "whole", 1_000_000)[
            "validation_fingerprint"
        ] = "ffffffffffffffff"
        text = render_pipeline_comparison(pipeline_results)
        assert (
            f"| validate | 700 ms | 1.4 s | {NOT_COMPARABLE} | -50% | {MISSING} |"
            in text
        )
        assert f"| total | 3.17 s | 6.34 s | 3.33 s | -50% | {SAME} |" in text

    def test_render_pipeline_comparison_reports_shared_validation_errors(
        self, pipeline_results
    ):
        text = render_pipeline_comparison(pipeline_results)
        assert "Every measured case found the same errors: 49 codelist_ids." in text

    def test_render_pipeline_comparison_reports_differing_validation_errors(
        self, pipeline_results
    ):
        _pipeline_record_of(pipeline_results, "v0.10.0", "whole", 1_000_000)[
            "validation"
        ] = {"codelist_ids": 49, "duplicates": 2}
        text = render_pipeline_comparison(pipeline_results)
        assert "The validation step found different errors: " in text
        assert "v0.10.0 (whole frame) 49 codelist_ids, 2 duplicates" in text

    def test_render_pipeline_comparison_folds_smaller_sizes_away(
        self, pipeline_results
    ):
        text = render_pipeline_comparison(pipeline_results)
        assert text.index("<details>") < text.index("**1,000 rows**")

    def test_render_pipeline_comparison_lists_pyarrow(self, pipeline_results):
        text = render_pipeline_comparison(pipeline_results)
        assert "| pyarrow |" in text
        assert (
            "| head | target | `abc1234` | 3.11.14 | 3.0.6 | 2.3.4 | 1.20.0 | 26.0.0 |"
            in text
        )
        assert f"| 2.3.3 | 2.3.4 | 1.20.0 | {MISSING} |" in text

    def test_render_pipeline_comparison_links_the_run(self, pipeline_results):
        text = render_pipeline_comparison(pipeline_results, "https://example.test/1")
        assert "[this run](https://example.test/1)" in text

    def test_pipeline_headline_gives_both_configurations(self, pipeline_results):
        assert pipeline_headline(pipeline_results) == (
            "**head** runs the WB template pipeline on 1,000,000 rows whole in "
            "**3.17 s**, peaking at **1 GiB** of RSS and in 200,000-row chunks "
            "with the lean load in **3.17 s**, peaking at **1 GiB** of RSS. "
            "Measured 2026-10-29 on a GitHub-hosted runner (AMD EPYC 7763 64-Core "
            "Processor, 4 CPUs, 16 GiB RAM)."
        )

    def test_pipeline_headline_without_a_measurement(self, pipeline_results):
        pipeline_results["results"] = []
        assert pipeline_headline(pipeline_results).startswith("**head**. Measured ")

    def test_pipeline_sizing_table_gives_time_and_rss_per_size(self, pipeline_results):
        text = pipeline_sizing_table(pipeline_results)
        assert "| Configuration | 1,000 rows | 1,000,000 rows |" in text
        assert "| whole frame | 31.7 ms, 154 MiB | 3.17 s, 1 GiB |" in text

    def test_pipeline_sizing_table_marks_an_unmeasured_size(self, pipeline_results):
        _pipeline_record_of(pipeline_results, "head", "chunked", 1_000)["status"] = (
            "error"
        )
        assert f"| chunked, lean load | {MISSING} | 3.17 s, 1 GiB |" in (
            pipeline_sizing_table(pipeline_results)
        )


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


def _record_of(results, version, scenario, rows):
    return next(
        r
        for r in results["results"]
        if (r["version"], r["scenario"], r["rows"]) == (version, scenario, rows)
    )


class TestIsComparable:
    def test_is_comparable_accepts_equal_fingerprints(self):
        assert is_comparable(
            {"output_fingerprint": "aaaa"}, {"output_fingerprint": "aaaa"}
        )

    def test_is_comparable_rejects_different_fingerprints(self):
        assert not is_comparable(
            {"output_fingerprint": "aaaa"}, {"output_fingerprint": "bbbb"}
        )

    def test_is_comparable_accepts_a_record_without_a_fingerprint(self):
        assert is_comparable({}, {"output_fingerprint": "aaaa"})

    def test_is_comparable_accepts_a_target_without_a_fingerprint(self):
        assert is_comparable({"output_fingerprint": "aaaa"}, {})

    @pytest.mark.parametrize(
        ("record", "target"),
        [
            (None, {"output_fingerprint": "aaaa"}),
            ({"output_fingerprint": "aaaa"}, None),
        ],
    )
    def test_is_comparable_accepts_a_missing_record(self, record, target):
        assert is_comparable(record, target)


class TestRenderComparisonFingerprints:
    @pytest.fixture
    def differing_baseline(self, benchmark_results):
        """v0.10.0 mapped the typical table at 1M rows differently from head."""
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        return benchmark_results

    def test_render_comparison_marks_a_version_with_different_output(
        self, differing_baseline
    ):
        row = f"| typical | 1 s | 2 s | {NOT_COMPARABLE} | -50% | {MISSING} |"
        assert row in render_comparison(differing_baseline)

    def test_render_comparison_marks_different_output_in_the_memory_table(
        self, differing_baseline
    ):
        row = f"| typical | 300 MiB | 600 MiB | {NOT_COMPARABLE} | -50% | {MISSING} |"
        assert row in render_comparison(differing_baseline)

    def test_render_comparison_explains_the_mark_in_a_footnote(
        self, differing_baseline
    ):
        note = (
            f"{NOT_COMPARABLE}: that version produced different results from head "
            "on this case"
        )
        assert note in render_comparison(differing_baseline)

    def test_render_comparison_footnotes_each_size_with_a_mark(
        self, differing_baseline
    ):
        # The 1M-row tables appear twice (up front and under All sizes); the
        # 1k-row tables match, so they get no note.
        text = render_comparison(differing_baseline)
        assert text.count("that version produced different results") == 2

    def test_render_comparison_keeps_the_targets_own_cells(self, benchmark_results):
        _record_of(benchmark_results, "head", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        row = f"| typical | 1 s | {NOT_COMPARABLE} | {NOT_COMPARABLE} | {MISSING} | "
        assert row in render_comparison(benchmark_results)

    def test_render_comparison_has_no_footnote_when_outputs_match(
        self, benchmark_results
    ):
        assert NOT_COMPARABLE not in render_comparison(benchmark_results)

    def test_render_comparison_compares_a_version_without_a_fingerprint(
        self, benchmark_results
    ):
        del _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ]
        assert NOT_COMPARABLE not in render_comparison(benchmark_results)

    def test_render_comparison_ignores_fingerprints_of_unmeasured_cases(
        self, benchmark_results
    ):
        # The wide table's base errored, so it has no fingerprint to disagree with.
        assert f"| wide | 4 s | {MISSING} |" in render_comparison(benchmark_results)

    def test_render_pr_comment_carries_the_footnote(self, differing_baseline):
        assert "that version produced different results" in render_pr_comment(
            differing_baseline
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

    def test_main_writes_the_pipeline_summary(self, tmp_path, pipeline_results):
        source = tmp_path / "pipeline.json"
        source.write_text(json.dumps(pipeline_results), encoding="utf-8")
        output = tmp_path / "summary.md"
        report.main(
            ["--format", "summary", "--input", str(source), "--output", str(output)]
        )
        assert output.read_text(encoding="utf-8").startswith(
            "### WB template pipeline benchmark"
        )


class TestRenderPage:
    def _page(self, results, pipeline=None):
        return render_page(
            results, TEMPLATE, charts=lambda _: "CHARTS", pipeline=pipeline
        )

    def test_render_page_shows_placeholder_without_results(self):
        assert "No performance profile has been published yet" in render_page(
            None, TEMPLATE
        )

    def test_render_page_replaces_the_marker(self, benchmark_results):
        assert RESULTS_MARKER not in self._page(benchmark_results)

    def test_render_page_rejects_template_without_marker(self, benchmark_results):
        with pytest.raises(ValueError, match="has no <!-- RESULTS --> marker"):
            render_page(benchmark_results, "# Performance\n")

    def test_render_page_shows_pipeline_placeholder_without_pipeline_results(
        self, benchmark_results
    ):
        page = self._page(benchmark_results)
        assert "No pipeline profile has been published yet" in page
        assert PIPELINE_MARKER not in page

    def test_render_page_rejects_template_without_pipeline_marker(
        self, benchmark_results
    ):
        with pytest.raises(ValueError, match="has no <!-- PIPELINE-RESULTS --> marker"):
            render_page(benchmark_results, f"# Performance\n\n{RESULTS_MARKER}\n")

    def test_render_page_fills_the_pipeline_section(
        self, benchmark_results, pipeline_results
    ):
        page = self._page(benchmark_results, pipeline_results)
        assert "### Step by step" in page
        assert f"| total | 3.17 s | 6.34 s | 3.33 s | -50% | {SAME} |" in page

    def test_render_page_keeps_the_pipeline_section_under_its_heading(
        self, benchmark_results, pipeline_results
    ):
        page = self._page(benchmark_results, pipeline_results)
        assert page.index("## WB template pipeline") < page.index("### Step by step")
        assert page.index("### Step by step") < page.index("## How it is measured")

    def test_render_page_renders_the_pipeline_without_mapping_results(
        self, pipeline_results
    ):
        page = render_page(None, TEMPLATE, pipeline=pipeline_results)
        assert "No performance profile has been published yet" in page
        assert "### Step by step" in page

    def test_render_page_links_pipeline_raw_data_for_a_release(
        self, benchmark_results, pipeline_results
    ):
        pipeline_results["versions"][0]["label"] = "v0.11.0"
        assert "releases/download/v0.11.0/pipeline-v0.11.0.json" in self._page(
            benchmark_results, pipeline_results
        )

    def test_render_page_headline_gives_time_and_memory(self, benchmark_results):
        assert "in **1 s**, allocating **300 MiB**" in self._page(benchmark_results)

    def test_render_page_sizing_relates_memory_to_input(self, benchmark_results):
        assert "300 MiB (3.1\u00d7 input)" in self._page(benchmark_results)

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

    def test_render_page_marks_a_version_with_different_output(self, benchmark_results):
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        page = self._page(benchmark_results)
        assert f"| typical | 1 s | 2 s | {NOT_COMPARABLE} |" in page

    def test_render_page_footnotes_the_comparison_tables(self, benchmark_results):
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        assert "that version produced different results" in self._page(
            benchmark_results
        )

    def test_render_page_explains_a_gap_in_the_charts(self, benchmark_results):
        # Different output at 1k rows only: no table cell shows it, but the lines do.
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        assert "A line leaves out a point" in self._page(benchmark_results)

    def test_render_page_has_no_notes_when_outputs_match(self, benchmark_results):
        page = self._page(benchmark_results)
        assert NOT_COMPARABLE not in page


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

    def test_render_charts_leaves_no_page_wide_stylesheet(self, rendered_charts):
        # An inline <style> is global to the page, so a `*{...}` rule would
        # restyle every other SVG on the site.
        assert "<style" not in rendered_charts

    def test_render_charts_keeps_round_joins_on_each_chart(self, rendered_charts):
        assert rendered_charts.count('<svg stroke-linejoin="round"') == 4

    def test_render_charts_gives_every_font_a_generic_fallback(self, rendered_charts):
        # Without one, a machine lacking DejaVu Sans falls back to a serif.
        assert not re.search(r"font-family: 'DejaVu Sans'(?!,)", rendered_charts)

    def test_render_charts_still_names_dejavu_sans_first(self, rendered_charts):
        assert "font-family: 'DejaVu Sans', sans-serif" in rendered_charts


class TestPlotPanel:
    @staticmethod
    def _time_lines(results):
        axes = Figure().subplots()
        report._plot_panel(axes, results, "typical", "time", report._THEMES["light"])
        return {line.get_label(): list(line.get_ydata()) for line in axes.lines}

    def test_plot_panel_draws_every_comparable_point(self, benchmark_results):
        assert self._time_lines(benchmark_results)["v0.10.0"] == [0.020, 1.05]

    def test_plot_panel_leaves_out_a_point_with_different_output(
        self, benchmark_results
    ):
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        line = self._time_lines(benchmark_results)["v0.10.0"]
        assert (line[0], math.isnan(line[1])) == (0.020, True)

    def test_plot_panel_keeps_the_targets_points(self, benchmark_results):
        _record_of(benchmark_results, "v0.10.0", "typical", 1_000_000)[
            "output_fingerprint"
        ] = "ffffffffffffffff"
        assert self._time_lines(benchmark_results)["head"] == [0.010, 1.0]


class TestLegendColumns:
    def test_legend_columns_keeps_short_labels_in_one_row(self):
        assert report._legend_columns(["head", "base", "v0.10.0", "v0.9.0"]) == 4

    def test_legend_columns_wraps_long_labels(self):
        labels = [
            "head",
            "base (= v0.11.0, v0.11.0-rc.2)",
            "v0.10.0",
            "v0.9.0 (= v0.9.1, v0.9.2)",
            "v0.8.0",
        ]
        assert report._legend_columns(labels) == 3

    def test_legend_columns_handles_a_single_label(self):
        assert report._legend_columns(["head"]) == 1


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

    def test_main_page_takes_the_pipeline_results(self, tmp_path, pipeline_results):
        source = tmp_path / "pipeline.json"
        source.write_text(json.dumps(pipeline_results), encoding="utf-8")
        output = tmp_path / "index.qmd"
        report.main(
            [
                "--format",
                "page",
                "--input",
                str(tmp_path / "absent.json"),
                "--pipeline-input",
                str(source),
                "--output",
                str(output),
            ]
        )
        page = output.read_text(encoding="utf-8")
        assert "No performance profile has been published yet" in page
        assert "### Step by step" in page

    def test_main_page_shows_pipeline_placeholder_for_a_missing_file(self, tmp_path):
        output = tmp_path / "index.qmd"
        report.main(
            [
                "--format",
                "page",
                "--input",
                str(tmp_path / "absent.json"),
                "--pipeline-input",
                str(tmp_path / "absent-pipeline.json"),
                "--output",
                str(output),
            ]
        )
        assert "No pipeline profile has been published yet" in output.read_text(
            encoding="utf-8"
        )
