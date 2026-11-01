"""Measure one pipeline benchmark case: ``python -m benchmarks.pipeline_case``.

``benchmarks.run --suite pipeline`` starts this module once per case, inside
the virtual environment of the tidysdmx version under test (with pyarrow and
psutil added) and with the harness root as the working directory. The record
goes to the file named by ``--result``, never to stdout. A scenario here is a
load configuration of the WB template pipeline (``benchmarks.wb_ids.pipeline``)
on a mock WB_IDS CSV that ``benchmarks.run`` generated once for every version:

- ``whole``: the notebook's run, one frame end to end, every column loaded;
- ``chunked``: the recommended production loop, ``CHUNKS`` chunks, without
  ``Series_Name`` and with the code columns as ``category``. It needs
  ``DatasetValidator``, so a version without it records a skipped case.
"""

import argparse
import gc
import json
import statistics
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.probe import assert_installed
from benchmarks.records import (
    PIPELINE_CONFIGS,
    PipelineRecord,
    PipelineStep,
    empty_pipeline_record,
)
from benchmarks.wb_ids import SCHEMA_JSON, TEMPLATE_XLSX

if TYPE_CHECKING:
    from pysdmx.model import Schema

    from benchmarks.wb_ids.pipeline import Result

CHUNKS: int = 5
SLOW_RUN_S: float = 30.0
CHUNKED_REASON: str = (
    "this version has no DatasetValidator, which the chunked configuration needs"
)
# pysdmx data types whose values the fingerprint sums as numbers rather than
# counting as text, so a version that keeps them as text still agrees.
NUMERIC_DATA_TYPES: frozenset[str] = frozenset(
    {
        "BIG_INTEGER",
        "COUNT",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INTEGER",
        "LONG",
        "NUMERIC",
        "SHORT",
    }
)


def repeats_for(rows: int) -> int:
    """Return how many runs a case of ``rows`` rows gets: 3 up to 1M rows, 1 above."""
    return 3 if rows <= 1_000_000 else 1


def pipeline_options(config: str, rows: int) -> dict[str, Any]:
    """Return the ``run_pipeline`` keywords for a configuration.

    Args:
        config: One of ``PIPELINE_CONFIGS``.
        rows: The case's row count; ``chunked`` splits it into ``CHUNKS``.

    Returns:
        Keyword arguments for ``benchmarks.wb_ids.pipeline.run_pipeline``.

    Raises:
        ValueError: If ``config`` is not a known configuration.
    """
    if config == "whole":
        return {}
    if config == "chunked":
        return {
            "chunk_rows": max(1, rows // CHUNKS),
            "skip_unused": True,
            "categories": True,
        }
    raise ValueError(f"Unknown configuration {config!r}; expected {PIPELINE_CONFIGS}")


def has_dataset_validator() -> bool:
    """Return whether the installed tidysdmx has ``DatasetValidator``."""
    import tidysdmx

    return hasattr(tidysdmx, "DatasetValidator")


def fingerprint_columns(schema: "Schema") -> list[tuple[str, bool]]:
    """Return the schema's components with whether each holds numbers.

    Args:
        schema: The target schema the pipeline standardises to.

    Returns:
        ``(component id, numeric)`` pairs, in the schema's order, for
        ``benchmarks.fingerprint.Fingerprint``.
    """
    return [
        (component.id, component.dtype.name in NUMERIC_DATA_TYPES)
        for component in schema.components
    ]


def run_case(
    config: str,
    rows: int,
    csv: Path,
    template: Path = TEMPLATE_XLSX,
    schema_json: Path = SCHEMA_JSON,
) -> PipelineRecord:
    """Run the pipeline in one configuration and measure every step.

    The pipeline runs ``repeats_for(rows)`` times, stopping early after a run
    slower than ``SLOW_RUN_S``. Each step's time is the median over the runs;
    its peak RSS is the first run's, in a fresh process, because later runs
    inherit the allocator's caches. The first run's output is fingerprinted
    chunk by chunk, so the whole and chunked configurations can be compared.

    Args:
        config: One of ``PIPELINE_CONFIGS``.
        rows: The mock's row count, which ``chunked`` divides by ``CHUNKS``.
        csv: The mock CSV ``benchmarks.run`` generated for this size.
        template: The mapping workbook.
        schema_json: The target schema, as ``benchmarks.wb_ids.schema`` writes it.

    Returns:
        A record with status ``"ok"``, or ``"skipped"`` when the configuration
        needs a feature this version lacks.
    """
    if config == "chunked" and not has_dataset_validator():
        return empty_pipeline_record(config, rows, "skipped", CHUNKED_REASON)

    # Imported here rather than at module level, so that a release that fails
    # to import becomes an error record in main() instead of a crash.
    from benchmarks.fingerprint import Fingerprint, digest_counts
    from benchmarks.wb_ids.pipeline import run_pipeline
    from benchmarks.wb_ids.schema import load_schema

    options = pipeline_options(config, rows)
    columns = fingerprint_columns(load_schema(schema_json))
    fingerprint = Fingerprint(columns)
    first: Result = run_pipeline(
        csv, template, schema_json, on_output=fingerprint.update, **options
    )
    totals = [first["total_seconds"]]
    step_samples = {step["step"]: [step["seconds"]] for step in first["steps"]}
    gc.collect()
    for _ in range(repeats_for(rows) - 1):
        if totals[-1] > SLOW_RUN_S:
            break
        result = run_pipeline(csv, template, schema_json, **options)
        totals.append(result["total_seconds"])
        for step in result["steps"]:
            step_samples.setdefault(step["step"], []).append(step["seconds"])
        gc.collect()

    steps: list[PipelineStep] = [
        {
            "step": step["step"],
            "samples_s": step_samples[step["step"]],
            "median_s": statistics.median(step_samples[step["step"]]),
            "peak_rss_bytes": step["rss_peak_bytes"],
            "rows": step["rows"],
        }
        for step in first["steps"]
    ]
    output = next((s for s in first["steps"] if s["step"] == "standardize"), None)
    by_type = first["validation"]["by_type"]
    return {
        "scenario": config,
        "rows": rows,
        "status": "ok",
        "reason": None,
        "input_bytes": csv.stat().st_size,
        "samples_s": totals,
        "median_s": statistics.median(totals),
        "min_s": min(totals),
        "peak_alloc_bytes": None,
        "output_shape": (
            None if output is None else [output["rows"] or 0, output["columns"] or 0]
        ),
        "output_fingerprint": fingerprint.digest(),
        "chunk_rows": options.get("chunk_rows"),
        "steps": steps,
        "peak_rss_bytes": first["peak_rss_bytes"],
        "validation": dict(by_type),
        "validation_fingerprint": digest_counts(by_type),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Measure one case and write its record to ``--result``.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 if the case was measured, 1 if it errored or was skipped. The record
        is written either way.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.pipeline_case",
        description="Measure one WB template pipeline benchmark case.",
    )
    parser.add_argument("--scenario", required=True, choices=PIPELINE_CONFIGS)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--template", type=Path, default=TEMPLATE_XLSX)
    parser.add_argument("--schema", type=Path, default=SCHEMA_JSON)
    parser.add_argument("--result", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        assert_installed()
        record = run_case(
            args.scenario, args.rows, args.csv, args.template, args.schema
        )
    except Exception as exc:
        # Not swallowed: the error becomes this case's record, which the run
        # reports. Any exception counts, since an old release can fail anywhere.
        record = empty_pipeline_record(
            args.scenario, args.rows, "error", f"{type(exc).__name__}: {exc}"
        )
    args.result.write_text(json.dumps(record), encoding="utf-8")
    return 0 if record["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
