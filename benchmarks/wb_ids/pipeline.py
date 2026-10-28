"""Run and measure the notebook pipeline on an IDS-shaped CSV.

``python -m benchmarks.wb_ids.pipeline --csv PATH`` runs the steps of
``tests/test_pipeline_integration.py`` (the WB template notebook) with public
tidysdmx functions only, and times each one and samples its memory. The
template and schema default to the committed mock ones, which match the real
pipeline's, so the same command measures real data and mock data alike.

Each step's input is freed once the step has produced its output, so a step's
peak memory is its own cost plus the one frame it consumes.
"""

import argparse
import gc
import json
import platform
import sys
import time
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, TypeVar

import numpy as np
import pandas as pd
import pyarrow as pa
import pysdmx

import tidysdmx
from benchmarks.records import Machine
from benchmarks.wb_ids import SAMPLE_CSV, SCHEMA_JSON, TEMPLATE_XLSX
from benchmarks.wb_ids.memory import PeakRss, machine
from benchmarks.wb_ids.schema import ARTEFACT_ID, load_schema
from tidysdmx import (
    build_structure_map_from_template_wb,
    map_structures,
    parse_mapping_template_wb,
    standardize_output,
    validate_dataset_local,
)

SCHEMA_VERSION: int = 1
SOURCE_STRUCTURE_ID: str = "WB.DEC.DG:DS_IDS_BRONZE(1.0.0)"
REQUESTED_SHEET: str = "REQUESTED_IND_RAW"
SAMPLE_ERRORS: int = 10
_MB: float = 1e6

T = TypeVar("T")


class Step(TypedDict):
    """One pipeline step's measurements."""

    step: str
    seconds: float
    rss_start_bytes: int
    rss_peak_bytes: int
    rss_end_bytes: int
    rows: int | None
    columns: int | None
    frame_bytes: int | None


class Validation(TypedDict):
    """What the final validation found."""

    errors: int
    by_type: dict[str, int]
    sample: list[str]


class Result(TypedDict):
    """A pipeline run: where, on what, and each step's measurements."""

    schema_version: int
    generated_at: str
    machine: Machine
    versions: dict[str, str]
    input: dict[str, Any]
    steps: list[Step]
    total_seconds: float
    peak_rss_bytes: int
    validation: Validation


def _measure(name: str, steps: list[Step], work: Callable[[], T]) -> T:
    """Run one step, append its measurements to ``steps`` and return its output."""
    gc.collect()
    with PeakRss() as peak:
        start = time.perf_counter()
        output = work()
        seconds = time.perf_counter() - start
    frame = output if isinstance(output, pd.DataFrame) else None
    steps.append(
        {
            "step": name,
            "seconds": seconds,
            "rss_start_bytes": peak.start_bytes,
            "rss_peak_bytes": peak.peak_bytes,
            "rss_end_bytes": peak.end_bytes,
            "rows": None if frame is None else len(frame),
            "columns": None if frame is None else len(frame.columns),
            "frame_bytes": (
                None if frame is None else int(frame.memory_usage(deep=True).sum())
            ),
        }
    )
    return output


def requested_series(mappings: dict[str, pd.DataFrame]) -> list[str]:
    """Return the series the template asks for, from its REQUESTED_IND_RAW sheet.

    Args:
        mappings: The workbook, as ``parse_mapping_template_wb`` returns it.

    Returns:
        The non-empty values of the sheet's first column, as written: a code
        with stray whitespace matches nothing, just as it would in ``isin``.

    Raises:
        ValueError: If the workbook has no REQUESTED_IND_RAW sheet.
    """
    sheet = mappings.get(REQUESTED_SHEET)
    if sheet is None:
        raise ValueError(f"The template has no {REQUESTED_SHEET} sheet")
    return sheet.iloc[:, 0].dropna().astype(str).tolist()


def run_pipeline(
    csv: Path, template: Path = TEMPLATE_XLSX, schema_json: Path = SCHEMA_JSON
) -> Result:
    """Run the notebook pipeline on ``csv`` and measure every step.

    Args:
        csv: An IDS-shaped bronze CSV.
        template: The WB mapping workbook.
        schema_json: The target schema, as ``benchmarks.wb_ids.schema`` writes it.

    Returns:
        Per-step time and memory, and what the final validation reported.
    """
    schema = load_schema(schema_json)
    steps: list[Step] = []
    started = time.perf_counter()
    mappings = _measure(
        "read_template", steps, lambda: parse_mapping_template_wb(template)
    )
    df = _measure("load", steps, lambda: pd.read_csv(csv))
    requested = requested_series(mappings)
    df = _measure(
        "filter",
        steps,
        lambda: df[df["Series"].isin(requested)].reset_index(drop=True),
    )
    structure_map = _measure(
        "structure_map",
        steps,
        lambda: build_structure_map_from_template_wb(
            mappings,
            source_structure_id=SOURCE_STRUCTURE_ID,
            target_structure_id=ARTEFACT_ID,
        ),
    )
    df = _measure("map", steps, lambda: map_structures(df, structure_map))
    df = _measure(
        "standardize",
        steps,
        lambda: standardize_output(df, artefact_id=ARTEFACT_ID, schema=schema),
    )
    errors = _measure(
        "validate", steps, lambda: validate_dataset_local(df, schema=schema)
    )
    total = time.perf_counter() - started
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine(),
        "versions": {
            "python": platform.python_version(),
            "tidysdmx": tidysdmx.__version__,
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
            "numpy": np.__version__,
            "pysdmx": pysdmx.__version__,
        },
        "input": {"csv": csv.name, "bytes": csv.stat().st_size},
        "steps": steps,
        "total_seconds": total,
        "peak_rss_bytes": max(step["rss_peak_bytes"] for step in steps),
        "validation": _summarise(errors),
    }


def _summarise(errors: pd.DataFrame) -> Validation:
    by_type = Counter(errors["Validation"].astype(str))
    return {
        "errors": len(errors),
        "by_type": dict(sorted(by_type.items())),
        "sample": [
            str(message)[:200] for message in errors["Error"].head(SAMPLE_ERRORS)
        ],
    }


def to_markdown(result: Result) -> str:
    """Render a run as a Markdown table, for a terminal or a CI job summary.

    Args:
        result: A run from ``run_pipeline``.

    Returns:
        A heading, one table row per step, and the validation summary.
    """
    lines = [
        f"### Pipeline on `{result['input']['csv']}` "
        f"({result['input']['bytes'] / _MB:,.0f} MB)",
        "",
        "| Step | Seconds | Rows | Columns | Frame MB | RSS start MB "
        "| RSS peak MB | RSS end MB |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for step in result["steps"]:
        lines.append(
            f"| {step['step']} | {step['seconds']:.2f} | {_count(step['rows'])} "
            f"| {_count(step['columns'])} | {_megabytes(step['frame_bytes'])} "
            f"| {step['rss_start_bytes'] / _MB:,.0f} "
            f"| {step['rss_peak_bytes'] / _MB:,.0f} "
            f"| {step['rss_end_bytes'] / _MB:,.0f} |"
        )
    validation = result["validation"]
    by_type = ", ".join(f"{k}: {v}" for k, v in validation["by_type"].items())
    lines += [
        "",
        f"Total {result['total_seconds']:.1f} s, peak RSS "
        f"{result['peak_rss_bytes'] / _MB:,.0f} MB. Validation errors: "
        f"{validation['errors']}" + (f" ({by_type})" if by_type else "") + ".",
    ]
    return "\n".join(lines) + "\n"


def _count(value: int | None) -> str:
    return "" if value is None else f"{value:,}"


def _megabytes(value: int | None) -> str:
    return "" if value is None else f"{value / _MB:,.0f}"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline on ``--csv``; print a table and optionally write JSON.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 when the pipeline ran, whatever validation found.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.wb_ids.pipeline",
        description="Run and measure the WB template pipeline on an IDS-shaped CSV.",
    )
    parser.add_argument("--csv", type=Path, default=SAMPLE_CSV)
    parser.add_argument("--template", type=Path, default=TEMPLATE_XLSX)
    parser.add_argument("--schema", type=Path, default=SCHEMA_JSON)
    parser.add_argument("--out", type=Path, help="write the run as JSON here")
    parser.add_argument("--summary", type=Path, help="append the table here")
    args = parser.parse_args(argv)
    result = run_pipeline(args.csv, args.template, args.schema)
    table = to_markdown(result)
    sys.stdout.write(table)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(result, indent=1) + "\n"
        args.out.write_text(text, encoding="utf-8", newline="\n")
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
