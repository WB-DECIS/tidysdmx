"""Run and measure the notebook pipeline on an IDS-shaped CSV.

``python -m benchmarks.wb_ids.pipeline --csv PATH`` runs the steps of
``tests/test_pipeline_integration.py`` (the WB template notebook) with public
tidysdmx functions only, and times each one and samples its memory. The
template and schema default to the committed mock ones, which match the real
pipeline's, so the same command measures real data and mock data alike.

Two ways of running the same steps:

- Whole frame (the default): the notebook's chain, one DataFrame end to end.
  Each step's input is freed once the step has produced its output, so a
  step's peak memory is its own cost plus the one frame it consumes.
- Chunked (``--chunk-rows N``): ``pd.read_csv(chunksize=N)``, with filter, map
  and standardize run on each chunk, ``DatasetValidator`` accumulating the
  checks, and ``--out-csv`` appending each chunk's output. Memory is bounded by
  the chunk, whatever the file's size. A step's row shows its time summed over
  the chunks and its highest peak.

``--skip-unused`` and ``--categories`` make the load lean in either mode: the
first does not read ``Series_Name``, which the pipeline never maps, and the
second loads the code columns and ``Year`` as ``category``, which
``map_structures`` keeps categorical through to the output.
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
from benchmarks.wb_ids.profile import CODE_COLUMNS, COLUMNS, TEXT_COLUMNS
from benchmarks.wb_ids.schema import ARTEFACT_ID, load_schema
from tidysdmx import (
    DatasetValidator,
    build_structure_map_from_template_wb,
    map_structures,
    parse_mapping_template_wb,
    standardize_output,
    validate_dataset_local,
)

SCHEMA_VERSION: int = 2
SOURCE_STRUCTURE_ID: str = "WB.DEC.DG:DS_IDS_BRONZE(1.0.0)"
REQUESTED_SHEET: str = "REQUESTED_IND_RAW"
SAMPLE_ERRORS: int = 10
# The column the pipeline never reads: 60% of the file's text.
UNUSED_COLUMN: str = "Series_Name"
# Loaded as category by --categories; the text columns get an explicit str so
# a chunk in which they are empty is not inferred as float.
CATEGORY_COLUMNS: tuple[str, ...] = (*CODE_COLUMNS, "Year")
# The order the steps are reported in, whichever mode interleaves them.
STEP_ORDER: tuple[str, ...] = (
    "read_template",
    "load",
    "filter",
    "structure_map",
    "map",
    "standardize",
    "validate",
    "write",
)
_MB: float = 1e6

T = TypeVar("T")


class Step(TypedDict):
    """One pipeline step's measurements, summed over chunks where it ran per chunk."""

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
    """A pipeline run: where, on what, how, and each step's measurements."""

    schema_version: int
    generated_at: str
    machine: Machine
    versions: dict[str, str]
    input: dict[str, Any]
    steps: list[Step]
    total_seconds: float
    peak_rss_bytes: int
    validation: Validation


class _Steps:
    """Per-step measurements; a step measured several times is summed."""

    def __init__(self) -> None:
        self._by_name: dict[str, Step] = {}

    @property
    def steps(self) -> list[Step]:
        """The steps measured so far, in ``STEP_ORDER``."""
        return sorted(self._by_name.values(), key=lambda s: STEP_ORDER.index(s["step"]))

    def measure(self, name: str, work: Callable[[], T]) -> T:
        """Run one step, record its time and memory, and return its output.

        A step run once per chunk sums its seconds and rows, keeps its highest
        peak RSS and largest frame, and its first start and last end.
        """
        gc.collect()
        with PeakRss() as peak:
            start = time.perf_counter()
            output = work()
            seconds = time.perf_counter() - start
        frame = output if isinstance(output, pd.DataFrame) else None
        rows = None if frame is None else len(frame)
        columns = None if frame is None else len(frame.columns)
        frame_bytes = (
            None if frame is None else int(frame.memory_usage(deep=True).sum())
        )
        step = self._by_name.get(name)
        if step is None:
            self._by_name[name] = {
                "step": name,
                "seconds": seconds,
                "rss_start_bytes": peak.start_bytes,
                "rss_peak_bytes": peak.peak_bytes,
                "rss_end_bytes": peak.end_bytes,
                "rows": rows,
                "columns": columns,
                "frame_bytes": frame_bytes,
            }
            return output
        step["seconds"] += seconds
        step["rss_peak_bytes"] = max(step["rss_peak_bytes"], peak.peak_bytes)
        step["rss_end_bytes"] = peak.end_bytes
        if rows is not None:
            step["rows"] = (step["rows"] or 0) + rows
        if columns is not None:
            step["columns"] = columns
        if frame_bytes is not None:
            step["frame_bytes"] = max(step["frame_bytes"] or 0, frame_bytes)
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


def read_csv_kwargs(skip_unused: bool, categories: bool) -> dict[str, Any]:
    """Return the ``pd.read_csv`` arguments for a lean load of an IDS CSV.

    Args:
        skip_unused: Do not read ``Series_Name``.
        categories: Read the code columns and ``Year`` as ``category``, and
            the two text columns as ``str``.

    Returns:
        Keyword arguments for ``pd.read_csv``; empty for the default load.
    """
    kwargs: dict[str, Any] = {}
    if skip_unused:
        kwargs["usecols"] = [column for column in COLUMNS if column != UNUSED_COLUMN]
    if categories:
        kwargs["dtype"] = {
            **dict.fromkeys(CATEGORY_COLUMNS, "category"),
            **dict.fromkeys(TEXT_COLUMNS, "str"),
        }
    return kwargs


def run_pipeline(
    csv: Path,
    template: Path = TEMPLATE_XLSX,
    schema_json: Path = SCHEMA_JSON,
    *,
    chunk_rows: int | None = None,
    skip_unused: bool = False,
    categories: bool = False,
    out_csv: Path | None = None,
) -> Result:
    """Run the notebook pipeline on ``csv`` and measure every step.

    Args:
        csv: An IDS-shaped bronze CSV.
        template: The WB mapping workbook.
        schema_json: The target schema, as ``benchmarks.wb_ids.schema`` writes it.
        chunk_rows: Rows per chunk. None runs the whole frame at once.
        skip_unused: Do not load ``Series_Name``.
        categories: Load the code columns and ``Year`` as ``category``.
        out_csv: Write the standardised output here (overwritten; appended to
            chunk by chunk in chunked mode).

    Returns:
        Per-step time and memory, and what the final validation reported.

    Raises:
        ValueError: If ``chunk_rows`` is given but not positive.
    """
    if chunk_rows is not None and chunk_rows <= 0:
        raise ValueError(f"chunk_rows must be positive, got {chunk_rows}")
    schema = load_schema(schema_json)
    steps = _Steps()
    started = time.perf_counter()
    mappings = steps.measure(
        "read_template", lambda: parse_mapping_template_wb(template)
    )
    requested = requested_series(mappings)
    kwargs = read_csv_kwargs(skip_unused, categories)
    if chunk_rows is None:
        chunks = 1
        errors = _run_whole(
            csv, mappings, requested, schema, steps, kwargs, out_csv=out_csv
        )
    else:
        chunks, errors = _run_chunked(
            csv, mappings, requested, schema, steps, kwargs, chunk_rows, out_csv
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
        "input": {
            "csv": csv.name,
            "bytes": csv.stat().st_size,
            "chunk_rows": chunk_rows,
            "chunks": chunks,
            "skip_unused": skip_unused,
            "categories": categories,
            "out_csv": None if out_csv is None else out_csv.name,
        },
        "steps": steps.steps,
        "total_seconds": total,
        "peak_rss_bytes": max(step["rss_peak_bytes"] for step in steps.steps),
        "validation": _summarise(errors),
    }


def _structure_map(mappings: dict[str, pd.DataFrame]) -> Any:
    return build_structure_map_from_template_wb(
        mappings,
        source_structure_id=SOURCE_STRUCTURE_ID,
        target_structure_id=ARTEFACT_ID,
    )


def _run_whole(
    csv: Path,
    mappings: dict[str, pd.DataFrame],
    requested: list[str],
    schema: Any,
    steps: _Steps,
    kwargs: dict[str, Any],
    out_csv: Path | None,
) -> pd.DataFrame:
    """The notebook's chain on one frame; returns the validation errors."""
    df = steps.measure("load", lambda: pd.read_csv(csv, **kwargs))
    df = steps.measure(
        "filter",
        lambda: df[df["Series"].isin(requested)].reset_index(drop=True),
    )
    structure_map = steps.measure("structure_map", lambda: _structure_map(mappings))
    df = steps.measure("map", lambda: map_structures(df, structure_map))
    df = steps.measure(
        "standardize",
        lambda: standardize_output(df, artefact_id=ARTEFACT_ID, schema=schema),
    )
    errors = steps.measure(
        "validate", lambda: validate_dataset_local(df, schema=schema)
    )
    if out_csv is not None:
        steps.measure("write", lambda: df.to_csv(out_csv, index=False))
    return errors


def _run_chunked(
    csv: Path,
    mappings: dict[str, pd.DataFrame],
    requested: list[str],
    schema: Any,
    steps: _Steps,
    kwargs: dict[str, Any],
    chunk_rows: int,
    out_csv: Path | None,
) -> tuple[int, pd.DataFrame]:
    """The same chain chunk by chunk; returns the chunk count and the errors."""
    structure_map = steps.measure("structure_map", lambda: _structure_map(mappings))
    validator = DatasetValidator(schema)
    chunks = 0
    written = False
    with pd.read_csv(csv, chunksize=chunk_rows, **kwargs) as reader:
        while True:
            chunk = steps.measure("load", lambda: next(reader, None))
            if chunk is None:
                break
            chunks += 1
            written = _process_chunk(
                chunk,
                requested,
                structure_map,
                schema,
                validator,
                steps,
                out_csv,
                written,
            )
            # Drop the last reference before the next chunk is read.
            del chunk
    errors = steps.measure("validate", validator.errors)
    return chunks, errors


def _process_chunk(
    chunk: pd.DataFrame,
    requested: list[str],
    structure_map: Any,
    schema: Any,
    validator: DatasetValidator,
    steps: _Steps,
    out_csv: Path | None,
    written: bool,
) -> bool:
    """Filter, map, standardise, validate and write one chunk.

    Returns:
        Whether output has been written so far, so the next chunk appends.
    """
    # No reset_index: the labels read_csv gives are file positions.
    kept = steps.measure("filter", lambda: chunk[chunk["Series"].isin(requested)])
    if kept.empty:
        return written
    mapped = steps.measure("map", lambda: map_structures(kept, structure_map))
    out = steps.measure(
        "standardize",
        lambda: standardize_output(mapped, artefact_id=ARTEFACT_ID, schema=schema),
    )
    steps.measure("validate", lambda: validator.update(out))
    if out_csv is None:
        return written
    steps.measure(
        "write",
        lambda: out.to_csv(
            out_csv, mode="a" if written else "w", header=not written, index=False
        ),
    )
    return True


def _summarise(errors: pd.DataFrame) -> Validation:
    by_type = Counter(errors["Validation"].astype(str))
    return {
        "errors": len(errors),
        "by_type": dict(sorted(by_type.items())),
        "sample": [
            str(message)[:200] for message in errors["Error"].head(SAMPLE_ERRORS)
        ],
    }


def describe_mode(result: Result) -> str:
    """Describe how a run loaded and processed its input, for a heading.

    Args:
        result: A run from ``run_pipeline``.

    Returns:
        ``whole frame`` or ``chunks of N rows (K chunks)``, then the lean-load
        options in force.
    """
    source = result["input"]
    chunk_rows = source.get("chunk_rows")
    if chunk_rows is None:
        mode = "whole frame"
    else:
        mode = f"chunks of {chunk_rows:,} rows ({source.get('chunks', 0):,} chunks)"
    lean = [
        name
        for name, flag in (
            ("without Series_Name", source.get("skip_unused")),
            ("code columns as category", source.get("categories")),
        )
        if flag
    ]
    return mode + (", " + ", ".join(lean) if lean else "")


def to_markdown(result: Result) -> str:
    """Render a run as a Markdown table, for a terminal or a CI job summary.

    Args:
        result: A run from ``run_pipeline``.

    Returns:
        A heading, the run's mode, one table row per step, and the validation
        summary.
    """
    lines = [
        f"### Pipeline on `{result['input']['csv']}` "
        f"({result['input']['bytes'] / _MB:,.0f} MB), {describe_mode(result)}",
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
    parser.add_argument(
        "--chunk-rows",
        type=int,
        help="process the file in chunks of this many rows (default: whole frame)",
    )
    parser.add_argument(
        "--skip-unused",
        action="store_true",
        help=f"do not load {UNUSED_COLUMN}, which the pipeline never maps",
    )
    parser.add_argument(
        "--categories",
        action="store_true",
        help="load the code columns and Year as category",
    )
    parser.add_argument(
        "--out-csv", type=Path, help="write the standardised output here"
    )
    parser.add_argument("--out", type=Path, help="write the run as JSON here")
    parser.add_argument("--summary", type=Path, help="append the table here")
    args = parser.parse_args(argv)
    result = run_pipeline(
        args.csv,
        args.template,
        args.schema,
        chunk_rows=args.chunk_rows,
        skip_unused=args.skip_unused,
        categories=args.categories,
        out_csv=args.out_csv,
    )
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
