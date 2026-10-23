"""Measure one benchmark case: ``python -m benchmarks.case``.

``benchmarks.run`` starts this module once per case, inside the virtual
environment of the tidysdmx version under test and with the harness root as
the working directory. The record goes to the file named by ``--result``,
never to stdout, because tidysdmx 0.8.0 reports progress with ``print()``.
"""

import argparse
import gc
import hashlib
import json
import statistics
import sys
import time
import tracemalloc
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path

import pandas as pd
from pysdmx.model.map import StructureMap

from benchmarks.records import SCENARIOS, Record, empty_record
from benchmarks.scenarios import build, targets

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


def output_fingerprint(df: pd.DataFrame, columns: Sequence[str]) -> str:
    """Return a short hash of the values that ``columns`` of a mapped output hold.

    Versions that map a case the same way get the same fingerprint, and one
    that leaves values unmapped, or maps them differently, gets another. It
    hashes each column's name, its null count and how many times each distinct
    non-null value occurs, as ``str``. So it ignores row order, the column's
    dtype (``object``, ``string`` or ``str``, which differ between pandas 2 and
    3) and how a null is spelled (``None``, ``NaN`` or ``pd.NA``).

    Args:
        df: The output of ``map_structures``.
        columns: The columns to hash, such as ``benchmarks.scenarios.targets``.
            One that ``df`` lacks changes the fingerprint instead of failing,
            so a version that drops a column is flagged, not lost.

    Returns:
        The first 16 hex digits of a SHA-256 digest.
    """
    digest = hashlib.sha256()
    for name in columns:
        if name not in df.columns:
            entry: list[object] = [name, None]
        else:
            column = df[name]
            counts: Counter[str] = Counter()
            for value, count in column.value_counts(dropna=True).items():
                counts[str(value)] += int(count)
            entry = [name, int(column.isna().sum()), sorted(counts.items())]
        digest.update(json.dumps(entry).encode("ascii"))
    return digest.hexdigest()[:16]


def run_case(scenario: str, rows: int, seed: int = 0) -> Record:
    """Time ``map_structures`` on one scenario and measure its peak allocation.

    A warm-up call on the first ``WARMUP_ROWS`` rows absorbs typeguard's
    instrumentation, imports and regex compilation. Timing stops early after a
    call slower than ``SLOW_CALL_S``. Memory is not traced when the median
    exceeds ``MEMORY_SKIP_S``, because tracemalloc slows Python-level code. The
    first call's output is fingerprinted once the clock has stopped.

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
    fingerprint = ""
    for attempt in range(repeats_for(rows)):
        start = time.perf_counter()
        result = map_structures(df, structure_map)
        samples.append(time.perf_counter() - start)
        output_shape = [int(n) for n in result.shape]
        if attempt == 0:
            fingerprint = output_fingerprint(result, targets(structure_map))
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
        "output_fingerprint": fingerprint,
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
