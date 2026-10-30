"""Profile an IDS-shaped CSV: ``python -m benchmarks.wb_ids.profile``.

The file is streamed with pyarrow in fixed-size blocks, so a 15 GB input is
profiled in a few minutes with memory bounded by its key count, not its size.
The profile is a JSON document of aggregate statistics only — sizes,
cardinalities, frequencies, orderings and value distributions — never rows. It
is what ``benchmarks.wb_ids.generate`` builds mock data from and what
``docs/datasets/wb-ids.md`` reports, and profiling a mock and comparing it with
the committed real profile is how the mock's fidelity is checked.
"""

import argparse
import gc
import json
import platform
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

from benchmarks.wb_ids import CODE_COLUMNS, COLUMNS, PROFILE_JSON, TEXT_COLUMNS
from benchmarks.wb_ids.memory import PeakRss, machine

SCHEMA_VERSION: int = 1
BLOCK_SIZE: int = 64 << 20
LOAD_ROWS: int = 2_000_000
QUANTILES: tuple[float, ...] = (
    0.0,
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    0.75,
    0.9,
    0.95,
    0.99,
    1.0,
)
# Non-zero |Value| is summarised as a histogram of log10|v| in 0.1-wide bins.
LOG10_LOW: float = -3.0
LOG10_HIGH: float = 17.0
LOG10_STEP: float = 0.1
# Each code column is numbered in order of appearance; a key packs the three
# numbers into one int64, 12 bits each, so at most 4096 distinct codes apiece.
_ID_BITS: int = 12
_ID_CAP: int = 1 << _ID_BITS
_ONE_DECIMAL: str = r"^-?\d+\.\d$"
_DISTINCT_TEXT_CAP: int = 100_000

Json = dict[str, Any]


def stream_batches(
    path: Path, block_size: int = BLOCK_SIZE
) -> Iterator[pa.RecordBatch]:
    """Read an IDS-shaped CSV block by block, every column as a string.

    Args:
        path: The CSV file.
        block_size: Bytes per block.

    Yields:
        One record batch per block. Empty fields are empty strings, not nulls.
    """
    reader = pcsv.open_csv(
        path,
        read_options=pcsv.ReadOptions(block_size=block_size),
        convert_options=pcsv.ConvertOptions(
            column_types=dict.fromkeys(COLUMNS, pa.string()),
            strings_can_be_null=False,
        ),
    )
    yield from reader


class _Vocabulary:
    """Numbers the distinct values of one code column in order of appearance."""

    def __init__(self, column: str) -> None:
        self.column = column
        self.ids: dict[str, int] = {}
        self.rows = np.zeros(0, dtype=np.int64)

    def encode(self, values: pa.Array) -> np.ndarray:
        encoded = pc.dictionary_encode(values)
        lookup = np.array(
            [
                self.ids.setdefault(v, len(self.ids))
                for v in encoded.dictionary.to_pylist()
            ],
            dtype=np.int64,
        )
        if len(self.ids) > _ID_CAP:
            raise ValueError(
                f"{self.column} has more than {_ID_CAP} distinct codes; "
                "the key packing in benchmarks.wb_ids.profile needs widening"
            )
        ids = lookup[encoded.indices.to_numpy(zero_copy_only=False)]
        counts = np.bincount(ids, minlength=len(self.ids))
        self.rows = np.pad(self.rows, (0, len(counts) - len(self.rows))) + counts
        return ids

    def codes(self) -> list[str]:
        return list(self.ids)


class _Order:
    """Tracks how the file is ordered: runs of non-decreasing Year, and ties."""

    def __init__(self) -> None:
        self.row = 0
        self.last: tuple[int, str, str] | None = None
        self.run_starts: list[tuple[int, int]] = []
        self.run_ends: list[int] = []
        self.run_countries: list[set[str]] = []
        self.country_breaks = 0
        self.counterpart_breaks = 0

    def update(
        self, years: np.ndarray, country: pa.Array, counterpart: pa.Array
    ) -> None:
        if self.last is None:
            self.run_starts.append((0, int(years[0])))
            self.run_countries.append(set())
        else:
            self._compare_boundary(self.last, years, country, counterpart)
        previous_year = np.concatenate(([years[0]], years[:-1]))
        decreasing = np.nonzero(years < previous_year)[0]
        for position in decreasing:
            self.run_ends.append(int(years[position - 1]))
            self.run_starts.append((self.row + int(position), int(years[position])))
        cuts = [0, *(int(position) for position in decreasing), len(years)]
        for i, (start, stop) in enumerate(pairwise(cuts)):
            if i > 0:
                self.run_countries.append(set())
            segment = country.slice(start, stop - start)
            self.run_countries[-1].update(pc.unique(segment).to_pylist())
        if len(years) > 1:
            same_year = years[1:] == years[:-1]
            head, tail = country.slice(0, len(country) - 1), country.slice(1)
            country_down = pc.less(tail, head).to_numpy(zero_copy_only=False)
            country_same = pc.equal(tail, head).to_numpy(zero_copy_only=False)
            cp_head, cp_tail = (
                counterpart.slice(0, len(country) - 1),
                counterpart.slice(1),
            )
            cp_down = pc.less(cp_tail, cp_head).to_numpy(zero_copy_only=False)
            self.country_breaks += int(np.sum(same_year & country_down))
            self.counterpart_breaks += int(np.sum(same_year & country_same & cp_down))
        self.row += len(years)
        self.last = (int(years[-1]), country[-1].as_py(), counterpart[-1].as_py())

    def _compare_boundary(
        self,
        last: tuple[int, str, str],
        years: np.ndarray,
        country: pa.Array,
        counterpart: pa.Array,
    ) -> None:
        """Check the row before this batch against the batch's first row."""
        year, first_country, first_cp = (
            int(years[0]),
            country[0].as_py(),
            counterpart[0].as_py(),
        )
        if year < last[0]:
            self.run_ends.append(last[0])
            self.run_starts.append((self.row, year))
            self.run_countries.append(set())
        elif year == last[0] and first_country < last[1]:
            self.country_breaks += 1
        elif (year, first_country) == last[:2] and first_cp < last[2]:
            self.counterpart_breaks += 1

    def result(self) -> Json:
        ends = [*self.run_ends, self.last[0] if self.last else 0]
        bounds = [start for start, _ in self.run_starts[1:]] + [self.row]
        runs = [
            {
                "start_row": start,
                "rows": end_row - start,
                "first_year": first,
                "last_year": last,
                "countries": sorted(countries),
            }
            for (start, first), last, end_row, countries in zip(
                self.run_starts, ends, bounds, self.run_countries, strict=True
            )
        ]
        return {
            "year_runs": len(runs),
            "runs": runs,
            "country_order_breaks": self.country_breaks,
            "counterpart_order_breaks": self.counterpart_breaks,
        }


class _Profiler:
    """Accumulates a profile one record batch at a time."""

    def __init__(self) -> None:
        self.rows = 0
        self.chars: Counter[str] = Counter()
        self.max_length: Counter[str] = Counter()
        self.empty: Counter[str] = Counter()
        self.vocab = {name: _Vocabulary(name) for name in CODE_COLUMNS}
        self.year_rows: Counter[int] = Counter()
        self.series_year: Counter[tuple[int, int]] = Counter()
        self.country_year: Counter[tuple[int, int]] = Counter()
        self.keys_by_year: dict[int, list[np.ndarray]] = {}
        self.names: dict[str, set[str]] = {}
        self.order = _Order()
        self.values: Counter[str] = Counter()
        self.log10_bins = np.arange(LOG10_LOW, LOG10_HIGH + LOG10_STEP / 2, LOG10_STEP)
        self.log10_counts = np.zeros(len(self.log10_bins) - 1, dtype=np.int64)
        self.value_lengths: Counter[int] = Counter()
        self.text_values: dict[str, Counter[str]] = {
            name: Counter() for name in TEXT_COLUMNS
        }
        self.text_lengths: dict[str, Counter[int]] = {
            name: Counter() for name in TEXT_COLUMNS
        }
        self.text_keys: dict[str, set[tuple[int, int]]] = {
            name: set() for name in TEXT_COLUMNS
        }

    def update(self, batch: pa.RecordBatch) -> None:
        self.rows += batch.num_rows
        self._lengths(batch)
        ids = {
            name: self.vocab[name].encode(batch.column(name)) for name in CODE_COLUMNS
        }
        years = pc.cast(batch.column("Year"), pa.int16()).to_numpy().astype(np.int64)
        self._frequencies(ids, years)
        self._keys(ids, years)
        self._names(batch)
        self.order.update(
            years, batch.column("Country_Code"), batch.column("Counterpart_Area_Code")
        )
        self._values(batch.column("Value"))
        for name in TEXT_COLUMNS:
            self._text(name, batch.column(name), ids)

    def _lengths(self, batch: pa.RecordBatch) -> None:
        for name in COLUMNS:
            lengths = pc.utf8_length(batch.column(name))
            self.chars[name] += int(pc.sum(lengths).as_py() or 0)
            self.max_length[name] = max(
                self.max_length[name], int(pc.max(lengths).as_py() or 0)
            )
            self.empty[name] += int(pc.sum(pc.equal(lengths, 0)).as_py() or 0)

    def _frequencies(self, ids: dict[str, np.ndarray], years: np.ndarray) -> None:
        unique, counts = np.unique(years, return_counts=True)
        self.year_rows.update(dict(zip(unique.tolist(), counts.tolist(), strict=True)))
        self.series_year.update(_count_pairs(ids["Series"], years))
        self.country_year.update(_count_pairs(ids["Country_Code"], years))

    def _keys(self, ids: dict[str, np.ndarray], years: np.ndarray) -> None:
        triple = _pack(ids["Country_Code"], ids["Counterpart_Area_Code"], ids["Series"])
        for year in np.unique(years):
            self.keys_by_year.setdefault(int(year), []).append(triple[years == year])

    def _names(self, batch: pa.RecordBatch) -> None:
        pairs = (
            pa.table({"s": batch.column("Series"), "n": batch.column("Series_Name")})
            .group_by(["s", "n"])
            .aggregate([])
        )
        for series, name in zip(
            pairs["s"].to_pylist(), pairs["n"].to_pylist(), strict=True
        ):
            self.names.setdefault(series, set()).add(name)

    def _values(self, text: pa.Array) -> None:
        values = pc.cast(text, pa.float64()).to_numpy()
        self.values["zero"] += int(np.sum(values == 0))
        self.values["negative"] += int(np.sum(values < 0))
        self.values["one_decimal"] += int(
            pc.sum(pc.match_substring_regex(text, _ONE_DECIMAL)).as_py() or 0
        )
        non_zero = np.abs(values[values != 0])
        logs = np.clip(np.log10(non_zero), LOG10_LOW, LOG10_HIGH - LOG10_STEP / 2)
        self.log10_counts += np.histogram(logs, bins=self.log10_bins)[0]
        lengths = pc.utf8_length(text).to_numpy()
        self.value_lengths.update(dict(enumerate(np.bincount(lengths).tolist())))

    def _text(self, name: str, column: pa.Array, ids: dict[str, np.ndarray]) -> None:
        present = pc.not_equal(pc.utf8_length(column), 0)
        positions = np.nonzero(present.to_numpy(zero_copy_only=False))[0]
        if len(positions) == 0:
            return
        texts = column.take(pa.array(positions)).to_pylist()
        if len(self.text_values[name]) < _DISTINCT_TEXT_CAP:
            self.text_values[name].update(texts)
        self.text_lengths[name].update(len(text) for text in texts)
        country, series = ids["Country_Code"][positions], ids["Series"][positions]
        self.text_keys[name].update(zip(country.tolist(), series.tolist(), strict=True))

    def result(self) -> Json:
        years = sorted(self.year_rows)
        country = self.vocab["Country_Code"]
        counterpart = self.vocab["Counterpart_Area_Code"]
        series = self.vocab["Series"]
        return {
            "file": {"rows": self.rows},
            "columns": self._columns(),
            "years": {str(year): self.year_rows[year] for year in years},
            "countries": _by_year(country, self.country_year, years),
            "counterparts": dict(
                zip(counterpart.codes(), counterpart.rows.tolist(), strict=True)
            ),
            "series": {
                code: {
                    "name": sorted(self.names[code])[0],
                    "names": len(self.names[code]),
                    **row,
                }
                for code, row in _by_year(series, self.series_year, years).items()
            },
            "keys": self._key_stats(),
            "sort": self.order.result(),
            "values": self._value_stats(),
            "text": {name: self._text_stats(name) for name in TEXT_COLUMNS},
        }

    def _columns(self) -> Json:
        distinct = {name: len(self.vocab[name].ids) for name in CODE_COLUMNS}
        distinct["Year"] = len(self.year_rows)
        distinct["Series_Name"] = len(
            {n for names in self.names.values() for n in names}
        )
        for name in TEXT_COLUMNS:
            distinct[name] = len(self.text_values[name])
        total = sum(self.chars.values())
        return {
            name: {
                "chars": self.chars[name],
                "mean_length": self.chars[name] / self.rows if self.rows else 0.0,
                "max_length": self.max_length[name],
                "empty": self.empty[name],
                "distinct": distinct.get(name),
                "chars_share": self.chars[name] / total if total else 0.0,
            }
            for name in COLUMNS
        }

    def _key_stats(self) -> Json:
        duplicates = 0
        triples = np.zeros(0, dtype=np.int64)
        years_per_triple = np.zeros(0, dtype=np.int64)
        for year in sorted(self.keys_by_year):
            keys = np.concatenate(self.keys_by_year[year])
            unique = np.unique(keys)
            duplicates += len(keys) - len(unique)
            triples, years_per_triple = _merge_counts(triples, years_per_triple, unique)
        self.keys_by_year.clear()
        pairs, pair_index = np.unique(triples >> _ID_BITS, return_inverse=True)
        rows_per_pair = np.bincount(pair_index, weights=years_per_triple).astype(
            np.int64
        )
        series_per_pair = np.bincount(pair_index)
        countries = self.vocab["Country_Code"].codes()
        counterparts = self.vocab["Counterpart_Area_Code"].codes()
        pair_country = pairs >> _ID_BITS
        pair_counterpart = pairs & (_ID_CAP - 1)
        return {
            "duplicates": duplicates,
            "distinct_pairs": len(pairs),
            "distinct_triples": len(triples),
            "years_per_triple": np.bincount(years_per_triple).tolist(),
            "series_per_pair": np.bincount(series_per_pair).tolist(),
            "rows_per_pair": _quantiles(rows_per_pair),
            "pairs_per_country": _named_counts(pair_country, countries),
            "pairs_per_counterpart": _named_counts(pair_counterpart, counterparts),
        }

    def _value_stats(self) -> Json:
        return {
            "zero": self.values["zero"],
            "negative": self.values["negative"],
            "one_decimal": self.values["one_decimal"],
            "abs_log10": {
                "low": LOG10_LOW,
                "step": LOG10_STEP,
                "counts": self.log10_counts.tolist(),
            },
            "text_length": _histogram(self.value_lengths),
        }

    def _text_stats(self, name: str) -> Json:
        lengths = self.text_lengths[name]
        return {
            "non_empty": sum(lengths.values()),
            "distinct": len(self.text_values[name]),
            "country_series_keys": len(self.text_keys[name]),
            "length": _histogram(lengths),
            "distinct_lengths": sorted(len(text) for text in self.text_values[name]),
        }


def _pack(
    country: np.ndarray, counterpart: np.ndarray, series: np.ndarray
) -> np.ndarray:
    return (((country << _ID_BITS) | counterpart) << _ID_BITS) | series


def _count_pairs(codes: np.ndarray, years: np.ndarray) -> dict[tuple[int, int], int]:
    """Count rows per (code id, year)."""
    unique, counts = np.unique(codes * 10_000 + years, return_counts=True)
    return {
        (int(u // 10_000), int(u % 10_000)): int(n)
        for u, n in zip(unique, counts, strict=True)
    }


def _by_year(
    vocab: _Vocabulary, counts: Counter[tuple[int, int]], years: list[int]
) -> Json:
    position = {year: i for i, year in enumerate(years)}
    out: Json = {}
    for code, code_id in vocab.ids.items():
        by_year = [0] * len(years)
        out[code] = {"rows": int(vocab.rows[code_id]), "by_year": by_year}
    codes = vocab.codes()
    for (code_id, year), n in counts.items():
        out[codes[code_id]]["by_year"][position[year]] = n
    return out


def _merge_counts(
    keys: np.ndarray, counts: np.ndarray, new: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    merged, inverse = np.unique(np.concatenate((keys, new)), return_inverse=True)
    weights = np.concatenate((counts, np.ones(len(new), dtype=np.int64)))
    return merged, np.bincount(inverse, weights=weights).astype(np.int64)


def _named_counts(ids: np.ndarray, names: list[str]) -> dict[str, int]:
    counts = np.bincount(ids, minlength=len(names))
    return {name: int(n) for name, n in zip(names, counts, strict=True) if n}


def _quantiles(values: np.ndarray) -> dict[str, float]:
    if len(values) == 0:
        return {}
    points = np.quantile(values, QUANTILES)
    return {f"{q:g}": float(v) for q, v in zip(QUANTILES, points, strict=True)}


def _histogram(counts: Counter[int]) -> list[int]:
    """Return counts indexed by value: ``[n(0), n(1), ...]``."""
    if not counts:
        return []
    out = [0] * (max(counts) + 1)
    for value, n in counts.items():
        out[value] += n
    return out


def _file_facts(path: Path, rows: int) -> Json:
    with path.open("rb") as handle:
        head = handle.readline() + handle.readline()
    size = path.stat().st_size
    first, second = head.splitlines(keepends=True)[:2]
    return {
        "rows": rows,
        "bytes": size,
        "bytes_per_row": size / rows if rows else 0.0,
        "line_terminator": "CRLF" if first.endswith(b"\r\n") else "LF",
        "quoted": first.startswith(b'"') and second.startswith(b'"'),
    }


def measure_load(path: Path, rows: int, total_rows: int) -> Json:
    """Measure what loading the first ``rows`` rows costs, several ways.

    Each way runs once, after a garbage collection, and its frame is freed
    before the next. The extrapolation is linear in rows.

    Args:
        path: The CSV file.
        rows: Rows to load.
        total_rows: The file's row count, for the extrapolation.

    Returns:
        ``rows``, and per way: seconds, in-memory bytes, peak RSS rise, bytes
        per row and the extrapolated bytes for the whole file.
    """
    code_types = dict.fromkeys((*CODE_COLUMNS, "Series_Name", "Year"), "category")
    ways = {
        "pandas_default": lambda: pd.read_csv(path, nrows=rows),
        "pandas_without_series_name": lambda: pd.read_csv(
            path, nrows=rows, usecols=[c for c in COLUMNS if c != "Series_Name"]
        ),
        "pandas_category": lambda: pd.read_csv(path, nrows=rows, dtype=code_types),
        "arrow_table": lambda: _arrow_head(path, rows),
    }
    out: Json = {"rows": rows}
    for name, load in ways.items():
        gc.collect()
        with PeakRss() as peak:
            start = time.perf_counter()
            frame = load()
            seconds = time.perf_counter() - start
        size = (
            int(frame.nbytes)
            if isinstance(frame, pa.Table)
            else int(frame.memory_usage(deep=True).sum())
        )
        loaded = frame.num_rows if isinstance(frame, pa.Table) else len(frame)
        del frame
        out[name] = {
            "seconds": seconds,
            "bytes": size,
            "peak_rss_delta_bytes": peak.peak_delta_bytes,
            "bytes_per_row": size / loaded,
            "extrapolated_bytes": int(size / loaded * total_rows),
        }
    return out


def _arrow_head(path: Path, rows: int) -> pa.Table:
    batches: list[pa.RecordBatch] = []
    taken = 0
    for batch in stream_batches(path):
        batches.append(batch.slice(0, rows - taken))
        taken += batches[-1].num_rows
        if taken >= rows:
            break
    return pa.Table.from_batches(batches)


def profile_csv(path: Path, load_rows: int = 0) -> Json:
    """Profile an IDS-shaped CSV.

    Args:
        path: The CSV file.
        load_rows: Rows to load with pandas for the memory measurement; 0 skips it.

    Returns:
        The profile, as a JSON-serialisable dict.

    Raises:
        ValueError: If the header differs from the IDS bronze layout.
    """
    profiler = _Profiler()
    started = time.perf_counter()
    with PeakRss() as peak:
        for batch in stream_batches(path):
            if tuple(batch.schema.names) != COLUMNS:
                raise ValueError(
                    f"Expected columns {list(COLUMNS)}, got {batch.schema.names}"
                )
            profiler.update(batch)
        body = profiler.result()
    seconds = time.perf_counter() - started
    rows = body["file"]["rows"]
    body["file"] = {**_file_facts(path, rows), "header": list(COLUMNS)}
    profile: Json = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": path.name,
        "machine": machine(),
        "versions": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
            "numpy": np.__version__,
        },
        "profiling": {"seconds": seconds, "peak_rss_bytes": peak.peak_bytes},
        **body,
    }
    if load_rows:
        profile["load"] = measure_load(path, min(load_rows, rows), rows)
    return profile


def dumps(profile: Json) -> str:
    """Serialise a profile with one key per line but each number list on one line."""
    text = json.dumps(profile, indent=1, ensure_ascii=False)
    return (
        re.sub(
            r"\[\s*([-\d.e+,\s]*?)\s*\]",
            lambda m: "[" + ", ".join(m.group(1).split()).replace(",,", ",") + "]",
            text,
        )
        + "\n"
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Profile ``--csv`` into ``--out``.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 on success.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.wb_ids.profile",
        description="Profile an IDS-shaped CSV into JSON.",
    )
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=PROFILE_JSON)
    parser.add_argument(
        "--load-rows",
        type=int,
        default=LOAD_ROWS,
        help="rows to load with pandas for the memory measurement (0 skips it)",
    )
    args = parser.parse_args(argv)
    profile = profile_csv(args.csv, args.load_rows)
    args.out.write_text(dumps(profile), encoding="utf-8", newline="\n")
    sys.stderr.write(
        f"Wrote {args.out}: {profile['file']['rows']:,} rows in "
        f"{profile['profiling']['seconds']:.0f} s\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
