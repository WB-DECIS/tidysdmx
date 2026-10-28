"""Write mock IDS bronze data of any size: ``python -m benchmarks.wb_ids.generate``.

The mock is drawn from the committed profile of the real file, never from the
real file itself, so it can be rebuilt anywhere. Its unit is the time series —
one (country, counterpart, series) triple observed over some years:

1. A universe of triples the size of the real one is built from the profile:
   each country gets as many counterparts as it has in the real file, and each
   (country, counterpart) pair as many series as the real distribution of
   series per pair says, weighted by how common each counterpart and series is.
2. The universe is shuffled, and triples are taken in that order until their
   year spans, drawn from the real years-per-series distribution, add up to
   exactly the rows requested. A small mock is thus a random sample of whole
   series, spread across countries and series; the full size is the whole
   universe. Each triple's years are drawn from its series' and its country's
   years in the real file.
3. Rows are written partition by partition, in the real file's partitions of
   countries, each partition ordered by year, country and counterpart.

Values, metadata and footnotes are synthetic, drawn from the profile's
distributions. Keys are unique by construction. The same seed and profile give
the same bytes.
"""

import argparse
import json
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

from benchmarks.wb_ids import PROFILE_JSON
from benchmarks.wb_ids.profile import COLUMNS

Json = dict[str, Any]
_CHUNK: int = 200_000
# Weight given to a year in which a triple's series or country never occurs, so
# a span longer than the years it does occur in still finds enough years.
_RARE_YEAR: float = 1e-9
_WORDS: tuple[str, ...] = (
    "data", "debt", "external", "are", "based", "on", "reported", "by", "the",
    "Debtor", "Reporting", "System", "estimates", "for", "years", "prior", "to",
    "include", "public", "and", "publicly", "guaranteed", "loans", "from",
    "official", "creditors", "private", "sources", "of", "International",
    "Monetary", "Fund", "Balance", "Payments", "statistics", "unless",
    "otherwise", "specified", "in", "specific", "this", "series", "covers",
)  # fmt: skip


@dataclass(frozen=True)
class _Codes:
    """The code vocabularies, and the per-code weights the model draws with."""

    countries: list[str]
    counterparts: list[str]
    series: list[str]
    names: list[str]
    years: list[int]
    country_years: np.ndarray
    series_years: np.ndarray
    year_rows: np.ndarray


def load_profile(path: Path = PROFILE_JSON) -> Json:
    """Load a profile written by ``benchmarks.wb_ids.profile``.

    Args:
        path: The profile JSON.

    Returns:
        The profile.
    """
    profile: Json = json.loads(path.read_text(encoding="utf-8"))
    return profile


def generate(out: Path, rows: int, seed: int = 0, profile: Json | None = None) -> int:
    """Write a mock IDS bronze CSV of exactly ``rows`` rows.

    Args:
        out: The CSV to write; its directory is created if needed.
        rows: How many data rows to write.
        seed: Seed for every random draw.
        profile: The profile to draw from; the committed one when ``None``.

    Returns:
        The number of data rows written.

    Raises:
        ValueError: If ``rows`` is below 1, or more than every series in the
            universe observed in every year.
    """
    if rows < 1:
        raise ValueError(f"rows must be at least 1, got {rows}")
    profile = profile if profile is not None else load_profile()
    rng = np.random.default_rng(seed)
    codes = _codes(profile)
    country, counterpart, series = _universe(profile, codes, rng)
    spans = _spans(profile, len(country), rows, codes, rng)
    taken = len(spans)
    country, counterpart, series = country[:taken], counterpart[:taken], series[:taken]
    cells = _years(country, series, spans, codes, rng)
    metadata = _metadata(profile, country, series, spans, rows, rng)
    out.parent.mkdir(parents=True, exist_ok=True)
    writer = _Writer(profile, codes, rng)
    with pcsv.CSVWriter(out, writer.schema, write_options=writer.options) as sink:
        for year, triples in _blocks(profile, codes, country, counterpart, cells):
            sink.write_table(
                writer.table(year, country, counterpart, series, metadata, triples)
            )
    return int(cells.sum())


def _codes(profile: Json) -> _Codes:
    years = [int(year) for year in profile["years"]]
    countries = list(profile["countries"])
    series = list(profile["series"])
    return _Codes(
        countries=countries,
        counterparts=list(profile["counterparts"]),
        series=series,
        names=[profile["series"][code]["name"] for code in series],
        years=years,
        country_years=np.array(
            [profile["countries"][code]["by_year"] for code in countries], dtype=float
        ),
        series_years=np.array(
            [profile["series"][code]["by_year"] for code in series], dtype=float
        ),
        year_rows=np.array([profile["years"][str(y)] for y in years], dtype=float),
    )


def _universe(
    profile: Json, codes: _Codes, rng: np.random.Generator
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build every (country, counterpart, series) triple, in a random order."""
    keys = profile["keys"]
    counterpart_weight = _weights(keys["pairs_per_counterpart"], codes.counterparts)
    pair_country: list[np.ndarray] = []
    pair_counterpart: list[np.ndarray] = []
    for index, code in enumerate(codes.countries):
        count = min(keys["pairs_per_country"].get(code, 0), len(codes.counterparts))
        chosen = rng.choice(
            len(codes.counterparts), size=count, replace=False, p=counterpart_weight
        )
        pair_country.append(np.full(count, index))
        pair_counterpart.append(np.sort(chosen))
    countries = np.concatenate(pair_country)
    counterparts = np.concatenate(pair_counterpart)
    # The largest series counts go to the pairs whose country and counterpart
    # have the most rows (with some noise), so a counterpart like WLD, which
    # carries every aggregate series, stays the largest, as in the real file.
    draws = np.sort(
        np.clip(_draw(keys["series_per_pair"], len(countries), rng), 1, None)
    )
    counterpart_rows = np.array(
        [profile["counterparts"][c] for c in codes.counterparts]
    )
    pair_rows = (
        codes.country_years.sum(axis=1)[countries]
        * counterpart_rows[counterparts]
        * rng.uniform(0.5, 1.5, size=len(countries))
    )
    per_pair = np.minimum(draws[np.argsort(np.argsort(pair_rows))], len(codes.series))
    series_weight = codes.series_years.sum(axis=1)
    order = _top(np.log(series_weight / series_weight.sum()), len(countries), rng)
    chosen_series = order[np.arange(len(codes.series)) < per_pair[:, None]]
    shuffle = rng.permutation(len(chosen_series))
    return (
        np.repeat(countries, per_pair)[shuffle],
        np.repeat(counterparts, per_pair)[shuffle],
        chosen_series[shuffle],
    )


def _spans(
    profile: Json, triples: int, rows: int, codes: _Codes, rng: np.random.Generator
) -> np.ndarray:
    """Draw each triple's year count; keep the prefix that adds up to ``rows``.

    Raises:
        ValueError: If ``rows`` exceeds every triple observed in every year.
    """
    years = len(codes.years)
    if rows > triples * years:
        raise ValueError(
            f"rows={rows:,} exceeds the {triples * years:,} the universe can hold"
        )
    spans = np.clip(_draw(profile["keys"]["years_per_triple"], triples, rng), 1, years)
    total = np.cumsum(spans)
    while total[-1] < rows:
        # More rows than the drawn spans hold: lengthen random short spans.
        short = np.nonzero(spans < years)[0]
        grow = rng.choice(
            short, size=min(len(short), rows - int(total[-1])), replace=False
        )
        spans[grow] += 1
        total = np.cumsum(spans)
    taken = int(np.searchsorted(total, rows)) + 1
    spans = spans[:taken].copy()
    spans[-1] -= int(total[taken - 1]) - rows
    return spans


def _years(
    country: np.ndarray,
    series: np.ndarray,
    spans: np.ndarray,
    codes: _Codes,
    rng: np.random.Generator,
) -> np.ndarray:
    """Choose each triple's years; return a (triple, year) boolean matrix."""
    share = codes.year_rows / codes.year_rows.sum()
    country_p = codes.country_years / codes.country_years.sum(axis=1, keepdims=True)
    series_p = codes.series_years / codes.series_years.sum(axis=1, keepdims=True)
    cells = np.zeros((len(spans), len(codes.years)), dtype=bool)
    for start in range(0, len(spans), _CHUNK):
        part = slice(start, start + _CHUNK)
        weight = country_p[country[part]] * series_p[series[part]] / share
        scores = np.log(weight + _RARE_YEAR) + rng.gumbel(size=weight.shape)
        rank = np.argsort(np.argsort(-scores, axis=1), axis=1)
        cells[part] = rank < spans[part, None]
    return cells


def _metadata(
    profile: Json,
    country: np.ndarray,
    series: np.ndarray,
    spans: np.ndarray,
    rows: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Give some (country, series) keys a metadata text; return each triple's.

    Index 0 is the empty text. Keys are taken in random order until their rows
    reach the real file's share of non-empty metadata.
    """
    stats = profile["text"]["country_series_metadata"]
    target = round(stats["non_empty"] / profile["file"]["rows"] * rows)
    pool = max(1, stats["distinct"])
    keys, inverse = np.unique(country * 10_000 + series, return_inverse=True)
    key_rows = np.bincount(inverse, weights=spans)
    order = rng.permutation(len(keys))
    covered = np.cumsum(key_rows[order])
    chosen = order[: int(np.searchsorted(covered, target)) + (1 if target else 0)]
    text = np.zeros(len(keys), dtype=np.int64)
    text[chosen] = 1 + np.arange(len(chosen)) % pool
    return text[inverse]


def _blocks(
    profile: Json,
    codes: _Codes,
    country: np.ndarray,
    counterpart: np.ndarray,
    cells: np.ndarray,
) -> Iterator[tuple[int, np.ndarray]]:
    """Yield (year index, triple indices) in the real file's row order."""
    run_of = {
        code: i
        for i, run in enumerate(profile["sort"]["runs"])
        for code in run["countries"]
    }
    last = len(profile["sort"]["runs"])
    country_run = np.array([run_of.get(code, last) for code in codes.countries])
    country_rank = _lexical_rank(codes.countries)
    counterpart_rank = _lexical_rank(codes.counterparts)
    order = np.lexsort(
        (
            np.arange(len(country)),
            counterpart_rank[counterpart],
            country_rank[country],
            country_run[country],
        )
    )
    runs = country_run[country][order]
    for run in np.unique(runs):
        members = order[runs == run]
        for year in range(len(codes.years)):
            triples = members[cells[members, year]]
            if len(triples):
                yield year, triples


class _Writer:
    """Turns one (year, triples) block into an Arrow table of CSV strings."""

    def __init__(self, profile: Json, codes: _Codes, rng: np.random.Generator) -> None:
        self.rng = rng
        self.schema = pa.schema([(name, pa.string()) for name in COLUMNS])
        self.options = pcsv.WriteOptions(quoting_style="all_valid")
        self.countries = pa.array(codes.countries)
        self.counterparts = pa.array(codes.counterparts)
        self.series = pa.array(codes.series)
        self.names = pa.array(codes.names)
        self.years = pa.array([str(year) for year in codes.years])
        self.digits = pa.array([str(d) for d in range(10)])
        self.signs = pa.array(["", "-"])
        values = profile["values"]
        rows = profile["file"]["rows"]
        self.p_zero = values["zero"] / rows
        self.p_negative = values["negative"] / max(1, rows - values["zero"])
        log10 = values["abs_log10"]
        counts = np.array(log10["counts"], dtype=float)
        self.log10_p = counts / counts.sum()
        self.log10_low, self.log10_step = log10["low"], log10["step"]
        meta = profile["text"]["country_series_metadata"]
        foot = profile["text"]["footnote"]
        self.metadata = pa.array(["", *_texts(meta["distinct_lengths"], rng)])
        self.footnotes = pa.array(["", *_texts(foot["distinct_lengths"], rng)])
        self.p_footnote = foot["non_empty"] / rows

    def table(
        self,
        year: int,
        country: np.ndarray,
        counterpart: np.ndarray,
        series: np.ndarray,
        metadata: np.ndarray,
        triples: np.ndarray,
    ) -> pa.Table:
        n = len(triples)
        s = series[triples]
        return pa.table(
            [
                self.countries.take(country[triples]),
                self.counterparts.take(counterpart[triples]),
                self.series.take(s),
                self.names.take(s),
                self.years.take(np.full(n, year)),
                self._values(n),
                self.metadata.take(metadata[triples]),
                self._footnotes(n),
            ],
            schema=self.schema,
        )

    def _values(self, n: int) -> pa.Array:
        """Draw values as one-decimal text: zero, or ±10^x with x from the profile."""
        zero = self.rng.random(n) < self.p_zero
        negative = (self.rng.random(n) < self.p_negative) & ~zero
        bins = self.rng.choice(len(self.log10_p), size=n, p=self.log10_p)
        exponent = self.log10_low + (bins + self.rng.random(n)) * self.log10_step
        tenths = np.where(zero, 0, np.rint(10.0 ** (exponent + 1))).astype(np.int64)
        whole = pc.cast(pa.array(tenths // 10), pa.string())
        signed = pc.binary_join_element_wise(
            self.signs.take(negative.astype(np.int64)), whole, ""
        )
        return pc.binary_join_element_wise(signed, self.digits.take(tenths % 10), ".")

    def _footnotes(self, n: int) -> pa.Array:
        present = self.rng.random(n) < self.p_footnote
        which = (
            self.rng.integers(1, len(self.footnotes), size=n)
            if len(self.footnotes) > 1
            else 0
        )
        return self.footnotes.take(np.where(present, which, 0))


def _texts(lengths: Sequence[int], rng: np.random.Generator) -> list[str]:
    """Return one synthetic sentence per length, each exactly that long."""
    texts = []
    for length in lengths:
        words: list[str] = []
        while sum(len(w) + 1 for w in words) <= length:
            word = str(rng.choice(_WORDS))
            words.append(word + ("," if rng.random() < 0.1 else ""))
        text = " ".join(words)[: max(0, length - 1)].rstrip()
        texts.append((text + "." if length else "").ljust(length, ".")[:length])
    return texts


def _weights(counts: dict[str, int], codes: Sequence[str]) -> np.ndarray:
    weight = np.array([counts.get(code, 0) for code in codes], dtype=float)
    if weight.sum() == 0:
        weight[:] = 1.0
    return weight / weight.sum()


def _draw(histogram: Sequence[int], size: int, rng: np.random.Generator) -> np.ndarray:
    """Draw ``size`` values from a histogram indexed by value."""
    counts = np.array(histogram, dtype=float)
    return rng.choice(len(counts), size=size, p=counts / counts.sum())


def _top(log_weight: np.ndarray, rows: int, rng: np.random.Generator) -> np.ndarray:
    """Return, per row, all items ordered as a weighted draw without replacement."""
    order = np.empty((rows, len(log_weight)), dtype=np.int64)
    for start in range(0, rows, _CHUNK):
        stop = min(rows, start + _CHUNK)
        scores = log_weight + rng.gumbel(size=(stop - start, len(log_weight)))
        order[start:stop] = np.argsort(-scores, axis=1)
    return order


def _lexical_rank(codes: Sequence[str]) -> np.ndarray:
    rank = np.empty(len(codes), dtype=np.int64)
    rank[np.argsort(np.array(codes, dtype=object))] = np.arange(len(codes))
    return rank


def main(argv: Sequence[str] | None = None) -> int:
    """Write a mock CSV of ``--rows`` rows (or ``--scale`` times the real count).

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 on success.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.wb_ids.generate",
        description="Write mock IDS bronze data drawn from the committed profile.",
    )
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--rows", type=int, help="data rows to write")
    size.add_argument("--scale", type=float, help="fraction of the real row count")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--profile", type=Path, default=PROFILE_JSON)
    args = parser.parse_args(argv)
    profile = load_profile(args.profile)
    rows = (
        args.rows
        if args.rows is not None
        else round(args.scale * profile["file"]["rows"])
    )
    written = generate(args.out, rows, args.seed, profile)
    sys.stderr.write(f"Wrote {args.out}: {written:,} rows\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
