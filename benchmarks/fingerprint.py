"""Fingerprints of a mapped output, so versions are compared like for like.

Versions that produce the same output get the same fingerprint, and one that
leaves values unmapped, drops a column or maps values differently gets another.
The report then leaves such a version out of the comparison: its time would say
nothing about speed. Both suites use it: ``benchmarks.case`` on one frame,
``benchmarks.pipeline_case`` on the whole output or on each chunk of it.
"""

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence

import pandas as pd

_DIGITS: int = 16
_WORD: int = 2**64


class Fingerprint:
    """A digest of the values one or more frames hold in given columns.

    Feed every frame of an output to :meth:`update`, then read
    :meth:`digest`. Each column contributes its name, its null count and, for a
    text column, how many times each distinct non-null value occurs as
    ``str``; for a numeric column, the count of its values and a wraparound sum
    of their ``float64`` bits. The digest therefore ignores row order, how the
    output was chunked, a column's dtype (``object``, ``string``, ``str`` or
    ``category``, which differ between pandas 2 and 3 and between load options)
    and how a null is spelled (``None``, ``NaN`` or ``pd.NA``). A categorical's
    unused categories do not count. A column no frame carried changes the
    digest instead of failing, so a version that drops a column is flagged, not
    lost.

    Args:
        columns: The columns to hash, each with whether it is numeric. A
            numeric column is coerced with ``pd.to_numeric`` first, so a
            version that keeps numbers as text agrees with one that does not.
    """

    def __init__(self, columns: Sequence[tuple[str, bool]]) -> None:
        self._columns = list(columns)
        self._seen: set[str] = set()
        self._nulls: Counter[str] = Counter()
        self._counts: dict[str, Counter[str]] = {}
        self._numeric: dict[str, tuple[int, int]] = {}

    def update(self, df: pd.DataFrame) -> None:
        """Add one frame's values to the digest.

        Args:
            df: The output, or one chunk of it.
        """
        for name, numeric in self._columns:
            if name not in df.columns:
                continue
            self._seen.add(name)
            column = df[name]
            self._nulls[name] += int(column.isna().sum())
            if numeric:
                values = (
                    pd.to_numeric(column, errors="coerce")
                    .dropna()
                    .to_numpy(dtype="float64")
                )
                # + 0.0 folds -0.0 into 0.0: equal values must hash alike.
                bits = (values + 0.0).view("uint64")
                count, checksum = self._numeric.get(name, (0, 0))
                checksum = (checksum + int(bits.sum(dtype="uint64"))) % _WORD
                self._numeric[name] = (count + len(values), checksum)
            else:
                counts = self._counts.setdefault(name, Counter())
                for value, count in column.value_counts(dropna=True).items():
                    if count:
                        counts[str(value)] += int(count)

    def digest(self) -> str:
        """Return the first 16 hex digits of a SHA-256 digest of what was fed."""
        digest = hashlib.sha256()
        for name, numeric in self._columns:
            entry: list[object]
            if name not in self._seen:
                entry = [name, None]
            elif numeric:
                count, checksum = self._numeric[name]
                entry = [name, self._nulls[name], count, checksum]
            else:
                entry = [name, self._nulls[name], sorted(self._counts[name].items())]
            digest.update(json.dumps(entry).encode("ascii"))
        return digest.hexdigest()[:_DIGITS]


def digest_counts(counts: Mapping[str, int]) -> str:
    """Return a short digest of a mapping of names to counts, such as errors by type.

    Args:
        counts: The counts; key order does not matter.

    Returns:
        The first 16 hex digits of a SHA-256 digest.
    """
    entry = sorted((str(key), int(value)) for key, value in counts.items())
    return hashlib.sha256(json.dumps(entry).encode("ascii")).hexdigest()[:_DIGITS]
