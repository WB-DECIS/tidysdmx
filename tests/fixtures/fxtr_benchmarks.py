"""Benchmark results for the report tests in tests/test_benchmarks.py."""

import pytest

# Every version maps the fixture's cases to the same output, until a test says not.
FINGERPRINTS = {"typical": "0123456789abcdef", "wide": "fedcba9876543210"}


def _record(version, scenario, rows, median, peak, status="ok", reason=None):
    measured = status == "ok"
    record = {
        "version": version,
        "scenario": scenario,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": rows * 100 if measured else None,
        "samples_s": [median] if measured else [],
        "median_s": median,
        "min_s": median,
        "peak_alloc_bytes": peak,
        "output_shape": [rows, 9] if measured else None,
    }
    if measured:
        record["output_fingerprint"] = FINGERPRINTS[scenario]
    return record


def _deps(pandas):
    return {
        "python": "3.11.14",
        "tidysdmx": "0.10.0",
        "pandas": pandas,
        "numpy": "2.3.4",
        "pysdmx": "1.20.0",
        "typeguard": "4.4.4",
    }


def make_results():
    """Return a fresh results dict: a PR head, its base, and one release.

    At 1M rows the head is twice as fast and half as hungry as the base on the
    typical table, and within 10% of v0.10.0. On the wide table the base
    errored and v0.10.0 was skipped.
    """
    mib = 2**20
    return {
        "schema_version": 1,
        "generated_at": "2026-10-23T12:00:00+00:00",
        "machine": {
            "platform": "Linux-6.8.0-x86_64-with-glibc2.39",
            "cpu": "AMD EPYC 7763 64-Core Processor",
            "cpu_count": 4,
            "memory_bytes": 16 * 2**30,
            "runner_image": "ubuntu24 20261020.1",
        },
        "sizes": [1_000, 1_000_000],
        "scenarios": ["typical", "wide"],
        "versions": [
            {
                "label": "head",
                "ref": "abc1234def",
                "commit": "abc1234def5678",
                "role": "target",
                "aliases": [],
                "deps": _deps("3.0.6"),
            },
            {
                "label": "base",
                "ref": "origin/dev",
                "commit": "def5678abc1234",
                "role": "compare",
                "aliases": [],
                "deps": _deps("2.3.3"),
            },
            {
                "label": "v0.10.0",
                "ref": "v0.10.0",
                "commit": "0123456789abcdef",
                "role": "baseline",
                "aliases": [],
                "deps": _deps("2.3.3"),
            },
        ],
        "results": [
            _record("head", "typical", 1_000, 0.010, 2 * mib),
            _record("base", "typical", 1_000, 0.011, 2 * mib),
            _record("v0.10.0", "typical", 1_000, 0.020, 4 * mib),
            _record("head", "typical", 1_000_000, 1.0, 300 * mib),
            _record("base", "typical", 1_000_000, 2.0, 600 * mib),
            _record("v0.10.0", "typical", 1_000_000, 1.05, 310 * mib),
            _record("head", "wide", 1_000, 0.02, 3 * mib),
            _record("base", "wide", 1_000, 0.02, 3 * mib),
            _record("v0.10.0", "wide", 1_000, 0.03, 5 * mib),
            _record("head", "wide", 1_000_000, 4.0, 900 * mib),
            _record("base", "wide", 1_000_000, None, None, "error", "MemoryError: "),
            _record(
                "v0.10.0",
                "wide",
                1_000_000,
                None,
                None,
                "skipped",
                "a smaller size took over 60 s",
            ),
        ],
    }


@pytest.fixture
def benchmark_results():
    """Return a fresh results dict. Function-scoped: tests modify it."""
    return make_results()
