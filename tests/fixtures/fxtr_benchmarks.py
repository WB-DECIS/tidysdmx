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


# Every version standardises the mock to the same output and finds the same
# validation errors, until a test says not.
PIPELINE_FINGERPRINT = "e64105b5214c7223"
VALIDATION_FINGERPRINT = "0f1e2d3c4b5a6978"
VALIDATION = {"codelist_ids": 49}
CHUNKED_REASON = (
    "this version has no DatasetValidator, which the chunked configuration needs"
)
# Each step's seconds at 1M rows on the head; base takes twice as long and
# v0.10.0 five percent longer. The 1k-row runs are a hundredth of that.
_HEAD_STEPS = {
    "read_template": 0.15,
    "load": 0.8,
    "filter": 0.1,
    "structure_map": 0.4,
    "map": 0.9,
    "standardize": 0.12,
    "validate": 0.7,
}
_FACTORS = {"head": 1.0, "base": 2.0, "v0.10.0": 1.05}


def _pipeline_record(version, config, rows, status="ok", reason=None):
    measured = status == "ok"
    factor = _FACTORS[version] * (1 if rows == 1_000_000 else 0.01)
    gib = 2**30
    record = {
        "version": version,
        "scenario": config,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": rows * 100 if measured else None,
        "samples_s": [],
        "median_s": None,
        "min_s": None,
        "peak_alloc_bytes": None,
        "output_shape": [rows // 2, 12] if measured else None,
        "chunk_rows": None,
        "steps": [],
        "peak_rss_bytes": None,
        "validation": None,
    }
    if not measured:
        return record
    total = round(sum(_HEAD_STEPS.values()) * factor, 6)
    peak = int(gib * _FACTORS[version] * (1 if rows == 1_000_000 else 0.15))
    record.update(
        samples_s=[total, total * 1.01, total * 0.99],
        median_s=total,
        min_s=total * 0.99,
        output_fingerprint=PIPELINE_FINGERPRINT,
        chunk_rows=rows // 5 if config == "chunked" else None,
        steps=[
            {
                "step": step,
                "samples_s": [round(seconds * factor, 6)],
                "median_s": round(seconds * factor, 6),
                "peak_rss_bytes": peak,
                "rows": rows // 2,
            }
            for step, seconds in _HEAD_STEPS.items()
        ],
        peak_rss_bytes=peak,
        validation=dict(VALIDATION),
        validation_fingerprint=VALIDATION_FINGERPRINT,
    )
    return record


def make_pipeline_results():
    """Return a fresh pipeline results dict: a PR head, its base, and one release.

    At 1M rows the head's whole-frame run takes 3.17 s against the base's
    6.34 s and v0.10.0's 3.33 s, peaking at 1, 2 and 1.05 GiB of RSS. The
    release has no DatasetValidator, so its chunked cases are skipped.
    """
    cases = [
        (config, rows) for rows in (1_000, 1_000_000) for config in ("whole", "chunked")
    ]
    records = []
    for config, rows in cases:
        for version in ("head", "base", "v0.10.0"):
            if version == "v0.10.0" and config == "chunked":
                records.append(
                    _pipeline_record(version, config, rows, "skipped", CHUNKED_REASON)
                )
            else:
                records.append(_pipeline_record(version, config, rows))
    deps = {**_deps("3.0.6"), "pyarrow": "26.0.0", "psutil": "7.2.2"}
    return {
        "schema_version": 1,
        "generated_at": "2026-10-29T12:00:00+00:00",
        "machine": {
            "platform": "Linux-6.8.0-x86_64-with-glibc2.39",
            "cpu": "AMD EPYC 7763 64-Core Processor",
            "cpu_count": 4,
            "memory_bytes": 16 * 2**30,
            "runner_image": "ubuntu24 20261020.1",
        },
        "sizes": [1_000, 1_000_000],
        "scenarios": ["whole", "chunked"],
        "versions": [
            {
                "label": "head",
                "ref": "abc1234def",
                "commit": "abc1234def5678",
                "role": "target",
                "aliases": [],
                "deps": deps,
            },
            {
                "label": "base",
                "ref": "origin/dev",
                "commit": "def5678abc1234",
                "role": "compare",
                "aliases": [],
                "deps": deps,
            },
            {
                "label": "v0.10.0",
                "ref": "v0.10.0",
                "commit": "0123456789abcdef",
                "role": "baseline",
                "aliases": [],
                "deps": {**_deps("2.3.3"), "pyarrow": None, "psutil": "7.2.2"},
            },
        ],
        "results": records,
        "suite": "pipeline",
    }


@pytest.fixture
def pipeline_results():
    """Return a fresh pipeline results dict. Function-scoped: tests modify it."""
    return make_pipeline_results()
