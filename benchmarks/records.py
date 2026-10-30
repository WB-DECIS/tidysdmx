"""The benchmark results schema and machine description, shared by ``benchmarks``.

Standard library only: ``benchmarks.run`` imports it under
``uv run --no-project``, where neither pandas nor tidysdmx is installed.

Two suites write this schema. The ``mapping`` suite measures ``map_structures``
on generated dataset shapes (``SCENARIOS``); the ``pipeline`` suite measures
the whole WB template pipeline on the WB_IDS mock, step by step, in two load
configurations (``PIPELINE_CONFIGS``). A pipeline record is a ``Record`` with
the per-step measurements added, and its ``scenario`` is the configuration.
"""

import os
import platform
import re
from pathlib import Path
from typing import Literal, NotRequired, TypedDict

SCHEMA_VERSION: int = 1
SCENARIOS: tuple[str, ...] = ("typical", "high_cardinality", "regex_heavy", "wide")
WORKTREE: str = "WORKTREE"

Suite = Literal["mapping", "pipeline"]
SUITES: tuple[Suite, ...] = ("mapping", "pipeline")
# The pipeline suite's "scenarios": how the mock is loaded and processed.
PIPELINE_CONFIGS: tuple[str, ...] = ("whole", "chunked")
# The pipeline's steps, in the order the runner reports them.
PIPELINE_STEPS: tuple[str, ...] = (
    "read_template",
    "load",
    "filter",
    "structure_map",
    "map",
    "standardize",
    "validate",
    "write",
)
# The release asset each suite's results are attached as: <prefix>-<tag>.json.
# docs.yml fetches them by these prefixes, so a new suite needs a new prefix.
ASSET_PREFIX: dict[str, str] = {"mapping": "benchmarks", "pipeline": "pipeline"}

Status = Literal["ok", "skipped", "error"]
Role = Literal["target", "compare", "baseline"]

_RELEASE_TAG: re.Pattern[str] = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


class Record(TypedDict):
    """One case (version, scenario, size): its measurements, or why there are none."""

    scenario: str
    rows: int
    status: Status
    reason: str | None
    input_bytes: int | None
    samples_s: list[float]
    median_s: float | None
    min_s: float | None
    peak_alloc_bytes: int | None
    output_shape: list[int] | None
    # Absent from records of cases that measured nothing, and from files written
    # before it was added; the report treats a missing fingerprint as comparable.
    output_fingerprint: NotRequired[str]
    version: NotRequired[str]


class PipelineStep(TypedDict):
    """One pipeline step's measurements over a case's repeats."""

    step: str
    samples_s: list[float]
    median_s: float
    # The first run's, in a fresh process: later runs inherit allocator caches.
    peak_rss_bytes: int
    rows: int | None


class PipelineRecord(Record):
    """A pipeline case: ``Record`` plus the steps, the peak RSS and the errors found.

    ``median_s`` and ``samples_s`` hold the whole run's seconds; ``scenario``
    is the configuration (see ``PIPELINE_CONFIGS``); ``peak_alloc_bytes`` is
    always ``None``, since memory is sampled as RSS, not traced.
    """

    chunk_rows: int | None
    steps: list[PipelineStep]
    peak_rss_bytes: int | None
    validation: dict[str, int] | None
    validation_fingerprint: NotRequired[str]


class VersionInfo(TypedDict):
    """A measured version: where it came from and what it ran with."""

    label: str
    ref: str
    commit: str
    role: Role
    aliases: list[str]
    deps: dict[str, str | None]


class Machine(TypedDict):
    """The machine a benchmark ran on."""

    platform: str
    cpu: str
    cpu_count: int | None
    memory_bytes: int | None
    runner_image: str | None


class Results(TypedDict):
    """A whole benchmark run, as written by ``benchmarks.run``."""

    schema_version: int
    generated_at: str
    machine: Machine
    sizes: list[int]
    scenarios: list[str]
    versions: list[VersionInfo]
    results: list[Record]
    # Absent from files written before the pipeline suite existed: mapping.
    suite: NotRequired[Suite]


def release_key(tag: str) -> tuple[int, int, int] | None:
    """Return a release tag's version as a sortable tuple.

    Args:
        tag: A git tag such as ``"v0.10.0"``.

    Returns:
        ``(major, minor, patch)``, or ``None`` if ``tag`` is not a final
        ``vX.Y.Z`` release tag.
    """
    match = _RELEASE_TAG.match(tag)
    if match is None:
        return None
    return int(match[1]), int(match[2]), int(match[3])


def empty_record(scenario: str, rows: int, status: Status, reason: str) -> Record:
    """Return the record of a case that produced no measurement.

    Args:
        scenario: The case's scenario.
        rows: The case's row count.
        status: ``"skipped"`` or ``"error"``.
        reason: Why nothing was measured.

    Returns:
        A record whose measurements are all empty.
    """
    return {
        "scenario": scenario,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": None,
        "samples_s": [],
        "median_s": None,
        "min_s": None,
        "peak_alloc_bytes": None,
        "output_shape": None,
    }


def empty_pipeline_record(
    config: str, rows: int, status: Status, reason: str
) -> PipelineRecord:
    """Return the record of a pipeline case that produced no measurement.

    Args:
        config: The case's configuration (see ``PIPELINE_CONFIGS``).
        rows: The case's row count.
        status: ``"skipped"`` or ``"error"``.
        reason: Why nothing was measured.

    Returns:
        A pipeline record whose measurements are all empty.
    """
    return {
        "scenario": config,
        "rows": rows,
        "status": status,
        "reason": reason,
        "input_bytes": None,
        "samples_s": [],
        "median_s": None,
        "min_s": None,
        "peak_alloc_bytes": None,
        "output_shape": None,
        "chunk_rows": None,
        "steps": [],
        "peak_rss_bytes": None,
        "validation": None,
    }


def suite_of(results: Results) -> Suite:
    """Return which suite wrote ``results``; files without the key are ``mapping``."""
    return results.get("suite", "mapping")


def asset_name(suite: Suite, tag: str) -> str:
    """Return the name of a suite's results file as attached to a release.

    Args:
        suite: The suite that wrote the results.
        tag: The release tag, such as ``"v0.11.0"``.

    Returns:
        ``benchmarks-<tag>.json`` for the mapping suite, ``pipeline-<tag>.json``
        for the pipeline suite.
    """
    return f"{ASSET_PREFIX[suite]}-{tag}.json"


def machine_info() -> Machine:
    """Describe the machine this runs on."""
    return {
        "platform": platform.platform(),
        "cpu": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": _memory_bytes(),
        "runner_image": _runner_image(),
    }


def _cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.partition(":")[2].strip()
    return platform.processor() or "unknown CPU"


def _memory_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def _runner_image() -> str | None:
    # GitHub-hosted runners really do export these mixed-case names, and
    # environment names are case-sensitive on Linux: capitalising them (SIM112)
    # would read nothing there.
    image = os.environ.get("ImageOS")  # noqa: SIM112
    version = os.environ.get("ImageVersion")  # noqa: SIM112
    return f"{image} {version}" if image and version else None
