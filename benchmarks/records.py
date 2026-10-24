"""The benchmark results schema, shared by every module in ``benchmarks``.

Standard library only: ``benchmarks.run`` imports it under
``uv run --no-project``, where neither pandas nor tidysdmx is installed.
"""

import re
from typing import Literal, NotRequired, TypedDict

SCHEMA_VERSION: int = 1
SCENARIOS: tuple[str, ...] = ("typical", "high_cardinality", "regex_heavy", "wide")
WORKTREE: str = "WORKTREE"

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
