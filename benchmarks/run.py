"""Benchmark ``map_structures`` across tidysdmx versions: ``python -m benchmarks.run``.

Each version is installed from its git ref into a virtual environment of its
own, with the dependencies it declares, so a release that requires pandas 2
and one that requires pandas 3 are each measured as users install them. Every
case runs in a fresh subprocess, and the cases are interleaved across
versions, so slow drift on the machine spreads over every version instead of
landing on one.

Standard library only: it runs under ``uv run --no-project``.
"""

import argparse
import io
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar, cast

from benchmarks.records import (
    SCENARIOS,
    SCHEMA_VERSION,
    WORKTREE,
    Machine,
    Record,
    Results,
    Role,
    VersionInfo,
    empty_record,
    release_key,
)

DEFAULT_SIZES: tuple[int, ...] = (1_000, 10_000, 100_000, 1_000_000)
BUDGET_S: float = 60.0
CASE_TIMEOUT_S: float = 900.0
PYTHON: str = "3.11"

# Not __name__: under `python -m` that is "__main__".
logger = logging.getLogger("benchmarks.run")
T = TypeVar("T")


@dataclass(frozen=True)
class Version:
    """One tidysdmx version to measure.

    Attributes:
        label: The name the results use for it.
        ref: The git ref it is built from, or ``WORKTREE``.
        role: ``"target"``, ``"compare"`` or ``"baseline"``.
        commit: The commit ``ref`` resolved to, or ``WORKTREE``.
        aliases: Labels of other requested versions at the same commit.
    """

    label: str
    ref: str
    role: Role
    commit: str
    aliases: tuple[str, ...] = ()


CaseRunner = Callable[[Version, str, int], Record]
Installer = Callable[[Version], tuple[Path, dict[str, str | None]]]


def select_baselines(tags: Iterable[str], target: str, count: int) -> list[str]:
    """Return the ``count`` newest release tags older than ``target``.

    Args:
        tags: The repository's tags; non-release tags are ignored.
        target: The profiled ref. When it is not a release tag (a branch, a
            commit, the working tree), every release counts as older.
        count: How many to return.

    Returns:
        Release tags, newest first.
    """
    releases = sorted(
        (key, tag) for tag in tags if (key := release_key(tag)) is not None
    )
    ceiling = release_key(target)
    older = [tag for key, tag in releases if ceiling is None or key < ceiling]
    return older[::-1][:count]


def parse_named_ref(text: str) -> tuple[str, str]:
    """Split ``LABEL=REF`` into its label and ref.

    Args:
        text: ``LABEL=REF``, or a bare ``REF`` that is its own label.

    Returns:
        ``(label, ref)``.

    Raises:
        ValueError: If either side of the ``=`` is empty.
    """
    label, separator, ref = text.partition("=")
    if not separator:
        return text, text
    if not label or not ref:
        raise ValueError(f"Expected LABEL=REF or REF, got {text!r}")
    return label, ref


def merge_same_commit(versions: Sequence[Version]) -> list[Version]:
    """Measure each commit once.

    A version at the same commit as an earlier one is dropped, and its label
    becomes one of the earlier version's aliases.

    Args:
        versions: Versions in priority order: target, compares, baselines.

    Returns:
        The versions, with duplicates folded into the first of their commit.
    """
    merged: list[Version] = []
    for version in versions:
        for position, kept in enumerate(merged):
            if kept.commit == version.commit:
                merged[position] = replace(kept, aliases=(*kept.aliases, version.label))
                break
        else:
            merged.append(version)
    return merged


def plan_versions(
    target: str,
    target_label: str | None,
    compare: Sequence[str],
    baselines: int,
    tags: Sequence[str],
    resolve: Callable[[str], str],
) -> list[Version]:
    """Return the versions to measure, target first.

    Args:
        target: The git ref to profile, or ``WORKTREE``.
        target_label: Its label; by default the ref, or ``"worktree"``.
        compare: Extra versions, each ``LABEL=REF`` or ``REF``.
        baselines: How many earlier releases to add.
        tags: The repository's tags.
        resolve: Maps a ref to its commit.

    Returns:
        Target, then compares, then baselines newest first; each commit once.
    """
    label = target_label or ("worktree" if target == WORKTREE else target)
    planned = [Version(label, target, "target", resolve(target))]
    for text in compare:
        name, ref = parse_named_ref(text)
        planned.append(Version(name, ref, "compare", resolve(ref)))
    for tag in select_baselines(tags, target, baselines):
        planned.append(Version(tag, tag, "baseline", resolve(tag)))
    return merge_same_commit(planned)


def rotated(items: Sequence[T], shift: int) -> list[T]:
    """Return ``items`` rotated left by ``shift`` places, modulo its length."""
    if not items:
        return []
    shift %= len(items)
    return [*items[shift:], *items[:shift]]


def exceeds_budget(record: Record) -> bool:
    """Return whether a measured case's median exceeded ``BUDGET_S``."""
    median = record["median_s"]
    return record["status"] == "ok" and median is not None and median > BUDGET_S


def run_grid(
    versions: Sequence[Version],
    scenarios: Sequence[str],
    sizes: Sequence[int],
    run_case: CaseRunner,
) -> list[Record]:
    """Measure every (scenario, size) case for every version.

    Sizes run smallest first. Within a case the version order rotates, so no
    version always runs first. Once a version's median for a scenario exceeds
    ``BUDGET_S``, its larger sizes of that scenario are recorded as skipped.

    Args:
        versions: The versions to measure.
        scenarios: The scenarios to run.
        sizes: Row counts.
        run_case: Measures one version on one case.

    Returns:
        One record per version and case, labelled with the version.
    """
    over_budget: set[tuple[str, str]] = set()
    records: list[Record] = []
    cases = [(scenario, rows) for scenario in scenarios for rows in sorted(sizes)]
    for number, (scenario, rows) in enumerate(cases):
        for version in rotated(versions, number):
            if (version.label, scenario) in over_budget:
                record = empty_record(
                    scenario,
                    rows,
                    "skipped",
                    f"a smaller size took over {BUDGET_S:.0f} s",
                )
            else:
                record = run_case(version, scenario, rows)
                if exceeds_budget(record):
                    over_budget.add((version.label, scenario))
            record["version"] = version.label
            records.append(record)
    return records


def _run(command: Sequence[str], cwd: Path | None = None) -> str:
    """Run ``command`` and return its stripped stdout.

    Raises:
        RuntimeError: If it exits non-zero; the message carries its stderr.
    """
    completed = subprocess.run(
        list(command), cwd=cwd, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed:\n{completed.stderr.strip()}")
    return completed.stdout.strip()


def _slug(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", label)


def resolve_commit(root: Path, ref: str) -> str:
    """Return the commit ``ref`` names, or ``WORKTREE`` for the working tree."""
    if ref == WORKTREE:
        return WORKTREE
    return _run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=root)


def export_source(root: Path, version: Version, work: Path) -> Path:
    """Return a directory holding ``version``'s source tree.

    Raises:
        RuntimeError: If ``git archive`` fails.
    """
    if version.commit == WORKTREE:
        return root
    destination = work / "src" / _slug(version.label)
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True)
    archive = subprocess.run(
        ["git", "archive", "--format=tar", version.commit],
        cwd=root,
        capture_output=True,
        check=False,
    )
    if archive.returncode != 0:
        stderr = archive.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git archive {version.commit} failed:\n{stderr}")
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(destination, filter="data")
    return destination


def create_env(version: Version, source: Path, work: Path) -> Path:
    """Install ``source`` into a fresh virtual environment of its own.

    Returns:
        The environment's Python interpreter.

    Raises:
        RuntimeError: If uv cannot create the environment or install into it.
    """
    venv = work / "venv" / _slug(version.label)
    shutil.rmtree(venv, ignore_errors=True)
    _run(["uv", "venv", "--quiet", "--python", PYTHON, str(venv)])
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    # --reinstall-package: uv keys a local directory's cached build on
    # pyproject.toml alone, so an edited working tree would otherwise be
    # benchmarked as it was when first built.
    _run(
        [
            "uv",
            "pip",
            "install",
            "--quiet",
            "--python",
            str(python),
            "--reinstall-package",
            "tidysdmx",
            str(source),
        ]
    )
    return python


def installed_versions(python: Path, harness: Path) -> dict[str, str | None]:
    """Return what ``python``'s environment has installed (see ``benchmarks.probe``)."""
    output = _run([str(python), "-m", "benchmarks.probe"], cwd=harness)
    return cast(dict[str, str | None], json.loads(output))


def measure(
    python: Path, harness: Path, work: Path, scenario: str, rows: int
) -> Record:
    """Run one case in ``python``'s environment and return its record.

    Args:
        python: The environment's interpreter.
        harness: The directory holding ``benchmarks/``; the working directory.
        work: Scratch directory for the result file.
        scenario: The scenario to run.
        rows: Its row count.

    Returns:
        The record the case wrote, or an error record if it timed out or
        exited without writing one.
    """
    result = work / "case.json"
    result.parent.mkdir(parents=True, exist_ok=True)
    result.unlink(missing_ok=True)
    command = [
        str(python),
        "-m",
        "benchmarks.case",
        "--scenario",
        scenario,
        "--rows",
        str(rows),
        "--result",
        str(result),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=harness,
            capture_output=True,
            text=True,
            timeout=CASE_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return empty_record(
            scenario, rows, "error", f"timed out after {CASE_TIMEOUT_S:.0f} s"
        )
    if result.exists():
        return cast(Record, json.loads(result.read_text(encoding="utf-8")))
    tail = " | ".join(completed.stderr.strip().splitlines()[-5:])
    return empty_record(
        scenario,
        rows,
        "error",
        f"exited {completed.returncode} without a result: {tail}",
    )


def install_versions(
    versions: Sequence[Version], install: Installer
) -> tuple[dict[str, Path], list[VersionInfo]]:
    """Install every version, skipping baselines and compares that fail.

    Args:
        versions: The versions to install; the target is the one whose
            ``role`` is ``"target"``.
        install: Installs one version and returns its interpreter and the
            dependency versions installed with it.

    Returns:
        The interpreter by label, and the description of each version that
        installed, in the order given.

    Raises:
        RuntimeError: If installing the target fails with it.
        OSError: If installing the target fails with it.
        tarfile.TarError: If installing the target fails with it.
    """
    pythons: dict[str, Path] = {}
    infos: list[VersionInfo] = []
    for version in versions:
        logger.info("Installing %s (%s)", version.label, version.ref)
        try:
            python, deps = install(version)
        except (RuntimeError, OSError, tarfile.TarError):
            if version.role == "target":
                raise
            logger.exception("Skipping %s: it could not be installed", version.label)
            continue
        pythons[version.label] = python
        infos.append(
            {
                "label": version.label,
                "ref": version.ref,
                "commit": version.commit,
                "role": version.role,
                "aliases": list(version.aliases),
                "deps": deps,
            }
        )
    return pythons, infos


def target_measured(records: Sequence[Record], target: str) -> bool:
    """Return whether the version labelled ``target`` has an ``ok`` record."""
    return any(
        record["status"] == "ok" and record.get("version") == target
        for record in records
    )


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.run",
        description="Benchmark map_structures across tidysdmx versions.",
    )
    parser.add_argument(
        "--target",
        required=True,
        help=f"git ref to profile, or {WORKTREE} for the working tree",
    )
    parser.add_argument("--target-label", help="the target's label (default: the ref)")
    parser.add_argument(
        "--compare",
        nargs="*",
        default=[],
        metavar="[LABEL=]REF",
        help="other refs to measure alongside the target",
    )
    parser.add_argument(
        "--baselines",
        type=int,
        default=3,
        help="how many earlier releases to measure (default: 3)",
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument(
        "--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS)
    )
    parser.add_argument("--work-dir", type=Path, default=Path(".bench"))
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Benchmark the requested versions and write the results JSON.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 if the target produced at least one measurement, 1 otherwise.

    Raises:
        RuntimeError: If the target version cannot be installed.
        OSError: If a tool the target's install needs cannot be run.
        tarfile.TarError: If the target's source archive cannot be extracted.
    """
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    harness = Path(_run(["git", "rev-parse", "--show-toplevel"]))
    work = args.work_dir.resolve()
    tags = _run(["git", "tag", "--list", "v*"], cwd=harness).splitlines()
    versions = plan_versions(
        args.target,
        args.target_label,
        args.compare,
        args.baselines,
        tags,
        lambda ref: resolve_commit(harness, ref),
    )

    def _install(version: Version) -> tuple[Path, dict[str, str | None]]:
        python = create_env(version, export_source(harness, version, work), work)
        return python, installed_versions(python, harness)

    pythons, infos = install_versions(versions, _install)

    def _run_case(version: Version, scenario: str, rows: int) -> Record:
        record = measure(pythons[version.label], harness, work, scenario, rows)
        median = record["median_s"]
        outcome = f"{median:.4f} s" if median is not None else record["reason"]
        logger.info("%-14s %-17s %9d rows  %s", version.label, scenario, rows, outcome)
        return record

    measured = [version for version in versions if version.label in pythons]
    records = run_grid(measured, args.scenarios, args.sizes, _run_case)
    results: Results = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine_info(),
        "sizes": sorted(args.sizes),
        "scenarios": list(args.scenarios),
        "versions": infos,
        "results": records,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    logger.info("Wrote %s", args.out)
    return 0 if target_measured(records, versions[0].label) else 1


if __name__ == "__main__":
    sys.exit(main())
