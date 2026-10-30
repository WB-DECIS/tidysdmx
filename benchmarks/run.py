"""Benchmark tidysdmx versions side by side: ``python -m benchmarks.run``.

Each version is installed from its git ref into a virtual environment of its
own, with the dependencies it declares, so a release that requires pandas 2
and one that requires pandas 3 are each measured as users install them. Every
case runs in a fresh subprocess, and the cases are interleaved across
versions, so slow drift on the machine spreads over every version instead of
landing on one.

Two suites share this orchestration (``--suite``):

- ``mapping`` measures ``map_structures`` on generated dataset shapes, one
  case per shape and size (``benchmarks.case``);
- ``pipeline`` measures the whole WB template pipeline, step by step, on a
  mock WB_IDS CSV generated once per size and read by every version
  (``benchmarks.pipeline_case``). Its environments also get pyarrow and
  psutil, which the runner needs; the mapping suite's never do, because its
  memory figures depend on strings not living in Arrow buffers.

Standard library only: it runs under ``uv run --no-project``.
"""

import argparse
import io
import json
import logging
import os
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
    PIPELINE_CONFIGS,
    SCENARIOS,
    SCHEMA_VERSION,
    WORKTREE,
    PipelineRecord,
    Record,
    Results,
    Role,
    Status,
    Suite,
    VersionInfo,
    empty_pipeline_record,
    empty_record,
    machine_info,
    release_key,
)

DEFAULT_SIZES: tuple[int, ...] = (1_000, 10_000, 100_000, 1_000_000)
PIPELINE_SIZES: tuple[int, ...] = (1_000_000,)
PIPELINE_PACKAGES: tuple[str, ...] = ("pyarrow", "psutil")
BUDGET_S: float = 60.0
# A size is skipped when the previous size's peak RSS, scaled to it, would take
# more than this share of the machine's memory.
MEMORY_BUDGET: float = 0.6
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


@dataclass(frozen=True)
class SuiteSpec:
    """What one suite measures and needs.

    Attributes:
        name: The suite's name, as the results record it.
        module: The per-case runner, started with ``python -m``.
        scenarios: Its scenarios; the pipeline suite's are load configurations.
        sizes: Its default row counts.
        packages: Packages installed into every environment beside tidysdmx.
        inputs: Whether a mock CSV is generated per size before the cases run.
    """

    name: Suite
    module: str
    scenarios: tuple[str, ...]
    sizes: tuple[int, ...]
    packages: tuple[str, ...]
    inputs: bool


SUITES: dict[str, SuiteSpec] = {
    "mapping": SuiteSpec(
        "mapping", "benchmarks.case", SCENARIOS, DEFAULT_SIZES, (), False
    ),
    "pipeline": SuiteSpec(
        "pipeline",
        "benchmarks.pipeline_case",
        PIPELINE_CONFIGS,
        PIPELINE_SIZES,
        PIPELINE_PACKAGES,
        True,
    ),
}

CaseRunner = Callable[[Version, str, int], Record]
Installer = Callable[[Version], tuple[Path, dict[str, str | None]]]
EmptyRecord = Callable[[str, int, Status, str], Record]
# Given the version's last measured record at a smaller size (None before any)
# and the next size, the reason to skip that size, or None to run it.
SkipRule = Callable[[Record | None, int], str | None]


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


def over_time_budget(previous: Record | None, rows: int) -> str | None:
    """Skip a size once a smaller one took over ``BUDGET_S``."""
    if previous is not None and exceeds_budget(previous):
        return f"a smaller size took over {BUDGET_S:.0f} s"
    return None


def over_memory_budget(
    memory_bytes: int | None, fraction: float = MEMORY_BUDGET
) -> SkipRule:
    """Return a rule that skips a size the machine's memory could not hold.

    It scales the previous size's peak RSS linearly to the next size, which is
    what a whole-frame run does, and skips when that exceeds ``fraction`` of
    the machine's memory. Mapping records carry no RSS, so it never fires there.

    Args:
        memory_bytes: The machine's memory; None disables the rule.
        fraction: The share of it a case may take.

    Returns:
        The rule.
    """

    def rule(previous: Record | None, rows: int) -> str | None:
        if memory_bytes is None or previous is None or previous["status"] != "ok":
            return None
        peak = cast(dict[str, object], previous).get("peak_rss_bytes")
        if not isinstance(peak, int) or peak <= 0 or previous["rows"] <= 0:
            return None
        projected = peak * rows / previous["rows"]
        if projected <= fraction * memory_bytes:
            return None
        return (
            f"a {previous['rows']:,}-row run peaked at {_gib(peak)}; {rows:,} rows "
            f"would need about {_gib(projected)}, over {fraction:.0%} of this "
            f"machine's {_gib(memory_bytes)}"
        )

    return rule


def _gib(count: float) -> str:
    return f"{count / 2**30:.1f} GiB"


def run_grid(
    versions: Sequence[Version],
    scenarios: Sequence[str],
    sizes: Sequence[int],
    run_case: CaseRunner,
    rules: Sequence[SkipRule] = (over_time_budget,),
    empty: EmptyRecord = empty_record,
) -> list[Record]:
    """Measure every (scenario, size) case for every version.

    Sizes run smallest first. Within a case the version order rotates, so no
    version always runs first. Before each size, every rule in ``rules`` sees
    the version's last measured record for the scenario; the first reason one
    gives skips that size and every larger one, so by default a scenario's
    larger sizes are recorded as skipped once a size took over ``BUDGET_S``.

    Args:
        versions: The versions to measure.
        scenarios: The scenarios to run.
        sizes: Row counts.
        run_case: Measures one version on one case.
        rules: Why to skip a size, each given the last measured record.
        empty: Builds the record of a skipped case.

    Returns:
        One record per version and case, labelled with the version.
    """
    stopped: dict[tuple[str, str], str] = {}
    last_ok: dict[tuple[str, str], Record] = {}
    records: list[Record] = []
    cases = [(scenario, rows) for scenario in scenarios for rows in sorted(sizes)]
    for number, (scenario, rows) in enumerate(cases):
        for version in rotated(versions, number):
            key = (version.label, scenario)
            reason = stopped.get(key)
            if reason is None:
                previous = last_ok.get(key)
                reason = next(
                    (
                        why
                        for rule in rules
                        if (why := rule(previous, rows)) is not None
                    ),
                    None,
                )
                if reason is not None:
                    stopped[key] = reason
            if reason is not None:
                record = empty(scenario, rows, "skipped", reason)
            else:
                record = run_case(version, scenario, rows)
                if record["status"] == "ok":
                    last_ok[key] = record
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


def create_env(
    version: Version, source: Path, work: Path, packages: Sequence[str] = ()
) -> Path:
    """Install ``source`` into a fresh virtual environment of its own.

    Args:
        version: The version being installed; names the environment.
        source: Its source tree.
        work: The scratch directory the environment is created under.
        packages: Packages to install beside it, for the harness's own needs
            (the pipeline suite's pyarrow and psutil). Resolved together with
            the version's dependencies, so they cannot change what it pins.

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
            *packages,
        ]
    )
    return python


def installed_versions(python: Path, harness: Path) -> dict[str, str | None]:
    """Return what ``python``'s environment has installed (see ``benchmarks.probe``)."""
    output = _run([str(python), "-m", "benchmarks.probe"], cwd=harness)
    return cast(dict[str, str | None], json.loads(output))


def generate_inputs(
    python: Path, harness: Path, work: Path, sizes: Sequence[int], seed: int = 0
) -> dict[int, Path]:
    """Write the pipeline suite's mock CSV for each size, once for every version.

    ``benchmarks.wb_ids.generate`` runs in ``python``'s environment, the
    target's, which the suite's packages equip for it; the mock is
    deterministic, so which environment writes it does not matter, and every
    version then reads the same file.

    Args:
        python: The interpreter to generate with.
        harness: The directory holding ``benchmarks/``; the working directory.
        work: Scratch directory; the files go under ``work/data``.
        sizes: The row counts to generate.
        seed: The generator's seed.

    Returns:
        The CSV for each size.

    Raises:
        RuntimeError: If the generator fails; without inputs there is nothing
            to measure, as when the target cannot be installed.
    """
    data = work / "data"
    data.mkdir(parents=True, exist_ok=True)
    inputs: dict[int, Path] = {}
    for rows in sorted(set(sizes)):
        path = data / f"wb_ids_{rows}.csv"
        logger.info("Generating %s rows of mock WB_IDS data", f"{rows:,}")
        _run(
            [
                str(python),
                "-m",
                "benchmarks.wb_ids.generate",
                "--rows",
                str(rows),
                "--seed",
                str(seed),
                "--out",
                str(path),
            ],
            cwd=harness,
        )
        inputs[rows] = path
    return inputs


def _measure_command(
    command: Sequence[str],
    harness: Path,
    work: Path,
    empty: Callable[[Status, str], Record],
) -> Record:
    """Run a case command and return the record it wrote, or an error record."""
    result = work / "case.json"
    result.parent.mkdir(parents=True, exist_ok=True)
    result.unlink(missing_ok=True)
    try:
        completed = subprocess.run(
            [*command, "--result", str(result)],
            cwd=harness,
            capture_output=True,
            text=True,
            timeout=CASE_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return empty("error", f"timed out after {CASE_TIMEOUT_S:.0f} s")
    if result.exists():
        return cast(Record, json.loads(result.read_text(encoding="utf-8")))
    tail = " | ".join(completed.stderr.strip().splitlines()[-5:])
    return empty("error", f"exited {completed.returncode} without a result: {tail}")


def measure(
    python: Path, harness: Path, work: Path, scenario: str, rows: int
) -> Record:
    """Run one mapping case in ``python``'s environment and return its record.

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
    command = [
        str(python),
        "-m",
        "benchmarks.case",
        "--scenario",
        scenario,
        "--rows",
        str(rows),
    ]
    return _measure_command(
        command,
        harness,
        work,
        lambda status, reason: empty_record(scenario, rows, status, reason),
    )


def measure_pipeline(
    python: Path, harness: Path, work: Path, config: str, rows: int, csv: Path
) -> PipelineRecord:
    """Run one pipeline case in ``python``'s environment and return its record.

    Args:
        python: The environment's interpreter.
        harness: The directory holding ``benchmarks/``; the working directory.
        work: Scratch directory for the result file.
        config: The load configuration to run (see ``PIPELINE_CONFIGS``).
        rows: The mock's row count.
        csv: The mock CSV for that size, from ``generate_inputs``.

    Returns:
        The record the case wrote, or an error record if it timed out or
        exited without writing one.
    """
    command = [
        str(python),
        "-m",
        "benchmarks.pipeline_case",
        "--scenario",
        config,
        "--rows",
        str(rows),
        "--csv",
        str(csv),
    ]
    record = _measure_command(
        command,
        harness,
        work,
        lambda status, reason: empty_pipeline_record(config, rows, status, reason),
    )
    return cast(PipelineRecord, record)


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.run",
        description="Benchmark map_structures, or the WB template pipeline, across "
        "tidysdmx versions.",
    )
    parser.add_argument(
        "--suite",
        choices=list(SUITES),
        default="mapping",
        help="what to measure (default: mapping)",
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
    parser.add_argument(
        "--sizes", type=int, nargs="+", help="row counts (default: the suite's)"
    )
    parser.add_argument(
        "--scenarios", nargs="+", help="scenarios to run (default: the suite's)"
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
        RuntimeError: If the target version cannot be installed, or the
            pipeline suite's inputs cannot be generated.
        OSError: If a tool the target's install needs cannot be run.
        tarfile.TarError: If the target's source archive cannot be extracted.
    """
    parser = _parser()
    args = parser.parse_args(argv)
    spec = SUITES[args.suite]
    sizes = sorted(args.sizes) if args.sizes else sorted(spec.sizes)
    scenarios = list(args.scenarios) if args.scenarios else list(spec.scenarios)
    unknown = [name for name in scenarios if name not in spec.scenarios]
    if unknown:
        parser.error(
            f"the {spec.name} suite has no scenario {unknown}; "
            f"choose from {', '.join(spec.scenarios)}"
        )
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    harness = Path(_run(["git", "rev-parse", "--show-toplevel"]))
    # Each suite works under its own directory, so their environments never
    # mix (pyarrow in one, not the other) and both can run side by side.
    work = (args.work_dir / spec.name).resolve()
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
        source = export_source(harness, version, work)
        python = create_env(version, source, work, packages=spec.packages)
        return python, installed_versions(python, harness)

    pythons, infos = install_versions(versions, _install)
    inputs = (
        generate_inputs(pythons[versions[0].label], harness, work, sizes)
        if spec.inputs
        else {}
    )

    def _run_case(version: Version, scenario: str, rows: int) -> Record:
        python = pythons[version.label]
        record: Record
        if spec.inputs:
            record = measure_pipeline(
                python, harness, work, scenario, rows, inputs[rows]
            )
        else:
            record = measure(python, harness, work, scenario, rows)
        median = record["median_s"]
        outcome = f"{median:.4f} s" if median is not None else record["reason"]
        logger.info("%-14s %-17s %9d rows  %s", version.label, scenario, rows, outcome)
        return record

    machine = machine_info()
    rules: list[SkipRule] = [over_time_budget]
    empty: EmptyRecord = empty_record
    if spec.inputs:
        rules.append(over_memory_budget(machine["memory_bytes"]))
        empty = empty_pipeline_record
    measured = [version for version in versions if version.label in pythons]
    records = run_grid(measured, scenarios, sizes, _run_case, rules, empty)
    results: Results = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine,
        "sizes": sizes,
        "scenarios": scenarios,
        "versions": infos,
        "results": records,
        "suite": spec.name,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    logger.info("Wrote %s", args.out)
    return 0 if target_measured(records, versions[0].label) else 1


if __name__ == "__main__":
    sys.exit(main())
