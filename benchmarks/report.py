"""Render benchmark results as Markdown: ``python -m benchmarks.report``.

One results file (``benchmarks.records.Results``) feeds two text formats:

- ``summary``: a GitHub Actions job summary;
- ``pr-comment``: the sticky pull-request comment, ``summary`` behind a marker.

Standard library only, so it runs under ``uv run --no-project``.
"""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, cast

from benchmarks.records import (
    SCHEMA_VERSION,
    WORKTREE,
    Record,
    Results,
    VersionInfo,
)

COMMENT_MARKER: str = "<!-- tidysdmx-benchmark -->"
RESULTS_MARKER: str = "<!-- RESULTS -->"
HEADLINE_SCENARIO: str = "typical"
SAME_BAND: float = 0.10
REPO_URL: str = "https://github.com/WB-DECIS/tidysdmx"

MISSING: str = "\u2014"  # em dash
SAME: str = "\u2248 same"  # approximately equal to
TIMES: str = "\u00d7"  # multiplication sign
DELTA: str = "\u0394"  # Greek capital delta
PLUS_MINUS: str = "\u00b1"  # plus-minus sign

Metric = Literal["time", "memory"]
Key = tuple[str, str, int]

_METRIC_TITLES: dict[str, str] = {
    "time": "Median time",
    "memory": "Peak extra memory",
}
_BYTE_UNITS: tuple[str, ...] = ("B", "KiB", "MiB", "GiB")


def load_results(path: Path) -> Results | None:
    """Read a results file written by ``benchmarks.run``.

    Args:
        path: The file.

    Returns:
        The results, or ``None`` if the file does not exist.

    Raises:
        ValueError: If the file is not a schema ``SCHEMA_VERSION`` results file.
    """
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"{path} is not a schema {SCHEMA_VERSION} benchmark results file"
        )
    return cast(Results, data)


def format_seconds(seconds: float) -> str:
    """Format a duration to three significant figures, e.g. ``"350 ms"``."""
    if seconds < 0.9995:
        return f"{seconds * 1000:.3g} ms"
    if seconds < 99.95:
        return f"{seconds:.3g} s"
    return f"{seconds:.0f} s"


def format_bytes(count: float) -> str:
    """Format a byte count in binary units to three figures, e.g. ``"300 MiB"``."""
    unit = 0
    while count >= 999.5 and unit < len(_BYTE_UNITS) - 1:
        count /= 1024
        unit += 1
    precision = ".3g" if count < 99.95 else ".0f"
    return f"{count:{precision}} {_BYTE_UNITS[unit]}"


def relative_change(new: float | None, old: float | None) -> float | None:
    """Return ``new`` relative to ``old`` (``-0.5`` is half), if both exist."""
    if new is None or old is None or old <= 0:
        return None
    return new / old - 1


def format_change(change: float | None) -> str:
    """Format a relative change; within ``SAME_BAND`` it reads ``SAME``."""
    if change is None:
        return MISSING
    if abs(change) < SAME_BAND:
        return SAME
    return f"{change:+.0%}"


def metric_value(record: Record | None, metric: Metric) -> float | None:
    """Return a record's median time or peak allocation, if it was measured."""
    if record is None or record["status"] != "ok":
        return None
    value = record["median_s"] if metric == "time" else record["peak_alloc_bytes"]
    return None if value is None else float(value)


def format_metric(value: float | None, metric: Metric) -> str:
    """Format a time or memory value, or ``MISSING``."""
    if value is None:
        return MISSING
    return format_seconds(value) if metric == "time" else format_bytes(value)


def index_records(results: Results) -> dict[Key, Record]:
    """Map ``(version label, scenario, rows)`` to its record."""
    return {
        (record.get("version", ""), record["scenario"], record["rows"]): record
        for record in results["results"]
    }


def display_label(info: VersionInfo) -> str:
    """Return a version's label with its aliases, e.g. ``"base (= v0.10.0)"``."""
    if not info["aliases"]:
        return info["label"]
    return f"{info['label']} (= {', '.join(info['aliases'])})"


def reference_versions(results: Results) -> list[VersionInfo]:
    """Return what the target is compared with.

    The first compare version, then the first baseline. Both come from the
    results file in the order ``benchmarks.run`` wrote them.
    """
    references: list[VersionInfo] = []
    for role in ("compare", "baseline"):
        found = next((v for v in results["versions"] if v["role"] == role), None)
        if found is not None:
            references.append(found)
    return references


def markdown_table(header: Sequence[str], body: Sequence[Sequence[str]]) -> str:
    """Return a Markdown table; the first column is left-aligned, the rest right."""
    separator = ["---", *(["---:"] * (len(header) - 1))]
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(separator) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in body]
    return "\n".join(lines)


def comparison_table(results: Results, rows: int, metric: Metric) -> str:
    """Compare every version on every scenario at one size.

    Args:
        results: The benchmark results.
        rows: The size to compare at.
        metric: ``"time"`` or ``"memory"``.

    Returns:
        A table with one column per version, then one change column per
        reference version (see ``reference_versions``), taken from the target.
    """
    index = index_records(results)
    versions = results["versions"]
    target = versions[0]["label"]
    references = reference_versions(results)
    header = [
        _METRIC_TITLES[metric],
        *(display_label(v) for v in versions),
        *(f"{DELTA} vs {r['label']}" for r in references),
    ]
    body = []
    for scenario in results["scenarios"]:
        values = {
            v["label"]: metric_value(index.get((v["label"], scenario, rows)), metric)
            for v in versions
        }
        body.append(
            [
                scenario,
                *(format_metric(values[v["label"]], metric) for v in versions),
                *(
                    format_change(relative_change(values[target], values[r["label"]]))
                    for r in references
                ),
            ]
        )
    return markdown_table(header, body)


def machine_description(results: Results) -> str:
    """Describe the benchmark machine.

    For example ``"a GitHub-hosted runner (CPU, 4 CPUs, 16 GiB RAM)"``.
    """
    machine = results["machine"]
    cpu_count, memory_bytes = machine["cpu_count"], machine["memory_bytes"]
    parts = [machine["cpu"]]
    if cpu_count:
        parts.append(f"{cpu_count} CPUs")
    if memory_bytes:
        parts.append(f"{format_bytes(memory_bytes)} RAM")
    description = ", ".join(parts)
    if machine["runner_image"]:
        return f"a GitHub-hosted runner ({description})"
    return description


def _short_commit(commit: str) -> str:
    return "working tree" if commit == WORKTREE else f"`{commit[:7]}`"


def environment_table(results: Results) -> str:
    """List what every version ran with."""
    header = ["Version", "Role", "Commit", "Python", "pandas", "numpy", "pysdmx"]
    body = [
        [
            display_label(info),
            info["role"],
            _short_commit(info["commit"]),
            *(
                info["deps"].get(name) or MISSING
                for name in ("python", "pandas", "numpy", "pysdmx")
            ),
        ]
        for info in results["versions"]
    ]
    return markdown_table(header, body)


def render_comparison(results: Results, run_url: str | None = None) -> str:
    """Return the job summary: every version compared at the largest size.

    Every other size follows inside a collapsed ``<details>`` block.

    Args:
        results: The benchmark results.
        run_url: The workflow run to link, if any.

    Returns:
        GitHub-flavoured Markdown.
    """
    target = results["versions"][0]
    largest = max(results["sizes"])
    source = f" in [this run]({run_url})" if run_url else ""
    by_size = [
        f"**{rows:,} rows**\n\n{comparison_table(results, rows, 'time')}"
        f"\n\n{comparison_table(results, rows, 'memory')}"
        for rows in results["sizes"]
    ]
    return "\n\n".join(
        [
            "### `map_structures` benchmark",
            f"{display_label(target)} at {_short_commit(target['commit'])}, measured "
            f"{results['generated_at']}{source} on {machine_description(results)}. "
            f"{DELTA} columns give the change from the named version: negative is "
            f"faster or smaller, and changes within {PLUS_MINUS}10% read "
            f'"{SAME}". {MISSING} marks a case that was not measured; the '
            "run's JSON says why.",
            f"**{largest:,} rows**",
            comparison_table(results, largest, "time"),
            comparison_table(results, largest, "memory"),
            "<details><summary>All sizes</summary>",
            *by_size,
            "</details>",
            environment_table(results),
        ]
    )


def render_pr_comment(results: Results, run_url: str | None = None) -> str:
    """Return the sticky PR comment.

    It is ``render_comparison`` behind ``COMMENT_MARKER``.
    """
    return f"{COMMENT_MARKER}\n{render_comparison(results, run_url)}"


def main(argv: Sequence[str] | None = None) -> int:
    """Render a results file in the requested format.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 on success. A missing results file ends the program through
        ``argparse`` with status 2 instead.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.report", description="Render benchmark results."
    )
    parser.add_argument("--format", required=True, choices=("summary", "pr-comment"))
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="file to write (default: stdout)")
    parser.add_argument("--run-url", help="the workflow run to link")
    args = parser.parse_args(argv)
    results = load_results(args.input)
    if results is None:
        parser.error(f"no results at {args.input}")
    if args.format == "summary":
        text = render_comparison(results, args.run_url)
    else:
        text = render_pr_comment(results, args.run_url)
    _write(text, args.output)
    return 0


def _write(text: str, output: Path | None) -> None:
    if output is None:
        # Bytes, not text: a Windows console's code page cannot encode the
        # em dash and approximately-equal signs.
        sys.stdout.buffer.write((text + "\n").encode("utf-8"))
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
