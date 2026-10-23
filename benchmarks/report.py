"""Render benchmark results as Markdown: ``python -m benchmarks.report``.

One results file (``benchmarks.records.Results``) feeds three formats:

- ``page``: the docs site's Performance page, with charts;
- ``summary``: a GitHub Actions job summary;
- ``pr-comment``: the sticky pull-request comment, ``summary`` behind a marker.

The text formats use the standard library only, so they run under
``uv run --no-project``. The charts need matplotlib, which is imported only when
one is drawn.
"""

import argparse
import io
import json
import math
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from benchmarks.records import (
    SCHEMA_VERSION,
    WORKTREE,
    Record,
    Results,
    VersionInfo,
    release_key,
)

if TYPE_CHECKING:
    from matplotlib.axes import Axes

TEMPLATE: Path = Path(__file__).with_name("page.qmd")
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
NOT_COMPARABLE: str = "\u2260 output"  # not-equal sign

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


def is_comparable(record: Record | None, target: Record | None) -> bool:
    """Return whether a version's result on a case can be set against the target's.

    Two results are comparable unless both carry an output fingerprint and the
    fingerprints differ: then the versions mapped the case differently (an old
    release may lack a feature the case uses), so a difference in time says
    nothing about speed. A missing record, or one without a fingerprint, is
    comparable, so older results files and unmeasured cases keep reading as
    before.

    Args:
        record: The version's record on the case.
        target: The target's record on the same case.

    Returns:
        ``False`` only for two fingerprints that differ.
    """
    if record is None or target is None:
        return True
    theirs, ours = record.get("output_fingerprint"), target.get("output_fingerprint")
    return theirs is None or ours is None or theirs == ours


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
        A version whose output differs from the target's on a case reads
        ``NOT_COMPARABLE`` there, and no change is computed from it.
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
        target_record = index.get((target, scenario, rows))
        values: dict[str, float | None] = {}
        cells: dict[str, str] = {}
        for v in versions:
            record = index.get((v["label"], scenario, rows))
            if is_comparable(record, target_record):
                values[v["label"]] = metric_value(record, metric)
                cells[v["label"]] = format_metric(values[v["label"]], metric)
            else:
                values[v["label"]] = None
                cells[v["label"]] = NOT_COMPARABLE
        body.append(
            [
                scenario,
                *(cells[v["label"]] for v in versions),
                *(
                    format_change(relative_change(values[target], values[r["label"]]))
                    for r in references
                ),
            ]
        )
    return markdown_table(header, body)


def has_different_output(results: Results, sizes: Sequence[int]) -> bool:
    """Return whether any version's output differs from the target's at ``sizes``."""
    index = index_records(results)
    target = results["versions"][0]["label"]
    return any(
        not is_comparable(
            index.get((v["label"], scenario, rows)), index.get((target, scenario, rows))
        )
        for v in results["versions"]
        for scenario in results["scenarios"]
        for rows in sizes
    )


def comparison_block(results: Results, rows: int) -> str:
    """Return the time and memory comparison tables at one size.

    The tables are followed by a footnote explaining ``NOT_COMPARABLE``, if
    either of them contains it.
    """
    parts = [
        comparison_table(results, rows, "time"),
        comparison_table(results, rows, "memory"),
    ]
    if has_different_output(results, [rows]):
        target = results["versions"][0]["label"]
        parts.append(
            f"{NOT_COMPARABLE}: that version produced different results from "
            f"{target} on this case (for example a feature it lacks), so its "
            "measurements are not comparable."
        )
    return "\n\n".join(parts)


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
        f"**{rows:,} rows**\n\n{comparison_block(results, rows)}"
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
            comparison_block(results, largest),
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


_PLACEHOLDER: str = (
    "::: {.callout-note}\n"
    "No performance profile has been published yet. The first one arrives with\n"
    "the next release; `make bench` produces one locally.\n"
    ":::"
)


def sizing_table(results: Results, metric: Metric) -> str:
    """Show the target's time or memory for every scenario and size.

    Memory cells add the peak's multiple of the input size, which is what
    sizing a job needs: its memory is roughly the input plus the peak.
    """
    index = index_records(results)
    target = results["versions"][0]["label"]
    header = [_METRIC_TITLES[metric], *(f"{rows:,} rows" for rows in results["sizes"])]
    body = []
    for scenario in results["scenarios"]:
        cells = [scenario]
        for rows in results["sizes"]:
            record = index.get((target, scenario, rows))
            value = metric_value(record, metric)
            input_bytes = record["input_bytes"] if record is not None else None
            cell = format_metric(value, metric)
            if metric == "memory" and value is not None and input_bytes:
                cell += f" ({value / input_bytes:.1f}{TIMES} input)"
            cells.append(cell)
        body.append(cells)
    return markdown_table(header, body)


def headline(results: Results) -> str:
    """Return the opening line: the target on the headline scenario, largest size."""
    target = results["versions"][0]
    rows = max(results["sizes"])
    record = index_records(results).get((target["label"], HEADLINE_SCENARIO, rows))
    measured = (
        f"Measured {results['generated_at'][:10]} on {machine_description(results)}."
    )
    seconds = metric_value(record, "time")
    peak = metric_value(record, "memory")
    input_bytes = record["input_bytes"] if record is not None else None
    if seconds is None or peak is None or not input_bytes:
        return f"**{display_label(target)}**. {measured}"
    return (
        f"**{display_label(target)}** maps the {HEADLINE_SCENARIO} indicator table "
        f"of {rows:,} rows in **{format_seconds(seconds)}**, allocating "
        f"**{format_bytes(peak)}** beyond its input "
        f"({peak / input_bytes:.1f}{TIMES} the input's size). {measured}"
    )


def raw_data_link(results: Results, repo_url: str = REPO_URL) -> str | None:
    """Link the results file attached to the target's Release, if it is a release."""
    tag = results["versions"][0]["label"]
    if release_key(tag) is None:
        return None
    url = f"{repo_url}/releases/download/{tag}/benchmarks-{tag}.json"
    return (
        "The raw measurements are attached to the release: "
        f"[benchmarks-{tag}.json]({url})."
    )


def render_page(
    results: Results | None,
    template: str,
    charts: Callable[[Results], str] | None = None,
) -> str:
    """Return the Performance page: ``template`` with its results marker filled in.

    Args:
        results: The results to show, or ``None`` for the placeholder.
        template: The page template's text; must contain ``RESULTS_MARKER``.
        charts: Draws the charts; ``render_charts`` by default.

    Returns:
        The page as Quarto Markdown.

    Raises:
        ValueError: If ``template`` has no ``RESULTS_MARKER``.
    """
    if RESULTS_MARKER not in template:
        raise ValueError(f"The page template has no {RESULTS_MARKER} marker")
    if results is None:
        return template.replace(RESULTS_MARKER, _PLACEHOLDER)
    draw = render_charts if charts is None else charts
    largest = max(results["sizes"])
    sections = [
        headline(results),
        "## Sizing",
        "Median time per call, by dataset shape and size:",
        sizing_table(results, "time"),
        "Peak memory allocated per call beyond the input, and its multiple of the "
        "input's size:",
        sizing_table(results, "memory"),
        "## Scaling",
        draw(results),
    ]
    if has_different_output(results, results["sizes"]):
        sections.append(
            "A line leaves out a point where that version's output differed from "
            f"{results['versions'][0]['label']}'s on the case ({NOT_COMPARABLE}), "
            "for example because it lacks a feature the case uses."
        )
    if reference_versions(results):
        sections += [
            "## Compared with earlier versions",
            f"At {largest:,} rows. {DELTA} columns give the change from the named "
            f"version: negative is faster or smaller, and changes within "
            f'{PLUS_MINUS}10% read "{SAME}".',
            comparison_block(results, largest),
        ]
    sections += [
        "## Environment",
        f"Measured on {machine_description(results)}.",
        environment_table(results),
    ]
    link = raw_data_link(results)
    if link is not None:
        sections.append(link)
    return template.replace(RESULTS_MARKER, "\n\n".join(sections))


@dataclass(frozen=True)
class _Theme:
    surface: str
    text: str
    muted: str
    grid: str
    accent: str
    context: tuple[str, ...]


# Surfaces, text and the accent come from the dataviz reference palette. Older
# versions are neutral context lines, darker for newer releases. Identity never
# rests on colour alone: every line also has its own marker and dash style, and
# the tables beside the charts carry the exact values.
_THEMES: dict[str, _Theme] = {
    "light": _Theme(
        surface="#fcfcfb",
        text="#0b0b0b",
        muted="#52514e",
        grid="#e4e3df",
        accent="#2a78d6",
        context=("#52514e", "#8a8984", "#b5b4ae"),
    ),
    "dark": _Theme(
        surface="#1a1a19",
        text="#f0efec",
        muted="#c3c2b7",
        grid="#3a3936",
        accent="#3987e5",
        context=("#c3c2b7", "#8f8e87", "#64635e"),
    ),
}
_MARKERS: tuple[str, ...] = ("o", "s", "^", "D", "v", "P")
_DASHES: tuple[str, ...] = ("-", "--", ":", "-.")
_METRICS: tuple[Metric, ...] = ("time", "memory")
# What fits in one legend row of the 8-inch figure, in characters of label text,
# and what a legend handle and its gap add to each label.
_LEGEND_ROW_CHARS: int = 85
_LEGEND_HANDLE_CHARS: int = 6
_SVG_DEFAULT_STYLE: re.Pattern[str] = re.compile(
    r'<style type="text/css">\*\{[^}]*\}</style>\s*'
)


def render_charts(results: Results) -> str:
    """Return the scaling charts, time then memory, each in a light and a dark variant.

    Each variant sits in a ``perf-chart-light`` or ``perf-chart-dark`` div; the
    page template's style block shows the one matching the site's theme.
    """
    blocks = []
    for metric in _METRICS:
        blocks.append(f"**{_METRIC_TITLES[metric]} per call**, against rows:")
        for mode, theme in _THEMES.items():
            svg = _chart_svg(results, metric, theme)
            blocks.append(f"::: {{.perf-chart-{mode}}}\n```{{=html}}\n{svg}\n```\n:::")
    return "\n\n".join(blocks)


@dataclass(frozen=True)
class _Style:
    color: str
    linewidth: float
    marker: str
    linestyle: str
    zorder: int


def _series_style(theme: _Theme, position: int) -> _Style:
    """Return the target's accent style for position 0, a context style after it."""
    if position == 0:
        return _Style(theme.accent, 2.5, "o", "-", 3)
    return _Style(
        theme.context[min(position, len(theme.context)) - 1],
        1.5,
        _MARKERS[position % len(_MARKERS)],
        _DASHES[position % len(_DASHES)],
        2,
    )


def _plot_panel(
    ax: "Axes", results: Results, scenario: str, metric: Metric, theme: _Theme
) -> None:
    """Draw one scenario's panel: one line per version, log-log."""
    index = index_records(results)
    target = results["versions"][0]["label"]
    sizes = sorted(results["sizes"])
    scale = 1.0 if metric == "time" else 1 / 2**20
    for position, info in enumerate(results["versions"]):
        values: list[float | None] = []
        for rows in sizes:
            record = index.get((info["label"], scenario, rows))
            comparable = is_comparable(record, index.get((target, scenario, rows)))
            values.append(metric_value(record, metric) if comparable else None)
        style = _series_style(theme, position)
        ax.plot(
            sizes,
            [math.nan if v is None else v * scale for v in values],
            label=display_label(info),
            color=style.color,
            linewidth=style.linewidth,
            marker=style.marker,
            markersize=6,
            linestyle=style.linestyle,
            zorder=style.zorder,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_facecolor(theme.surface)
    ax.set_title(scenario.replace("_", " "), loc="left", color=theme.text, fontsize=11)
    ax.grid(visible=True, which="major", color=theme.grid, linewidth=0.8)
    ax.tick_params(colors=theme.muted, labelsize=9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(theme.grid)


def _legend_columns(labels: Sequence[str]) -> int:
    """Return the legend's column count: as many as fit in the figure's width.

    One row of long labels (versions with several aliases) would run off the
    figure's edges, so the legend wraps to the rows it needs.
    """
    characters = sum(len(label) + _LEGEND_HANDLE_CHARS for label in labels)
    rows = max(1, math.ceil(characters / _LEGEND_ROW_CHARS))
    return max(1, math.ceil(len(labels) / rows))


def _chart_svg(results: Results, metric: Metric, theme: _Theme) -> str:
    """Draw one metric as small multiples, one panel per scenario, as inline SVG."""
    from matplotlib import rc_context
    from matplotlib.figure import Figure

    scenarios = results["scenarios"]
    nrows = math.ceil(len(scenarios) / 2)
    figure = Figure(
        figsize=(8, 3.2 * nrows + 0.6), layout="constrained", facecolor=theme.surface
    )
    axes = figure.subplots(nrows, 2, sharex=True, sharey=True, squeeze=False)
    for ax, scenario in zip(axes.flat, scenarios, strict=False):
        _plot_panel(ax, results, scenario, metric, theme)
    for ax in axes.flat[len(scenarios) :]:
        ax.set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel("Rows", color=theme.muted)
    for ax in axes[:, 0]:
        ax.set_ylabel("Seconds" if metric == "time" else "MiB", color=theme.muted)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    figure.legend(
        handles,
        labels,
        loc="outside upper center",
        ncols=_legend_columns(labels),
        frameon=False,
        labelcolor=theme.text,
    )
    buffer = io.StringIO()
    # Text as text, not glyph paths: smaller, selectable, and in the page's font.
    with rc_context({"svg.fonttype": "none", "font.family": "sans-serif"}):
        figure.savefig(
            buffer, format="svg", facecolor=theme.surface, metadata={"Date": None}
        )
    svg = buffer.getvalue()
    # Drop the XML declaration and DOCTYPE, which are invalid inside HTML, and
    # let the viewBox scale the chart to the column instead of a fixed pt size.
    svg = svg[svg.index("<svg") :]
    svg = re.sub(r'width="[\d.]+pt" height="[\d.]+pt"', 'width="100%"', svg, count=1)
    # matplotlib's default stylesheet is `*{...}`, and a <style> element inline in
    # a page is global to it: left in, it would restyle every SVG on the site.
    # The same two defaults, set on the root, apply to this chart alone.
    svg = _SVG_DEFAULT_STYLE.sub("", svg)
    return svg.replace(
        "<svg ", '<svg stroke-linejoin="round" stroke-linecap="butt" ', 1
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Render a results file in the requested format.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0. A missing results file is fine for ``page`` (it renders the
        placeholder) and ends the program through ``argparse`` otherwise.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.report", description="Render benchmark results."
    )
    parser.add_argument(
        "--format", required=True, choices=("page", "summary", "pr-comment")
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="file to write (default: stdout)")
    parser.add_argument("--template", type=Path, default=TEMPLATE)
    parser.add_argument("--run-url", help="the workflow run to link")
    args = parser.parse_args(argv)
    results = load_results(args.input)
    if args.format == "page":
        text = render_page(results, args.template.read_text(encoding="utf-8"))
    elif results is None:
        parser.error(f"no results at {args.input}")
    elif args.format == "summary":
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
