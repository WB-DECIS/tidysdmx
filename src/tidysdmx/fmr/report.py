"""Pandas DataFrame views of diffs, plans, and publication reports.

This is the only :mod:`tidysdmx.fmr` module that imports pandas: the
core workflow objects render themselves as plain text via their
``summary()`` methods, while these helpers serve the DataFrame-first
audience (notebooks, pipelines, QA reports).

Column names follow the tidysdmx UPPER_SNAKE_CASE convention.
"""

from collections.abc import Sequence

import pandas as pd
from typeguard import typechecked

from .diff import ArtefactDiff, ChangeImpact
from .publish import PublicationPlan, PublicationReport

_DIFF_COLUMNS = [
    "SHORT_URN",
    "ARTEFACT_TYPE",
    "KIND",
    "IMPACT",
    "PATH",
    "MESSAGE",
    "OLD",
    "NEW",
]

_PLAN_COLUMNS = [
    "SHORT_URN",
    "ACTION",
    "REGISTRY_VERSION",
    "PROPOSED_VERSION",
    "IMPACT",
    "N_BREAKING",
    "N_ADDITIVE",
    "N_COSMETIC",
    "BLOCKED",
    "ISSUES",
]

_REPORT_COLUMNS = ["SHORT_URN", "ACTION", "STATUS", "ERROR"]


@typechecked
def diff_to_dataframe(
    diffs: ArtefactDiff | Sequence[ArtefactDiff],
) -> pd.DataFrame:
    """Render one or more artefact diffs as a DataFrame.

    Args:
        diffs: A single :class:`~tidysdmx.fmr.diff.ArtefactDiff` or a
            sequence of them.

    Returns:
        One row per detected change with columns ``SHORT_URN``,
        ``ARTEFACT_TYPE``, ``KIND``, ``IMPACT``, ``PATH``, ``MESSAGE``,
        ``OLD``, ``NEW``. Unchanged diffs contribute no rows.
    """
    if isinstance(diffs, ArtefactDiff):
        diffs = [diffs]
    rows = [
        {
            "SHORT_URN": diff.short_urn,
            "ARTEFACT_TYPE": diff.artefact_type,
            "KIND": change.kind.value,
            "IMPACT": change.impact.value,
            "PATH": change.path,
            "MESSAGE": change.message,
            "OLD": change.old,
            "NEW": change.new,
        }
        for diff in diffs
        for change in diff.changes
    ]
    return pd.DataFrame(rows, columns=_DIFF_COLUMNS)


@typechecked
def plan_to_dataframe(plan: PublicationPlan) -> pd.DataFrame:
    """Render a publication plan as a DataFrame.

    Args:
        plan: The plan built by
            :func:`~tidysdmx.fmr.publish.plan_publication`.

    Returns:
        One row per planned action with the proposed action, versions,
        change counts by impact, and any attached issues (rendered as
        ``RULE_ID: message`` joined by ``"; "``).
    """
    rows = []
    for action in plan.actions:
        diff = action.diff
        rows.append(
            {
                "SHORT_URN": action.short_urn,
                "ACTION": action.kind.value,
                "REGISTRY_VERSION": action.registry_version,
                "PROPOSED_VERSION": action.proposed_version,
                "IMPACT": (
                    diff.impact.value
                    if diff is not None and diff.impact is not None
                    else None
                ),
                "N_BREAKING": (
                    len(diff.by_impact(ChangeImpact.BREAKING)) if diff else 0
                ),
                "N_ADDITIVE": (
                    len(diff.by_impact(ChangeImpact.ADDITIVE)) if diff else 0
                ),
                "N_COSMETIC": (
                    len(diff.by_impact(ChangeImpact.COSMETIC)) if diff else 0
                ),
                "BLOCKED": action.is_blocked,
                "ISSUES": "; ".join(f"{i.rule_id}: {i.message}" for i in action.issues),
            }
        )
    return pd.DataFrame(rows, columns=_PLAN_COLUMNS)


@typechecked
def report_to_dataframe(report: PublicationReport) -> pd.DataFrame:
    """Render a publication report as a DataFrame.

    Args:
        report: The report returned by
            :func:`~tidysdmx.fmr.publish.execute_plan` or
            :func:`~tidysdmx.fmr.publish.publish`.

    Returns:
        One row per result with ``SHORT_URN``, ``ACTION``, ``STATUS``,
        and ``ERROR`` columns.
    """
    rows = [
        {
            "SHORT_URN": r.short_urn,
            "ACTION": r.kind.value,
            "STATUS": r.status,
            "ERROR": r.error,
        }
        for r in report.results
    ]
    return pd.DataFrame(rows, columns=_REPORT_COLUMNS)
