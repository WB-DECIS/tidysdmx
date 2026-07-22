"""Enhanced FMR client layer built on top of pysdmx.

This subpackage removes the friction of real-world FMR (Fusion Metadata
Registry) publication workflows. On top of pysdmx's CRUD clients it adds:

- :class:`FmrClient` — a unified read/write facade with env-var
  credentials and registry-agnostic URLs.
- :func:`compare_artefacts` — change detection between an existing
  (registry) artefact and an updated local one, with every change
  classified as breaking, additive, or cosmetic.
- :func:`parse_version` / :func:`bump_version` / :func:`suggest_version`
  — SDMX version algebra and policy-driven automated bumping.
- :func:`plan_publication` / :func:`execute_plan` / :func:`publish` — a
  dry-run-able upsert workflow (fetch → diff → skip unchanged → bump
  versions → propagate references → submit).
- :func:`diff_to_dataframe` / :func:`plan_to_dataframe` /
  :func:`report_to_dataframe` — pandas views of diffs and plans.

Typical usage::

    from tidysdmx.fmr import FmrClient, plan_publication, execute_plan

    client = FmrClient()  # URL/credentials from TIDYSDMX_FMR_* env vars
    plan = plan_publication(client, artefacts)
    print(plan.summary())
    report = execute_plan(client, plan)

The core modules depend only on pysdmx, the standard library, and
typeguard (plus :mod:`tidysdmx.artefact_validation`, itself pysdmx-only),
so the subpackage stays extractable into a standalone distribution.
pandas is used only by :mod:`tidysdmx.fmr.report`.
"""

from pysdmx.api.fmr.maintenance import StructureAction

from .client import (
    ENV_FMR_PASSWORD,
    ENV_FMR_TOKEN,
    ENV_FMR_URL,
    ENV_FMR_USER,
    ArtefactType,
    FmrClient,
)
from .diff import (
    ArtefactChange,
    ArtefactDiff,
    ChangeImpact,
    ChangeKind,
    compare_artefacts,
)
from .publish import (
    PlannedAction,
    PlannedActionKind,
    PublicationPlan,
    PublicationReport,
    PublicationResult,
    execute_plan,
    inplace_breaking_actions,
    plan_publication,
    publish,
    rebase_to_registry,
)
from .report import (
    diff_to_dataframe,
    plan_to_dataframe,
    report_to_dataframe,
)
from .versioning import (
    DEFAULT_VERSION_POLICY,
    SdmxVersion,
    VersionPolicy,
    VersionScheme,
    bump_version,
    compare_versions,
    parse_version,
    suggest_version,
)

__all__ = [
    "DEFAULT_VERSION_POLICY",
    "ENV_FMR_PASSWORD",
    "ENV_FMR_TOKEN",
    "ENV_FMR_URL",
    "ENV_FMR_USER",
    "ArtefactChange",
    "ArtefactDiff",
    "ArtefactType",
    "ChangeImpact",
    "ChangeKind",
    "FmrClient",
    "PlannedAction",
    "PlannedActionKind",
    "PublicationPlan",
    "PublicationReport",
    "PublicationResult",
    "SdmxVersion",
    "StructureAction",
    "VersionPolicy",
    "VersionScheme",
    "bump_version",
    "compare_artefacts",
    "compare_versions",
    "diff_to_dataframe",
    "execute_plan",
    "inplace_breaking_actions",
    "parse_version",
    "plan_publication",
    "plan_to_dataframe",
    "publish",
    "rebase_to_registry",
    "report_to_dataframe",
    "suggest_version",
]
