"""Upsert workflow: plan and execute artefact publications to the FMR.

The workflow has two phases:

1. :func:`plan_publication` — a **read-only** planning pass. For each
   artefact it fetches the registry's current copy, diffs it (see
   :mod:`tidysdmx.fmr.diff`), and proposes an action: ``CREATE`` (not in
   the registry), ``UPDATE`` at a bumped version (see
   :mod:`tidysdmx.fmr.versioning`), or ``SKIP`` (content identical).
   Artefacts are ordered dependencies-first, publish-readiness is
   validated, version conflicts are detected, and version bumps are
   propagated to intra-batch references (e.g. a Dataflow pointing at a
   bumped DSD). The resulting :class:`PublicationPlan` is printable and
   fully dry-run-able.
2. :func:`execute_plan` — submits the plan via
   :meth:`tidysdmx.fmr.client.FmrClient.put_artefacts` and reports
   per-artefact outcomes in a :class:`PublicationReport`.

:func:`publish` chains both for the common case.

Plan-level issues reuse :class:`tidysdmx.artefact_validation.ValidationIssue`.
Publish-readiness failures keep their original rule ids (``M001`` etc.);
workflow-specific issues use these codes:

- ``P002`` — the registry holds a newer version than the local baseline
  (blocking for updates, informational for identical content);
- ``P003`` — a version string could not be parsed;
- ``P004`` — an intra-batch reference was retargeted to a bumped version
  (warning);
- ``P005`` — a breaking update was planned while ``allow_breaking`` is
  false (blocking);
- ``P006`` — StructureAction.Append combined with an update (warning:
  the FMR rejects overwrites under Append);
- ``P007`` — StructureAction.Merge combined with item removals
  (warning: Merge unions item schemes, so removals will not apply);
- ``P008`` — the proposed version carries a prerelease (``-draft``)
  extension while the policy is in :attr:`VersioningMode.SEMVER_ONLY`
  (blocking: the current FMR rejects such versions).

Known limitations (v1): references *from* registry artefacts outside
the submitted batch are not updated, and the plan is trusted at execute
time (no re-fetch between planning and submission — the ``P002`` check
is the mitigation). Item-level URNs inside bumped artefacts are left
untouched; the FMR derives canonical URNs on ingestion.
"""

import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from pysdmx.api.fmr.maintenance import StructureAction
from pysdmx.errors import Invalid, PysdmxError
from pysdmx.model import (
    AgencyScheme,
    Categorisation,
    CategoryScheme,
    Codelist,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    Hierarchy,
    MultiRepresentationMap,
    ProvisionAgreement,
    RepresentationMap,
    StructureMap,
)
from pysdmx.model.submission import SubmissionResult
from typeguard import typechecked

from tidysdmx.artefact_validation import (
    ValidationError,
    ValidationIssue,
    validate,
)

from ._compat import MaintainableArtefact
from ._compat import agency_id as _agency_id
from ._refs import apply_version as _apply_version
from ._refs import map_references as _map_references
from .client import FmrClient
from .diff import (
    ArtefactChange,
    ArtefactDiff,
    ChangeImpact,
    ChangeKind,
    compare_artefacts,
)
from .versioning import (
    DEFAULT_VERSION_POLICY,
    VersioningMode,
    VersionPolicy,
    bump_version_for_impact,
    compare_versions,
    parse_version,
    suggest_version,
)

logger = logging.getLogger(__name__)


class PlannedActionKind(StrEnum):
    """The action planned for one artefact.

    Attributes:
        CREATE: The artefact does not exist in the registry yet.
        UPDATE: The registry copy differs; publish at a bumped version.
        SKIP: The registry copy is identical; nothing to publish.
    """

    CREATE = "create"
    UPDATE = "update"
    SKIP = "skip"


@dataclass(frozen=True)
class PlannedAction:
    """The planned treatment of a single artefact.

    Attributes:
        artefact: The updated local artefact (original version; the
            version bump is applied by
            :meth:`PublicationPlan.to_publish`). Intra-batch references
            have already been retargeted to bumped versions.
        kind: The planned action.
        diff: The registry-vs-local diff (``None`` for CREATE).
        registry_version: The latest version found in the registry
            (``None`` for CREATE).
        proposed_version: The version the artefact will be published at.
        issues: Validation and workflow issues attached to this action.
    """

    artefact: MaintainableArtefact
    kind: PlannedActionKind
    diff: ArtefactDiff | None
    registry_version: str | None
    proposed_version: str
    issues: tuple[ValidationIssue, ...] = ()

    @property
    def short_urn(self) -> str:
        """The artefact identity at the proposed version."""
        agency = _agency_id(self.artefact.agency)
        return (
            f"{type(self.artefact).__name__}={agency}:"
            f"{self.artefact.id}({self.proposed_version})"
        )

    @property
    def is_blocked(self) -> bool:
        """Whether any attached issue blocks execution."""
        return any(i.severity == "error" for i in self.issues)


@dataclass(frozen=True)
class PublicationPlan:
    """A dependency-ordered, dry-run-able publication plan.

    Attributes:
        actions: The planned actions, dependencies first.
        structure_action: The FMR action the plan will be submitted
            with (Append, Merge, or Replace).
    """

    actions: tuple[PlannedAction, ...]
    structure_action: StructureAction

    @property
    def has_blocking_issues(self) -> bool:
        """Whether any action carries a blocking (error) issue."""
        return any(a.is_blocked for a in self.actions)

    def to_publish(self) -> list[MaintainableArtefact]:
        """Return the artefacts to submit, with version bumps applied.

        SKIP actions are excluded; CREATE and UPDATE artefacts get
        their proposed version applied via ``msgspec.structs.replace``.

        Returns:
            The publishable artefacts in dependency order.
        """
        out: list[MaintainableArtefact] = []
        for action in self.actions:
            if action.kind == PlannedActionKind.SKIP:
                continue
            out.append(_apply_version(action.artefact, action.proposed_version))
        return out

    def summary(self) -> str:
        """Render a human-readable multi-line summary of the plan.

        Returns:
            A header with action counts, one line per action, and one
            indented line per attached issue.
        """
        counts = Counter(a.kind for a in self.actions)
        lines = [
            f"Publication plan (action={self.structure_action.value}): "
            f"{counts.get(PlannedActionKind.CREATE, 0)} create, "
            f"{counts.get(PlannedActionKind.UPDATE, 0)} update, "
            f"{counts.get(PlannedActionKind.SKIP, 0)} skip"
        ]
        for action in self.actions:
            if action.kind == PlannedActionKind.UPDATE:
                impact = action.diff.impact if action.diff else None
                n_changes = len(action.diff.changes) if action.diff else 0
                lines.append(
                    f"  UPDATE {action.short_urn} "
                    f"({action.registry_version} -> "
                    f"{action.proposed_version}, "
                    f"{impact.value if impact else 'unknown'}, "
                    f"{n_changes} change(s))"
                )
            elif action.kind == PlannedActionKind.CREATE:
                lines.append(f"  CREATE {action.short_urn}")
            else:
                lines.append(f"  SKIP   {action.short_urn} (unchanged)")
            for issue in action.issues:
                lines.append(
                    f"    ! [{issue.rule_id}/{issue.severity}] {issue.message}"
                )
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return :meth:`summary`."""
        return self.summary()


@dataclass(frozen=True)
class PublicationResult:
    """The outcome of one planned action after execution.

    Attributes:
        short_urn: The artefact identity at the published version.
        kind: The planned action kind.
        status: ``published``, ``skipped`` (SKIP actions and dry runs),
            ``failed``, or ``not_attempted`` (aborted after an earlier
            failure).
        error: The error message for failed actions.
        submission: The FMR submission result, when available. Always
            ``None`` in v1: pysdmx's maintenance client discards the
            FMR response body. Reserved for when response parsing is
            upstreamed.
    """

    short_urn: str
    kind: PlannedActionKind
    status: Literal["published", "skipped", "failed", "not_attempted"]
    error: str | None = None
    submission: SubmissionResult | None = None


@dataclass(frozen=True)
class PublicationReport:
    """Per-artefact outcomes of a plan execution.

    Attributes:
        results: One result per planned action, in plan order.
    """

    results: tuple[PublicationResult, ...]

    @property
    def ok(self) -> bool:
        """Whether every action was published or legitimately skipped."""
        return all(r.status in ("published", "skipped") for r in self.results)

    def summary(self) -> str:
        """Render a human-readable multi-line summary of the report.

        Returns:
            A header with status counts and one line per result.
        """
        counts = Counter(r.status for r in self.results)
        parts = ", ".join(f"{n} {s}" for s, n in sorted(counts.items()))
        lines = [f"Publication report: {parts or 'nothing to do'}"]
        for r in self.results:
            line = f"  {r.status.upper():13s} {r.short_urn}"
            if r.error:
                line += f" — {r.error}"
            lines.append(line)
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return :meth:`summary`."""
        return self.summary()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: Dependency layer per artefact type; lower layers are published first.
#: ConceptScheme sits above the other item schemes because concepts can
#: reference codelists (``enum_ref``/``codes``) — reference propagation
#: processes drafts in this order, so referrers must come after their
#: possible dependencies.
_LAYERS: dict[type[MaintainableArtefact], int] = {
    Codelist: 0,
    AgencyScheme: 0,
    CategoryScheme: 0,
    ConceptScheme: 1,
    Hierarchy: 2,
    RepresentationMap: 2,
    MultiRepresentationMap: 2,
    DataStructureDefinition: 3,
    Dataflow: 4,
    StructureMap: 4,
    ProvisionAgreement: 5,
    Categorisation: 5,
}

#: Layer used for artefact types without an explicit entry.
_DEFAULT_LAYER = 4


def _identity(artefact: MaintainableArtefact) -> tuple[str, str, str]:
    return (
        type(artefact).__name__,
        _agency_id(artefact.agency),
        artefact.id,
    )


def _issue(
    rule_id: str,
    artefact: MaintainableArtefact,
    message: str,
    field: str | None = None,
    severity: Literal["error", "warning"] = "error",
) -> ValidationIssue:
    return ValidationIssue(
        rule_id=rule_id,
        path=artefact.short_urn,
        message=message,
        field=field,
        severity=severity,
    )


def _dependency_order(
    artefacts: Sequence[MaintainableArtefact],
) -> list[MaintainableArtefact]:
    """Order artefacts dependencies-first; reject duplicates."""
    seen: set[tuple[str, str, str]] = set()
    for artefact in artefacts:
        key = _identity(artefact)
        if key in seen:
            raise Invalid(
                "Duplicate artefact",
                f"{key[0]} {key[1]}:{key[2]} appears more than once in "
                "the batch; a publication plan needs a single candidate "
                "per artefact.",
            )
        seen.add(key)
    return sorted(
        artefacts,
        key=lambda a: _LAYERS.get(type(a), _DEFAULT_LAYER),
    )


def _collect_reference_keys(
    artefact: MaintainableArtefact,
) -> set[tuple[str, str]]:
    """Collect the (agency, id) pairs referenced by an artefact."""
    keys: set[tuple[str, str]] = set()

    def recorder(agency: str, aid: str, version: str) -> str | None:
        keys.add((agency, aid))
        return None

    _map_references(artefact, recorder)
    return keys


def _safe_compare(a: str, b: str) -> int | None:
    try:
        return compare_versions(a, b)
    except ValueError:
        return None


class _Draft:
    """Mutable intermediate for one action while the plan is built."""

    def __init__(
        self,
        artefact: MaintainableArtefact,
        kind: PlannedActionKind,
        diff: ArtefactDiff | None,
        existing: MaintainableArtefact | None,
        proposed_version: str,
        issues: list[ValidationIssue],
    ):
        self.artefact = artefact
        self.kind = kind
        self.diff = diff
        self.existing = existing
        self.proposed_version = proposed_version
        self.issues = issues

    @property
    def registry_version(self) -> str | None:
        return self.existing.version if self.existing is not None else None

    def freeze(self) -> PlannedAction:
        return PlannedAction(
            artefact=self.artefact,
            kind=self.kind,
            diff=self.diff,
            registry_version=self.registry_version,
            proposed_version=self.proposed_version,
            issues=tuple(self.issues),
        )


def _plan_one(
    client: FmrClient,
    artefact: MaintainableArtefact,
    policy: VersionPolicy,
    allow_breaking: bool,
) -> _Draft:
    issues: list[ValidationIssue] = []
    existing = client.get_existing(artefact)
    if existing is None:
        kind = PlannedActionKind.CREATE
        diff = None
        proposed = artefact.version
        try:
            parse_version(artefact.version)
        except ValueError as err:
            issues.append(_issue("P003", artefact, str(err), "version"))
    else:
        # Ignore reference *versions*: an artefact whose only difference from
        # the registry copy is that a reference points at a co-bumped (or
        # registry-normalised) version of the same dependency has not really
        # changed. The version follow is classified in _retarget_references.
        diff = compare_artefacts(existing, artefact, ignore_reference_versions=True)
        if diff.is_unchanged:
            kind = PlannedActionKind.SKIP
            proposed = existing.version
            cmp = _safe_compare(artefact.version, existing.version)
            if cmp is not None and cmp < 0:
                issues.append(
                    _issue(
                        "P002",
                        artefact,
                        f"Registry already holds {existing.version} "
                        f"(local baseline is {artefact.version}); "
                        "content is identical.",
                        "version",
                        severity="warning",
                    )
                )
        else:
            kind = PlannedActionKind.UPDATE
            try:
                proposed = suggest_version(diff, existing.version, policy)
            except ValueError as err:
                proposed = existing.version
                issues.append(_issue("P003", artefact, str(err), "version"))
            cmp = _safe_compare(artefact.version, existing.version)
            if cmp is not None and cmp < 0:
                issues.append(
                    _issue(
                        "P002",
                        artefact,
                        f"Registry holds {existing.version}, newer than "
                        f"the local baseline {artefact.version}: another "
                        "publisher may have updated this artefact since "
                        "it was fetched. Refresh and re-plan.",
                        "version",
                    )
                )
            if not allow_breaking and diff.impact == ChangeImpact.BREAKING:
                issues.append(
                    _issue(
                        "P005",
                        artefact,
                        "Breaking changes are not allowed by this plan "
                        "(allow_breaking=False).",
                    )
                )
    if kind != PlannedActionKind.SKIP:
        issues.extend(validate(artefact))
    return _Draft(artefact, kind, diff, existing, proposed, issues)


_IMPACT_ORDER: dict[ChangeImpact, int] = {
    ChangeImpact.COSMETIC: 0,
    ChangeImpact.ADDITIVE: 1,
    ChangeImpact.BREAKING: 2,
}


def _max_impact(*impacts: ChangeImpact | None) -> ChangeImpact | None:
    """Return the most severe non-``None`` impact, or ``None`` if all are."""
    present = [i for i in impacts if i is not None]
    if not present:
        return None
    return max(present, key=lambda i: _IMPACT_ORDER[i])


def _retarget_references(
    drafts: list[_Draft], policy: VersionPolicy, allow_breaking: bool
) -> None:
    """Rewrite intra-batch references to bumped versions (in order).

    Processes drafts in dependency order, so a dependent always sees its
    dependencies' final proposed versions. A reference re-pointed to a
    co-bumped dependency is a mechanical follow, not a content change: the
    dependent's genuine diff is recomputed ignoring reference versions,
    and the version bump it *inherits* is the most severe impact among the
    dependencies it followed (breaking dependency → major, additive →
    minor, and so on). A SKIP whose references were rewritten is promoted
    to UPDATE so its reference stays current in the registry.
    """
    # (agency, id) -> (versions to replace, replacement, impact bumped for)
    rewrites: dict[tuple[str, str], tuple[set[str], str, ChangeImpact | None]] = {}
    followed: set[tuple[str, str]] = set()

    def mapper(agency: str, aid: str, version: str) -> str | None:
        entry = rewrites.get((agency, aid))
        if entry is None:
            return None
        old_versions, new_version, _impact = entry
        if version in old_versions:
            followed.add((agency, aid))
            return new_version
        return None

    for draft in drafts:
        followed.clear()
        bump_impact = (
            draft.diff.impact
            if draft.kind == PlannedActionKind.UPDATE and draft.diff is not None
            else None
        )
        if rewrites:
            new_artefact, paths = _map_references(draft.artefact, mapper)
            if paths:
                draft.artefact = new_artefact
                draft.issues.append(
                    _issue(
                        "P004",
                        draft.artefact,
                        "Intra-batch references retargeted to bumped "
                        f"versions: {', '.join(sorted(set(paths)))}.",
                        severity="warning",
                    )
                )
            # A CREATE keeps the retargeted references but is not bumped.
            if paths and draft.existing is not None:
                # Genuine content diff ignores the reference version follow;
                # the dependent inherits the impact of the dependencies it
                # followed instead of a fabricated breaking change.
                draft.diff = compare_artefacts(
                    draft.existing, draft.artefact, ignore_reference_versions=True
                )
                followed_impact = _max_impact(
                    *(rewrites[k][2] for k in followed if k in rewrites)
                )
                if followed_impact is not None:
                    # Record the follow as a change carrying the inherited
                    # impact, so the plan summary and reports explain why the
                    # dependent bumps even with no content change of its own.
                    follow = ArtefactChange(
                        kind=ChangeKind.MODIFIED,
                        impact=followed_impact,
                        path=", ".join(sorted(set(paths))),
                        message="References retargeted to co-bumped dependencies.",
                    )
                    draft.diff = ArtefactDiff(
                        short_urn=draft.diff.short_urn,
                        artefact_type=draft.diff.artefact_type,
                        changes=(*draft.diff.changes, follow),
                    )
                combined = draft.diff.impact
                if combined is not None:
                    bump_impact = combined
                    if draft.kind == PlannedActionKind.SKIP:
                        draft.kind = PlannedActionKind.UPDATE
                        draft.issues.extend(validate(draft.artefact))
                    try:
                        draft.proposed_version = bump_version_for_impact(
                            draft.existing.version, combined, policy
                        )
                    except ValueError as err:
                        draft.issues.append(
                            _issue("P003", draft.artefact, str(err), "version")
                        )
                    if (
                        not allow_breaking
                        and combined == ChangeImpact.BREAKING
                        and not any(i.rule_id == "P005" for i in draft.issues)
                    ):
                        draft.issues.append(
                            _issue(
                                "P005",
                                draft.artefact,
                                "Breaking changes are not allowed by this "
                                "plan (allow_breaking=False).",
                            )
                        )
        old_versions = {draft.artefact.version}
        if draft.registry_version:
            old_versions.add(draft.registry_version)
        old_versions.discard(draft.proposed_version)
        if old_versions:
            key = (_agency_id(draft.artefact.agency), draft.artefact.id)
            rewrites[key] = (old_versions, draft.proposed_version, bump_impact)


def _action_warnings(draft: _Draft, structure_action: StructureAction) -> None:
    if (
        structure_action == StructureAction.Append
        and draft.kind == PlannedActionKind.UPDATE
    ):
        draft.issues.append(
            _issue(
                "P006",
                draft.artefact,
                "Action 'Append' cannot overwrite existing structures; "
                "this update is likely to be rejected by the FMR.",
                severity="warning",
            )
        )
    if (
        structure_action == StructureAction.Merge
        and draft.diff is not None
        and any(c.kind == ChangeKind.REMOVED for c in draft.diff.changes)
    ):
        draft.issues.append(
            _issue(
                "P007",
                draft.artefact,
                "Action 'Merge' unions item schemes: the removals in "
                "this diff will not be applied by the FMR.",
                severity="warning",
            )
        )


def _semver_mode_guard(draft: _Draft, policy: VersionPolicy) -> None:
    """Block a prerelease proposed version under ``SEMVER_ONLY`` (P008).

    The current FMR rejects prerelease (``-draft``) versions with a 500,
    so under :attr:`VersioningMode.SEMVER_ONLY` any parseable proposed
    version carrying a hyphen extension is a blocking issue. Wildcard and
    otherwise-unparseable versions surface as ``P003`` elsewhere, so this
    guard only needs the parseable-prerelease case. SKIP actions are not
    published and are left untouched.
    """
    if policy.mode != VersioningMode.SEMVER_ONLY:
        return
    if draft.kind == PlannedActionKind.SKIP:
        return
    try:
        parsed = parse_version(draft.proposed_version)
    except ValueError:
        return
    if parsed.extension is None:
        return
    draft.issues.append(
        _issue(
            "P008",
            draft.artefact,
            f"Version '{draft.proposed_version}' carries a prerelease "
            "extension, which the target FMR does not accept yet (SDMX "
            "3.0 draft/prerelease versioning ships in a later FMR "
            "release). Publish a plain semver (X.Y.Z), or set "
            "VersionPolicy(mode=VersioningMode.SDMX_3) once the registry "
            "supports it.",
            "version",
        )
    )


@typechecked
def rebase_to_registry(
    client: FmrClient,
    artefacts: Sequence[MaintainableArtefact],
) -> list[MaintainableArtefact]:
    """Seed each artefact's version from the registry before planning.

    In a build-fresh pipeline the local artefacts are rebuilt at a fixed
    baseline every run, so once the registry has advanced past that
    baseline :func:`plan_publication` raises a blocking ``P002`` and the
    version bump is computed off a stale baseline. Rebasing removes that
    friction by making the registry's current version the baseline.

    Pass 1 sets each artefact's version to the registry's latest
    counterpart (same type, agency, and id); artefacts absent from the
    registry keep their build-time initial version. Pass 2 retargets every
    intra-batch reference — a StructureMap's ``source``/``target`` and its
    embedded RepresentationMaps, a DSD's embedded codelists and concept
    references, and so on — to the seeded version of the artefact it points
    at, so the batch is internally consistent with the baselines it is
    about to be diffed against. References to artefacts outside the batch
    are left untouched.

    Non-final artefacts (e.g. a ``"1.0.0-draft"`` scratch schema under a
    :class:`~tidysdmx.fmr.versioning.VersionPolicy` with
    ``replace_non_final=True`` **and**
    ``mode=VersioningMode.SDMX_3``) therefore resolve to an in-place
    replace at the same version rather than a spurious bump. Under the
    default ``SEMVER_ONLY`` mode such drafts are instead bumped to a plain
    semver, and publishing a ``-draft`` version is blocked (``P008``).

    Args:
        client: The FMR client (read access is sufficient).
        artefacts: The freshly-built local artefacts to rebase.

    Returns:
        The artefacts in input order, re-versioned and with intra-batch
        references normalised to the seeded versions.
    """
    targets: dict[tuple[str, str], str] = {}
    seeded: list[MaintainableArtefact] = []
    for artefact in artefacts:
        existing = client.get_existing(artefact)
        target = existing.version if existing is not None else artefact.version
        targets[(_agency_id(artefact.agency), artefact.id)] = target
        seeded.append(_apply_version(artefact, target))

    def mapper(agency: str, aid: str, version: str) -> str | None:
        return targets.get((agency, aid))

    return [_map_references(artefact, mapper)[0] for artefact in seeded]


@typechecked
def plan_publication(
    client: FmrClient,
    artefacts: Sequence[MaintainableArtefact],
    policy: VersionPolicy = DEFAULT_VERSION_POLICY,
    action: StructureAction = StructureAction.Replace,
    allow_breaking: bool = True,
    propagate_references: bool = True,
) -> PublicationPlan:
    """Plan the publication of a batch of artefacts (read-only).

    For each artefact, fetches the registry's latest copy, diffs it,
    and proposes CREATE, UPDATE (with a suggested version bump), or
    SKIP. The plan is safe to build repeatedly: nothing is written.

    Args:
        client: The FMR client (read access is sufficient).
        artefacts: The updated local artefacts to publish.
        policy: The version bump policy.
        action: The FMR structure action the plan will execute with.
        allow_breaking: If ``False``, updates with breaking impact get
            a blocking ``P005`` issue.
        propagate_references: Rewrite intra-batch references to bumped
            versions (e.g. a Dataflow pointing at a bumped DSD) and
            promote affected SKIPs to UPDATEs.

    Returns:
        The :class:`PublicationPlan`, dependencies first.

    Raises:
        Invalid: If the same artefact appears more than once.

    Examples:
        >>> plan = plan_publication(client, [codelist, dataflow])
        ... # doctest: +SKIP
        >>> print(plan.summary())  # doctest: +SKIP
        >>> report = execute_plan(client, plan, dry_run=True)
        ... # doctest: +SKIP
    """
    ordered = _dependency_order(artefacts)
    drafts = [
        _plan_one(client, artefact, policy, allow_breaking) for artefact in ordered
    ]
    if propagate_references:
        _retarget_references(drafts, policy, allow_breaking)
    for draft in drafts:
        _action_warnings(draft, action)
        _semver_mode_guard(draft, policy)
    return PublicationPlan(
        actions=tuple(d.freeze() for d in drafts),
        structure_action=action,
    )


@typechecked
def inplace_breaking_actions(plan: PublicationPlan) -> tuple[PlannedAction, ...]:
    """Return plan actions that overwrite an artefact in place with a breaking diff.

    These are updates whose proposed version equals the version already in
    the registry — a breaking change applied under ``replace_non_final``
    without a new version, so the prior artefact is silently overwritten.
    A pipeline can hard-stop on a non-empty result rather than clobber a
    non-final (e.g. draft) artefact with an incompatible structure.

    Args:
        plan: The publication plan to inspect.

    Returns:
        The in-place breaking :class:`PlannedAction` records, in plan order.
    """
    return tuple(
        action
        for action in plan.actions
        if action.kind == PlannedActionKind.UPDATE
        and action.diff is not None
        and action.diff.impact == ChangeImpact.BREAKING
        and action.registry_version is not None
        and action.proposed_version == action.registry_version
    )


@typechecked
def execute_plan(
    client: FmrClient,
    plan: PublicationPlan,
    dry_run: bool = False,
    batch: bool = True,
    continue_on_error: bool = False,
) -> PublicationReport:
    """Execute a publication plan against the FMR.

    Blocking issues abort before any network call. With ``batch=True``
    (default) all publishable artefacts go up in a single submission,
    which the FMR applies transactionally. With ``batch=False`` they
    are submitted one by one in dependency order; after a failure the
    remaining actions are marked ``not_attempted`` unless
    ``continue_on_error`` is set (dependents of a failed artefact are
    never attempted).

    Args:
        client: The FMR client (write credentials required unless every
            action is a SKIP or ``dry_run`` is set).
        plan: The plan to execute.
        dry_run: Report what would happen without any network call;
            every publishable action is reported as ``skipped``.
        batch: Submit everything in one FMR call (default) or
            artefact-by-artefact.
        continue_on_error: In unbatched mode, keep submitting
            independent artefacts after a failure.

    Returns:
        The :class:`PublicationReport` with one result per action.

    Raises:
        ValidationError: If the plan carries blocking issues.
    """
    if plan.has_blocking_issues:
        blocking = [
            issue
            for action in plan.actions
            for issue in action.issues
            if issue.severity == "error"
        ]
        raise ValidationError(blocking)

    results: dict[int, PublicationResult] = {}
    publishable: list[tuple[int, PlannedAction]] = []
    for idx, action in enumerate(plan.actions):
        if action.kind == PlannedActionKind.SKIP:
            results[idx] = PublicationResult(
                short_urn=action.short_urn,
                kind=action.kind,
                status="skipped",
            )
        else:
            publishable.append((idx, action))

    if dry_run:
        for idx, action in publishable:
            results[idx] = PublicationResult(
                short_urn=action.short_urn,
                kind=action.kind,
                status="skipped",
            )
    elif batch:
        artefacts = plan.to_publish()
        status: Literal["published", "failed"] = "published"
        error: str | None = None
        if artefacts:
            try:
                client.put_artefacts(
                    artefacts,
                    action=plan.structure_action,
                    validate=False,
                )
            except PysdmxError as err:
                status = "failed"
                error = str(err)
                logger.error("Batch submission failed: %s", err)
        for idx, action in publishable:
            results[idx] = PublicationResult(
                short_urn=action.short_urn,
                kind=action.kind,
                status=status,
                error=error,
            )
    else:
        failed_keys: set[tuple[str, str]] = set()
        aborted = False
        for idx, action in publishable:
            if aborted:
                results[idx] = PublicationResult(
                    short_urn=action.short_urn,
                    kind=action.kind,
                    status="not_attempted",
                )
                continue
            deps = _collect_reference_keys(action.artefact)
            if deps & failed_keys:
                results[idx] = PublicationResult(
                    short_urn=action.short_urn,
                    kind=action.kind,
                    status="not_attempted",
                    error="A dependency failed to publish.",
                )
                continue
            artefact = _apply_version(action.artefact, action.proposed_version)
            try:
                client.put_artefacts(
                    [artefact],
                    action=plan.structure_action,
                    validate=False,
                )
            except PysdmxError as err:
                logger.error("Submission of %s failed: %s", action.short_urn, err)
                results[idx] = PublicationResult(
                    short_urn=action.short_urn,
                    kind=action.kind,
                    status="failed",
                    error=str(err),
                )
                failed_keys.add(
                    (
                        _agency_id(action.artefact.agency),
                        action.artefact.id,
                    )
                )
                if not continue_on_error:
                    aborted = True
                continue
            results[idx] = PublicationResult(
                short_urn=action.short_urn,
                kind=action.kind,
                status="published",
            )

    return PublicationReport(
        results=tuple(results[i] for i in range(len(plan.actions)))
    )


@typechecked
def publish(
    client: FmrClient,
    artefacts: Sequence[MaintainableArtefact],
    policy: VersionPolicy = DEFAULT_VERSION_POLICY,
    action: StructureAction = StructureAction.Replace,
    allow_breaking: bool = True,
    dry_run: bool = False,
) -> PublicationReport:
    """Plan and execute the publication of a batch of artefacts.

    Convenience wrapper chaining :func:`plan_publication` and
    :func:`execute_plan`. To inspect the plan before writing anything,
    call :func:`plan_publication` yourself or pass ``dry_run=True``.

    Args:
        client: The FMR client.
        artefacts: The updated local artefacts to publish.
        policy: The version bump policy.
        action: The FMR structure action (Append, Merge, or Replace).
        allow_breaking: If ``False``, breaking updates block the plan.
        dry_run: Plan and report without writing to the registry.

    Returns:
        The :class:`PublicationReport`.

    Raises:
        ValidationError: If the plan carries blocking issues.
        Invalid: If the same artefact appears more than once.

    Examples:
        >>> report = publish(client, [codelist], dry_run=True)
        ... # doctest: +SKIP
        >>> print(report.summary())  # doctest: +SKIP
    """
    plan = plan_publication(
        client,
        artefacts,
        policy=policy,
        action=action,
        allow_breaking=allow_breaking,
    )
    report = execute_plan(client, plan, dry_run=dry_run)
    logger.info("%s", report.summary())
    return report
