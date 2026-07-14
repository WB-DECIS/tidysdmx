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
  (warning: Merge unions item schemes, so removals will not apply).

Known limitations (v1): references *from* registry artefacts outside
the submitted batch are not updated, and the plan is trusted at execute
time (no re-fetch between planning and submission — the ``P002`` check
is the mitigation). Item-level URNs inside bumped artefacts are left
untouched; the FMR derives canonical URNs on ingestion.
"""

import logging
import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

import msgspec
from pysdmx.api.fmr.maintenance import StructureAction
from pysdmx.errors import Invalid, PysdmxError
from pysdmx.model import (
    Agency,
    AgencyScheme,
    Categorisation,
    CategoryScheme,
    Codelist,
    ComponentMap,
    Components,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    HierarchicalCode,
    Hierarchy,
    ItemReference,
    MultiComponentMap,
    MultiRepresentationMap,
    ProvisionAgreement,
    RepresentationMap,
    StructureMap,
)
from pysdmx.model.__base import MaintainableArtefact
from pysdmx.model.submission import SubmissionResult
from typeguard import typechecked

from tidysdmx.artefact_validation import (
    ValidationError,
    ValidationIssue,
    validate,
)

from .client import FmrClient
from .diff import ArtefactDiff, ChangeImpact, ChangeKind, compare_artefacts
from .versioning import (
    DEFAULT_VERSION_POLICY,
    VersionPolicy,
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
_LAYERS: dict[type[MaintainableArtefact], int] = {
    Codelist: 0,
    ConceptScheme: 0,
    AgencyScheme: 0,
    CategoryScheme: 0,
    Hierarchy: 1,
    RepresentationMap: 1,
    MultiRepresentationMap: 1,
    DataStructureDefinition: 2,
    Dataflow: 3,
    StructureMap: 3,
    ProvisionAgreement: 4,
    Categorisation: 4,
}

#: Layer used for artefact types without an explicit entry.
_DEFAULT_LAYER = 3

_REF_TOKEN_RE = re.compile(
    r"(?P<agency>[A-Za-z0-9_.\-]+):(?P<id>[A-Za-z0-9_.\-@$]+)"
    r"\((?P<version>[^)]+)\)"
)


def _agency_id(agency: str | Agency) -> str:
    return agency.id if isinstance(agency, Agency) else agency


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


def _apply_version(
    artefact: MaintainableArtefact, version: str
) -> MaintainableArtefact:
    """Return the artefact at the given version, fixing its own URN."""
    if artefact.version == version:
        return artefact
    updated = msgspec.structs.replace(artefact, version=version)
    if artefact.urn:
        suffix = f"({artefact.version})"
        if artefact.urn.endswith(suffix):
            new_urn = artefact.urn[: -len(suffix)] + f"({version})"
        else:
            new_urn = None
        updated = msgspec.structs.replace(updated, urn=new_urn)
    return updated


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


# A mapper receives (agency, id, version) of a reference and returns the
# replacement version, or None to leave the reference untouched.
_RefMapper = Callable[[str, str, str], str | None]


def _map_references(
    artefact: MaintainableArtefact, mapper: _RefMapper
) -> tuple[MaintainableArtefact, list[str]]:
    """Apply ``mapper`` to every known reference-bearing field.

    Covers URN strings (full, short, or ``agency:id(version)`` tokens),
    embedded maintainable artefacts, and item references. Returns the
    (possibly rebuilt) artefact and the list of changed field paths.
    """
    changed: list[str] = []

    def map_text(text: str | None, path: str) -> str | None:
        if not text:
            return text

        def sub(m: re.Match) -> str:
            new_version = mapper(m.group("agency"), m.group("id"), m.group("version"))
            if new_version is None or new_version == m.group("version"):
                return m.group(0)
            changed.append(path)
            return f"{m.group('agency')}:{m.group('id')}({new_version})"

        return _REF_TOKEN_RE.sub(sub, text)

    def map_embedded(obj: Any, path: str) -> Any:
        if obj is None:
            return None
        new_version = mapper(_agency_id(obj.agency), obj.id, obj.version)
        if new_version is not None and new_version != obj.version:
            changed.append(path)
            return _apply_version(obj, new_version)
        return obj

    def map_item_ref(ref: ItemReference, path: str) -> ItemReference:
        new_version = mapper(ref.agency, ref.id, ref.version)
        if new_version is not None and new_version != ref.version:
            changed.append(path)
            return msgspec.structs.replace(ref, version=new_version)
        return ref

    a: Any = artefact
    if isinstance(a, Dataflow):
        if isinstance(a.structure, str):
            new = map_text(a.structure, "structure")
            if new != a.structure:
                a = msgspec.structs.replace(a, structure=new)
        elif a.structure is not None:
            emb = map_embedded(a.structure, "structure")
            if emb is not a.structure:
                a = msgspec.structs.replace(a, structure=emb)
    elif isinstance(a, StructureMap):
        new_source = map_text(a.source, "source")
        new_target = map_text(a.target, "target")
        rules = list(a.maps)
        dirty = False
        for i, rule in enumerate(rules):
            if isinstance(rule, ComponentMap | MultiComponentMap):
                values = rule.values
                path = f"maps[{i}].values"
                if isinstance(values, str):
                    new_values = map_text(values, path)
                    if new_values != values:
                        rules[i] = msgspec.structs.replace(rule, values=new_values)
                        dirty = True
                else:
                    emb = map_embedded(values, path)
                    if emb is not values:
                        rules[i] = msgspec.structs.replace(rule, values=emb)
                        dirty = True
        if new_source != a.source or new_target != a.target or dirty:
            a = msgspec.structs.replace(
                a, source=new_source, target=new_target, maps=tuple(rules)
            )
    elif isinstance(a, DataStructureDefinition):
        comps = []
        dirty = False
        for c in a.components:
            c2 = c
            new_ref = map_text(c.local_enum_ref, f"components.{c.id}.local_enum_ref")
            if new_ref != c.local_enum_ref:
                c2 = msgspec.structs.replace(c2, local_enum_ref=new_ref)
            emb = map_embedded(c.local_codes, f"components.{c.id}.local_codes")
            if emb is not c.local_codes:
                c2 = msgspec.structs.replace(c2, local_codes=emb)
            if isinstance(c.concept, ItemReference):
                new_concept = map_item_ref(c.concept, f"components.{c.id}.concept")
                if new_concept is not c.concept:
                    c2 = msgspec.structs.replace(c2, concept=new_concept)
            if c2 is not c:
                dirty = True
            comps.append(c2)
        if dirty:
            a = msgspec.structs.replace(a, components=Components(comps))
    elif isinstance(a, ConceptScheme):
        items = list(a.items)
        dirty = False
        for i, concept in enumerate(items):
            c2 = concept
            new_ref = map_text(concept.enum_ref, f"items.{concept.id}.enum_ref")
            if new_ref != concept.enum_ref:
                c2 = msgspec.structs.replace(c2, enum_ref=new_ref)
            emb = map_embedded(concept.codes, f"items.{concept.id}.codes")
            if emb is not concept.codes:
                c2 = msgspec.structs.replace(c2, codes=emb)
            if c2 is not concept:
                items[i] = c2
                dirty = True
        if dirty:
            a = msgspec.structs.replace(a, items=tuple(items))
    elif isinstance(a, ProvisionAgreement):
        new_flow = map_text(a.dataflow, "dataflow")
        new_provider = map_text(a.provider, "provider")
        if new_flow != a.dataflow or new_provider != a.provider:
            a = msgspec.structs.replace(a, dataflow=new_flow, provider=new_provider)
    elif isinstance(a, Categorisation):
        new_source = map_text(a.source, "source")
        new_target = map_text(a.target, "target")
        if new_source != a.source or new_target != a.target:
            a = msgspec.structs.replace(a, source=new_source, target=new_target)
    elif isinstance(a, RepresentationMap | MultiRepresentationMap):
        for fld in ("source", "target"):
            value = getattr(a, fld)
            if isinstance(value, str) or value is None:
                new = map_text(value, fld)
                if new != value:
                    a = msgspec.structs.replace(a, **{fld: new})
            else:
                new_seq = tuple(map_text(v, f"{fld}[{i}]") for i, v in enumerate(value))
                if tuple(value) != new_seq:
                    a = msgspec.structs.replace(a, **{fld: new_seq})
    elif isinstance(a, Hierarchy):

        def rewrite_codes(
            codes: Sequence[HierarchicalCode], prefix: str
        ) -> tuple[tuple[HierarchicalCode, ...], bool]:
            out = []
            dirty = False
            for hc in codes:
                new_urn = map_text(hc.urn, f"codes.{prefix}{hc.id}.urn")
                kids, kids_dirty = rewrite_codes(hc.codes, f"{prefix}{hc.id}/")
                if new_urn != hc.urn or kids_dirty:
                    hc = msgspec.structs.replace(hc, urn=new_urn, codes=kids)
                    dirty = True
                out.append(hc)
            return tuple(out), dirty

        new_codes, dirty = rewrite_codes(a.codes, "")
        if dirty:
            a = msgspec.structs.replace(a, codes=new_codes)
    return a, changed


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
        diff = compare_artefacts(existing, artefact)
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


def _retarget_references(
    drafts: list[_Draft], policy: VersionPolicy, allow_breaking: bool
) -> None:
    """Rewrite intra-batch references to bumped versions (in order).

    Processes drafts in dependency order, so a dependent always sees
    its dependencies' final proposed versions. A SKIP draft whose
    references were rewritten is re-diffed and promoted to UPDATE.
    """
    rewrites: dict[tuple[str, str], tuple[set[str], str]] = {}

    def mapper(agency: str, aid: str, version: str) -> str | None:
        entry = rewrites.get((agency, aid))
        if entry is None:
            return None
        old_versions, new_version = entry
        return new_version if version in old_versions else None

    for draft in drafts:
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
                if draft.existing is not None:
                    draft.diff = compare_artefacts(draft.existing, draft.artefact)
                    if (
                        draft.kind == PlannedActionKind.SKIP
                        and not draft.diff.is_unchanged
                    ):
                        draft.kind = PlannedActionKind.UPDATE
                        draft.issues.extend(validate(draft.artefact))
                    if draft.kind == PlannedActionKind.UPDATE:
                        try:
                            draft.proposed_version = suggest_version(
                                draft.diff,
                                draft.existing.version,
                                policy,
                            )
                        except ValueError as err:
                            draft.issues.append(
                                _issue(
                                    "P003",
                                    draft.artefact,
                                    str(err),
                                    "version",
                                )
                            )
                        if (
                            not allow_breaking
                            and draft.diff.impact == ChangeImpact.BREAKING
                            and not any(i.rule_id == "P005" for i in draft.issues)
                        ):
                            draft.issues.append(
                                _issue(
                                    "P005",
                                    draft.artefact,
                                    "Breaking changes are not allowed "
                                    "by this plan "
                                    "(allow_breaking=False).",
                                )
                            )
        old_versions = {draft.artefact.version}
        if draft.registry_version:
            old_versions.add(draft.registry_version)
        old_versions.discard(draft.proposed_version)
        if old_versions:
            key = (_agency_id(draft.artefact.agency), draft.artefact.id)
            rewrites[key] = (old_versions, draft.proposed_version)


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
    return PublicationPlan(
        actions=tuple(d.freeze() for d in drafts),
        structure_action=action,
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
