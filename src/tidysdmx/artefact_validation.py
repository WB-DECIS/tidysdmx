"""Publish-readiness validation for maintainable SDMX artefacts.

These checks complement the structural invariants that pysdmx already
enforces in each artefact's ``__post_init__``. They describe rules that
an artefact must satisfy **before it is published** to a registry such
as FMR, but that may legitimately not hold for artefacts round-tripped
from a registry (e.g. partial or draft records). For that reason the
checks are opt-in and are not moved into ``__post_init__``.

This is a temporary home for the logic: once the upstream pysdmx PR
on branch ``claude/optimize-sdmx-packages-Z92qC`` is released, callers
can switch to ``pysdmx.model.validate`` / ``validate_many`` and this
module can be deleted without any API change here beyond re-exporting
from the new location.

The public entry points are :func:`validate` and :func:`validate_many`,
plus the :class:`ValidationIssue` and :class:`ValidationError` types.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pysdmx.model.__base import ItemScheme, MaintainableArtefact
from pysdmx.model.category import CategoryScheme
from pysdmx.model.code import Codelist, Hierarchy
from pysdmx.model.concept import ConceptScheme
from pysdmx.model.dataflow import Dataflow, DataStructureDefinition, Role
from pysdmx.model.map import MultiRepresentationMap, RepresentationMap
from pysdmx.model.organisation import AgencyScheme
from typeguard import typechecked


@dataclass(frozen=True)
class ValidationIssue:
    """A single publish-readiness validation failure.

    Attributes:
        rule_id: Stable identifier of the broken rule (e.g. ``M001``).
        path: Short URN of the offending artefact.
        message: Human-readable description of the problem.
        field: Name of the field in error, if applicable.
        severity: ``error`` (blocks publishing) or ``warning``.
    """

    rule_id: str
    path: str
    message: str
    field: str | None = None
    severity: Literal["error", "warning"] = "error"


class ValidationError(ValueError):
    """Raised when one or more artefacts fail publish-readiness checks.

    Subclasses :class:`ValueError`, which matches the convention already
    used by ``structure_map_writer.validate_structure_map_references``.

    Attributes:
        issues: The collected :class:`ValidationIssue` instances.
    """

    def __init__(self, issues: Sequence[ValidationIssue]) -> None:
        """Build an aggregated error message from the supplied issues."""
        self.issues: tuple[ValidationIssue, ...] = tuple(issues)
        lines = [
            f"  - [{i.rule_id}] {i.path}"
            + (f".{i.field}" if i.field else "")
            + f": {i.message}"
            for i in self.issues
        ]
        super().__init__("\n".join(["The following issues were found:", *lines]))


def _issue(
    rule_id: str,
    artefact: MaintainableArtefact,
    message: str,
    field: str | None = None,
) -> ValidationIssue:
    return ValidationIssue(
        rule_id=rule_id,
        path=artefact.short_urn,
        message=message,
        field=field,
    )


def _check_common(a: MaintainableArtefact) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not a.id:
        issues.append(_issue("M001", a, "id must be non-empty.", "id"))
    if not a.version:
        issues.append(_issue("M002", a, "version must be non-empty.", "version"))
    if a.name is None or not a.name.strip():
        issues.append(_issue("M003", a, "name must be a non-empty string.", "name"))
    return issues


def _items_rule(rule_id: str, a: ItemScheme, label: str) -> list[ValidationIssue]:
    if not a.items:
        return [
            _issue(
                rule_id,
                a,
                f"{label} must contain at least one item.",
                "items",
            )
        ]
    return []


def _check_codelist(a: Codelist) -> list[ValidationIssue]:
    return _items_rule("C001", a, "Codelist")


def _check_concept_scheme(a: ConceptScheme) -> list[ValidationIssue]:
    return _items_rule("CS001", a, "ConceptScheme")


def _check_category_scheme(a: CategoryScheme) -> list[ValidationIssue]:
    return _items_rule("CAT001", a, "CategoryScheme")


def _check_agency_scheme(a: AgencyScheme) -> list[ValidationIssue]:
    return _items_rule("AS001", a, "AgencyScheme")


def _check_hierarchy(a: Hierarchy) -> list[ValidationIssue]:
    if not a.codes:
        return [
            _issue(
                "H001",
                a,
                "Hierarchy must contain at least one code.",
                "codes",
            )
        ]
    return []


def _check_representation_map(a: RepresentationMap) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not a.source:
        issues.append(_issue("R001", a, "source must be populated.", "source"))
    if not a.target:
        issues.append(_issue("R002", a, "target must be populated.", "target"))
    if not a.maps:
        issues.append(
            _issue(
                "R003",
                a,
                "maps must contain at least one value mapping.",
                "maps",
            )
        )
    return issues


def _check_multi_representation_map(
    a: MultiRepresentationMap,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not a.source:
        issues.append(_issue("R001", a, "source must be populated.", "source"))
    if not a.target:
        issues.append(_issue("R002", a, "target must be populated.", "target"))
    if not a.maps:
        issues.append(
            _issue(
                "R003",
                a,
                "maps must contain at least one value mapping.",
                "maps",
            )
        )
    return issues


def _check_dsd(a: DataStructureDefinition) -> list[ValidationIssue]:
    components = list(a.components) if a.components else []
    if not components:
        return [
            _issue(
                "D001",
                a,
                "DataStructureDefinition must define at least one component.",
                "components",
            )
        ]
    if not any(c.role == Role.DIMENSION for c in components):
        return [
            _issue(
                "D002",
                a,
                "DataStructureDefinition must define at least one dimension.",
                "components",
            )
        ]
    return []


def _check_dataflow(a: Dataflow) -> list[ValidationIssue]:
    if a.structure is None:
        return [
            _issue(
                "DF001",
                a,
                "Dataflow must reference a data structure.",
                "structure",
            )
        ]
    return []


_SPECIFIC: dict[
    type[MaintainableArtefact],
    Callable[[Any], list[ValidationIssue]],
] = {
    Codelist: _check_codelist,
    ConceptScheme: _check_concept_scheme,
    CategoryScheme: _check_category_scheme,
    AgencyScheme: _check_agency_scheme,
    Hierarchy: _check_hierarchy,
    RepresentationMap: _check_representation_map,
    MultiRepresentationMap: _check_multi_representation_map,
    DataStructureDefinition: _check_dsd,
    Dataflow: _check_dataflow,
}


@typechecked
def validate(artefact: MaintainableArtefact) -> list[ValidationIssue]:
    """Check that an artefact is ready to be published.

    Runs the common maintainable rules and any type-specific rules
    registered for the artefact's concrete class.

    Args:
        artefact: The artefact to validate.

    Returns:
        The list of :class:`ValidationIssue` found (empty if the
        artefact is publish-ready).
    """
    issues = _check_common(artefact)
    checker = _SPECIFIC.get(type(artefact))
    if checker is not None:
        issues.extend(checker(artefact))
    return issues


@typechecked
def validate_many(
    artefacts: Sequence[MaintainableArtefact],
) -> list[ValidationIssue]:
    """Validate a sequence of artefacts.

    Args:
        artefacts: The artefacts to validate.

    Returns:
        The concatenated list of issues across every artefact, in
        input order. Empty if every artefact is publish-ready.
    """
    out: list[ValidationIssue] = []
    for a in artefacts:
        out.extend(validate(a))
    return out


@typechecked
def raise_if_invalid(
    artefacts: MaintainableArtefact | Sequence[MaintainableArtefact],
) -> None:
    """Raise :class:`ValidationError` if any artefact has issues.

    Convenience wrapper for the common "validate, fail fast" pattern.

    Args:
        artefacts: A single artefact or a sequence of them.

    Raises:
        ValidationError: If one or more artefacts fail validation.
    """
    seq: Sequence[MaintainableArtefact] = (
        [artefacts] if isinstance(artefacts, MaintainableArtefact) else artefacts
    )
    issues = validate_many(seq)
    if issues:
        raise ValidationError(issues)
