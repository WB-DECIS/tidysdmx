"""Change detection between two versions of an SDMX maintainable artefact.

The entry point is :func:`compare_artefacts`, which compares an *existing*
artefact (typically fetched from an FMR registry) with an *updated* local
artefact of the same type/agency/id and returns an :class:`ArtefactDiff` —
a flat list of typed :class:`ArtefactChange` records, each classified by
:class:`ChangeImpact`:

- ``BREAKING`` — consumers of the artefact can break (item or component
  removed, representation narrowed, references changed).
- ``ADDITIVE`` — new capability; existing consumers keep working (item or
  optional component added).
- ``COSMETIC`` — names, descriptions, annotations, ordering.

Specialized differs cover item schemes (Codelist, ConceptScheme,
CategoryScheme, AgencyScheme), Hierarchy, DataStructureDefinition,
Dataflow, RepresentationMap/MultiRepresentationMap, and StructureMap.
Every other field — and every unregistered artefact type — falls through
to a generic field walk that conservatively classifies changes as
breaking, so no change escapes the diff silently.

The ``version`` field is deliberately excluded from the comparison:
computing the next version is the *output* of the workflow (see
:mod:`tidysdmx.fmr.versioning`), not an input difference.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pysdmx.errors import Invalid
from pysdmx.model import (
    AgencyScheme,
    CategoryScheme,
    Codelist,
    Component,
    ComponentMap,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    DatePatternMap,
    Facets,
    FixedValueMap,
    HierarchicalCode,
    Hierarchy,
    MultiComponentMap,
    MultiRepresentationMap,
    RepresentationMap,
    StructureMap,
)
from pysdmx.util import parse_urn
from typeguard import typechecked

from ._compat import ItemScheme, MaintainableArtefact
from ._compat import agency_id as _agency_id
from ._refs import normalize_reference_versions

logger = logging.getLogger(__name__)

_MAX_VALUE_LEN = 120


class ChangeImpact(StrEnum):
    """How a change affects consumers of the artefact.

    Attributes:
        BREAKING: Consumers of the artefact can break (e.g. an item or
            component was removed, a representation was narrowed, or a
            structural reference changed).
        ADDITIVE: New capability was added; existing consumers keep
            working (e.g. a new item or optional component).
        COSMETIC: Presentation-only change (names, descriptions,
            annotations, ordering).
    """

    BREAKING = "breaking"
    ADDITIVE = "additive"
    COSMETIC = "cosmetic"


_IMPACT_RANK: dict[ChangeImpact, int] = {
    ChangeImpact.COSMETIC: 0,
    ChangeImpact.ADDITIVE: 1,
    ChangeImpact.BREAKING: 2,
}


class ChangeKind(StrEnum):
    """The kind of difference detected between two artefacts.

    Attributes:
        ADDED: An element exists in the updated artefact only.
        REMOVED: An element exists in the existing artefact only.
        MODIFIED: An element or field changed value.
        RENAMED: The ``name`` of an element changed.
        DESCRIPTION_CHANGED: The ``description`` of an element changed.
        MOVED: An element was re-parented (hierarchies, category trees).
        REORDERED: Sibling order changed without membership changes.
    """

    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    RENAMED = "renamed"
    DESCRIPTION_CHANGED = "description_changed"
    MOVED = "moved"
    REORDERED = "reordered"


@dataclass(frozen=True)
class ArtefactChange:
    """A single detected change between two versions of an artefact.

    Attributes:
        kind: The kind of change (see :class:`ChangeKind`).
        impact: The classified impact (see :class:`ChangeImpact`).
        path: Dotted/indexed locator of the changed element, e.g.
            ``items.RED.name``, ``components.FREQ.local_dtype``,
            ``maps[EUR->EUR]``.
        message: Human-readable description of the change.
        old: Stringified old value (``None`` for additions).
        new: Stringified new value (``None`` for removals).
    """

    kind: ChangeKind
    impact: ChangeImpact
    path: str
    message: str
    old: str | None = None
    new: str | None = None


@dataclass(frozen=True)
class ArtefactDiff:
    """The full set of changes between two versions of an artefact.

    Attributes:
        short_urn: Short URN of the existing artefact.
        artefact_type: Concrete pysdmx class name (e.g. ``Codelist``).
        changes: The detected :class:`ArtefactChange` records.
    """

    short_urn: str
    artefact_type: str
    changes: tuple[ArtefactChange, ...]

    @property
    def is_unchanged(self) -> bool:
        """Whether the two artefacts have identical content."""
        return not self.changes

    @property
    def impact(self) -> ChangeImpact | None:
        """The most severe impact present, or ``None`` if unchanged."""
        if not self.changes:
            return None
        return max(
            (c.impact for c in self.changes),
            key=lambda i: _IMPACT_RANK[i],
        )

    def by_impact(self, impact: ChangeImpact) -> tuple[ArtefactChange, ...]:
        """Return the changes with the given impact.

        Args:
            impact: The impact level to filter on.

        Returns:
            The changes classified with ``impact``, in detection order.
        """
        return tuple(c for c in self.changes if c.impact == impact)

    def summary(self) -> str:
        """Render a human-readable multi-line summary of the diff.

        Returns:
            One header line with change counts, then one line per change.
        """
        if self.is_unchanged:
            return f"{self.short_urn}: no changes"
        n_breaking = len(self.by_impact(ChangeImpact.BREAKING))
        n_additive = len(self.by_impact(ChangeImpact.ADDITIVE))
        n_cosmetic = len(self.by_impact(ChangeImpact.COSMETIC))
        lines = [
            f"{self.short_urn}: {len(self.changes)} change(s) "
            f"({n_breaking} breaking, {n_additive} additive, "
            f"{n_cosmetic} cosmetic)"
        ]
        lines += [f"  - [{c.impact.value}] {c.path}: {c.message}" for c in self.changes]
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return :meth:`summary`."""
        return self.summary()

    def __bool__(self) -> bool:
        """Whether there are any changes."""
        return bool(self.changes)


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    text = value.isoformat() if isinstance(value, datetime) else str(value)
    if len(text) > _MAX_VALUE_LEN:
        text = text[: _MAX_VALUE_LEN - 3] + "..."
    return text


def _eq(a: Any, b: Any) -> bool:
    """Compare values, treating sequences of any concrete type alike."""
    if (
        isinstance(a, Sequence)
        and isinstance(b, Sequence)
        and not isinstance(a, str)
        and not isinstance(b, str)
    ):
        return len(a) == len(b) and all(_eq(x, y) for x, y in zip(a, b, strict=True))
    return bool(a == b)


def _change(
    kind: ChangeKind,
    impact: ChangeImpact,
    path: str,
    message: str,
    old: Any = None,
    new: Any = None,
) -> ArtefactChange:
    return ArtefactChange(
        kind=kind,
        impact=impact,
        path=path,
        message=message,
        old=_stringify(old),
        new=_stringify(new),
    )


def _ref_key(value: Any) -> str | None:
    """Normalize a reference (URN string or embedded artefact) to an id.

    Returns an ``agency:id(version)`` identity string (with the item id
    appended for item references), the raw string if it cannot be parsed
    as a URN, or ``None`` for ``None``.
    """
    if value is None:
        return None
    if isinstance(value, MaintainableArtefact):
        agency = _agency_id(value.agency)
        return f"{agency}:{value.id}({value.version})"
    if isinstance(value, str):
        try:
            ref = parse_urn(value)
        except Invalid:
            return value
        key = f"{ref.agency}:{ref.id}({ref.version})"
        item_id = getattr(ref, "item_id", None)
        return f"{key}.{item_id}" if item_id else key
    return str(value)


# ---------------------------------------------------------------------------
# Common maintainable-level fields
# ---------------------------------------------------------------------------

#: Identity fields and registry-managed noise, never diffed. ``is_final`` is
#: excluded because it is not stored but derived from the version string
#: (pysdmx computes ``is_final(version)`` on read), while locally-built
#: artefacts keep the default ``is_final=False``; comparing it would re-surface
#: the already-excluded ``version`` as a phantom cosmetic change.
_SKIP_ALWAYS = frozenset({"id", "agency", "version", "uri", "urn", "is_final"})

#: Maintainable-level fields handled by ``_diff_common``.
_COMMON_FIELDS = frozenset(
    {"name", "description", "annotations", "valid_from", "valid_to"}
)

#: Fields classified as cosmetic by the generic field walk.
_GENERIC_COSMETIC = frozenset({"service_url", "structure_url"})


def _diff_common(
    old: MaintainableArtefact, new: MaintainableArtefact
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    if not _eq(old.name, new.name):
        changes.append(
            _change(
                ChangeKind.RENAMED,
                ChangeImpact.COSMETIC,
                "name",
                "Artefact renamed.",
                old.name,
                new.name,
            )
        )
    if not _eq(old.description, new.description):
        changes.append(
            _change(
                ChangeKind.DESCRIPTION_CHANGED,
                ChangeImpact.COSMETIC,
                "description",
                "Artefact description changed.",
                old.description,
                new.description,
            )
        )
    for fld in ("annotations", "valid_from", "valid_to"):
        ov, nv = getattr(old, fld), getattr(new, fld)
        if not _eq(ov, nv):
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.COSMETIC,
                    fld,
                    f"Artefact {fld} changed.",
                    ov,
                    nv,
                )
            )
    return changes


def _diff_generic(
    old: MaintainableArtefact,
    new: MaintainableArtefact,
    skip: frozenset[str],
) -> list[ArtefactChange]:
    """Field-walk fallback for fields not covered by a specialized differ."""
    changes: list[ArtefactChange] = []
    for fld in type(old).__struct_fields__:
        if fld in skip:
            continue
        ov, nv = getattr(old, fld), getattr(new, fld)
        if not _eq(ov, nv):
            impact = (
                ChangeImpact.COSMETIC
                if fld in _GENERIC_COSMETIC
                else ChangeImpact.BREAKING
            )
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    impact,
                    fld,
                    f"Field '{fld}' changed.",
                    ov,
                    nv,
                )
            )
    return changes


# ---------------------------------------------------------------------------
# Item schemes (Codelist, ConceptScheme, CategoryScheme, AgencyScheme)
# ---------------------------------------------------------------------------

#: Item-level fields treated as cosmetic when they change.
_ITEM_COSMETIC_FIELDS = frozenset({"annotations", "contacts"})

#: Item-level fields excluded from the per-item field walk.
_ITEM_SKIP_FIELDS = frozenset(
    {
        "id",
        "uri",
        "urn",
        "name",
        "description",
        "dataflows",
        "other_references",
        "categories",
    }
)


def _index_items(
    scheme: ItemScheme,
) -> dict[str, tuple[int, str | None, Any]]:
    """Index scheme items by id as ``{id: (position, parent, item)}``.

    ``CategoryScheme`` items are flattened recursively so nested
    categories are compared too; ``parent`` is the ``/``-joined chain of
    ancestor ids (``None`` for top-level items and for flat schemes).
    """
    index: dict[str, tuple[int, str | None, Any]] = {}
    if isinstance(scheme, CategoryScheme):

        def walk(items: Sequence[Any], parent: str | None) -> None:
            for pos, item in enumerate(items):
                index[item.id] = (pos, parent, item)
                nested = getattr(item, "categories", ()) or ()
                child_parent = f"{parent}/{item.id}" if parent else item.id
                walk(nested, child_parent)

        walk(scheme.items, None)
    else:
        for pos, item in enumerate(scheme.items):
            index[item.id] = (pos, None, item)
    return index


def _diff_item_fields(old_item: Any, new_item: Any, path: str) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    label = type(old_item).__name__
    if not _eq(old_item.name, new_item.name):
        changes.append(
            _change(
                ChangeKind.RENAMED,
                ChangeImpact.COSMETIC,
                f"{path}.name",
                f"{label} '{old_item.id}' renamed.",
                old_item.name,
                new_item.name,
            )
        )
    if not _eq(old_item.description, new_item.description):
        changes.append(
            _change(
                ChangeKind.DESCRIPTION_CHANGED,
                ChangeImpact.COSMETIC,
                f"{path}.description",
                f"{label} '{old_item.id}' description changed.",
                old_item.description,
                new_item.description,
            )
        )
    for fld in type(old_item).__struct_fields__:
        if fld in _ITEM_SKIP_FIELDS:
            continue
        ov, nv = getattr(old_item, fld), getattr(new_item, fld)
        if _eq(ov, nv):
            continue
        impact = (
            ChangeImpact.COSMETIC
            if fld in _ITEM_COSMETIC_FIELDS
            else ChangeImpact.BREAKING
        )
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                impact,
                f"{path}.{fld}",
                f"{label} '{old_item.id}' {fld} changed.",
                ov,
                nv,
            )
        )
    return changes


def _diff_item_scheme(old: ItemScheme, new: ItemScheme) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    old_idx = _index_items(old)
    new_idx = _index_items(new)
    moved_ids: set[str] = set()

    for item_id, (_, _, old_item) in old_idx.items():
        if item_id not in new_idx:
            changes.append(
                _change(
                    ChangeKind.REMOVED,
                    ChangeImpact.BREAKING,
                    f"items.{item_id}",
                    f"{type(old_item).__name__} '{item_id}' removed.",
                    old=old_item.name,
                )
            )
    for item_id, (_, _, new_item) in new_idx.items():
        if item_id not in old_idx:
            changes.append(
                _change(
                    ChangeKind.ADDED,
                    ChangeImpact.ADDITIVE,
                    f"items.{item_id}",
                    f"{type(new_item).__name__} '{item_id}' added.",
                    new=new_item.name,
                )
            )
    for item_id, (_, old_parent, old_item) in old_idx.items():
        if item_id not in new_idx:
            continue
        _, new_parent, new_item = new_idx[item_id]
        if old_parent != new_parent:
            moved_ids.add(item_id)
            changes.append(
                _change(
                    ChangeKind.MOVED,
                    ChangeImpact.BREAKING,
                    f"items.{item_id}",
                    f"{type(old_item).__name__} '{item_id}' moved.",
                    old=old_parent,
                    new=new_parent,
                )
            )
        changes.extend(_diff_item_fields(old_item, new_item, f"items.{item_id}"))

    surviving_old = [i for i in old_idx if i in new_idx and i not in moved_ids]
    surviving_new = [i for i in new_idx if i in old_idx and i not in moved_ids]
    if surviving_old != surviving_new:
        changes.append(
            _change(
                ChangeKind.REORDERED,
                ChangeImpact.COSMETIC,
                "items",
                "Item order changed.",
            )
        )
    return changes


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------

#: HierarchicalCode fields treated as cosmetic when they change.
_HCODE_COSMETIC_FIELDS = frozenset({"annotations", "level"})

_HCODE_SKIP_FIELDS = frozenset({"id", "urn", "codes", "name", "description"})


def _flatten_hierarchy(
    codes: Sequence[HierarchicalCode], parent: str | None = None
) -> list[tuple[str, str | None, int, HierarchicalCode]]:
    """Flatten a hierarchical code tree to ``(path, parent, pos, code)``."""
    flat: list[tuple[str, str | None, int, HierarchicalCode]] = []
    for pos, code in enumerate(codes):
        path = f"{parent}/{code.id}" if parent else code.id
        flat.append((path, parent, pos, code))
        flat.extend(_flatten_hierarchy(code.codes, path))
    return flat


def _diff_hcode_fields(
    old_code: HierarchicalCode, new_code: HierarchicalCode, path: str
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    if not _eq(old_code.name, new_code.name):
        changes.append(
            _change(
                ChangeKind.RENAMED,
                ChangeImpact.COSMETIC,
                f"codes.{path}.name",
                f"Hierarchical code '{old_code.id}' renamed.",
                old_code.name,
                new_code.name,
            )
        )
    if not _eq(old_code.description, new_code.description):
        changes.append(
            _change(
                ChangeKind.DESCRIPTION_CHANGED,
                ChangeImpact.COSMETIC,
                f"codes.{path}.description",
                f"Hierarchical code '{old_code.id}' description changed.",
                old_code.description,
                new_code.description,
            )
        )
    for fld in type(old_code).__struct_fields__:
        if fld in _HCODE_SKIP_FIELDS:
            continue
        ov, nv = getattr(old_code, fld), getattr(new_code, fld)
        if _eq(ov, nv):
            continue
        impact = (
            ChangeImpact.COSMETIC
            if fld in _HCODE_COSMETIC_FIELDS
            else ChangeImpact.BREAKING
        )
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                impact,
                f"codes.{path}.{fld}",
                f"Hierarchical code '{old_code.id}' {fld} changed.",
                ov,
                nv,
            )
        )
    return changes


def _diff_hierarchy(old: Hierarchy, new: Hierarchy) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    old_flat = _flatten_hierarchy(old.codes)
    new_flat = _flatten_hierarchy(new.codes)

    def by_id(
        flat: list[tuple[str, str | None, int, HierarchicalCode]],
    ) -> dict[str, list[tuple[str, str | None, int, HierarchicalCode]]]:
        grouped: dict[str, list[tuple[str, str | None, int, HierarchicalCode]]] = {}
        for entry in flat:
            grouped.setdefault(entry[3].id, []).append(entry)
        return grouped

    old_by_id = by_id(old_flat)
    new_by_id = by_id(new_flat)
    moved_ids: set[str] = set()

    for code_id, old_entries in old_by_id.items():
        new_entries = new_by_id.get(code_id)
        if new_entries is None:
            changes.append(
                _change(
                    ChangeKind.REMOVED,
                    ChangeImpact.BREAKING,
                    f"codes.{old_entries[0][0]}",
                    f"Hierarchical code '{code_id}' removed.",
                    old=old_entries[0][3].name,
                )
            )
            continue
        if len(old_entries) == 1 and len(new_entries) == 1:
            _, old_parent, _, old_code = old_entries[0]
            new_path, new_parent, _, new_code = new_entries[0]
            if old_parent != new_parent:
                moved_ids.add(code_id)
                # Re-parenting changes the aggregation paths consumers
                # roll up along — breaking, like item-scheme moves.
                changes.append(
                    _change(
                        ChangeKind.MOVED,
                        ChangeImpact.BREAKING,
                        f"codes.{new_path}",
                        f"Hierarchical code '{code_id}' moved.",
                        old=old_parent,
                        new=new_parent,
                    )
                )
            changes.extend(_diff_hcode_fields(old_code, new_code, new_path))
        else:
            # The same code id appears more than once (legal in a
            # hierarchy): fall back to path-keyed comparison without
            # move detection.
            old_paths = {e[0]: e for e in old_entries}
            new_paths = {e[0]: e for e in new_entries}
            for path, entry in old_paths.items():
                if path not in new_paths:
                    changes.append(
                        _change(
                            ChangeKind.REMOVED,
                            ChangeImpact.BREAKING,
                            f"codes.{path}",
                            f"Hierarchical code '{code_id}' removed "
                            f"from '{entry[1] or '<root>'}'.",
                            old=entry[3].name,
                        )
                    )
                else:
                    changes.extend(
                        _diff_hcode_fields(entry[3], new_paths[path][3], path)
                    )
            for path, entry in new_paths.items():
                if path not in old_paths:
                    changes.append(
                        _change(
                            ChangeKind.ADDED,
                            ChangeImpact.ADDITIVE,
                            f"codes.{path}",
                            f"Hierarchical code '{code_id}' added "
                            f"under '{entry[1] or '<root>'}'.",
                            new=entry[3].name,
                        )
                    )
    for code_id, new_entries in new_by_id.items():
        if code_id not in old_by_id:
            changes.append(
                _change(
                    ChangeKind.ADDED,
                    ChangeImpact.ADDITIVE,
                    f"codes.{new_entries[0][0]}",
                    f"Hierarchical code '{code_id}' added.",
                    new=new_entries[0][3].name,
                )
            )

    surviving_old = [
        e[0] for e in old_flat if e[3].id in new_by_id and e[3].id not in moved_ids
    ]
    surviving_new = [
        e[0] for e in new_flat if e[3].id in old_by_id and e[3].id not in moved_ids
    ]
    if sorted(surviving_old) == sorted(surviving_new) and (
        surviving_old != surviving_new
    ):
        changes.append(
            _change(
                ChangeKind.REORDERED,
                ChangeImpact.COSMETIC,
                "codes",
                "Hierarchical code order changed.",
            )
        )
    return changes


# ---------------------------------------------------------------------------
# DataStructureDefinition
# ---------------------------------------------------------------------------

#: Facets fields where a *larger* new value narrows the representation.
_FACET_LOWER_BOUNDS = frozenset({"min_length", "min_value"})

#: Facets fields where a *smaller* new value narrows the representation.
_FACET_UPPER_BOUNDS = frozenset({"max_length", "max_value", "decimals"})

_COMPONENT_SKIP_FIELDS = frozenset(
    {
        "id",
        "urn",
        "required",
        "role",
        "concept",
        "local_dtype",
        "local_facets",
        "local_codes",
        "local_enum_ref",
        "name",
        "description",
    }
)


def _classify_facet_change(fld: str, old_value: Any, new_value: Any) -> ChangeImpact:
    """Classify a facet field change as narrowing or widening.

    Narrowing an allowed value space is breaking; widening it is
    additive. Unknown facet fields are conservatively breaking.
    """
    if fld in _FACET_UPPER_BOUNDS:
        if new_value is None:
            return ChangeImpact.ADDITIVE
        if old_value is None or new_value < old_value:
            return ChangeImpact.BREAKING
        return ChangeImpact.ADDITIVE
    if fld in _FACET_LOWER_BOUNDS:
        if new_value is None:
            return ChangeImpact.ADDITIVE
        if old_value is None or new_value > old_value:
            return ChangeImpact.BREAKING
        return ChangeImpact.ADDITIVE
    if fld == "pattern":
        return ChangeImpact.ADDITIVE if new_value is None else ChangeImpact.BREAKING
    return ChangeImpact.BREAKING


def _diff_facets(
    old_facets: Facets | None, new_facets: Facets | None, path: str, cid: str
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    old_f = old_facets or Facets()
    new_f = new_facets or Facets()
    for fld in Facets.__struct_fields__:
        ov, nv = getattr(old_f, fld), getattr(new_f, fld)
        if _eq(ov, nv):
            continue
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                _classify_facet_change(fld, ov, nv),
                f"{path}.{fld}",
                f"Component '{cid}' facet {fld} changed.",
                ov,
                nv,
            )
        )
    return changes


def _concept_key(concept: Any) -> str:
    if concept is None:
        return ""
    urn = getattr(concept, "urn", None)
    if isinstance(urn, str) and urn:
        return _ref_key(urn) or urn
    item_id = getattr(concept, "item_id", None)
    if item_id is not None:
        return f"{concept.agency}:{concept.id}({concept.version}).{item_id}"
    return str(getattr(concept, "id", concept))


def _enum_key(component: Component) -> str | None:
    if component.local_codes is not None:
        return _ref_key(component.local_codes)
    if component.local_enum_ref:
        return _ref_key(component.local_enum_ref)
    return None


def _diff_component_pair(old_c: Component, new_c: Component) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    cid = old_c.id
    path = f"components.{cid}"
    if old_c.role != new_c.role:
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                ChangeImpact.BREAKING,
                f"{path}.role",
                f"Component '{cid}' role changed.",
                old_c.role,
                new_c.role,
            )
        )
    if old_c.required != new_c.required:
        impact = ChangeImpact.BREAKING if new_c.required else ChangeImpact.ADDITIVE
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                impact,
                f"{path}.required",
                f"Component '{cid}' required flag changed.",
                old_c.required,
                new_c.required,
            )
        )
    if _concept_key(old_c.concept) != _concept_key(new_c.concept):
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                ChangeImpact.BREAKING,
                f"{path}.concept",
                f"Component '{cid}' concept reference changed.",
                _concept_key(old_c.concept),
                _concept_key(new_c.concept),
            )
        )
    if old_c.local_dtype != new_c.local_dtype:
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                ChangeImpact.BREAKING,
                f"{path}.local_dtype",
                f"Component '{cid}' data type changed.",
                old_c.local_dtype,
                new_c.local_dtype,
            )
        )
    if _enum_key(old_c) != _enum_key(new_c):
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                ChangeImpact.BREAKING,
                f"{path}.enumeration",
                f"Component '{cid}' enumeration reference changed.",
                _enum_key(old_c),
                _enum_key(new_c),
            )
        )
    changes.extend(
        _diff_facets(
            old_c.local_facets, new_c.local_facets, f"{path}.local_facets", cid
        )
    )
    if not _eq(old_c.name, new_c.name):
        changes.append(
            _change(
                ChangeKind.RENAMED,
                ChangeImpact.COSMETIC,
                f"{path}.name",
                f"Component '{cid}' renamed.",
                old_c.name,
                new_c.name,
            )
        )
    if not _eq(old_c.description, new_c.description):
        changes.append(
            _change(
                ChangeKind.DESCRIPTION_CHANGED,
                ChangeImpact.COSMETIC,
                f"{path}.description",
                f"Component '{cid}' description changed.",
                old_c.description,
                new_c.description,
            )
        )
    for fld in Component.__struct_fields__:
        if fld in _COMPONENT_SKIP_FIELDS:
            continue
        ov, nv = getattr(old_c, fld), getattr(new_c, fld)
        if not _eq(ov, nv):
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    f"{path}.{fld}",
                    f"Component '{cid}' {fld} changed.",
                    ov,
                    nv,
                )
            )
    return changes


def _diff_dsd(
    old: DataStructureDefinition, new: DataStructureDefinition
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    old_comps = {c.id: c for c in old.components}
    new_comps = {c.id: c for c in new.components}

    for cid, old_c in old_comps.items():
        if cid not in new_comps:
            changes.append(
                _change(
                    ChangeKind.REMOVED,
                    ChangeImpact.BREAKING,
                    f"components.{cid}",
                    f"Component '{cid}' ({old_c.role.name}) removed.",
                    old=old_c.name or cid,
                )
            )
    for cid, new_c in new_comps.items():
        if cid not in old_comps:
            impact = ChangeImpact.BREAKING if new_c.required else ChangeImpact.ADDITIVE
            changes.append(
                _change(
                    ChangeKind.ADDED,
                    impact,
                    f"components.{cid}",
                    f"Component '{cid}' ({new_c.role.name}, "
                    f"{'required' if new_c.required else 'optional'}) "
                    "added.",
                    new=new_c.name or cid,
                )
            )
    for cid, old_c in old_comps.items():
        if cid in new_comps:
            changes.extend(_diff_component_pair(old_c, new_comps[cid]))

    surviving_old = [c.id for c in old.components if c.id in new_comps]
    surviving_new = [c.id for c in new.components if c.id in old_comps]
    if surviving_old != surviving_new:
        changes.append(
            _change(
                ChangeKind.REORDERED,
                ChangeImpact.COSMETIC,
                "components",
                "Component order changed.",
            )
        )
    return changes


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------


def _diff_dataflow(old: Dataflow, new: Dataflow) -> list[ArtefactChange]:
    # series_count/obs_count are registry-computed statistics, not model
    # content, and are deliberately ignored.
    changes: list[ArtefactChange] = []
    old_ref = _ref_key(old.structure)
    new_ref = _ref_key(new.structure)
    if old_ref != new_ref:
        changes.append(
            _change(
                ChangeKind.MODIFIED,
                ChangeImpact.BREAKING,
                "structure",
                "Dataflow structure reference changed.",
                old_ref,
                new_ref,
            )
        )
    return changes


# ---------------------------------------------------------------------------
# Representation maps and structure maps
# ---------------------------------------------------------------------------


def _group_value_maps(
    maps: Sequence[Any],
) -> dict[tuple[Any, Any], list[Any]]:
    grouped: dict[tuple[Any, Any], list[Any]] = {}
    for vm in maps:
        source, target = vm.source, vm.target
        if not isinstance(source, str):
            source = tuple(source)
        if not isinstance(target, str):
            target = tuple(target)
        grouped.setdefault((source, target), []).append(vm)
    return grouped


def _validity_signature(value_maps: list[Any]) -> list[tuple[str, str]]:
    return sorted((str(vm.valid_from), str(vm.valid_to)) for vm in value_maps)


def _fmt_rule_key(key: tuple[Any, Any]) -> str:
    def side(value: Any) -> str:
        return ",".join(value) if isinstance(value, tuple) else str(value)

    return f"{side(key[0])}->{side(key[1])}"


def _diff_representation_map(
    old: RepresentationMap | MultiRepresentationMap,
    new: RepresentationMap | MultiRepresentationMap,
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    for fld in ("source", "target"):
        ov, nv = getattr(old, fld), getattr(new, fld)
        ok = (
            _ref_key(ov)
            if isinstance(ov, str | type(None))
            else [_ref_key(v) for v in ov]
        )
        nk = (
            _ref_key(nv)
            if isinstance(nv, str | type(None))
            else [_ref_key(v) for v in nv]
        )
        if not _eq(ok, nk):
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    fld,
                    f"Representation map {fld} changed.",
                    ok,
                    nk,
                )
            )
    old_groups = _group_value_maps(old.maps)
    new_groups = _group_value_maps(new.maps)
    for key, old_vms in old_groups.items():
        label = _fmt_rule_key(key)
        if key not in new_groups:
            changes.append(
                _change(
                    ChangeKind.REMOVED,
                    ChangeImpact.BREAKING,
                    f"maps[{label}]",
                    f"Value mapping '{label}' removed.",
                    old=label,
                )
            )
        elif _validity_signature(old_vms) != _validity_signature(new_groups[key]):
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    f"maps[{label}]",
                    f"Value mapping '{label}' validity changed.",
                )
            )
    for key in new_groups:
        if key not in old_groups:
            label = _fmt_rule_key(key)
            changes.append(
                _change(
                    ChangeKind.ADDED,
                    ChangeImpact.ADDITIVE,
                    f"maps[{label}]",
                    f"Value mapping '{label}' added.",
                    new=label,
                )
            )
    return changes


def _map_rule_key(rule: Any) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    """Build a stable identity for a StructureMap mapping rule."""
    kind = type(rule).__name__
    if isinstance(rule, FixedValueMap):
        return (kind, (rule.target,), (str(rule.located_in),))
    source = rule.source
    target = rule.target
    sources = (source,) if isinstance(source, str) else tuple(source)
    targets = (target,) if isinstance(target, str) else tuple(target)
    return (kind, sources, targets)


def _diff_map_rule_pair(
    old_rule: Any, new_rule: Any, path: str
) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    if isinstance(old_rule, ComponentMap | MultiComponentMap):
        old_values = _ref_key(old_rule.values)
        new_values = _ref_key(new_rule.values)
        if old_values != new_values:
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    f"{path}.values",
                    "Mapping rule representation map changed.",
                    old_values,
                    new_values,
                )
            )
    elif isinstance(old_rule, FixedValueMap):
        if not _eq(old_rule.value, new_rule.value):
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    f"{path}.value",
                    "Fixed value changed.",
                    old_rule.value,
                    new_rule.value,
                )
            )
    elif isinstance(old_rule, DatePatternMap):
        for fld in (
            "pattern",
            "frequency",
            "locale",
            "pattern_type",
            "resolve_period",
        ):
            ov, nv = getattr(old_rule, fld), getattr(new_rule, fld)
            if not _eq(ov, nv):
                changes.append(
                    _change(
                        ChangeKind.MODIFIED,
                        ChangeImpact.BREAKING,
                        f"{path}.{fld}",
                        f"Date pattern {fld} changed.",
                        ov,
                        nv,
                    )
                )
    return changes


def _diff_structure_map(old: StructureMap, new: StructureMap) -> list[ArtefactChange]:
    changes: list[ArtefactChange] = []
    for fld in ("source", "target"):
        ok = _ref_key(getattr(old, fld))
        nk = _ref_key(getattr(new, fld))
        if ok != nk:
            changes.append(
                _change(
                    ChangeKind.MODIFIED,
                    ChangeImpact.BREAKING,
                    fld,
                    f"Structure map {fld} changed.",
                    ok,
                    nk,
                )
            )
    old_rules = {_map_rule_key(r): r for r in old.maps}
    new_rules = {_map_rule_key(r): r for r in new.maps}
    for key, old_rule in old_rules.items():
        label = f"{key[0]}[{','.join(key[1])}->{','.join(key[2])}]"
        if key not in new_rules:
            changes.append(
                _change(
                    ChangeKind.REMOVED,
                    ChangeImpact.BREAKING,
                    f"maps.{label}",
                    f"Mapping rule {label} removed.",
                    old=label,
                )
            )
        else:
            changes.extend(
                _diff_map_rule_pair(old_rule, new_rules[key], f"maps.{label}")
            )
    for key in new_rules:
        if key not in old_rules:
            label = f"{key[0]}[{','.join(key[1])}->{','.join(key[2])}]"
            changes.append(
                _change(
                    ChangeKind.ADDED,
                    ChangeImpact.ADDITIVE,
                    f"maps.{label}",
                    f"Mapping rule {label} added.",
                    new=label,
                )
            )
    return changes


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_DIFFERS: dict[
    type[MaintainableArtefact],
    Callable[[Any, Any], list[ArtefactChange]],
] = {
    Codelist: _diff_item_scheme,
    ConceptScheme: _diff_item_scheme,
    CategoryScheme: _diff_item_scheme,
    AgencyScheme: _diff_item_scheme,
    Hierarchy: _diff_hierarchy,
    DataStructureDefinition: _diff_dsd,
    Dataflow: _diff_dataflow,
    RepresentationMap: _diff_representation_map,
    MultiRepresentationMap: _diff_representation_map,
    StructureMap: _diff_structure_map,
}

#: Fields consumed by each specialized differ (excluded from the generic
#: field walk that runs afterwards).
_HANDLED: dict[type[MaintainableArtefact], frozenset[str]] = {
    Codelist: frozenset({"items"}),
    ConceptScheme: frozenset({"items"}),
    CategoryScheme: frozenset({"items"}),
    AgencyScheme: frozenset({"items"}),
    Hierarchy: frozenset({"codes"}),
    DataStructureDefinition: frozenset({"components"}),
    Dataflow: frozenset({"structure", "series_count", "obs_count"}),
    RepresentationMap: frozenset({"source", "target", "maps"}),
    MultiRepresentationMap: frozenset({"source", "target", "maps"}),
    StructureMap: frozenset({"source", "target", "maps"}),
}


@typechecked
def compare_artefacts(
    existing: MaintainableArtefact,
    updated: MaintainableArtefact,
    ignore_reference_versions: bool = False,
) -> ArtefactDiff:
    """Detect the changes between two versions of the same artefact.

    Compares an *existing* artefact (typically the registry's current
    copy) with an *updated* local artefact, and classifies every change
    as breaking, additive, or cosmetic. The ``version`` field is
    excluded from the comparison: the new version is the *output* of the
    publication workflow (see
    :func:`tidysdmx.fmr.versioning.suggest_version`), not an input.

    Args:
        existing: The current artefact (e.g. fetched from FMR).
        updated: The updated artefact of the same type, agency, and id.
        ignore_reference_versions: When ``True``, outbound reference
            *versions* (e.g. a DSD's enumeration/concept references, a
            StructureMap's ``source``/``target``) are collapsed to a
            sentinel on both sides before diffing, so a reference
            re-pointed only to a different *version* of the same artefact
            is not reported as a change. Reference *identity* (agency:id)
            and all non-reference content are still compared. The publish
            workflow uses this so a co-bumped dependency does not
            manufacture a spurious change in its dependents; the version
            follow is classified by the publish layer instead.

    Returns:
        An :class:`ArtefactDiff` with one record per detected change
        (empty if the content is identical).

    Raises:
        Invalid: If the two artefacts differ in concrete type, agency,
            or id — comparing different artefacts is a caller bug.

    Examples:
        >>> from pysdmx.model import Code, Codelist
        >>> old = Codelist(
        ...     id="CL_COLOUR", agency="WB", name="Colours",
        ...     items=[Code(id="RED", name="Red")],
        ... )
        >>> new = Codelist(
        ...     id="CL_COLOUR", agency="WB", name="Colours",
        ...     items=[Code(id="RED", name="Red"),
        ...            Code(id="BLUE", name="Blue")],
        ... )
        >>> diff = compare_artefacts(old, new)
        >>> diff.impact
        <ChangeImpact.ADDITIVE: 'additive'>
        >>> diff.changes[0].path
        'items.BLUE'
    """
    if type(existing) is not type(updated):
        raise Invalid(
            "Type mismatch",
            "Cannot compare artefacts of different types: "
            f"{type(existing).__name__} vs {type(updated).__name__}.",
        )
    if existing.id != updated.id or _agency_id(existing.agency) != _agency_id(
        updated.agency
    ):
        raise Invalid(
            "Identity mismatch",
            "Cannot compare different artefacts: "
            f"{existing.short_urn} vs {updated.short_urn}. "
            "compare_artefacts expects two versions of the same "
            "artefact (same type, agency, and id).",
        )
    if ignore_reference_versions:
        existing = normalize_reference_versions(existing)
        updated = normalize_reference_versions(updated)
    changes = _diff_common(existing, updated)
    differ = _DIFFERS.get(type(existing))
    handled = _HANDLED.get(type(existing), frozenset())
    if differ is not None:
        changes.extend(differ(existing, updated))
    else:
        logger.debug(
            "No specialized differ for %s; using generic field walk.",
            type(existing).__name__,
        )
    changes.extend(
        _diff_generic(existing, updated, _SKIP_ALWAYS | _COMMON_FIELDS | handled)
    )
    return ArtefactDiff(
        short_urn=existing.short_urn,
        artefact_type=type(existing).__name__,
        changes=tuple(changes),
    )
