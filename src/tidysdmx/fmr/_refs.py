"""Reference rewriting shared by the diff and publish layers.

Both the diff engine (:mod:`tidysdmx.fmr.diff`) and the publish workflow
(:mod:`tidysdmx.fmr.publish`) need to walk an artefact's outbound
references — a StructureMap's ``source``/``target`` and its embedded
RepresentationMaps, a DSD's component enumeration/concept references, a
ConceptScheme concept's ``enum_ref``/``codes``, a Dataflow's ``structure``,
and so on — and rewrite the version of each one.

- Publish uses it to retarget intra-batch references to bumped versions.
- Diff uses it (via :func:`normalize_reference_versions`) to collapse every
  reference version to a sentinel, so that a reference re-pointed only to a
  different *version* of the same artefact does not register as a content
  change.

Keeping a single implementation here avoids drift between the fields the two
layers consider to be references.
"""

import re
from collections.abc import Callable, Sequence
from typing import Any

import msgspec
from pysdmx.model import (
    Categorisation,
    ComponentMap,
    Components,
    Concept,
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

from ._compat import MaintainableArtefact
from ._compat import agency_id as _agency_id

#: Matches an ``agency:id(version)`` reference token inside any string form
#: (short token, ``agency:id(version)`` fragment, or a full URN).
_REF_TOKEN_RE = re.compile(
    r"(?P<agency>[A-Za-z0-9_.\-]+):(?P<id>[A-Za-z0-9_.\-@$]+)"
    r"\((?P<version>[^)]+)\)"
)

#: The sentinel version every reference is collapsed to by
#: :func:`normalize_reference_versions`. Only ever used for comparison, so
#: the concrete value is irrelevant as long as it is applied to both sides.
_SENTINEL_REF_VERSION = "0.0.0"

# A mapper receives (agency, id, version) of a reference and returns the
# replacement version, or None to leave the reference untouched.
RefMapper = Callable[[str, str, str], str | None]


def apply_version(artefact: MaintainableArtefact, version: str) -> MaintainableArtefact:
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


def map_references(
    artefact: MaintainableArtefact, mapper: RefMapper
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
            return apply_version(obj, new_version)
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
            elif isinstance(c.concept, Concept) and c.concept.urn:
                # An FMR read-back returns a component's concept as an embedded
                # Concept whose ``urn`` carries the concept scheme version; the
                # local build uses a versionless ItemReference. Normalise the
                # version inside the urn so the two forms compare equal. Concept
                # has no ``version`` field, so rewrite the urn token directly
                # rather than via ``apply_version``.
                new_curn = map_text(c.concept.urn, f"components.{c.id}.concept")
                if new_curn != c.concept.urn:
                    c2 = msgspec.structs.replace(
                        c2, concept=msgspec.structs.replace(c.concept, urn=new_curn)
                    )
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


def normalize_reference_versions(
    artefact: MaintainableArtefact,
) -> MaintainableArtefact:
    """Return a copy with every outbound reference version collapsed.

    Rewrites the version of every intra-artefact reference to a single
    sentinel. Applied to both sides of a comparison, this neutralises
    differences that are *only* a reference version change (e.g. a
    dependency that was co-bumped) while preserving reference *identity*
    (agency:id) and all non-reference content.
    """
    return map_references(artefact, lambda agency, aid, version: _SENTINEL_REF_VERSION)[
        0
    ]
