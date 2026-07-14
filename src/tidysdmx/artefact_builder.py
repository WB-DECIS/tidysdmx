"""Programmatic builders for pysdmx maintainable artefacts.

Each ``build_*`` function constructs a pysdmx model instance **and
validates it** against the publish-readiness rules in
:mod:`tidysdmx.artefact_validation` before returning. Construction
therefore fails fast: a :class:`~tidysdmx.artefact_validation.ValidationError`
is raised the moment an invalid artefact would otherwise escape into
the FMR-upload path.

These builders complement the existing DataFrame-driven helpers in
:mod:`tidysdmx.structures`; they take plain Python values and
pysdmx model objects rather than pandas DataFrames, and exist so
``data360-mngt-app`` and similar callers can construct artefacts
without reinventing validation.
"""

from collections.abc import Sequence
from typing import Literal

from pysdmx.model import Agency
from pysdmx.model.category import Category, CategoryScheme
from pysdmx.model.code import Code, Codelist, HierarchicalCode, Hierarchy
from pysdmx.model.concept import Concept, ConceptScheme
from pysdmx.model.dataflow import (
    Component,
    Components,
    Dataflow,
    DataStructureDefinition,
)
from pysdmx.model.map import (
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    ValueMap,
)
from pysdmx.model.organisation import AgencyScheme
from typeguard import typechecked

from .artefact_validation import raise_if_invalid


@typechecked
def build_codelist(
    id: str,
    agency: str,
    name: str,
    codes: Sequence[Code] = (),
    version: str = "1.0",
    description: str | None = None,
    sdmx_type: Literal["codelist", "valuelist"] = "codelist",
    urn: str | None = None,
) -> Codelist:
    """Build a validated :class:`Codelist`.

    Args:
        id: Codelist identifier (e.g. ``CL_FREQ``).
        agency: Maintaining agency ID.
        name: Human-readable name.
        codes: The codes to include. Must contain at least one code.
        version: Codelist version. Defaults to ``"1.0"``.
        description: Optional description.
        sdmx_type: Either ``"codelist"`` (default) or ``"valuelist"``.
        urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.

    Returns:
        A publish-ready :class:`Codelist`.

    Raises:
        ValidationError: If the resulting codelist would not pass
            publish-readiness checks.
    """
    cl = Codelist(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        items=tuple(codes),
        sdmx_type=sdmx_type,
        urn=urn,
    )
    raise_if_invalid(cl)
    return cl


@typechecked
def build_concept_scheme(
    id: str,
    agency: str,
    name: str,
    concepts: Sequence[Concept] = (),
    version: str = "1.0",
    description: str | None = None,
    urn: str | None = None,
) -> ConceptScheme:
    """Build a validated :class:`ConceptScheme`.

    Args:
        id: ConceptScheme identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        concepts: The concepts to include. Must be non-empty.
        version: ConceptScheme version. Defaults to ``"1.0"``.
        description: Optional description.
        urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.

    Returns:
        A publish-ready :class:`ConceptScheme`.

    Raises:
        ValidationError: If the resulting scheme would not pass
            publish-readiness checks.
    """
    cs = ConceptScheme(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        items=tuple(concepts),
        urn=urn,
    )
    raise_if_invalid(cs)
    return cs


@typechecked
def build_category_scheme(
    id: str,
    agency: str,
    name: str,
    categories: Sequence[Category] = (),
    version: str = "1.0",
    description: str | None = None,
) -> CategoryScheme:
    """Build a validated :class:`CategoryScheme`.

    Args:
        id: CategoryScheme identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        categories: The categories to include. Must be non-empty.
        version: CategoryScheme version. Defaults to ``"1.0"``.
        description: Optional description.

    Returns:
        A publish-ready :class:`CategoryScheme`.

    Raises:
        ValidationError: If the resulting scheme would not pass
            publish-readiness checks.
    """
    cs = CategoryScheme(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        items=tuple(categories),
    )
    raise_if_invalid(cs)
    return cs


@typechecked
def build_agency_scheme(
    id: str,
    agency: str,
    name: str,
    agencies: Sequence[Agency] = (),
    version: str = "1.0",
    description: str | None = None,
) -> AgencyScheme:
    """Build a validated :class:`AgencyScheme`.

    Args:
        id: AgencyScheme identifier (SDMX convention is ``"AGENCIES"``).
        agency: Maintaining agency ID.
        name: Human-readable name.
        agencies: The agencies to include. Must be non-empty.
        version: AgencyScheme version. Defaults to ``"1.0"``.
        description: Optional description.

    Returns:
        A publish-ready :class:`AgencyScheme`.

    Raises:
        ValidationError: If the resulting scheme would not pass
            publish-readiness checks.
    """
    scheme = AgencyScheme(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        items=tuple(agencies),
    )
    raise_if_invalid(scheme)
    return scheme


@typechecked
def build_hierarchy(
    id: str,
    agency: str,
    name: str,
    codes: Sequence[HierarchicalCode] = (),
    version: str = "1.0",
    description: str | None = None,
    operator: str | None = None,
    is_partial: bool = True,
) -> Hierarchy:
    """Build a validated :class:`Hierarchy`.

    Args:
        id: Hierarchy identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        codes: The hierarchical codes. Must be non-empty.
        version: Hierarchy version. Defaults to ``"1.0"``.
        description: Optional description.
        operator: Optional VTL operator URN applied to the hierarchy.
        is_partial: Whether the hierarchy is partial. Defaults to
            ``True`` to match the pysdmx class default.

    Returns:
        A publish-ready :class:`Hierarchy`.

    Raises:
        ValidationError: If the resulting hierarchy would not pass
            publish-readiness checks.
    """
    h = Hierarchy(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        codes=tuple(codes),
        operator=operator,
        is_partial=is_partial,
    )
    raise_if_invalid(h)
    return h


@typechecked
def build_data_structure_definition(
    id: str,
    agency: str,
    name: str,
    components: Sequence[Component] | Components,
    version: str = "1.0",
    description: str | None = None,
    urn: str | None = None,
) -> DataStructureDefinition:
    """Build a validated :class:`DataStructureDefinition`.

    The components must include at least one dimension; this mirrors
    rule ``D002`` in :mod:`tidysdmx.artefact_validation`.

    Args:
        id: DSD identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        components: Either a :class:`Components` collection or a
            sequence of :class:`Component` instances.
        version: DSD version. Defaults to ``"1.0"``.
        description: Optional description.
        urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.

    Returns:
        A publish-ready :class:`DataStructureDefinition`.

    Raises:
        ValidationError: If the resulting DSD would not pass
            publish-readiness checks.
    """
    comps = (
        components
        if isinstance(components, Components)
        else Components(list(components))
    )
    dsd = DataStructureDefinition(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        components=comps,
        urn=urn,
    )
    raise_if_invalid(dsd)
    return dsd


@typechecked
def build_dataflow(
    id: str,
    agency: str,
    name: str,
    structure: DataStructureDefinition | str,
    version: str = "1.0",
    description: str | None = None,
) -> Dataflow:
    """Build a validated :class:`Dataflow`.

    Args:
        id: Dataflow identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        structure: Either a :class:`DataStructureDefinition` or the
            URN of one. Must not be ``None``.
        version: Dataflow version. Defaults to ``"1.0"``.
        description: Optional description.

    Returns:
        A publish-ready :class:`Dataflow`.

    Raises:
        ValidationError: If the resulting dataflow would not pass
            publish-readiness checks.
    """
    df = Dataflow(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        structure=structure,
    )
    raise_if_invalid(df)
    return df


@typechecked
def build_representation_map(
    id: str,
    agency: str,
    name: str,
    source: str,
    target: str,
    maps: Sequence[ValueMap] = (),
    version: str = "1.0",
    description: str | None = None,
    urn: str | None = None,
) -> RepresentationMap:
    """Build a validated :class:`RepresentationMap` from values.

    This is the programmatic counterpart of
    :func:`tidysdmx.structures.build_representation_map`, which takes
    a pandas DataFrame instead.

    Args:
        id: RepresentationMap identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        source: URN of the source codelist / valuelist, or a data type
            name (e.g. ``"String"``).
        target: URN of the target codelist / valuelist, or a data type.
        maps: The individual value mappings. Must be non-empty.
        version: RepresentationMap version. Defaults to ``"1.0"``.
        description: Optional description.
        urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.

    Returns:
        A publish-ready :class:`RepresentationMap`.

    Raises:
        ValidationError: If the resulting map would not pass
            publish-readiness checks.
    """
    rm = RepresentationMap(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        source=source,
        target=target,
        maps=list(maps),
        urn=urn,
    )
    raise_if_invalid(rm)
    return rm


@typechecked
def build_multi_representation_map(
    id: str,
    agency: str,
    name: str,
    source: Sequence[str],
    target: Sequence[str],
    maps: Sequence[MultiValueMap] = (),
    version: str = "1.0",
    description: str | None = None,
    urn: str | None = None,
) -> MultiRepresentationMap:
    """Build a validated :class:`MultiRepresentationMap` from values.

    Args:
        id: MultiRepresentationMap identifier.
        agency: Maintaining agency ID.
        name: Human-readable name.
        source: URNs of the source codelists / valuelists / data types.
        target: URNs of the target codelists / valuelists / data types.
        maps: The individual multi-value mappings. Must be non-empty.
        version: MultiRepresentationMap version. Defaults to ``"1.0"``.
        description: Optional description.
        urn: Optional full SDMX URN; when omitted pysdmx derives the short URN.

    Returns:
        A publish-ready :class:`MultiRepresentationMap`.

    Raises:
        ValidationError: If the resulting map would not pass
            publish-readiness checks.
    """
    mrm = MultiRepresentationMap(
        id=id,
        agency=agency,
        name=name,
        version=version,
        description=description,
        source=list(source),
        target=list(target),
        maps=list(maps),
        urn=urn,
    )
    raise_if_invalid(mrm)
    return mrm
