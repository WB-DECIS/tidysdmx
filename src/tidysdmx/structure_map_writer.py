"""Utility functions for writing complete StructureMaps with dependencies."""

from pysdmx.model.__base import MaintainableArtefact
from pysdmx.model.map import (
    ComponentMap,
    DatePatternMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    RepresentationMap,
    StructureMap,
)
from typeguard import typechecked

from .artefact_validation import raise_if_invalid

MapRule = (
    ComponentMap
    | DatePatternMap
    | FixedValueMap
    | ImplicitComponentMap
    | MultiComponentMap
)


def _get_embedded_rep_map(
    map_rule: MapRule,
) -> RepresentationMap | MultiRepresentationMap | None:
    """Return the embedded RepresentationMap/MultiRepresentationMap, or None."""
    if isinstance(map_rule, ComponentMap) and isinstance(
        map_rule.values, RepresentationMap
    ):
        return map_rule.values
    if isinstance(map_rule, MultiComponentMap) and isinstance(
        map_rule.values, MultiRepresentationMap
    ):
        return map_rule.values
    return None


def _replace_values_with_urn(map_rule: MapRule) -> MapRule:
    """Return a copy of the map rule with the embedded rep map replaced by its URN.

    If the map rule has no embedded rep map, returns it unchanged.
    Works for both ComponentMap and MultiComponentMap since they share
    the same (source, target, values) constructor signature.
    """
    rep_map = _get_embedded_rep_map(map_rule)
    if rep_map is None:
        return map_rule
    # Derive the reference from pysdmx's own short_urn so the SDMX class name is
    # always correct. Both RepresentationMap and MultiRepresentationMap
    # serialize under the information-model class "RepresentationMap"
    # (MultiRepresentationMap is a pysdmx typing convenience, not an IM class),
    # and short_urn reflects that — unlike ``type(rep_map).__name__``.
    urn = rep_map.urn or (
        f"urn:sdmx:org.sdmx.infomodel.structuremapping.{rep_map.short_urn}"
    )
    return type(map_rule)(source=map_rule.source, target=map_rule.target, values=urn)


def _convert_to_urn_references(
    structure_map: StructureMap,
) -> StructureMap:
    """Convert embedded RepresentationMap objects to URN references.

    This creates a new StructureMap where ComponentMaps that contain
    RepresentationMap objects are replaced with URN string references.

    Args:
        structure_map: The original StructureMap.

    Returns:
        A new StructureMap with URN references instead of objects.
    """
    new_maps = [_replace_values_with_urn(m) for m in structure_map.maps]
    return structure_map.__replace__(maps=new_maps)


@typechecked
def collect_structure_map_artifacts(
    structure_map: StructureMap,
    convert_to_urns: bool = True,
) -> list[MaintainableArtefact]:
    """Collect the StructureMap and all its dependent RepresentationMaps.

    When a StructureMap contains RepresentationMap objects, this function
    extracts them and converts the StructureMap to use URN references.

    Args:
        structure_map: The StructureMap to process.
        convert_to_urns: If True, converts embedded RepresentationMap objects
            to URN references in the output StructureMap. Defaults to True.

    Returns:
        A list containing RepresentationMaps followed by the StructureMap.

    Example:
        >>> from pysdmx.io import write_sdmx
        >>> from pysdmx.io.format import Format
        >>>
        >>> # Collect all artifacts
        >>> artifacts = collect_structure_map_artifacts(my_structure_map)
        >>>
        >>> # Write them all together
        >>> xml = write_sdmx(
        ...     artifacts,
        ...     sdmx_format=Format.STRUCTURE_SDMX_ML_3_0,
        ...     prettyprint=True
        ... )
    """
    artifacts: list[MaintainableArtefact] = [
        rep_map
        for m in structure_map.maps
        if (rep_map := _get_embedded_rep_map(m)) is not None
    ]

    # Convert RepresentationMap objects to URN references if requested
    if convert_to_urns and artifacts:
        structure_map = _convert_to_urn_references(structure_map)

    # Add the StructureMap itself at the end
    # (dependencies should come before the structure that references them)
    artifacts.append(structure_map)

    return artifacts


@typechecked
def validate_structure_map_references(structure_map: StructureMap) -> None:
    """Validate that all RepresentationMap references are resolved.

    This function checks if ComponentMap and MultiComponentMap rules
    contain actual RepresentationMap objects rather than just URN strings.
    It also validates that RepresentationMaps have required fields set.

    Args:
        structure_map: The StructureMap to validate.

    Raises:
        ValidationError: If the StructureMap fails publish-readiness
            validation, e.g. a ComponentMap or MultiComponentMap contains
            only a URN string reference instead of the actual object, or
            an embedded RepresentationMap has missing required fields.

    Example:
        >>> try:
        ...     validate_structure_map_references(my_structure_map)
        ...     print("All references are resolved!")
        ... except ValidationError as e:
        ...     print(f"Unresolved references: {e}")
    """
    raise_if_invalid(structure_map)


@typechecked
def prepare_structure_map_for_upload(
    structure_map: StructureMap,
    validate: bool = True,
) -> list[MaintainableArtefact]:
    """Prepare a StructureMap for upload by collecting all dependencies.

    This is a convenience function that combines validation (optional)
    and artifact collection.

    Args:
        structure_map: The StructureMap to prepare.
        validate: If True, validates that all references are resolved.
            Defaults to True.

    Returns:
        A list of all artifacts ready to write/upload.

    Raises:
        ValidationError: If validate=True and the StructureMap fails
            publish-readiness validation, e.g. an empty source/target, an
            unresolved RepresentationMap URN reference, or an embedded
            RepresentationMap/MultiRepresentationMap with missing required
            fields.

    Example:
        >>> from pysdmx.api.fmr.maintenance import (
        ...     RegistryMaintenanceClient, StructureAction,
        ... )
        >>> from tidysdmx.structure_map_writer import prepare_structure_map_for_upload
        >>>
        >>> # Prepare artifacts
        >>> artifacts = prepare_structure_map_for_upload(my_structure_map)
        >>>
        >>> # Upload to FMR
        >>> client = RegistryMaintenanceClient(
        ...     api_endpoint="https://your-fmr/sdmx/v2/",
        ...     user="username",
        ...     password="password"
        ... )
        >>> client.put_structures(artifacts, action=StructureAction.Replace)
    """
    if validate:
        validate_structure_map_references(structure_map)

    return collect_structure_map_artifacts(structure_map)
