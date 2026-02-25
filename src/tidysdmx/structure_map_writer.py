"""Utility functions for writing complete StructureMaps with dependencies."""

from typing import List, Sequence, Union

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
    new_maps: List[
        Union[
            ComponentMap,
            DatePatternMap,
            FixedValueMap,
            ImplicitComponentMap,
            MultiComponentMap,
        ]
    ] = []

    for map_rule in structure_map.maps:
        if isinstance(map_rule, ComponentMap):
            if isinstance(map_rule.values, RepresentationMap):
                # Replace the object with its URN
                # Use urn if available, otherwise construct from short_urn
                urn = (
                    map_rule.values.urn
                    if map_rule.values.urn
                    else (
                        "urn:sdmx:org.sdmx.infomodel.structuremapping."
                        f"{map_rule.values.short_urn}"
                    )
                )
                new_maps.append(
                    ComponentMap(
                        source=map_rule.source,
                        target=map_rule.target,
                        values=urn,
                    )
                )
            else:
                new_maps.append(map_rule)
        elif isinstance(map_rule, MultiComponentMap):
            if isinstance(map_rule.values, MultiRepresentationMap):
                # Replace the object with its URN
                # Use urn if available, otherwise construct from short_urn
                urn = (
                    map_rule.values.urn
                    if map_rule.values.urn
                    else (
                        "urn:sdmx:org.sdmx.infomodel.structuremapping."
                        f"{map_rule.values.short_urn}"
                    )
                )
                new_maps.append(
                    MultiComponentMap(
                        source=map_rule.source,
                        target=map_rule.target,
                        values=urn,
                    )
                )
            else:
                new_maps.append(map_rule)
        else:
            # Keep other map types as-is
            new_maps.append(map_rule)

    # Create a new StructureMap with URN references
    return StructureMap(
        id=structure_map.id,
        name=structure_map.name,
        agency=structure_map.agency,
        version=structure_map.version,
        source=structure_map.source,
        target=structure_map.target,
        maps=new_maps,
        description=structure_map.description,
        annotations=structure_map.annotations,
    )


def collect_structure_map_artifacts(
    structure_map: StructureMap,
    convert_to_urns: bool = True,
) -> List[MaintainableArtefact]:
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
    artifacts: List[MaintainableArtefact] = []

    # Add all RepresentationMaps from ComponentMaps
    for map_rule in structure_map.maps:
        if isinstance(map_rule, ComponentMap):
            if isinstance(map_rule.values, RepresentationMap):
                artifacts.append(map_rule.values)
        elif isinstance(map_rule, MultiComponentMap):
            if isinstance(map_rule.values, MultiRepresentationMap):
                artifacts.append(map_rule.values)

    # Convert RepresentationMap objects to URN references if requested
    if convert_to_urns and artifacts:
        structure_map = _convert_to_urn_references(structure_map)

    # Add the StructureMap itself at the end
    # (dependencies should come before the structure that references them)
    artifacts.append(structure_map)

    return artifacts


def validate_structure_map_references(structure_map: StructureMap) -> None:
    """Validate that all RepresentationMap references are resolved.

    This function checks if ComponentMap and MultiComponentMap rules
    contain actual RepresentationMap objects rather than just URN strings.
    It also validates that RepresentationMaps have required fields set.

    Args:
        structure_map: The StructureMap to validate.

    Raises:
        ValueError: If any ComponentMap or MultiComponentMap contains
            only a URN string reference instead of the actual object,
            or if RepresentationMaps have missing required fields.

    Example:
        >>> try:
        ...     validate_structure_map_references(my_structure_map)
        ...     print("All references are resolved!")
        ... except ValueError as e:
        ...     print(f"Unresolved references: {e}")
    """
    unresolved = []
    invalid_rep_maps = []

    for i, map_rule in enumerate(structure_map.maps):
        if isinstance(map_rule, ComponentMap):
            if isinstance(map_rule.values, str):
                unresolved.append(
                    f"ComponentMap[{i}] (source={map_rule.source}, "
                    f"target={map_rule.target}): URN reference '{map_rule.values}'"
                )
            elif isinstance(map_rule.values, RepresentationMap):
                # Validate the RepresentationMap itself
                rep_map = map_rule.values
                issues = []
                
                if not rep_map.source:
                    issues.append("source is None or empty")
                if not rep_map.target:
                    issues.append("target is None or empty")
                if not rep_map.maps:
                    issues.append("no value mappings defined")
                
                if issues:
                    invalid_rep_maps.append(
                        f"ComponentMap[{i}] RepresentationMap '{rep_map.id}': "
                        f"{', '.join(issues)}"
                    )
                    
        elif isinstance(map_rule, MultiComponentMap):
            if isinstance(map_rule.values, str):
                unresolved.append(
                    f"MultiComponentMap[{i}] "
                    f"(source={map_rule.source}, target={map_rule.target}): "
                    f"URN reference '{map_rule.values}'"
                )
            elif isinstance(map_rule.values, MultiRepresentationMap):
                # Validate the MultiRepresentationMap itself
                rep_map = map_rule.values
                issues = []
                
                if not rep_map.source:
                    issues.append("source is None or empty")
                if not rep_map.target:
                    issues.append("target is None or empty")
                if not rep_map.maps:
                    issues.append("no value mappings defined")
                
                if issues:
                    invalid_rep_maps.append(
                        f"MultiComponentMap[{i}] MultiRepresentationMap '{rep_map.id}': "
                        f"{', '.join(issues)}"
                    )

    errors = []
    
    if unresolved:
        errors.append(
            "StructureMap contains unresolved RepresentationMap references. "
            "These will only be written as URN strings, not full objects:\n"
            + "\n".join(f"  - {ref}" for ref in unresolved)
        )
    
    if invalid_rep_maps:
        errors.append(
            "StructureMap contains invalid RepresentationMaps. "
            "These will cause errors when uploading to FMR:\n"
            + "\n".join(f"  - {ref}" for ref in invalid_rep_maps)
        )
    
    if errors:
        raise ValueError("\n\n".join(errors))


def prepare_structure_map_for_upload(
    structure_map: StructureMap,
    validate: bool = True,
) -> List[MaintainableArtefact]:
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
        ValueError: If validate=True and unresolved references are found.

    Example:
        >>> from pysdmx.api.fmr import RegistryMaintenanceClient, StructureAction
        >>> from pysdmx.util.structure_map_writer import prepare_structure_map_for_upload
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
