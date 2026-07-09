"""SDMX URN generation helpers."""

from typeguard import typechecked

SDMX_PACKAGE_MAP: dict[str, str] = {
    "StructureMap": "structuremapping",
    "RepresentationMap": "structuremapping",
    "MultiRepresentationMap": "structuremapping",
    "Codelist": "codelist",
    "ConceptScheme": "conceptscheme",
    "DataStructure": "datastructure",
    "DataStructureDefinition": "datastructure",
    "Dataflow": "datastructure",
    "AgencyScheme": "base",
    "ProvisionAgreement": "registry",
}


@typechecked
def gen_urn(
    artefact_type: str, agency: str, artefact_id: str, version: str = "1.0"
) -> str:
    """Generate a full SDMX URN for any maintainable artefact.

    Args:
        artefact_type: The type of artefact (e.g., "StructureMap", "RepresentationMap")
        agency: The agency ID
        artefact_id: The artefact ID
        version: The version (default "1.0")

    Returns:
        Full URN string

    Example:
        >>> gen_urn("StructureMap", "BIS", "SM_TEST", "1.0")
        'urn:sdmx:org.sdmx.infomodel.structuremapping.StructureMap=BIS:SM_TEST(1.0)'
    """
    package = SDMX_PACKAGE_MAP.get(artefact_type, "base")
    urn = (
        f"urn:sdmx:org.sdmx.infomodel.{package}.{artefact_type}"
        f"={agency}:{artefact_id}({version})"
    )
    return urn
