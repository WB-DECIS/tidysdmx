"""Build SDMX schema artefacts (DSD, codelists, concept scheme) from a table."""

from collections import namedtuple

import pandas as pd
from pysdmx.model import (
    Code,
    Codelist,
    Component,
    Components,
    Concept,
    DataType,
    ItemReference,
    Role,
)
from typeguard import typechecked

from ..artefact_builder import (
    build_codelist,
    build_concept_scheme,
    build_data_structure_definition,
)
from ._ids import _code_id, _to_identifier
from .urn import gen_urn


@typechecked
def _infer_sdmx_type(dtype: object) -> DataType:
    """Infer the SDMX DataType from a pandas/numpy dtype.

    Args:
        dtype: The pandas/numpy data type.

    Returns:
        The corresponding SDMX DataType.
    """
    # Unwrap categorical dtypes to the dtype of their categories so a
    # category of numbers/dates is classified by its underlying type.
    if isinstance(dtype, pd.CategoricalDtype):
        dtype = dtype.categories.dtype

    # Use pandas' dtype predicates (not substring checks on ``str(dtype)``)
    # so nullable extension dtypes (Int64, Float64, boolean) are handled.
    if pd.api.types.is_bool_dtype(dtype):
        return DataType.BOOLEAN
    if pd.api.types.is_integer_dtype(dtype):
        return DataType.INTEGER
    if pd.api.types.is_float_dtype(dtype):
        return DataType.DOUBLE
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return DataType.DATE_TIME
    return DataType.STRING


SchemaComponents = namedtuple(
    "SchemaComponents", ["dsd", "concept_scheme", "codelists"]
)


@typechecked
def _create_codelist_for_component(
    dataframe: pd.DataFrame,
    column: str,
    comp_id: str,
    agency_id: str,
    version: str,
    uppercase_code_ids: bool = True,
) -> Codelist:
    """Create a codelist from unique values in a DataFrame column."""
    values = dataframe[column].dropna().astype(str).unique()
    codes = [
        Code(id=_code_id(value, uppercase=uppercase_code_ids), name=value)
        for value in values
    ]
    cl_id = f"CL_{comp_id}"

    return build_codelist(
        id=cl_id,
        agency=agency_id,
        name=f"{column} codelist",
        codes=codes,
        version=version,
        urn=gen_urn("Codelist", agency_id, cl_id, version),
    )


@typechecked
def _create_dimension_component(
    dataframe: pd.DataFrame,
    column: str,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
    codelists: list[Codelist],
    uppercase_code_ids: bool = True,
) -> Component:
    """Create a dimension component with its concept and codelist."""
    comp_id = _to_identifier(column)
    dtype = _infer_sdmx_type(dataframe[column].dtype)

    # Create concept reference
    ref = _mk_concept_helper(
        column, comp_id, dtype, agency_id, scheme_id, version, concept_items
    )

    # Create codelist
    codelist = _create_codelist_for_component(
        dataframe,
        column,
        comp_id,
        agency_id,
        version,
        uppercase_code_ids=uppercase_code_ids,
    )
    codelists.append(codelist)

    return Component(
        id=comp_id,
        name=column,
        required=True,
        role=Role.DIMENSION,
        concept=ref,
        local_codes=codelist,
    )


@typechecked
def _create_attribute_component(
    dataframe: pd.DataFrame,
    column: str,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
    codelists: list[Codelist],
    uppercase_code_ids: bool = True,
) -> Component:
    """Create an attribute component with optional codelist."""
    comp_id = _to_identifier(column)
    dtype = _infer_sdmx_type(dataframe[column].dtype)

    # Create concept reference
    ref = _mk_concept_helper(
        column, comp_id, dtype, agency_id, scheme_id, version, concept_items
    )

    # Create codelist only for string types
    local_codes = None
    if dtype == DataType.STRING:
        local_codes = _create_codelist_for_component(
            dataframe,
            column,
            comp_id,
            agency_id,
            version,
            uppercase_code_ids=uppercase_code_ids,
        )
        codelists.append(local_codes)

    return Component(
        id=comp_id,
        name=column,
        required=False,
        role=Role.ATTRIBUTE,
        concept=ref,
        local_codes=local_codes,
        attachment_level="O",
    )


@typechecked
def _mk_concept_helper(
    column: str,
    concept_id: str,
    dtype: DataType | None,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
) -> ItemReference:
    """Create a concept and return its item reference."""
    urn = (
        f"urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept="
        f"{agency_id}:{scheme_id}({version}).{concept_id}"
    )
    concept_items.append(Concept(id=concept_id, name=column, dtype=dtype, urn=urn))
    return ItemReference(
        sdmx_type="Concept",
        agency=agency_id,
        id=scheme_id,
        version=version,
        item_id=concept_id,
    )


@typechecked
def create_schema_from_table(
    dataframe: pd.DataFrame,
    dimensions: list[str],
    measure: str,
    time_dimension: str,
    attributes: list[str] | None = None,
    agency_id: str = "WB.DP",
    schema_id: str = "DP_SCHEMA",
    version: str = "1.0",
    uppercase_code_ids: bool = True,
) -> SchemaComponents:
    """Create a DSD, ConceptScheme, and Codelists from a DataFrame.

    Args:
        dataframe: The source DataFrame.
        dimensions: Column names to use as SDMX dimensions.
        measure: Column name to use as the SDMX measure.
        time_dimension: Column name to use as the SDMX time dimension.
        attributes: Optional column names to use as SDMX attributes.
        agency_id: Agency identifier for the generated artefacts.
        schema_id: Base identifier for the generated DSD and concept scheme.
        version: Version string for the generated artefacts.
        uppercase_code_ids: If True (default), codelist code IDs are uppercased.
            Set to False to preserve the original casing of code values.

    Returns:
        SchemaComponents: A named tuple with ``dsd``, ``concept_scheme``,
            and ``codelists`` fields.

    Raises:
        ValueError: If any of the specified column names are missing from
            the DataFrame.
        ValidationError: If the generated artefacts are not publish-ready
            (e.g. an empty codelist).
    """
    attributes = attributes or []
    required = [*dimensions, measure, time_dimension, *attributes]
    missing = [col for col in required if col not in dataframe.columns]
    if missing:
        raise ValueError(f"Columns not found in dataframe: {missing}")

    dsd_id = _to_identifier(schema_id)
    scheme_id = f"{dsd_id}_CS"
    concept_items: list[Concept] = []
    codelists: list[Codelist] = []
    components: list[Component] = []

    # Process dimensions
    for column in dimensions:
        component = _create_dimension_component(
            dataframe,
            column,
            agency_id,
            scheme_id,
            version,
            concept_items,
            codelists,
            uppercase_code_ids=uppercase_code_ids,
        )
        components.append(component)

    # Process time dimension
    time_ref = _mk_concept_helper(
        time_dimension,
        "TIME_PERIOD",
        DataType.PERIOD,
        agency_id,
        scheme_id,
        version,
        concept_items,
    )
    components.append(
        Component(
            id="TIME_PERIOD",
            name=time_dimension,
            required=True,
            role=Role.DIMENSION,
            concept=time_ref,
            local_dtype=DataType.PERIOD,
        )
    )

    # Process measure
    meas_id = _to_identifier(measure)
    meas_dtype = _infer_sdmx_type(dataframe[measure].dtype)
    meas_ref = _mk_concept_helper(
        measure, meas_id, meas_dtype, agency_id, scheme_id, version, concept_items
    )
    components.append(
        Component(
            id=meas_id,
            name=measure,
            required=True,
            role=Role.MEASURE,
            concept=meas_ref,
            local_dtype=meas_dtype,
        )
    )

    # Process attributes
    for column in attributes:
        component = _create_attribute_component(
            dataframe,
            column,
            agency_id,
            scheme_id,
            version,
            concept_items,
            codelists,
            uppercase_code_ids=uppercase_code_ids,
        )
        components.append(component)

    # Create concept scheme and DSD
    concept_scheme = build_concept_scheme(
        id=scheme_id,
        agency=agency_id,
        name=f"{schema_id} generated concept scheme",
        concepts=concept_items,
        version=version,
        urn=gen_urn("ConceptScheme", agency_id, scheme_id, version),
    )

    dsd = build_data_structure_definition(
        id=dsd_id,
        agency=agency_id,
        name=f"{schema_id} generated DSD",
        components=Components(components),
        version=version,
        urn=gen_urn("DataStructure", agency_id, dsd_id, version),
    )

    return SchemaComponents(dsd=dsd, concept_scheme=concept_scheme, codelists=codelists)
