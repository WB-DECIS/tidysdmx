from typing import Dict, List
import pysdmx as px
from typeguard import typechecked

@typechecked
def extract_validation_info(schema: px.model.dataflow.Schema) -> Dict[str, object]:
    """
    Extract validation information from a given schema.

    Args:
    ---------_
    schema: A pysdmx.model.dataflow.Schema object.
    The schema object contains all necessary validation information.

    Returns:
        dict: A dictionary containing validation information with the following keys:
            - valid_comp: List of valid component names.
            - mandatory_comp: List of mandatory component names.
            - coded_comp: List of coded component names.
            - codelist_ids: Dictionary with coded components as keys and list of codelist IDs as values.
            - dim_comp: List of dimension component names.
    """
    comp = schema.components
    # Precompute reusable objects
    valid_comp = [c.id for c in comp]
    mandatory_comp = [c.id for c in comp if comp[c.id].required]
    coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]
    dim_comp = [c.id for c in comp if comp[c.id].role == px.model.Role.DIMENSION]

    out = {
        "valid_comp": valid_comp,
        "mandatory_comp": mandatory_comp,
        "coded_comp": coded_comp,
        "codelist_ids": get_codelist_ids(comp, coded_comp),
        "dim_comp": dim_comp,
    }

    return out

@typechecked
def get_codelist_ids(comp: px.model.dataflow.Components, coded_comp: List) -> Dict[str, list[str]]:
    """
    Retrieve all codelist IDs for given coded components.

    Args:
        comp (list): List of components.
        coded_comp (list): List of coded components.

    Returns:
        dict: Dictionary with coded components as keys and list of codelist IDs as values.
    """
    codelist_dict = {}
    for component in coded_comp:
        codes = comp[component].local_codes.items
        codelist_dict[component] = [code.id for code in codes]
    
    return codelist_dict
