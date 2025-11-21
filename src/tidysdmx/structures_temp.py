from typing import Optional
from typeguard import typechecked
from pysdmx.model.map import FixedValueMap

@typechecked
def build_fixed_mapping(target: str, value: str, located_in: Optional[str] = "target") -> FixedValueMap:
    """
    Build a pysdmx FixedValueMap for setting a component to a fixed value.

    Args:
        target (str): The ID of the target component in the structure map.
        value (str): The fixed value to assign to the target component.
        located_in (Optional[str]): Indicates whether the mapping is located in 'source' or 'target'.
            Defaults to 'target'.

    Returns:
        FixedValueMap: A pysdmx FixedValueMap object representing the fixed mapping.

    Raises:
        ValueError: If `target` or `value` is empty.
        ValueError: If `located_in` is not 'source' or 'target'.

    Examples:
        >>> mapping = build_fixed_mapping("CONF_STATUS", "F")
        >>> isinstance(mapping, FixedValueMap)
        True
        >>> str(mapping)
        'target: CONF_STATUS, value: F, located_in: target'
    """
    if not target or not value:
        raise ValueError("Both 'target' and 'value' must be non-empty strings.")
    if located_in not in {"source", "target"}:
        raise ValueError("Parameter 'located_in' must be either 'source' or 'target'.")

    return FixedValueMap(target=target, value=value, located_in=located_in)
