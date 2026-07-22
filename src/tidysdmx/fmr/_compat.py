"""Package-internal shims over private pysdmx modules, plus shared helpers.

pysdmx does not export its abstract base classes (``MaintainableArtefact``,
``ItemScheme``) from any public module, so the whole subpackage imports
them from the private ``pysdmx.model.__base`` through this single
indirection: when pysdmx gives them a public home, this file is the only
place to update.
"""

from pysdmx.model import Agency
from pysdmx.model.__base import (  # noqa: F401
    ItemScheme,
    MaintainableArtefact,
)


def agency_id(agency: str | Agency) -> str:
    """Return the agency id whether given as a string or an ``Agency``."""
    return agency.id if isinstance(agency, Agency) else agency
