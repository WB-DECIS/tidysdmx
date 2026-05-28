"""Minimal reproducible example: missing URN validation in pysdmx.

Context
-------
While building the tidysdmx artefact-validation layer (see
`src/tidysdmx/structure_map_writer.py`), we found that pysdmx silently
accepts artefacts whose `urn` (and URN-shaped string fields like
`StructureMap.source` / `.target` and `ComponentMap.values`) are
malformed or inconsistent with the artefact's own identity. The
malformed values then flow straight through to SDMX-ML / SDMX-JSON
output and are rejected only at FMR upload time, far from the place
the mistake was made.

This script reproduces the gap with pysdmx 1.13.0. Run it with:

    pip install 'pysdmx[xml]==1.13.0'
    python mre_pysdmx_urn_validation.py
"""

from pysdmx.io import write_sdmx
from pysdmx.io.format import Format
from pysdmx.model.map import (
    ComponentMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)

# A well-formed URN looks like:
#   urn:sdmx:org.sdmx.infomodel.<package>.<Type>=<Agency>:<Id>(<Version>)
# Examples below all violate that pattern in different ways, yet pysdmx
# raises no error.

# ---------------------------------------------------------------------------
# 1. Garbage URN on a MaintainableArtefact subclass.
# ---------------------------------------------------------------------------
rm_garbage = RepresentationMap(
    id="RM_CTRY",
    name="Country Map",
    agency="ECB",
    version="1.0",
    source="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_SRC(1.0)",
    target="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_TGT(1.0)",
    maps=[ValueMap(source="BE", target="BEL")],
    urn="not-a-real-urn",  # <-- not a URN at all
)
print("[1] garbage URN accepted: ", rm_garbage.urn)

# ---------------------------------------------------------------------------
# 2. Well-formed URN whose embedded agency/id/version disagrees with the
#    artefact's own agency/id/version. `short_urn` is derived from the
#    fields, so the two URN views of the same object diverge.
# ---------------------------------------------------------------------------
rm_mismatch = RepresentationMap(
    id="RM_CTRY",
    name="Country Map",
    agency="ECB",
    version="1.0",
    source="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_SRC(1.0)",
    target="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_TGT(1.0)",
    maps=[ValueMap(source="BE", target="BEL")],
    urn=(
        "urn:sdmx:org.sdmx.infomodel.structuremapping."
        "RepresentationMap=BIS:OTHER_ID(9.9)"
    ),
)
print("[2] mismatched URN accepted:")
print("    urn       =", rm_mismatch.urn)
print("    short_urn =", rm_mismatch.short_urn)  # derived from fields

# ---------------------------------------------------------------------------
# 3. Wrong SDMX package segment (`codelist` instead of `structuremapping`).
# ---------------------------------------------------------------------------
rm_wrong_pkg = RepresentationMap(
    id="RM_CTRY",
    name="Country Map",
    agency="ECB",
    version="1.0",
    source="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_SRC(1.0)",
    target="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ECB:CL_TGT(1.0)",
    maps=[ValueMap(source="BE", target="BEL")],
    urn=(
        "urn:sdmx:org.sdmx.infomodel.codelist."
        "RepresentationMap=ECB:RM_CTRY(1.0)"
    ),
)
print("[3] wrong-package URN accepted:", rm_wrong_pkg.urn)

# ---------------------------------------------------------------------------
# 4. URN-shaped string fields on StructureMap are unchecked too.
# ---------------------------------------------------------------------------
sm_bad_refs = StructureMap(
    id="SM_TEST",
    name="Test",
    agency="ECB",
    version="1.0",
    source="totally-not-a-urn",
    target="also-not-a-urn",
    maps=[ComponentMap(source="COUNTRY", target="GEO", values=rm_garbage)],
)
print("[4] StructureMap.source/.target accept arbitrary strings:")
print("    source =", sm_bad_refs.source)
print("    target =", sm_bad_refs.target)

# ---------------------------------------------------------------------------
# 5. Malformed values pass through write_sdmx into the produced SDMX-ML.
#    FMR (and any conformant validator) rejects the output downstream.
# ---------------------------------------------------------------------------
xml = write_sdmx([rm_garbage, sm_bad_refs], sdmx_format=Format.STRUCTURE_SDMX_ML_3_0)
print("[5] write_sdmx happily emits the malformed URN:")
for line in xml.splitlines():
    if 'urn="not-a-real-urn"' in line or "not-a-urn" in line:
        print("   ", line.strip())

print()
print("Expected behaviour:")
print("  pysdmx should reject (or at least warn on) construction when:")
print("    a) `urn` does not match the SDMX URN grammar")
print("       urn:sdmx:org.sdmx.infomodel.<package>.<Type>"
      "=<Agency>:<Id>(<Version>)")
print("    b) `urn`'s embedded agency/id/version disagrees with the")
print("       artefact's own agency/id/version")
print("    c) URN-shaped reference fields (StructureMap.source/.target,")
print("       ComponentMap.values when a string) are not valid URNs")
