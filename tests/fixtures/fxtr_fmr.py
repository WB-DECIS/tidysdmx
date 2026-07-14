"""Fixtures for the tidysdmx.fmr subpackage tests.

Provides in-memory artefact pairs (a *base* variant plus mutated
variants exercising each change kind) and a fake FMR client so the
publish workflow can be tested without any network access.
"""

import msgspec
import pytest
from pysdmx.errors import NotFound
from pysdmx.model import (
    Code,
    Component,
    ComponentMap,
    Components,
    Concept,
    DataType,
    Facets,
    FixedValueMap,
    HierarchicalCode,
    ImplicitComponentMap,
    ItemReference,
    Role,
    StructureMap,
    ValueMap,
)

from tidysdmx.artefact_builder import (
    build_codelist,
    build_concept_scheme,
    build_data_structure_definition,
    build_dataflow,
    build_hierarchy,
    build_representation_map,
)

AGENCY = "WB.TEST"


def _codes(*ids: str) -> list[Code]:
    return [Code(id=i, name=i.title()) for i in ids]


@pytest.fixture
def codelist_base():
    """Codelist with codes RED, GREEN, BLUE."""
    return build_codelist(
        id="CL_COLOUR",
        agency=AGENCY,
        name="Colours",
        codes=_codes("RED", "GREEN", "BLUE"),
    )


@pytest.fixture
def codelist_item_added(codelist_base):
    """codelist_base plus a new code YELLOW."""
    return msgspec.structs.replace(
        codelist_base,
        items=(*codelist_base.items, Code(id="YELLOW", name="Yellow")),
    )


@pytest.fixture
def codelist_item_removed(codelist_base):
    """codelist_base without the code BLUE."""
    return msgspec.structs.replace(
        codelist_base,
        items=tuple(c for c in codelist_base.items if c.id != "BLUE"),
    )


@pytest.fixture
def codelist_renamed_item(codelist_base):
    """codelist_base with the code RED renamed."""
    items = tuple(
        msgspec.structs.replace(c, name="Bright red") if c.id == "RED" else c
        for c in codelist_base.items
    )
    return msgspec.structs.replace(codelist_base, items=items)


@pytest.fixture
def codelist_reordered(codelist_base):
    """codelist_base with the same codes in a different order."""
    return msgspec.structs.replace(
        codelist_base, items=tuple(reversed(codelist_base.items))
    )


@pytest.fixture
def concept_scheme_base():
    """Concept scheme with concepts FREQ and OBS_VALUE."""
    return build_concept_scheme(
        id="CS_MAIN",
        agency=AGENCY,
        name="Main concepts",
        concepts=[
            Concept(id="FREQ", name="Frequency", dtype=DataType.STRING),
            Concept(id="OBS_VALUE", name="Value", dtype=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def concept_scheme_dtype_changed(concept_scheme_base):
    """concept_scheme_base with FREQ's data type changed."""
    items = tuple(
        msgspec.structs.replace(c, dtype=DataType.ALPHA) if c.id == "FREQ" else c
        for c in concept_scheme_base.items
    )
    return msgspec.structs.replace(concept_scheme_base, items=items)


def _concept_ref(item_id: str) -> ItemReference:
    return ItemReference(
        sdmx_type="Concept",
        agency=AGENCY,
        id="CS_MAIN",
        version="1.0",
        item_id=item_id,
    )


def _dsd_components() -> list[Component]:
    return [
        Component(
            id="FREQ",
            required=True,
            role=Role.DIMENSION,
            concept=_concept_ref("FREQ"),
            local_enum_ref=(
                f"urn:sdmx:org.sdmx.infomodel.codelist.Codelist={AGENCY}:CL_FREQ(1.0)"
            ),
        ),
        Component(
            id="TIME_PERIOD",
            required=True,
            role=Role.DIMENSION,
            concept=_concept_ref("TIME_PERIOD"),
        ),
        Component(
            id="OBS_VALUE",
            required=True,
            role=Role.MEASURE,
            concept=_concept_ref("OBS_VALUE"),
        ),
        Component(
            id="UNIT",
            required=False,
            role=Role.ATTRIBUTE,
            concept=_concept_ref("UNIT"),
            local_facets=Facets(max_length=10),
            attachment_level="O",
        ),
    ]


@pytest.fixture
def dsd_base():
    """DSD with FREQ/TIME_PERIOD dimensions, OBS_VALUE, UNIT attribute."""
    return build_data_structure_definition(
        id="DSD_TEST",
        agency=AGENCY,
        name="Test DSD",
        components=Components(_dsd_components()),
    )


@pytest.fixture
def dsd_component_removed(dsd_base):
    """dsd_base without the UNIT attribute."""
    comps = Components([c for c in dsd_base.components if c.id != "UNIT"])
    return msgspec.structs.replace(dsd_base, components=comps)


@pytest.fixture
def dsd_optional_attr_added(dsd_base):
    """dsd_base with an extra optional attribute COMMENT."""
    extra = Component(
        id="COMMENT",
        required=False,
        role=Role.ATTRIBUTE,
        concept=_concept_ref("COMMENT"),
        attachment_level="O",
    )
    comps = Components([*dsd_base.components, extra])
    return msgspec.structs.replace(dsd_base, components=comps)


@pytest.fixture
def dsd_facet_narrowed(dsd_base):
    """dsd_base with UNIT's max_length narrowed from 10 to 5."""
    comps = Components(
        [
            msgspec.structs.replace(c, local_facets=Facets(max_length=5))
            if c.id == "UNIT"
            else c
            for c in dsd_base.components
        ]
    )
    return msgspec.structs.replace(dsd_base, components=comps)


@pytest.fixture
def dataflow_base():
    """Dataflow referencing DSD_TEST version 1.0."""
    return build_dataflow(
        id="DF_TEST",
        agency=AGENCY,
        name="Test dataflow",
        structure=f"DataStructure={AGENCY}:DSD_TEST(1.0)",
    )


@pytest.fixture
def dataflow_restructured(dataflow_base):
    """dataflow_base pointing at a different DSD version."""
    return msgspec.structs.replace(
        dataflow_base, structure=f"DataStructure={AGENCY}:DSD_TEST(2.0)"
    )


@pytest.fixture
def hierarchy_base():
    """Hierarchy with roots A (children A1, A2) and B."""
    return build_hierarchy(
        id="H_TEST",
        agency=AGENCY,
        name="Test hierarchy",
        codes=[
            HierarchicalCode(
                id="A",
                name="A",
                codes=[
                    HierarchicalCode(id="A1", name="A1"),
                    HierarchicalCode(id="A2", name="A2"),
                ],
            ),
            HierarchicalCode(id="B", name="B"),
        ],
    )


@pytest.fixture
def hierarchy_moved_code(hierarchy_base):
    """hierarchy_base with A2 moved from under A to under B."""
    return msgspec.structs.replace(
        hierarchy_base,
        codes=(
            HierarchicalCode(
                id="A",
                name="A",
                codes=[HierarchicalCode(id="A1", name="A1")],
            ),
            HierarchicalCode(
                id="B",
                name="B",
                codes=[HierarchicalCode(id="A2", name="A2")],
            ),
        ),
    )


def _rep_map_urns() -> tuple[str, str]:
    return (
        f"Codelist={AGENCY}:CL_SRC(1.0)",
        f"Codelist={AGENCY}:CL_TGT(1.0)",
    )


@pytest.fixture
def rep_map_base():
    """Representation map with two value mappings."""
    source, target = _rep_map_urns()
    return build_representation_map(
        id="RM_TEST",
        agency=AGENCY,
        name="Test map",
        source=source,
        target=target,
        maps=[
            ValueMap(source="UY", target="URY"),
            ValueMap(source="FR", target="FRA"),
        ],
    )


@pytest.fixture
def rep_map_rule_added(rep_map_base):
    """rep_map_base with an extra value mapping."""
    return msgspec.structs.replace(
        rep_map_base,
        maps=(*rep_map_base.maps, ValueMap(source="DE", target="DEU")),
    )


@pytest.fixture
def rep_map_rule_removed(rep_map_base):
    """rep_map_base without the FR mapping."""
    return msgspec.structs.replace(
        rep_map_base,
        maps=tuple(m for m in rep_map_base.maps if m.source != "FR"),
    )


def _structure_map_rules() -> tuple:
    return (
        ImplicitComponentMap(source="OBS_VALUE", target="OBS_VALUE"),
        ComponentMap(
            source="COUNTRY",
            target="REF_AREA",
            values=f"RepresentationMap={AGENCY}:RM_TEST(1.0)",
        ),
        FixedValueMap(target="FREQ", value="A"),
    )


@pytest.fixture
def structure_map_base():
    """Structure map with implicit, representation, and fixed rules."""
    return StructureMap(
        id="SM_TEST",
        agency=AGENCY,
        name="Test structure map",
        version="1.0",
        source=f"DataStructure={AGENCY}:DSD_SRC(1.0)",
        target=f"DataStructure={AGENCY}:DSD_TEST(1.0)",
        maps=_structure_map_rules(),
    )


@pytest.fixture
def structure_map_rule_added(structure_map_base):
    """structure_map_base with an extra implicit mapping rule."""
    extra = ImplicitComponentMap(source="UNIT", target="UNIT")
    return msgspec.structs.replace(
        structure_map_base, maps=(*structure_map_base.maps, extra)
    )


class FakeFmrClient:
    """In-memory stand-in for FmrClient used by publish workflow tests.

    Stores registry artefacts keyed by ``(type name, agency, id)`` and
    records every ``put_artefacts`` call in ``put_calls``.
    """

    def __init__(self, artefacts=()):
        self._store = {}
        self.put_calls = []
        for artefact in artefacts:
            self.add(artefact)

    @staticmethod
    def _key(artefact):
        agency = artefact.agency
        agency_id = getattr(agency, "id", agency)
        return (type(artefact).__name__, agency_id, artefact.id)

    def add(self, artefact):
        """Store an artefact as the registry's current copy."""
        self._store[self._key(artefact)] = artefact

    def get_existing(self, artefact, version="~"):
        """Return the stored counterpart of ``artefact`` or ``None``."""
        return self._store.get(self._key(artefact))

    def get_artefact(self, ref, artefact_type=None):
        """Return a stored artefact by reference or raise NotFound."""
        for (_, agency, aid), artefact in self._store.items():
            if agency == ref.agency and aid == ref.id:
                return artefact
        raise NotFound("Not found", f"No artefact for {ref}.")

    def put_artefacts(self, artefacts, action=None, header=None, validate=True):
        """Record the submission and update the in-memory registry."""
        self.put_calls.append({"artefacts": list(artefacts), "action": action})
        for artefact in artefacts:
            self.add(artefact)


@pytest.fixture
def fake_fmr_client():
    """An empty in-memory FMR client double."""
    return FakeFmrClient()
