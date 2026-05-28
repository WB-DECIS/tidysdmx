import pytest
from pysdmx.model import (
    Agency,
    AgencyScheme,
    Category,
    CategoryScheme,
    Code,
    Codelist,
    Component,
    Components,
    Concept,
    ConceptScheme,
    Dataflow,
    DataStructureDefinition,
    HierarchicalCode,
    Hierarchy,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    Role,
    ValueMap,
)

from tidysdmx.artefact_builder import (
    build_agency_scheme,
    build_category_scheme,
    build_codelist,
    build_concept_scheme,
    build_data_structure_definition,
    build_dataflow,
    build_hierarchy,
    build_multi_representation_map,
    build_representation_map,
)
from tidysdmx.artefact_validation import ValidationError


class TestBuildCodelist:  # noqa: D101
    def test_happy_path(self):
        """Returns a Codelist when inputs are valid."""
        cl = build_codelist(
            id="CL_FREQ",
            agency="AGY",
            name="Frequency",
            codes=[Code(id="A"), Code(id="M")],
        )
        assert isinstance(cl, Codelist)
        assert len(cl.items) == 2
        assert cl.version == "1.0"

    def test_empty_codes_raise(self):
        """Building a codelist with no codes raises C001."""
        with pytest.raises(ValidationError, match=r"\[C001\]"):
            build_codelist(id="CL", agency="AGY", name="n")

    def test_empty_name_raises(self):
        """Building a codelist with whitespace name raises M003."""
        with pytest.raises(ValidationError, match=r"\[M003\]"):
            build_codelist(id="CL", agency="AGY", name="  ", codes=[Code(id="A")])

    def test_valuelist_type(self):
        """sdmx_type='valuelist' is preserved on the returned artefact."""
        cl = build_codelist(
            id="VL",
            agency="AGY",
            name="n",
            codes=[Code(id="A")],
            sdmx_type="valuelist",
        )
        assert cl.sdmx_type == "valuelist"


class TestBuildConceptScheme:  # noqa: D101
    def test_happy_path(self):
        """Returns a ConceptScheme when inputs are valid."""
        cs = build_concept_scheme(
            id="CS",
            agency="AGY",
            name="Concepts",
            concepts=[Concept(id="FREQ")],
        )
        assert isinstance(cs, ConceptScheme)
        assert len(cs.items) == 1

    def test_empty_concepts_raise(self):
        """No concepts raises CS001."""
        with pytest.raises(ValidationError, match=r"\[CS001\]"):
            build_concept_scheme(id="CS", agency="AGY", name="n")


class TestBuildCategoryScheme:  # noqa: D101
    def test_happy_path(self):
        """Returns a CategoryScheme when inputs are valid."""
        cs = build_category_scheme(
            id="CAT",
            agency="AGY",
            name="Cats",
            categories=[Category(id="ECON")],
        )
        assert isinstance(cs, CategoryScheme)

    def test_empty_categories_raise(self):
        """No categories raises CAT001."""
        with pytest.raises(ValidationError, match=r"\[CAT001\]"):
            build_category_scheme(id="CAT", agency="AGY", name="n")


class TestBuildAgencyScheme:  # noqa: D101
    def test_happy_path(self):
        """Returns an AgencyScheme when inputs are valid."""
        scheme = build_agency_scheme(
            id="AGENCIES",
            agency="SDMX",
            name="Agencies",
            agencies=[Agency(id="AGY", name="An agency")],
        )
        assert isinstance(scheme, AgencyScheme)

    def test_empty_agencies_raise(self):
        """No agencies raises AS001."""
        with pytest.raises(ValidationError, match=r"\[AS001\]"):
            build_agency_scheme(id="AGENCIES", agency="SDMX", name="n")


class TestBuildHierarchy:  # noqa: D101
    def test_happy_path(self):
        """Returns a Hierarchy when inputs are valid."""
        h = build_hierarchy(
            id="H",
            agency="AGY",
            name="n",
            codes=[HierarchicalCode(id="X")],
        )
        assert isinstance(h, Hierarchy)

    def test_empty_codes_raise(self):
        """No codes raises H001."""
        with pytest.raises(ValidationError, match=r"\[H001\]"):
            build_hierarchy(id="H", agency="AGY", name="n")


def _dim(id_):
    return Component(
        id=id_, required=True, role=Role.DIMENSION, concept=Concept(id=id_)
    )


def _measure(id_):
    return Component(id=id_, required=True, role=Role.MEASURE, concept=Concept(id=id_))


class TestBuildDataStructureDefinition:  # noqa: D101
    def test_accepts_plain_sequence(self):
        """A plain list of Component instances is wrapped in Components."""
        dsd = build_data_structure_definition(
            id="DSD",
            agency="AGY",
            name="n",
            components=[_dim("FREQ"), _measure("OBS_VALUE")],
        )
        assert isinstance(dsd, DataStructureDefinition)
        assert isinstance(dsd.components, Components)

    def test_accepts_components_object(self):
        """An existing Components instance is used as-is."""
        comps = Components([_dim("FREQ"), _measure("OBS_VALUE")])
        dsd = build_data_structure_definition(
            id="DSD", agency="AGY", name="n", components=comps
        )
        assert dsd.components is comps

    def test_no_components_raises(self):
        """Empty components raises D001."""
        with pytest.raises(ValidationError, match=r"\[D001\]"):
            build_data_structure_definition(
                id="DSD", agency="AGY", name="n", components=[]
            )

    def test_no_dimension_raises(self):
        """Components with no dimension raises D002."""
        with pytest.raises(ValidationError, match=r"\[D002\]"):
            build_data_structure_definition(
                id="DSD",
                agency="AGY",
                name="n",
                components=[_measure("OBS_VALUE")],
            )


class TestBuildDataflow:  # noqa: D101
    def test_happy_path(self):
        """Returns a Dataflow referencing the DSD."""
        df = build_dataflow(
            id="DF",
            agency="AGY",
            name="n",
            structure="urn:sdmx:org.sdmx.infomodel.datastructure."
            "DataStructure=AGY:DSD(1.0)",
        )
        assert isinstance(df, Dataflow)

    def test_empty_name_raises(self):
        """An empty name raises M003 via the validator."""
        with pytest.raises(ValidationError, match=r"\[M003\]"):
            build_dataflow(
                id="DF",
                agency="AGY",
                name="",
                structure="DataStructure=A:S(1.0)",
            )


class TestBuildRepresentationMap:  # noqa: D101
    def test_happy_path(self):
        """Returns a RepresentationMap when inputs are valid."""
        rm = build_representation_map(
            id="RM",
            agency="AGY",
            name="n",
            source="String",
            target="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=AGY:CL(1.0)",
            maps=[ValueMap(source="a", target="A")],
        )
        assert isinstance(rm, RepresentationMap)

    def test_empty_source_raises(self):
        """An empty source raises R001."""
        with pytest.raises(ValidationError, match=r"\[R001\]"):
            build_representation_map(
                id="RM",
                agency="AGY",
                name="n",
                source="",
                target="String",
                maps=[ValueMap(source="a", target="A")],
            )

    def test_empty_maps_raises(self):
        """An empty maps list raises R003."""
        with pytest.raises(ValidationError, match=r"\[R003\]"):
            build_representation_map(
                id="RM",
                agency="AGY",
                name="n",
                source="String",
                target="urn:sdmx:...:A:B(1.0)",
            )


class TestBuildMultiRepresentationMap:  # noqa: D101
    def test_happy_path(self):
        """Returns a MultiRepresentationMap when inputs are valid."""
        mrm = build_multi_representation_map(
            id="MRM",
            agency="AGY",
            name="n",
            source=["urn:sdmx:...:A:B(1.0)"],
            target=["String"],
            maps=[MultiValueMap(source=["a"], target=["A"])],
        )
        assert isinstance(mrm, MultiRepresentationMap)

    def test_empty_maps_raises(self):
        """An empty maps list raises R003."""
        with pytest.raises(ValidationError, match=r"\[R003\]"):
            build_multi_representation_map(
                id="MRM",
                agency="AGY",
                name="n",
                source=["urn:sdmx:...:A:B(1.0)"],
                target=["String"],
            )
