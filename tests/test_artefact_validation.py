import pytest
from pysdmx.model import (
    Agency,
    AgencyScheme,
    CategoryScheme,
    Code,
    Codelist,
    Component,
    ComponentMap,
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
    StructureMap,
    ValueMap,
)

from tidysdmx.artefact_validation import (
    ValidationError,
    ValidationIssue,
    raise_if_invalid,
    validate,
    validate_many,
)


def _codelist(**over):
    kw = {
        "id": "CL_TEST",
        "agency": "AGY",
        "name": "Test",
        "items": (Code(id="A"),),
    }
    kw.update(over)
    return Codelist(**kw)


def _dim(id_):
    return Component(
        id=id_, required=True, role=Role.DIMENSION, concept=Concept(id=id_)
    )


def _measure(id_):
    return Component(id=id_, required=True, role=Role.MEASURE, concept=Concept(id=id_))


class TestValidationIssue:
    def test_defaults(self):
        """Severity defaults to error and field to None."""
        issue = ValidationIssue(rule_id="M001", path="p", message="m")
        assert issue.severity == "error"
        assert issue.field is None

    def test_fields_preserved(self):
        """All fields round-trip through the struct."""
        issue = ValidationIssue(
            rule_id="M001",
            path="Codelist=A:B(1.0)",
            message="bad",
            field="id",
        )
        assert issue.rule_id == "M001"
        assert issue.path == "Codelist=A:B(1.0)"
        assert issue.field == "id"


class TestValidationError:
    def test_is_value_error_subclass(self):
        """Existing ValueError handlers keep working."""
        err = ValidationError([ValidationIssue(rule_id="M001", path="p", message="m")])
        assert isinstance(err, ValueError)

    def test_issues_preserved_as_tuple(self):
        """Issues are stored as an immutable tuple."""
        i = ValidationIssue(rule_id="M001", path="p", message="m")
        err = ValidationError([i])
        assert err.issues == (i,)

    def test_message_includes_rule_id_and_field(self):
        """Rendered message contains every issue."""
        issues = [
            ValidationIssue(rule_id="R001", path="p", message="m", field="source"),
            ValidationIssue(rule_id="DF001", path="q", message="m"),
        ]
        msg = str(ValidationError(issues))
        assert "[R001]" in msg
        assert "p.source" in msg
        assert "q:" in msg


class TestCommonRules:
    def test_m001_empty_id_flagged(self):
        """Empty id triggers M001."""
        issues = validate(_codelist(id=""))
        assert any(i.rule_id == "M001" for i in issues)

    def test_m002_empty_version_flagged(self):
        """Empty version triggers M002."""
        issues = validate(_codelist(version=""))
        assert any(i.rule_id == "M002" for i in issues)

    def test_m003_missing_name_flagged(self):
        """None name triggers M003."""
        issues = validate(_codelist(name=None))
        assert any(i.rule_id == "M003" for i in issues)

    def test_m003_whitespace_name_flagged(self):
        """Whitespace-only name triggers M003."""
        issues = validate(_codelist(name="   "))
        assert any(i.rule_id == "M003" for i in issues)

    def test_m003_dict_name_ok(self):
        """Localized i18n dict names pass M003 without crashing."""
        issues = validate(_codelist(name={"en": "Frequency"}))
        assert not any(i.rule_id == "M003" for i in issues)

    def test_m003_empty_dict_name_flagged(self):
        """An empty dict name triggers M003."""
        issues = validate(_codelist(name={}))
        assert any(i.rule_id == "M003" for i in issues)

    def test_m003_dict_with_whitespace_values_flagged(self):
        """A dict name whose values are all blank triggers M003."""
        issues = validate(_codelist(name={"en": "   "}))
        assert any(i.rule_id == "M003" for i in issues)


class TestCodelist:
    def test_populated_codelist_ok(self):
        """A populated codelist has no issues."""
        assert validate(_codelist()) == []

    def test_empty_items_flagged(self):
        """An empty codelist triggers C001."""
        assert any(i.rule_id == "C001" for i in validate(_codelist(items=())))

    def test_subclass_reuses_codelist_checker(self):
        """A Codelist subclass still triggers C001 via MRO dispatch (BUG-14)."""

        class MyCodelist(Codelist):
            pass

        cl = MyCodelist(id="CL_SUB", agency="AGY", name="Test", items=())
        assert any(i.rule_id == "C001" for i in validate(cl))


class TestSchemes:
    def test_concept_scheme_empty_flagged(self):
        """Empty ConceptScheme triggers CS001."""
        cs = ConceptScheme(id="CS", agency="AGY", name="n", items=())
        assert any(i.rule_id == "CS001" for i in validate(cs))

    def test_concept_scheme_populated_ok(self):
        """A populated ConceptScheme has no issues."""
        cs = ConceptScheme(id="CS", agency="AGY", name="n", items=(Concept(id="X"),))
        assert validate(cs) == []

    def test_category_scheme_empty_flagged(self):
        """Empty CategoryScheme triggers CAT001."""
        cat = CategoryScheme(id="CAT", agency="AGY", name="n", items=())
        assert any(i.rule_id == "CAT001" for i in validate(cat))

    def test_agency_scheme_empty_flagged(self):
        """Empty AgencyScheme triggers AS001."""
        ag = AgencyScheme(id="AGENCIES", agency="AGY", name="n", items=())
        assert any(i.rule_id == "AS001" for i in validate(ag))


class TestHierarchy:
    def test_empty_codes_flagged(self):
        """Hierarchy without codes triggers H001."""
        h = Hierarchy(id="H", agency="AGY", name="n", codes=())
        assert any(i.rule_id == "H001" for i in validate(h))

    def test_populated_ok(self):
        """A populated hierarchy has no issues."""
        h = Hierarchy(
            id="H",
            agency="AGY",
            name="n",
            codes=(HierarchicalCode(id="X"),),
        )
        assert validate(h) == []


class TestRepresentationMap:
    def test_missing_fields_flagged(self):
        """Missing source/target/maps all trigger rules."""
        rm = RepresentationMap(id="R", agency="AGY", name="n")
        ids = {i.rule_id for i in validate(rm)}
        assert {"R001", "R002", "R003"} <= ids

    def test_populated_ok(self):
        """A populated RepresentationMap has no issues."""
        rm = RepresentationMap(
            id="R",
            agency="AGY",
            name="n",
            source="String",
            target="urn:sdmx:org.sdmx.infomodel.codelist.Codelist=A:B(1.0)",
            maps=[ValueMap(source="a", target="b")],
        )
        assert validate(rm) == []


class TestMultiRepresentationMap:
    def test_missing_fields_flagged(self):
        """Missing source/target/maps all trigger rules."""
        mrm = MultiRepresentationMap(id="MR", agency="AGY", name="n")
        ids = {i.rule_id for i in validate(mrm)}
        assert {"R001", "R002", "R003"} <= ids

    def test_populated_ok(self):
        """A populated MultiRepresentationMap has no issues."""
        mrm = MultiRepresentationMap(
            id="MR",
            agency="AGY",
            name="n",
            source=["urn:sdmx:...:A:B(1.0)"],
            target=["String"],
            maps=[MultiValueMap(source=["a"], target=["b"])],
        )
        assert validate(mrm) == []


class TestDataStructureDefinition:
    def test_no_components_flagged(self):
        """DSD without components triggers D001."""
        dsd = DataStructureDefinition(
            id="DSD", agency="AGY", name="n", components=Components([])
        )
        assert any(i.rule_id == "D001" for i in validate(dsd))

    def test_no_dimension_flagged(self):
        """DSD without a dimension triggers D002."""
        dsd = DataStructureDefinition(
            id="DSD",
            agency="AGY",
            name="n",
            components=Components([_measure("OBS_VALUE")]),
        )
        assert any(i.rule_id == "D002" for i in validate(dsd))

    def test_with_dimension_ok(self):
        """A DSD with at least one dimension has no issues."""
        dsd = DataStructureDefinition(
            id="DSD",
            agency="AGY",
            name="n",
            components=Components([_dim("FREQ"), _measure("OBS_VALUE")]),
        )
        assert validate(dsd) == []


class TestDataflow:
    def test_no_structure_flagged(self):
        """Dataflow without structure triggers DF001."""
        df = Dataflow(id="DF", agency="AGY", name="n")
        assert any(i.rule_id == "DF001" for i in validate(df))

    def test_with_structure_ok(self):
        """A Dataflow referencing a DSD has no issues."""
        df = Dataflow(
            id="DF",
            agency="AGY",
            name="n",
            structure="DataStructure=AGY:DSD(1.0)",
        )
        assert validate(df) == []

    def test_df001_empty_string_structure_flagged(self):
        """An empty-string structure triggers DF001."""
        df = Dataflow(id="DF", agency="AGY", name="n", structure="")
        assert any(i.rule_id == "DF001" for i in validate(df))

    def test_df001_whitespace_structure_flagged(self):
        """A whitespace-only structure triggers DF001."""
        df = Dataflow(id="DF", agency="AGY", name="n", structure="   ")
        assert any(i.rule_id == "DF001" for i in validate(df))


class TestValidate:
    def test_artefact_without_specific_checker(self):
        """A StructureMap has no type-specific rules registered."""
        sm = StructureMap(
            id="SM",
            agency="AGY",
            name="n",
            source="DataStructure=A:S(1.0)",
            target="DataStructure=A:T(1.0)",
        )
        assert validate(sm) == []

    def test_agency_object_in_agency_field(self):
        """short_urn works when agency is an Agency object."""
        cl = Codelist(
            id="CL",
            agency=Agency(id="AGY", name="Agency"),
            name="n",
            items=(Code(id="A"),),
        )
        assert validate(cl) == []


class TestValidateMany:
    def test_empty_input_returns_empty_list(self):
        """Empty sequence returns empty list."""
        assert validate_many([]) == []

    def test_concatenates_issues(self):
        """Issues across artefacts are concatenated in order."""
        bad_cl = _codelist(items=())
        bad_h = Hierarchy(id="H", agency="AGY", name="n", codes=())
        issues = validate_many([bad_cl, bad_h])
        assert {i.rule_id for i in issues} == {"C001", "H001"}


def _structure_map(**over):
    kw = {
        "id": "SM",
        "agency": "AGY",
        "name": "SM",
        "version": "1.0",
        "source": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=A:SRC(1.0)",
        "target": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=A:TGT(1.0)",
        "maps": (),
    }
    kw.update(over)
    return StructureMap(**kw)


class TestStructureMap:
    def test_empty_source_flagged(self):
        """A StructureMap with empty source triggers SM001."""
        issues = validate(_structure_map(source=""))
        assert any(i.rule_id == "SM001" for i in issues)

    def test_empty_target_flagged(self):
        """A StructureMap with empty target triggers SM002."""
        issues = validate(_structure_map(target=""))
        assert any(i.rule_id == "SM002" for i in issues)

    def test_unresolved_urn_reference_flagged(self):
        """A ComponentMap whose values is a bare URN string triggers SM003."""
        cm = ComponentMap(
            source="COUNTRY",
            target="GEO",
            values="urn:sdmx:org.sdmx.infomodel.structuremapping.RepresentationMap=A:RM(1.0)",
        )
        issues = validate(_structure_map(maps=(cm,)))
        assert any(i.rule_id == "SM003" for i in issues)

    def test_embedded_rep_map_empty_source_flagged(self):
        """An embedded RepresentationMap with empty source triggers R001."""
        rm = RepresentationMap(
            id="RM",
            agency="AGY",
            name="RM",
            source="",
            target="urn:t",
            maps=[ValueMap(source="A", target="B")],
        )
        cm = ComponentMap(source="COUNTRY", target="GEO", values=rm)
        issues = validate(_structure_map(maps=(cm,)))
        assert any(i.rule_id == "R001" for i in issues)

    def test_valid_structure_map_ok(self):
        """A StructureMap with non-empty source/target and no maps is valid."""
        assert validate(_structure_map()) == []


class TestRaiseIfInvalid:
    def test_accepts_single_artefact(self):
        """A single valid artefact does not raise."""
        raise_if_invalid(_codelist())

    def test_accepts_sequence(self):
        """A sequence of valid artefacts does not raise."""
        raise_if_invalid([_codelist()])

    def test_raises_validation_error_on_bad_single(self):
        """A single invalid artefact raises ValidationError."""
        with pytest.raises(ValidationError, match=r"\[C001\]"):
            raise_if_invalid(_codelist(items=()))

    def test_raises_on_bad_sequence(self):
        """Any invalid artefact in a sequence triggers a raise."""
        with pytest.raises(ValidationError):
            raise_if_invalid([_codelist(), _codelist(items=())])
