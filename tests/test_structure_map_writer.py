import pytest
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)

from tidysdmx.structure_map_writer import (
    _convert_to_urn_references,
    _get_embedded_rep_map,
    _replace_values_with_urn,
    _validate_rep_map_fields,
    collect_structure_map_artifacts,
    prepare_structure_map_for_upload,
    validate_structure_map_references,
)


# Fixtures
@pytest.fixture
def make_rep_map():
    """Factory fixture that builds a minimal RepresentationMap with one ValueMap."""

    def _factory(
        id: str = "RM_CTRY",
        name: str = "Country Map",
        agency: str = "ECB",
        version: str = "1.0",
        urn: str | None = None,
    ) -> RepresentationMap:
        kwargs = dict(
            id=id,
            name=name,
            agency=agency,
            version=version,
            source="String",
            target="String",
            maps=[ValueMap(source="BE", target="BEL")],
        )
        if urn is not None:
            kwargs["urn"] = urn
        return RepresentationMap(**kwargs)

    return _factory


@pytest.fixture
def make_multi_rep_map():
    """Factory fixture for a minimal MultiRepresentationMap."""

    def _factory(
        id: str = "MRM_CTRY",
        name: str = "Multi Country Map",
        agency: str = "ECB",
        version: str = "1.0",
        urn: str | None = None,
    ) -> MultiRepresentationMap:
        kwargs = dict(
            id=id,
            name=name,
            agency=agency,
            version=version,
            source=["String"],
            target=["String"],
            maps=[MultiValueMap(source=["BE", "A"], target=["BEL"])],
        )
        if urn is not None:
            kwargs["urn"] = urn
        return MultiRepresentationMap(**kwargs)

    return _factory


@pytest.fixture
def make_structure_map(make_rep_map, make_multi_rep_map):
    """Factory fixture that builds a StructureMap for testing.

    Keyword args forwarded to the StructureMap constructor; defaults produce
    a map with one ComponentMap (embedded RepresentationMap), one
    FixedValueMap, and one ImplicitComponentMap.
    """

    def _factory(
        id: str = "SM_TEST",
        name: str = "Test Structure Map",
        agency: str = "ECB",
        version: str = "1.0",
        maps=None,
    ) -> StructureMap:
        if maps is None:
            maps = [
                ComponentMap(
                    source="COUNTRY",
                    target="GEO",
                    values=make_rep_map(),
                ),
                FixedValueMap(target="CONF_STATUS", value="F"),
                ImplicitComponentMap(source="FREQ", target="FREQUENCY"),
            ]
        return StructureMap(
            id=id,
            name=name,
            agency=agency,
            version=version,
            source="urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=ECB:ECB_SRC(1.0)",
            target="urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=ECB:ECB_TGT(1.0)",
            maps=maps,
        )

    return _factory


@pytest.mark.unit
class TestGetEmbeddedRepMap:
    """Tests for _get_embedded_rep_map."""

    def test_component_map_with_rep_map_returns_rep_map(self, make_rep_map):
        """ComponentMap with an embedded RepresentationMap should return it."""
        rep_map = make_rep_map()
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _get_embedded_rep_map(cm)

        assert result is rep_map

    def test_multi_component_map_with_multi_rep_map_returns_it(
        self, make_multi_rep_map
    ):
        """MultiComponentMap with embedded MultiRepresentationMap returns it."""
        mrm = make_multi_rep_map()
        mcm = MultiComponentMap(
            source=["COUNTRY", "CURRENCY"],
            target=["CURRENCY"],
            values=mrm,
        )

        result = _get_embedded_rep_map(mcm)

        assert result is mrm

    def test_component_map_with_urn_string_returns_none(self):
        """ComponentMap whose values is a URN string should return None."""
        urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=urn)

        result = _get_embedded_rep_map(cm)

        assert result is None

    def test_multi_component_map_with_urn_string_returns_none(self):
        """MultiComponentMap whose values is a URN string should return None."""
        urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:MRM(1.0)"
        )
        mcm = MultiComponentMap(source=["COUNTRY"], target=["GEO"], values=urn)

        result = _get_embedded_rep_map(mcm)

        assert result is None

    def test_fixed_value_map_returns_none(self):
        """FixedValueMap has no rep map — should return None."""
        fvm = FixedValueMap(target="CONF_STATUS", value="F")

        result = _get_embedded_rep_map(fvm)

        assert result is None

    def test_implicit_component_map_returns_none(self):
        """ImplicitComponentMap has no rep map — should return None."""
        icm = ImplicitComponentMap(source="FREQ", target="FREQUENCY")

        result = _get_embedded_rep_map(icm)

        assert result is None


@pytest.mark.unit
class TestReplaceValuesWithUrn:
    """Tests for _replace_values_with_urn with ComponentMap."""

    def test_returns_component_map_with_urn_string(self, make_rep_map):
        """Embedded RepresentationMap should be replaced with its URN string."""
        rep_map = make_rep_map()
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert isinstance(result, ComponentMap)
        assert isinstance(result.values, str)

    def test_urn_string_contains_agency_and_id(self, make_rep_map):
        """Generated URN must contain agency and artefact ID."""
        rep_map = make_rep_map(id="RM_CTRY", agency="ECB", version="1.0")
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert "ECB" in result.values
        assert "RM_CTRY" in result.values
        assert "1.0" in result.values

    def test_uses_existing_urn_when_present(self, make_rep_map):
        """If the RepresentationMap already has a URN, it should be used as-is."""
        explicit_urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        rep_map = make_rep_map(urn=explicit_urn)
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert result.values == explicit_urn

    def test_source_and_target_preserved(self, make_rep_map):
        """ComponentMap source and target must be preserved after replacement."""
        rep_map = make_rep_map()
        cm = ComponentMap(source="INDICATOR", target="SERIES_CODE", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert result.source == "INDICATOR"
        assert result.target == "SERIES_CODE"

    def test_returns_component_map_type(self, make_rep_map):
        """Result must still be a ComponentMap, not any other type."""
        rep_map = make_rep_map()
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert type(result) is ComponentMap

    def test_input_not_mutated(self, make_rep_map):
        """Original ComponentMap must remain unchanged (frozen dataclass)."""
        rep_map = make_rep_map()
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        _replace_values_with_urn(cm)

        assert isinstance(cm.values, RepresentationMap)

    def test_urn_format_matches_sdmx_pattern(self, make_rep_map):
        """Generated URN should follow the SDMX URN pattern."""
        rep_map = make_rep_map(id="RM_CTRY", agency="ECB", version="2.0")
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=rep_map)

        result = _replace_values_with_urn(cm)

        assert result.values.startswith("urn:sdmx:org.sdmx.infomodel.")

    def test_component_map_with_urn_string_unchanged(self):
        """ComponentMap with URN string values should be returned as-is."""
        existing_urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        cm = ComponentMap(source="COUNTRY", target="COUNTRY", values=existing_urn)

        result = _replace_values_with_urn(cm)

        assert result is cm

    def test_fixed_value_map_returned_unchanged(self):
        """A FixedValueMap has no embedded rep map and must be returned unchanged."""
        fvm = FixedValueMap(target="CONF_STATUS", value="F")

        result = _replace_values_with_urn(fvm)

        assert result is fvm

    def test_implicit_component_map_returned_unchanged(self):
        """An ImplicitComponentMap must be returned unchanged."""
        icm = ImplicitComponentMap(source="FREQ", target="FREQUENCY")

        result = _replace_values_with_urn(icm)

        assert result is icm

    def test_returns_multi_component_map_with_urn_string(self, make_multi_rep_map):
        """Embedded MultiRepresentationMap should be replaced with its URN string."""
        mrm = make_multi_rep_map()
        mcm = MultiComponentMap(
            source=["COUNTRY", "CURRENCY"],
            target=["CURRENCY"],
            values=mrm,
        )

        result = _replace_values_with_urn(mcm)

        assert isinstance(result, MultiComponentMap)
        assert isinstance(result.values, str)

    def test_multi_uses_existing_urn_when_present(self, make_multi_rep_map):
        """If the MultiRepresentationMap has a URN, it should be used as-is."""
        explicit_urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".MultiRepresentationMap=ECB:MRM_CTRY(1.0)"
        )
        mrm = make_multi_rep_map(urn=explicit_urn)
        mcm = MultiComponentMap(
            source=["COUNTRY", "CURRENCY"],
            target=["CURRENCY"],
            values=mrm,
        )

        result = _replace_values_with_urn(mcm)

        assert result.values == explicit_urn

    def test_multi_source_and_target_preserved(self, make_multi_rep_map):
        """MultiComponentMap source and target lists must be preserved."""
        mrm = make_multi_rep_map()
        mcm = MultiComponentMap(
            source=["COUNTRY", "CURRENCY"],
            target=["CURRENCY"],
            values=mrm,
        )

        result = _replace_values_with_urn(mcm)

        assert result.source == ["COUNTRY", "CURRENCY"]
        assert result.target == ["CURRENCY"]

    def test_multi_component_map_urn_contains_id(self, make_multi_rep_map):
        """Generated URN must contain the MultiRepresentationMap ID."""
        mrm = make_multi_rep_map(id="MRM_CTRY", agency="BIS", version="1.0")
        mcm = MultiComponentMap(
            source=["COUNTRY", "CURRENCY"],
            target=["CURRENCY"],
            values=mrm,
        )

        result = _replace_values_with_urn(mcm)

        assert "MRM_CTRY" in result.values
        assert "BIS" in result.values

    def test_multi_component_map_with_urn_string_unchanged(self):
        """MultiComponentMap whose values is already a URN string is returned as-is."""
        existing_urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:MRM(1.0)"
        )
        mcm = MultiComponentMap(
            source=["COUNTRY"],
            target=["GEO"],
            values=existing_urn,
        )

        result = _replace_values_with_urn(mcm)

        assert result is mcm


@pytest.mark.unit
class TestValidateRepMapFields:
    """Tests for _validate_rep_map_fields."""

    def test_valid_rep_map_returns_empty_list(self, make_rep_map):
        """A fully populated RepresentationMap should return no issues."""
        rep_map = make_rep_map()

        issues = _validate_rep_map_fields(rep_map)

        assert issues == []

    def test_missing_source_reported(self):
        """A RepresentationMap with no source should report it."""
        rep_map = RepresentationMap(
            id="RM",
            name="RM",
            agency="ECB",
            source=None,
            target="urn:target",
            maps=[ValueMap(source="A", target="B")],
        )

        issues = _validate_rep_map_fields(rep_map)

        assert any("source" in i for i in issues)

    def test_missing_target_reported(self):
        """A RepresentationMap with no target should report it."""
        rep_map = RepresentationMap(
            id="RM",
            name="RM",
            agency="ECB",
            source="urn:source",
            target=None,
            maps=[ValueMap(source="A", target="B")],
        )

        issues = _validate_rep_map_fields(rep_map)

        assert any("target" in i for i in issues)

    def test_empty_maps_reported(self):
        """A RepresentationMap with no value mappings should report it."""
        rep_map = RepresentationMap(
            id="RM",
            name="RM",
            agency="ECB",
            source="urn:source",
            target="urn:target",
            maps=[],
        )

        issues = _validate_rep_map_fields(rep_map)

        assert any("no value mappings" in i for i in issues)

    def test_all_fields_missing_returns_three_issues(self):
        """source, target and maps all missing should produce three issues."""
        rep_map = RepresentationMap(id="RM", name="RM", agency="ECB")

        issues = _validate_rep_map_fields(rep_map)

        assert len(issues) == 3

    def test_returns_list(self, make_rep_map):
        """Return value must always be a list."""
        issues = _validate_rep_map_fields(make_rep_map())

        assert isinstance(issues, list)


@pytest.mark.unit
class TestConvertToUrnReferences:
    """Tests for _convert_to_urn_references."""

    def test_returns_structure_map(self, make_structure_map):
        """Result must be a StructureMap."""
        result = _convert_to_urn_references(make_structure_map())

        assert isinstance(result, StructureMap)

    def test_component_map_values_become_urn_strings(self, make_structure_map):
        """All ComponentMaps with embedded rep maps must have URN string values."""
        result = _convert_to_urn_references(make_structure_map())

        component_maps = [m for m in result.maps if isinstance(m, ComponentMap)]
        for cm in component_maps:
            assert isinstance(cm.values, str)

    def test_non_component_maps_preserved(self, make_structure_map):
        """FixedValueMap and ImplicitComponentMap rules must be kept unchanged."""
        sm = make_structure_map()
        result = _convert_to_urn_references(sm)

        fixed = [m for m in result.maps if isinstance(m, FixedValueMap)]
        implicit = [m for m in result.maps if isinstance(m, ImplicitComponentMap)]
        assert len(fixed) == 1
        assert len(implicit) == 1

    def test_structural_attributes_preserved(self, make_structure_map):
        """id, name, agency, version, source, and target must be unchanged."""
        sm = make_structure_map(id="SM_ORIG", agency="BIS", version="2.0")
        result = _convert_to_urn_references(sm)

        assert result.id == sm.id
        assert result.name == sm.name
        assert result.agency == sm.agency
        assert result.version == sm.version
        assert result.source == sm.source
        assert result.target == sm.target

    def test_map_count_unchanged(self, make_structure_map):
        """The number of map rules must stay the same after conversion."""
        sm = make_structure_map()
        result = _convert_to_urn_references(sm)

        assert len(result.maps) == len(sm.maps)

    def test_urn_preserved(self, make_rep_map):
        """An explicit URN on the StructureMap must survive URN conversion."""
        explicit_urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping.StructureMap=ECB:SM_TEST(1.0)"
        )
        sm = StructureMap(
            id="SM_TEST",
            name="Test",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            urn=explicit_urn,
            maps=[
                ComponentMap(source="COUNTRY", target="GEO", values=make_rep_map()),
            ],
        )

        result = _convert_to_urn_references(sm)

        assert result.urn == explicit_urn

    def test_uri_and_validity_preserved(self, make_rep_map):
        """uri, valid_from, and valid_to must be carried over."""
        from datetime import datetime

        vf = datetime(2024, 1, 1)
        vt = datetime(2025, 12, 31)
        sm = StructureMap(
            id="SM_TEST",
            name="Test",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            uri="https://example.com/sm",
            valid_from=vf,
            valid_to=vt,
            maps=[
                ComponentMap(source="COUNTRY", target="GEO", values=make_rep_map()),
            ],
        )

        result = _convert_to_urn_references(sm)

        assert result.uri == "https://example.com/sm"
        assert result.valid_from == vf
        assert result.valid_to == vt


@pytest.mark.unit
class TestCollectStructureMapArtifacts:
    """Tests for collect_structure_map_artifacts."""

    def test_structure_map_is_last_element(self, make_structure_map):
        """StructureMap must be the final element of the returned list."""
        sm = make_structure_map()
        artifacts = collect_structure_map_artifacts(sm)

        assert isinstance(artifacts[-1], StructureMap)

    def test_rep_maps_precede_structure_map(self, make_structure_map, make_rep_map):
        """RepresentationMaps must appear before the StructureMap."""
        sm = make_structure_map()
        artifacts = collect_structure_map_artifacts(sm)

        rep_maps = [a for a in artifacts if isinstance(a, RepresentationMap)]
        sm_index = next(
            i for i, a in enumerate(artifacts) if isinstance(a, StructureMap)
        )
        for rm in rep_maps:
            assert artifacts.index(rm) < sm_index

    def test_embedded_rep_maps_are_extracted(self, make_structure_map):
        """All embedded RepresentationMaps must appear in the result list."""
        sm = make_structure_map()  # default: one ComponentMap with RepresentationMap
        artifacts = collect_structure_map_artifacts(sm)

        rep_maps = [a for a in artifacts if isinstance(a, RepresentationMap)]
        assert len(rep_maps) == 1

    def test_convert_to_urns_true_replaces_embedded_objects(self, make_structure_map):
        """With convert_to_urns=True the returned StructureMap must use URN strings."""
        sm = make_structure_map()
        artifacts = collect_structure_map_artifacts(sm, convert_to_urns=True)

        returned_sm = artifacts[-1]
        component_maps = [m for m in returned_sm.maps if isinstance(m, ComponentMap)]
        for cm in component_maps:
            assert isinstance(cm.values, str)

    def test_convert_to_urns_false_keeps_embedded_objects(self, make_structure_map):
        """convert_to_urns=False keeps embedded objects in StructureMap."""
        sm = make_structure_map()
        artifacts = collect_structure_map_artifacts(sm, convert_to_urns=False)

        returned_sm = artifacts[-1]
        component_maps = [m for m in returned_sm.maps if isinstance(m, ComponentMap)]
        for cm in component_maps:
            assert isinstance(cm.values, RepresentationMap)

    def test_no_embedded_rep_maps_returns_only_structure_map(self):
        """A StructureMap with no embedded rep maps should return just itself."""
        sm = StructureMap(
            id="SM_PLAIN",
            name="Plain",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[
                FixedValueMap(target="CONF_STATUS", value="F"),
                ImplicitComponentMap(source="FREQ", target="FREQUENCY"),
            ],
        )
        artifacts = collect_structure_map_artifacts(sm)

        assert artifacts == [sm]

    def test_multiple_rep_maps_all_extracted(self, make_rep_map):
        """Each ComponentMap with a rep map must contribute one entry to the list."""
        sm = StructureMap(
            id="SM_MULTI",
            name="Multi",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[
                ComponentMap(
                    source="COUNTRY",
                    target="GEO",
                    values=make_rep_map(id="RM_A"),
                ),
                ComponentMap(
                    source="SECTOR",
                    target="SECTOR",
                    values=make_rep_map(id="RM_B"),
                ),
            ],
        )
        artifacts = collect_structure_map_artifacts(sm)

        rep_maps = [a for a in artifacts if isinstance(a, RepresentationMap)]
        assert len(rep_maps) == 2


@pytest.mark.unit
class TestValidateStructureMapReferences:
    """Tests for validate_structure_map_references."""

    def test_valid_structure_map_raises_nothing(self, make_structure_map):
        """A fully resolved StructureMap should not raise."""
        validate_structure_map_references(make_structure_map())

    def test_unresolved_urn_reference_raises_value_error(self):
        """ComponentMap with URN string instead of object must raise."""
        urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[ComponentMap(source="COUNTRY", target="GEO", values=urn)],
        )

        with pytest.raises(ValueError, match="unresolved"):
            validate_structure_map_references(sm)

    def test_rep_map_missing_source_raises_value_error(self):
        """RepresentationMap with no source must raise ValueError."""
        rep_map = RepresentationMap(
            id="RM",
            name="RM",
            agency="ECB",
            source=None,
            target="urn:tgt",
            maps=[ValueMap(source="A", target="B")],
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[ComponentMap(source="COUNTRY", target="GEO", values=rep_map)],
        )

        with pytest.raises(ValueError, match="invalid"):
            validate_structure_map_references(sm)

    def test_rep_map_empty_maps_raises_value_error(self):
        """A RepresentationMap with no value mappings must raise ValueError."""
        rep_map = RepresentationMap(
            id="RM",
            name="RM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[],
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[ComponentMap(source="COUNTRY", target="GEO", values=rep_map)],
        )

        with pytest.raises(ValueError, match="invalid"):
            validate_structure_map_references(sm)

    def test_non_component_map_rules_are_ignored(self):
        """FixedValueMap and ImplicitComponentMap should not cause any error."""
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[
                FixedValueMap(target="CONF_STATUS", value="F"),
                ImplicitComponentMap(source="FREQ", target="FREQUENCY"),
            ],
        )

        validate_structure_map_references(sm)  # must not raise


@pytest.mark.unit
class TestPrepareStructureMapForUpload:
    """Tests for prepare_structure_map_for_upload."""

    def test_returns_list(self, make_structure_map):
        """Result must be a list of MaintainableArtefacts."""
        result = prepare_structure_map_for_upload(make_structure_map())

        assert isinstance(result, list)
        assert len(result) > 0

    def test_structure_map_is_in_result(self, make_structure_map):
        """The StructureMap itself must appear in the returned list."""
        result = prepare_structure_map_for_upload(make_structure_map())

        assert any(isinstance(a, StructureMap) for a in result)

    def test_validate_true_raises_for_unresolved_urn(self):
        """With validate=True, an unresolved URN reference must raise ValueError."""
        urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[ComponentMap(source="COUNTRY", target="GEO", values=urn)],
        )

        with pytest.raises(ValueError):
            prepare_structure_map_for_upload(sm, validate=True)

    def test_validate_false_skips_validation(self):
        """With validate=False, an unresolved URN reference must not raise."""
        urn = (
            "urn:sdmx:org.sdmx.infomodel.structuremapping"
            ".RepresentationMap=ECB:RM_CTRY(1.0)"
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="ECB",
            source="urn:src",
            target="urn:tgt",
            maps=[ComponentMap(source="COUNTRY", target="GEO", values=urn)],
        )

        result = prepare_structure_map_for_upload(sm, validate=False)

        assert isinstance(result, list)

    def test_rep_maps_precede_structure_map(self, make_structure_map):
        """Extracted RepresentationMaps must appear before the StructureMap."""
        result = prepare_structure_map_for_upload(make_structure_map())

        sm_index = next(i for i, a in enumerate(result) if isinstance(a, StructureMap))
        for i, a in enumerate(result):
            if isinstance(a, RepresentationMap):
                assert i < sm_index
