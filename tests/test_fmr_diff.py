import msgspec
import pytest
from pysdmx.errors import Invalid
from pysdmx.model import (
    Category,
    CategoryScheme,
    Code,
    Codelist,
    ComponentMap,
    Components,
    ProvisionAgreement,
)

from tidysdmx.fmr.diff import (
    ArtefactChange,
    ChangeImpact,
    ChangeKind,
    compare_artefacts,
)


def _kinds(diff):
    return [c.kind for c in diff.changes]


class TestCompareArtefactsGuards:
    def test_compare_artefacts_identical_is_unchanged(self, codelist_base):
        """Identical artefacts yield an empty diff."""
        diff = compare_artefacts(codelist_base, codelist_base)
        assert diff.is_unchanged
        assert not diff
        assert diff.impact is None

    def test_compare_artefacts_version_only_change_is_unchanged(self, codelist_base):
        """The version field is excluded from the comparison."""
        bumped = msgspec.structs.replace(codelist_base, version="2.0")
        diff = compare_artefacts(codelist_base, bumped)
        assert diff.is_unchanged

    def test_compare_artefacts_different_type_raises_invalid(
        self, codelist_base, dsd_base
    ):
        """Comparing different artefact types is a caller bug."""
        with pytest.raises(Invalid, match="Type mismatch"):
            compare_artefacts(codelist_base, dsd_base)

    def test_compare_artefacts_different_id_raises_invalid(self, codelist_base):
        """Comparing artefacts with different ids is a caller bug."""
        other = msgspec.structs.replace(codelist_base, id="CL_OTHER")
        with pytest.raises(Invalid, match="Identity mismatch"):
            compare_artefacts(codelist_base, other)

    def test_compare_artefacts_different_agency_raises_invalid(self, codelist_base):
        """Comparing artefacts with different agencies is a caller bug."""
        other = msgspec.structs.replace(codelist_base, agency="OTHER")
        with pytest.raises(Invalid, match="Identity mismatch"):
            compare_artefacts(codelist_base, other)


class TestCommonFields:
    def test_compare_artefacts_artefact_renamed_is_cosmetic(self, codelist_base):
        """Renaming the artefact itself is a cosmetic change."""
        renamed = msgspec.structs.replace(codelist_base, name="New name")
        diff = compare_artefacts(codelist_base, renamed)
        assert _kinds(diff) == [ChangeKind.RENAMED]
        assert diff.impact == ChangeImpact.COSMETIC

    def test_compare_artefacts_description_changed_is_cosmetic(self, codelist_base):
        """Changing the artefact description is cosmetic."""
        updated = msgspec.structs.replace(codelist_base, description="d")
        diff = compare_artefacts(codelist_base, updated)
        assert _kinds(diff) == [ChangeKind.DESCRIPTION_CHANGED]
        assert diff.impact == ChangeImpact.COSMETIC


class TestItemSchemes:
    def test_compare_artefacts_item_added_is_additive(
        self, codelist_base, codelist_item_added
    ):
        """A new code is an additive change."""
        diff = compare_artefacts(codelist_base, codelist_item_added)
        assert diff.impact == ChangeImpact.ADDITIVE
        change = diff.changes[0]
        assert change.kind == ChangeKind.ADDED
        assert change.path == "items.YELLOW"

    def test_compare_artefacts_item_removed_is_breaking(
        self, codelist_base, codelist_item_removed
    ):
        """A removed code is a breaking change."""
        diff = compare_artefacts(codelist_base, codelist_item_removed)
        assert diff.impact == ChangeImpact.BREAKING
        change = diff.changes[0]
        assert change.kind == ChangeKind.REMOVED
        assert change.path == "items.BLUE"

    def test_compare_artefacts_item_renamed_is_cosmetic(
        self, codelist_base, codelist_renamed_item
    ):
        """An item rename is cosmetic."""
        diff = compare_artefacts(codelist_base, codelist_renamed_item)
        assert _kinds(diff) == [ChangeKind.RENAMED]
        assert diff.changes[0].path == "items.RED.name"
        assert diff.changes[0].old == "Red"
        assert diff.changes[0].new == "Bright red"

    def test_compare_artefacts_item_description_changed(self, codelist_base):
        """An item description change is cosmetic."""
        items = tuple(
            msgspec.structs.replace(c, description="desc") if c.id == "RED" else c
            for c in codelist_base.items
        )
        updated = msgspec.structs.replace(codelist_base, items=items)
        diff = compare_artefacts(codelist_base, updated)
        assert _kinds(diff) == [ChangeKind.DESCRIPTION_CHANGED]
        assert diff.impact == ChangeImpact.COSMETIC

    def test_compare_artefacts_items_reordered_single_record(
        self, codelist_base, codelist_reordered
    ):
        """Reordering surviving items yields one cosmetic record."""
        diff = compare_artefacts(codelist_base, codelist_reordered)
        assert _kinds(diff) == [ChangeKind.REORDERED]
        assert diff.impact == ChangeImpact.COSMETIC

    def test_compare_artefacts_added_item_does_not_flag_reorder(
        self, codelist_base, codelist_item_added
    ):
        """Adding an item must not produce a spurious reorder record."""
        diff = compare_artefacts(codelist_base, codelist_item_added)
        assert ChangeKind.REORDERED not in _kinds(diff)

    def test_compare_artefacts_concept_dtype_changed_is_breaking(
        self, concept_scheme_base, concept_scheme_dtype_changed
    ):
        """A concept data type change narrows the representation."""
        diff = compare_artefacts(concept_scheme_base, concept_scheme_dtype_changed)
        assert diff.impact == ChangeImpact.BREAKING
        assert diff.changes[0].path == "items.FREQ.dtype"

    def test_compare_artefacts_category_moved_is_breaking(self):
        """Re-parenting a category is breaking (its URN path changes)."""
        old = CategoryScheme(
            id="CAT_TEST",
            agency="WB.TEST",
            name="Categories",
            version="1.0",
            items=[
                Category(
                    id="TOP1",
                    name="Top 1",
                    categories=[Category(id="CHILD", name="Child")],
                ),
                Category(id="TOP2", name="Top 2"),
            ],
        )
        new = CategoryScheme(
            id="CAT_TEST",
            agency="WB.TEST",
            name="Categories",
            version="1.0",
            items=[
                Category(id="TOP1", name="Top 1"),
                Category(
                    id="TOP2",
                    name="Top 2",
                    categories=[Category(id="CHILD", name="Child")],
                ),
            ],
        )
        diff = compare_artefacts(old, new)
        moved = [c for c in diff.changes if c.kind == ChangeKind.MOVED]
        assert len(moved) == 1
        assert moved[0].impact == ChangeImpact.BREAKING
        assert moved[0].old == "TOP1"
        assert moved[0].new == "TOP2"


class TestHierarchy:
    def test_compare_artefacts_hierarchy_code_moved(
        self, hierarchy_base, hierarchy_moved_code
    ):
        """Moving a hierarchical code is cosmetic (presentation only)."""
        diff = compare_artefacts(hierarchy_base, hierarchy_moved_code)
        moved = [c for c in diff.changes if c.kind == ChangeKind.MOVED]
        assert len(moved) == 1
        assert moved[0].impact == ChangeImpact.COSMETIC
        assert moved[0].old == "A"
        assert moved[0].new == "B"

    def test_compare_artefacts_hierarchy_code_removed_is_breaking(self, hierarchy_base):
        """Removing a hierarchical code entirely is breaking."""
        pruned = msgspec.structs.replace(
            hierarchy_base, codes=tuple(hierarchy_base.codes[:1])
        )
        diff = compare_artefacts(hierarchy_base, pruned)
        removed = [c for c in diff.changes if c.kind == ChangeKind.REMOVED]
        assert [c.path for c in removed] == ["codes.B"]
        assert diff.impact == ChangeImpact.BREAKING

    def test_compare_artefacts_hierarchy_code_added_is_additive(self, hierarchy_base):
        """Adding a hierarchical code is additive."""
        from pysdmx.model import HierarchicalCode

        grown = msgspec.structs.replace(
            hierarchy_base,
            codes=(*hierarchy_base.codes, HierarchicalCode(id="C", name="C")),
        )
        diff = compare_artefacts(hierarchy_base, grown)
        assert _kinds(diff) == [ChangeKind.ADDED]
        assert diff.impact == ChangeImpact.ADDITIVE


class TestDsd:
    def test_compare_artefacts_dsd_component_removed_breaking(
        self, dsd_base, dsd_component_removed
    ):
        """Removing a component is breaking."""
        diff = compare_artefacts(dsd_base, dsd_component_removed)
        assert diff.impact == ChangeImpact.BREAKING
        assert diff.changes[0].path == "components.UNIT"
        assert diff.changes[0].kind == ChangeKind.REMOVED

    def test_compare_artefacts_dsd_optional_attribute_added_additive(
        self, dsd_base, dsd_optional_attr_added
    ):
        """Adding an optional attribute is additive."""
        diff = compare_artefacts(dsd_base, dsd_optional_attr_added)
        assert diff.impact == ChangeImpact.ADDITIVE
        assert diff.changes[0].path == "components.COMMENT"

    def test_compare_artefacts_dsd_required_dimension_added_breaking(self, dsd_base):
        """Adding a required dimension is breaking."""
        from pysdmx.model import Component, ItemReference, Role

        extra = Component(
            id="SEX",
            required=True,
            role=Role.DIMENSION,
            concept=ItemReference(
                sdmx_type="Concept",
                agency="WB.TEST",
                id="CS_MAIN",
                version="1.0",
                item_id="SEX",
            ),
        )
        grown = msgspec.structs.replace(
            dsd_base, components=Components([*dsd_base.components, extra])
        )
        diff = compare_artefacts(dsd_base, grown)
        assert diff.impact == ChangeImpact.BREAKING
        assert diff.changes[0].kind == ChangeKind.ADDED

    def test_compare_artefacts_dsd_facet_narrowed_breaking(
        self, dsd_base, dsd_facet_narrowed
    ):
        """Shrinking max_length narrows the representation: breaking."""
        diff = compare_artefacts(dsd_base, dsd_facet_narrowed)
        assert diff.impact == ChangeImpact.BREAKING
        assert diff.changes[0].path == "components.UNIT.local_facets.max_length"

    def test_compare_artefacts_dsd_facet_widened_additive(
        self, dsd_base, dsd_facet_narrowed
    ):
        """Growing max_length widens the representation: additive."""
        diff = compare_artefacts(dsd_facet_narrowed, dsd_base)
        assert diff.impact == ChangeImpact.ADDITIVE

    def test_compare_artefacts_dsd_required_relaxed_additive(self, dsd_base):
        """Making a required component optional is additive."""
        comps = Components(
            [
                msgspec.structs.replace(c, required=False) if c.id == "OBS_VALUE" else c
                for c in dsd_base.components
            ]
        )
        relaxed = msgspec.structs.replace(dsd_base, components=comps)
        diff = compare_artefacts(dsd_base, relaxed)
        assert _kinds(diff) == [ChangeKind.MODIFIED]
        assert diff.impact == ChangeImpact.ADDITIVE

    def test_compare_artefacts_dsd_enum_ref_changed_breaking(self, dsd_base):
        """Pointing a component at another codelist is breaking."""
        comps = Components(
            [
                msgspec.structs.replace(
                    c,
                    local_enum_ref=(
                        "urn:sdmx:org.sdmx.infomodel.codelist."
                        "Codelist=WB.TEST:CL_FREQ(2.0)"
                    ),
                )
                if c.id == "FREQ"
                else c
                for c in dsd_base.components
            ]
        )
        repointed = msgspec.structs.replace(dsd_base, components=comps)
        diff = compare_artefacts(dsd_base, repointed)
        assert diff.changes[0].path == "components.FREQ.enumeration"
        assert diff.impact == ChangeImpact.BREAKING


class TestDataflowAndMaps:
    def test_compare_artefacts_dataflow_structure_changed_breaking(
        self, dataflow_base, dataflow_restructured
    ):
        """Re-pointing a dataflow at another DSD version is breaking."""
        diff = compare_artefacts(dataflow_base, dataflow_restructured)
        assert _kinds(diff) == [ChangeKind.MODIFIED]
        assert diff.changes[0].path == "structure"
        assert diff.impact == ChangeImpact.BREAKING

    def test_compare_artefacts_rep_map_rule_added(
        self, rep_map_base, rep_map_rule_added
    ):
        """A new value mapping is additive."""
        diff = compare_artefacts(rep_map_base, rep_map_rule_added)
        assert _kinds(diff) == [ChangeKind.ADDED]
        assert diff.changes[0].path == "maps[DE->DEU]"
        assert diff.impact == ChangeImpact.ADDITIVE

    def test_compare_artefacts_rep_map_rule_removed_breaking(
        self, rep_map_base, rep_map_rule_removed
    ):
        """A removed value mapping is breaking."""
        diff = compare_artefacts(rep_map_base, rep_map_rule_removed)
        assert _kinds(diff) == [ChangeKind.REMOVED]
        assert diff.impact == ChangeImpact.BREAKING

    def test_compare_artefacts_structure_map_rule_added(
        self, structure_map_base, structure_map_rule_added
    ):
        """A new mapping rule on a structure map is additive."""
        diff = compare_artefacts(structure_map_base, structure_map_rule_added)
        assert _kinds(diff) == [ChangeKind.ADDED]
        assert diff.impact == ChangeImpact.ADDITIVE

    def test_compare_artefacts_structure_map_rule_changed(self, structure_map_base):
        """Re-pointing a rule at another representation map is breaking."""
        rules = tuple(
            msgspec.structs.replace(r, values="RepresentationMap=WB.TEST:RM_TEST(2.0)")
            if isinstance(r, ComponentMap)
            else r
            for r in structure_map_base.maps
        )
        updated = msgspec.structs.replace(structure_map_base, maps=rules)
        diff = compare_artefacts(structure_map_base, updated)
        assert len(diff.changes) == 1
        assert diff.changes[0].path.endswith(".values")
        assert diff.impact == ChangeImpact.BREAKING

    def test_compare_artefacts_structure_map_source_changed(self, structure_map_base):
        """Changing the mapped source structure is breaking."""
        updated = msgspec.structs.replace(
            structure_map_base,
            source="DataStructure=WB.TEST:DSD_OTHER(1.0)",
        )
        diff = compare_artefacts(structure_map_base, updated)
        assert diff.changes[0].path == "source"
        assert diff.impact == ChangeImpact.BREAKING


class TestGenericFallback:
    def test_compare_artefacts_unregistered_type_uses_generic_walk(self):
        """Types without a specialized differ fall back to a field walk."""
        old = ProvisionAgreement(
            id="PA_TEST",
            agency="WB.TEST",
            name="PA",
            version="1.0",
            dataflow="Dataflow=WB.TEST:DF_TEST(1.0)",
            provider="DataProvider=WB.TEST:DP(1.0).WB",
        )
        new = msgspec.structs.replace(old, dataflow="Dataflow=WB.TEST:DF_TEST(2.0)")
        diff = compare_artefacts(old, new)
        assert _kinds(diff) == [ChangeKind.MODIFIED]
        assert diff.changes[0].path == "dataflow"
        assert diff.impact == ChangeImpact.BREAKING

    def test_compare_artefacts_codelist_sdmx_type_change_breaking(self, codelist_base):
        """Leftover fields on specialized types still get diffed."""
        updated = msgspec.structs.replace(codelist_base, sdmx_type="valuelist")
        diff = compare_artefacts(codelist_base, updated)
        assert _kinds(diff) == [ChangeKind.MODIFIED]
        assert diff.changes[0].path == "sdmx_type"
        assert diff.impact == ChangeImpact.BREAKING


class TestArtefactDiffApi:
    def test_artefact_diff_impact_is_max_severity(
        self, codelist_base, codelist_item_added
    ):
        """The diff impact is the most severe change present."""
        mixed = msgspec.structs.replace(codelist_item_added, name="Renamed too")
        diff = compare_artefacts(codelist_base, mixed)
        assert len(diff.changes) == 2
        assert diff.impact == ChangeImpact.ADDITIVE

    def test_artefact_diff_by_impact_filters(self, codelist_base):
        """by_impact returns only changes at the requested level."""
        updated = msgspec.structs.replace(
            msgspec.structs.replace(codelist_base, name="Renamed"),
            items=tuple(codelist_base.items[:2]),
        )
        diff = compare_artefacts(codelist_base, updated)
        assert len(diff.by_impact(ChangeImpact.BREAKING)) == 1
        assert len(diff.by_impact(ChangeImpact.COSMETIC)) == 1
        assert len(diff.by_impact(ChangeImpact.ADDITIVE)) == 0

    def test_artefact_diff_summary_renders_all_changes(
        self, codelist_base, codelist_item_removed
    ):
        """The summary contains the header counts and each change."""
        diff = compare_artefacts(codelist_base, codelist_item_removed)
        text = diff.summary()
        assert "Codelist=WB.TEST:CL_COLOUR(1.0)" in text
        assert "1 breaking" in text
        assert "items.BLUE" in text
        assert str(diff) == text

    def test_artefact_diff_summary_unchanged(self, codelist_base):
        """An empty diff renders a 'no changes' summary."""
        diff = compare_artefacts(codelist_base, codelist_base)
        assert diff.summary().endswith("no changes")

    def test_artefact_change_is_frozen(self):
        """Change records are immutable."""
        change = ArtefactChange(
            kind=ChangeKind.ADDED,
            impact=ChangeImpact.ADDITIVE,
            path="items.X",
            message="added",
        )
        with pytest.raises(AttributeError):
            change.path = "other"

    def test_artefact_diff_truncates_long_values(self, codelist_base):
        """Stringified old/new values are truncated to a bounded length."""
        items = tuple(
            msgspec.structs.replace(c, name="x" * 500) if c.id == "RED" else c
            for c in codelist_base.items
        )
        updated = msgspec.structs.replace(codelist_base, items=items)
        diff = compare_artefacts(codelist_base, updated)
        assert len(diff.changes[0].new) <= 120

    def test_artefact_diff_container_is_empty_codelist(self):
        """Two empty codelists compare as unchanged."""
        old = Codelist(id="CL_E", agency="WB.TEST", name="Empty")
        new = Codelist(id="CL_E", agency="WB.TEST", name="Empty")
        diff = compare_artefacts(old, new)
        assert diff.is_unchanged

    def test_artefact_diff_tuple_vs_list_items_equal(self):
        """Sequence container types must not cause false positives."""
        old = Codelist(id="CL_S", agency="WB.TEST", name="S", items=[Code(id="A")])
        new = Codelist(id="CL_S", agency="WB.TEST", name="S", items=(Code(id="A"),))
        diff = compare_artefacts(old, new)
        assert diff.is_unchanged
