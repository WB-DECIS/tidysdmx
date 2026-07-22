"""Tests for rebase_to_registry and the inplace_breaking_actions guard."""

import msgspec
from fixtures.fxtr_fmr import AGENCY, FakeFmrClient
from pysdmx.model import (
    Component,
    ComponentMap,
    Components,
    ItemReference,
    Role,
    StructureMap,
)

from tidysdmx.artefact_builder import build_data_structure_definition
from tidysdmx.fmr.diff import ChangeImpact
from tidysdmx.fmr.publish import (
    PlannedActionKind,
    inplace_breaking_actions,
    plan_publication,
    rebase_to_registry,
)
from tidysdmx.fmr.versioning import VersioningMode, VersionPolicy

DEV_POLICY = VersionPolicy(mode=VersioningMode.SDMX_3, replace_non_final=True)


def _dsd_src(version="2.0"):
    comp = Component(
        id="DIM1",
        required=True,
        role=Role.DIMENSION,
        concept=ItemReference(
            sdmx_type="Concept", agency=AGENCY, id="CS", version="1.0", item_id="DIM1"
        ),
    )
    return build_data_structure_definition(
        id="DSD_SRC",
        agency=AGENCY,
        name="Source DSD",
        components=Components([comp]),
        version=version,
    )


# --- Pass 1: version seeding -------------------------------------------------


def test_rebase_leaves_absent_artefact_version_unchanged(
    fake_fmr_client, codelist_base
):
    """An artefact missing from the registry keeps its build-time version."""
    [rebased] = rebase_to_registry(fake_fmr_client, [codelist_base])
    assert rebased.version == codelist_base.version


def test_rebase_seeds_existing_to_registry_latest(codelist_base):
    """An artefact in the registry is seeded to the registry's version."""
    registry = FakeFmrClient([msgspec.structs.replace(codelist_base, version="3.0")])
    [rebased] = rebase_to_registry(registry, [codelist_base])
    assert rebased.version == "3.0"


def test_rebase_fixes_urn_when_present(codelist_base):
    """Seeding a versioned URN rewrites its version suffix."""
    local = msgspec.structs.replace(
        codelist_base,
        urn=f"urn:sdmx:org.sdmx.infomodel.codelist.Codelist={AGENCY}:CL_COLOUR(1.0)",
    )
    registry = FakeFmrClient([msgspec.structs.replace(local, version="3.0")])
    [rebased] = rebase_to_registry(registry, [local])
    assert rebased.version == "3.0"
    assert rebased.urn.endswith("(3.0)")


def test_rebase_seeds_to_draft_version(dsd_base):
    """A registry draft version seeds the local artefact to that draft."""
    local = msgspec.structs.replace(dsd_base, version="1.0.0-draft")
    registry = FakeFmrClient([msgspec.structs.replace(dsd_base, version="1.0.0-draft")])
    [rebased] = rebase_to_registry(registry, [local])
    assert rebased.version == "1.0.0-draft"


# --- Pass 2: reference retargeting -------------------------------------------


def test_rebase_retargets_structuremap_source_and_embedded_urn(
    structure_map_base, rep_map_base
):
    """A StructureMap's source and embedded rep-map URN follow seeded versions."""
    registry = FakeFmrClient(
        [
            _dsd_src(version="2.0"),
            msgspec.structs.replace(rep_map_base, version="1.1"),
        ]
    )
    batch = [structure_map_base, _dsd_src(version="1.0"), rep_map_base]
    rebased = rebase_to_registry(registry, batch)
    sm = next(a for a in rebased if a.id == "SM_TEST")
    assert f"{AGENCY}:DSD_SRC(2.0)" in sm.source
    assert f"{AGENCY}:DSD_TEST(1.0)" in sm.target  # external, untouched
    cmap = next(r for r in sm.maps if isinstance(r, ComponentMap))
    assert f"{AGENCY}:RM_TEST(1.1)" in cmap.values


def test_rebase_retargets_embedded_repmap_object(rep_map_base):
    """An embedded rep-map object is seeded to match its standalone entry."""
    embedded_map = StructureMap(
        id="SM_OBJ",
        agency=AGENCY,
        name="Obj map",
        version="1.0",
        source=f"DataStructure={AGENCY}:DSD_SRC(1.0)",
        target=f"DataStructure={AGENCY}:DSD_TEST(1.0)",
        maps=(ComponentMap(source="COUNTRY", target="REF_AREA", values=rep_map_base),),
    )
    registry = FakeFmrClient([msgspec.structs.replace(rep_map_base, version="1.1")])
    rebased = rebase_to_registry(registry, [rep_map_base, embedded_map])
    standalone = next(a for a in rebased if a.id == "RM_TEST")
    sm = next(a for a in rebased if a.id == "SM_OBJ")
    assert standalone.version == "1.1"
    assert sm.maps[0].values.version == "1.1"


def test_rebase_leaves_external_references_untouched(structure_map_base):
    """References to artefacts not in the batch are preserved verbatim."""
    [rebased] = rebase_to_registry(FakeFmrClient(), [structure_map_base])
    assert rebased.source == structure_map_base.source
    assert rebased.target == structure_map_base.target
    cmap = next(r for r in rebased.maps if isinstance(r, ComponentMap))
    orig = next(r for r in structure_map_base.maps if isinstance(r, ComponentMap))
    assert cmap.values == orig.values


def test_rebase_all_absent_is_noop_on_versions(
    fake_fmr_client, codelist_base, dsd_base
):
    """Rebasing a fully-absent batch leaves every version unchanged."""
    batch = [codelist_base, dsd_base]
    rebased = rebase_to_registry(fake_fmr_client, batch)
    assert [a.version for a in rebased] == [a.version for a in batch]


# --- rebase then plan --------------------------------------------------------


def test_plan_without_rebase_blocks_on_p002(rep_map_base, rep_map_rule_added):
    """Without rebasing, a registry ahead of the local baseline blocks (P002)."""
    registry = FakeFmrClient([msgspec.structs.replace(rep_map_base, version="1.0.1")])
    plan = plan_publication(registry, [rep_map_rule_added], policy=DEV_POLICY)
    action = plan.actions[0]
    assert any(i.rule_id == "P002" and i.severity == "error" for i in action.issues)
    assert plan.has_blocking_issues


def test_rebase_then_plan_no_p002_and_bumps(rep_map_base, rep_map_rule_added):
    """Rebasing removes the false P002 and bumps off the registry version."""
    registry = FakeFmrClient([msgspec.structs.replace(rep_map_base, version="1.0.1")])
    seeded = rebase_to_registry(registry, [rep_map_rule_added])
    plan = plan_publication(registry, seeded, policy=DEV_POLICY)
    action = plan.actions[0]
    assert action.kind == PlannedActionKind.UPDATE
    assert action.registry_version == "1.0.1"
    assert action.proposed_version == "1.1.0"
    assert not any(i.rule_id == "P002" for i in action.issues)


def test_rebase_then_plan_draft_stays_in_place(dsd_base, dsd_component_removed):
    """A non-final draft with a breaking change is replaced in place, no P002."""
    draft = "1.0.0-draft"
    registry = FakeFmrClient([msgspec.structs.replace(dsd_base, version=draft)])
    local = msgspec.structs.replace(dsd_component_removed, version=draft)
    seeded = rebase_to_registry(registry, [local])
    plan = plan_publication(registry, seeded, policy=DEV_POLICY)
    action = plan.actions[0]
    assert action.kind == PlannedActionKind.UPDATE
    assert action.registry_version == draft
    assert action.proposed_version == draft
    assert action.diff.impact == ChangeImpact.BREAKING
    assert not any(i.rule_id == "P002" for i in action.issues)


# --- inplace_breaking_actions guard ------------------------------------------


def test_inplace_breaking_actions_flags_draft_overwrite(
    dsd_base, dsd_component_removed
):
    """The guard flags a breaking in-place overwrite of a draft artefact."""
    draft = "1.0.0-draft"
    registry = FakeFmrClient([msgspec.structs.replace(dsd_base, version=draft)])
    local = msgspec.structs.replace(dsd_component_removed, version=draft)
    plan = plan_publication(
        registry, rebase_to_registry(registry, [local]), policy=DEV_POLICY
    )
    flagged = inplace_breaking_actions(plan)
    assert [a.artefact.id for a in flagged] == ["DSD_TEST"]


def test_inplace_breaking_actions_empty_for_normal_bump(
    rep_map_base, rep_map_rule_added
):
    """A normal version bump is not flagged as an in-place overwrite."""
    registry = FakeFmrClient([msgspec.structs.replace(rep_map_base, version="1.0.1")])
    plan = plan_publication(
        registry, rebase_to_registry(registry, [rep_map_rule_added]), policy=DEV_POLICY
    )
    assert inplace_breaking_actions(plan) == ()
