import msgspec
import pytest
from fixtures.fxtr_fmr import FakeFmrClient
from pysdmx.api.fmr.maintenance import StructureAction
from pysdmx.errors import Invalid
from pysdmx.model import Components, Concept

from tidysdmx.artefact_validation import ValidationError
from tidysdmx.fmr.diff import ChangeImpact
from tidysdmx.fmr.publish import (
    PlannedActionKind,
    execute_plan,
    plan_publication,
    publish,
    rebase_to_registry,
)
from tidysdmx.fmr.versioning import VersioningMode, VersionPolicy


def _action_for(plan, artefact_id):
    return next(a for a in plan.actions if a.artefact.id == artefact_id)


class TestPlanPublication:
    def test_plan_publication_new_artefact_is_create(
        self, fake_fmr_client, codelist_base
    ):
        """An artefact missing from the registry is planned as CREATE."""
        plan = plan_publication(fake_fmr_client, [codelist_base])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.CREATE
        assert action.diff is None
        assert action.registry_version is None
        assert action.proposed_version == "1.0"

    def test_plan_publication_unchanged_is_skip(self, codelist_base):
        """An identical registry copy is planned as SKIP."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_base])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.SKIP
        assert action.proposed_version == "1.0"
        assert not action.issues

    def test_plan_publication_changed_is_update_with_bumped_version(
        self, codelist_base, codelist_item_removed
    ):
        """A breaking change plans an UPDATE with a major bump."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.UPDATE
        assert action.registry_version == "1.0"
        assert action.proposed_version == "2.0"
        assert action.diff.impact == ChangeImpact.BREAKING

    def test_plan_publication_additive_bumps_minor(
        self, codelist_base, codelist_item_added
    ):
        """An additive change plans a minor bump."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_added])
        assert plan.actions[0].proposed_version == "1.1"

    def test_plan_publication_registry_newer_flags_conflict(
        self, codelist_base, codelist_item_added
    ):
        """A registry ahead of the local baseline blocks the update."""
        registry_copy = msgspec.structs.replace(codelist_item_added, version="3.0")
        client = FakeFmrClient([registry_copy])
        plan = plan_publication(client, [codelist_base])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.UPDATE
        assert any(i.rule_id == "P002" and i.severity == "error" for i in action.issues)
        assert plan.has_blocking_issues

    def test_plan_publication_registry_newer_identical_only_warns(self, codelist_base):
        """A newer registry copy with identical content only warns."""
        registry_copy = msgspec.structs.replace(codelist_base, version="3.0")
        client = FakeFmrClient([registry_copy])
        plan = plan_publication(client, [codelist_base])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.SKIP
        assert any(
            i.rule_id == "P002" and i.severity == "warning" for i in action.issues
        )
        assert not plan.has_blocking_issues

    def test_plan_publication_invalid_registry_version_blocks_action(
        self, codelist_base, codelist_item_added
    ):
        """An unparseable registry version cannot be bumped: blocked."""
        registry_copy = msgspec.structs.replace(codelist_base, version="vNext")
        client = FakeFmrClient([registry_copy])
        plan = plan_publication(client, [codelist_item_added])
        action = plan.actions[0]
        assert any(i.rule_id == "P003" for i in action.issues)
        assert plan.has_blocking_issues

    def test_plan_publication_invalid_local_version_blocks_create(
        self, fake_fmr_client, codelist_base
    ):
        """An unparseable version on a new artefact is blocked."""
        bad = msgspec.structs.replace(codelist_base, version="one-dot-oh")
        plan = plan_publication(fake_fmr_client, [bad])
        assert any(i.rule_id == "P003" for i in plan.actions[0].issues)
        assert plan.has_blocking_issues

    def test_plan_publication_draft_create_blocks_p008(
        self, fake_fmr_client, codelist_base
    ):
        """SEMVER_ONLY (default) blocks publishing a -draft version."""
        draft = msgspec.structs.replace(codelist_base, version="1.0.0-draft")
        plan = plan_publication(fake_fmr_client, [draft])
        action = plan.actions[0]
        assert any(i.rule_id == "P008" and i.severity == "error" for i in action.issues)
        assert plan.has_blocking_issues
        with pytest.raises(ValidationError):
            execute_plan(fake_fmr_client, plan)

    def test_plan_publication_draft_create_allowed_under_sdmx3(
        self, fake_fmr_client, codelist_base
    ):
        """Under SDMX_3, a -draft CREATE is not flagged with P008."""
        draft = msgspec.structs.replace(codelist_base, version="1.0.0-draft")
        plan = plan_publication(
            fake_fmr_client,
            [draft],
            policy=VersionPolicy(mode=VersioningMode.SDMX_3),
        )
        action = plan.actions[0]
        assert not any(i.rule_id == "P008" for i in action.issues)

    @pytest.mark.parametrize("version", ["1.0", "0.1.0", "1.0.0"])
    def test_plan_publication_semver_only_allows_plain_versions(
        self, fake_fmr_client, codelist_base, version
    ):
        """Two-part, 0.x.y, and plain semver CREATEs are not P008-flagged."""
        artefact = msgspec.structs.replace(codelist_base, version=version)
        plan = plan_publication(fake_fmr_client, [artefact])
        assert not any(i.rule_id == "P008" for i in plan.actions[0].issues)

    def test_plan_publication_disallow_breaking_blocks_update(
        self, codelist_base, codelist_item_removed
    ):
        """allow_breaking=False turns breaking updates into blockers."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed], allow_breaking=False)
        assert any(i.rule_id == "P005" for i in plan.actions[0].issues)
        assert plan.has_blocking_issues

    def test_plan_publication_validation_failure_blocks(
        self, fake_fmr_client, codelist_base
    ):
        """Publish-readiness failures block with their original ids."""
        empty = msgspec.structs.replace(codelist_base, items=())
        plan = plan_publication(fake_fmr_client, [empty])
        assert any(i.rule_id == "C001" for i in plan.actions[0].issues)
        assert plan.has_blocking_issues

    def test_plan_publication_orders_dependencies_first(
        self, fake_fmr_client, codelist_base, dsd_base, dataflow_base
    ):
        """Codelists come before DSDs, DSDs before dataflows."""
        plan = plan_publication(
            fake_fmr_client, [dataflow_base, dsd_base, codelist_base]
        )
        assert [a.artefact.id for a in plan.actions] == [
            "CL_COLOUR",
            "DSD_TEST",
            "DF_TEST",
        ]

    def test_plan_publication_duplicate_short_urn_raises(
        self, fake_fmr_client, codelist_base, codelist_renamed_item
    ):
        """Two candidates for the same artefact are rejected."""
        with pytest.raises(Invalid, match="more than once"):
            plan_publication(fake_fmr_client, [codelist_base, codelist_renamed_item])

    def test_plan_publication_propagates_reference_bumps(self, dsd_base, codelist_base):
        """A dependency's bump rewrites references and promotes SKIPs."""
        cl_freq = msgspec.structs.replace(codelist_base, id="CL_FREQ", name="Frequency")
        cl_freq_updated = msgspec.structs.replace(
            cl_freq, items=tuple(cl_freq.items[:2])
        )
        client = FakeFmrClient([cl_freq, dsd_base])
        plan = plan_publication(client, [dsd_base, cl_freq_updated])

        cl_action = _action_for(plan, "CL_FREQ")
        assert cl_action.kind == PlannedActionKind.UPDATE
        assert cl_action.proposed_version == "2.0"

        dsd_action = _action_for(plan, "DSD_TEST")
        assert dsd_action.kind == PlannedActionKind.UPDATE
        assert any(i.rule_id == "P004" for i in dsd_action.issues)
        freq = next(c for c in dsd_action.artefact.components if c.id == "FREQ")
        assert "CL_FREQ(2.0)" in freq.local_enum_ref

    def test_plan_publication_propagates_bumps_between_item_schemes(
        self, concept_scheme_base, codelist_base, codelist_item_removed
    ):
        """A ConceptScheme referencing a batch Codelist sees its bump.

        Regression test: ConceptScheme orders after the item schemes it
        can reference (enum_ref), even when passed first in the batch.
        """
        ref = "Codelist=WB.TEST:CL_COLOUR(1.0)"
        items = tuple(
            msgspec.structs.replace(c, enum_ref=ref) if c.id == "FREQ" else c
            for c in concept_scheme_base.items
        )
        cs = msgspec.structs.replace(concept_scheme_base, items=items)
        client = FakeFmrClient([cs, codelist_base])
        plan = plan_publication(client, [cs, codelist_item_removed])

        cs_action = _action_for(plan, "CS_MAIN")
        assert cs_action.kind == PlannedActionKind.UPDATE
        assert any(i.rule_id == "P004" for i in cs_action.issues)
        freq = next(c for c in cs_action.artefact.items if c.id == "FREQ")
        assert "CL_COLOUR(2.0)" in freq.enum_ref

    def test_plan_publication_propagation_can_be_disabled(
        self, dsd_base, codelist_base
    ):
        """propagate_references=False leaves dependents untouched."""
        cl_freq = msgspec.structs.replace(codelist_base, id="CL_FREQ", name="Frequency")
        cl_freq_updated = msgspec.structs.replace(
            cl_freq, items=tuple(cl_freq.items[:2])
        )
        client = FakeFmrClient([cl_freq, dsd_base])
        plan = plan_publication(
            client,
            [dsd_base, cl_freq_updated],
            propagate_references=False,
        )
        assert _action_for(plan, "DSD_TEST").kind == PlannedActionKind.SKIP

    def test_plan_publication_append_with_update_warns(
        self, codelist_base, codelist_renamed_item
    ):
        """Append cannot overwrite: updates get a P006 warning."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(
            client,
            [codelist_renamed_item],
            action=StructureAction.Append,
        )
        assert any(
            i.rule_id == "P006" and i.severity == "warning"
            for i in plan.actions[0].issues
        )

    def test_plan_publication_merge_with_removals_warns(
        self, codelist_base, codelist_item_removed
    ):
        """Merge unions item schemes: removals get a P007 warning."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(
            client,
            [codelist_item_removed],
            action=StructureAction.Merge,
        )
        assert any(i.rule_id == "P007" for i in plan.actions[0].issues)

    def test_publication_plan_to_publish_applies_versions(
        self, codelist_base, codelist_item_removed
    ):
        """to_publish() returns bump-applied artefacts, skips SKIPs."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed])
        artefacts = plan.to_publish()
        assert len(artefacts) == 1
        assert artefacts[0].version == "2.0"

    def test_publication_plan_summary_renders_actions(
        self, fake_fmr_client, codelist_base
    ):
        """The plan summary lists counts and one line per action."""
        plan = plan_publication(fake_fmr_client, [codelist_base])
        text = plan.summary()
        assert "1 create" in text
        assert "CREATE Codelist=WB.TEST:CL_COLOUR(1.0)" in text
        assert str(plan) == text

    def test_plan_publication_custom_policy_is_used(
        self, codelist_base, codelist_item_added
    ):
        """The version policy parameter drives the proposed version."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(
            client,
            [codelist_item_added],
            policy=VersionPolicy(additive="major"),
        )
        assert plan.actions[0].proposed_version == "2.0"


class TestExecutePlan:
    def test_execute_plan_blocking_issues_raise_before_network(
        self, codelist_base, codelist_item_removed
    ):
        """Blocking plans never reach the registry."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed], allow_breaking=False)
        with pytest.raises(ValidationError, match="P005"):
            execute_plan(client, plan)
        assert client.put_calls == []

    def test_execute_plan_dry_run_makes_no_calls(self, fake_fmr_client, codelist_base):
        """Dry runs report without writing."""
        plan = plan_publication(fake_fmr_client, [codelist_base])
        report = execute_plan(fake_fmr_client, plan, dry_run=True)
        assert fake_fmr_client.put_calls == []
        assert report.results[0].status == "skipped"
        assert report.ok

    def test_execute_plan_skip_actions_reported_skipped(self, codelist_base):
        """SKIP actions appear as skipped in the report."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_base])
        report = execute_plan(client, plan)
        assert report.results[0].status == "skipped"
        assert client.put_calls == []

    def test_execute_plan_batch_single_put_structures_call(
        self, codelist_base, codelist_item_added, dsd_base
    ):
        """Batch mode submits everything in one registry call."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_added, dsd_base])
        report = execute_plan(client, plan)
        assert len(client.put_calls) == 1
        submitted = client.put_calls[0]["artefacts"]
        assert {a.id for a in submitted} == {"CL_COLOUR", "DSD_TEST"}
        assert report.ok
        assert {r.status for r in report.results} == {"published"}

    def test_execute_plan_batch_applies_bumped_versions(
        self, codelist_base, codelist_item_removed
    ):
        """The submitted artefacts carry the proposed versions."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed])
        execute_plan(client, plan)
        assert client.put_calls[0]["artefacts"][0].version == "2.0"

    def test_execute_plan_batch_failure_marks_all_failed(
        self, fake_fmr_client, codelist_base, dsd_base
    ):
        """A failed batch marks every publishable action failed."""
        fake_fmr_client.fail_ids = {"CL_COLOUR"}
        plan = plan_publication(fake_fmr_client, [codelist_base, dsd_base])
        report = execute_plan(fake_fmr_client, plan)
        assert not report.ok
        assert {r.status for r in report.results} == {"failed"}
        assert all(r.error for r in report.results)

    def test_execute_plan_unbatched_fail_fast_marks_not_attempted(
        self, fake_fmr_client, codelist_base, concept_scheme_base
    ):
        """Unbatched fail-fast stops after the first failure."""
        fake_fmr_client.fail_ids = {"CL_COLOUR"}
        plan = plan_publication(fake_fmr_client, [codelist_base, concept_scheme_base])
        report = execute_plan(fake_fmr_client, plan, batch=False)
        by_id = {r.short_urn: r.status for r in report.results}
        assert by_id["Codelist=WB.TEST:CL_COLOUR(1.0)"] == "failed"
        assert by_id["ConceptScheme=WB.TEST:CS_MAIN(1.0)"] == "not_attempted"

    def test_execute_plan_continue_on_error_attempts_independents(
        self, fake_fmr_client, codelist_base, concept_scheme_base
    ):
        """continue_on_error still publishes independent artefacts."""
        fake_fmr_client.fail_ids = {"CL_COLOUR"}
        plan = plan_publication(fake_fmr_client, [codelist_base, concept_scheme_base])
        report = execute_plan(
            fake_fmr_client, plan, batch=False, continue_on_error=True
        )
        by_id = {r.short_urn: r.status for r in report.results}
        assert by_id["Codelist=WB.TEST:CL_COLOUR(1.0)"] == "failed"
        assert by_id["ConceptScheme=WB.TEST:CS_MAIN(1.0)"] == "published"

    def test_execute_plan_continue_on_error_skips_dependents(
        self, fake_fmr_client, dsd_base, dataflow_base, codelist_base
    ):
        """Dependents of a failed artefact are never attempted."""
        fake_fmr_client.fail_ids = {"DSD_TEST"}
        plan = plan_publication(
            fake_fmr_client, [dataflow_base, dsd_base, codelist_base]
        )
        report = execute_plan(
            fake_fmr_client, plan, batch=False, continue_on_error=True
        )
        by_id = {
            r.short_urn.split("=")[1].split("(")[0]: r.status for r in report.results
        }
        assert by_id["WB.TEST:CL_COLOUR"] == "published"
        assert by_id["WB.TEST:DSD_TEST"] == "failed"
        assert by_id["WB.TEST:DF_TEST"] == "not_attempted"

    def test_publication_report_summary_lists_statuses(
        self, fake_fmr_client, codelist_base
    ):
        """The report summary carries counts and per-result lines."""
        plan = plan_publication(fake_fmr_client, [codelist_base])
        report = execute_plan(fake_fmr_client, plan)
        text = report.summary()
        assert "1 published" in text
        assert "CL_COLOUR" in text
        assert str(report) == text


class TestPublish:
    def test_publish_end_to_end_with_fake_client(self, fake_fmr_client, codelist_base):
        """Create, then re-publishing identical content is a no-op."""
        report = publish(fake_fmr_client, [codelist_base])
        assert report.ok
        assert report.results[0].status == "published"

        again = publish(fake_fmr_client, [codelist_base])
        assert again.results[0].status == "skipped"
        assert len(fake_fmr_client.put_calls) == 1

    def test_publish_update_bumps_version_in_registry(
        self, fake_fmr_client, codelist_base, codelist_item_added
    ):
        """An update lands in the registry at the bumped version."""
        publish(fake_fmr_client, [codelist_base])
        publish(fake_fmr_client, [codelist_item_added])
        stored = fake_fmr_client.get_existing(codelist_base)
        assert stored.version == "1.1"

    def test_publish_dry_run_writes_nothing(self, fake_fmr_client, codelist_base):
        """publish(dry_run=True) plans and reports without writing."""
        report = publish(fake_fmr_client, [codelist_base], dry_run=True)
        assert fake_fmr_client.put_calls == []
        assert report.results[0].status == "skipped"


class TestInheritedImpactPropagation:
    """A dependent following a co-bumped dependency inherits its impact.

    Rather than a fabricated breaking change, the dependent inherits the
    dependency's impact and is republished so its reference stays current;
    a clean re-plan of an already-published batch is idempotent.
    """

    @staticmethod
    def _cl_freq(codelist_base, items=None):
        cl = msgspec.structs.replace(codelist_base, id="CL_FREQ", name="Frequency")
        if items is not None:
            cl = msgspec.structs.replace(cl, items=items)
        return cl

    def test_dependent_inherits_additive_dependency(self, dsd_base, codelist_base):
        """An additive dependency bump makes the dependent a MINOR bump.

        Regression: the old re-diff fabricated a breaking change from the
        reference-version repoint, forcing a major (2.0) bump.
        """
        cl_small = self._cl_freq(codelist_base, items=codelist_base.items[:2])
        cl_full = self._cl_freq(codelist_base)  # +1 code vs cl_small -> additive
        client = FakeFmrClient([cl_small, dsd_base])
        plan = plan_publication(client, [dsd_base, cl_full])

        cl_action = _action_for(plan, "CL_FREQ")
        assert cl_action.kind == PlannedActionKind.UPDATE
        assert cl_action.proposed_version == "1.1"

        dsd_action = _action_for(plan, "DSD_TEST")
        assert dsd_action.kind == PlannedActionKind.UPDATE
        assert dsd_action.proposed_version == "1.1"  # inherited additive, not 2.0
        assert dsd_action.diff.impact == ChangeImpact.ADDITIVE  # not breaking
        assert any(i.rule_id == "P004" for i in dsd_action.issues)
        freq = next(c for c in dsd_action.artefact.components if c.id == "FREQ")
        assert "CL_FREQ(1.1)" in freq.local_enum_ref

    def test_dependent_inherits_breaking_dependency(self, dsd_base, codelist_base):
        """A breaking dependency bump still makes the dependent a MAJOR bump."""
        cl_freq = self._cl_freq(codelist_base)
        cl_removed = msgspec.structs.replace(cl_freq, items=cl_freq.items[:2])
        client = FakeFmrClient([cl_freq, dsd_base])
        plan = plan_publication(client, [dsd_base, cl_removed])

        dsd_action = _action_for(plan, "DSD_TEST")
        assert dsd_action.kind == PlannedActionKind.UPDATE
        assert dsd_action.proposed_version == "2.0"  # inherited breaking

    def test_replan_after_publish_is_idempotent(self, dsd_base, codelist_base):
        """Re-planning an already-published batch is all SKIP (hand-off).

        This is the production hand-off: after publishing, rebasing and
        re-planning the same inputs must not bump anything again.
        """
        cl_small = self._cl_freq(codelist_base, items=codelist_base.items[:2])
        cl_full = self._cl_freq(codelist_base)
        client = FakeFmrClient([cl_small, dsd_base])

        plan = plan_publication(client, [dsd_base, cl_full])
        report = execute_plan(client, plan, dry_run=False)
        assert report.ok

        handoff_batch = rebase_to_registry(client, [dsd_base, cl_full])
        handoff = plan_publication(client, handoff_batch)
        assert all(a.kind == PlannedActionKind.SKIP for a in handoff.actions)
        assert not any(a.issues for a in handoff.actions)

    def test_unchanged_batch_never_bumps(self, dsd_base, codelist_base):
        """A batch already in the registry, rebased, plans as all SKIP."""
        cl_freq = self._cl_freq(codelist_base)
        client = FakeFmrClient([cl_freq, dsd_base])
        batch = rebase_to_registry(client, [dsd_base, cl_freq])
        plan = plan_publication(client, batch)
        assert all(a.kind == PlannedActionKind.SKIP for a in plan.actions)

    def test_dsd_concept_form_round_trip_is_skip(self, dsd_base):
        """A DSD round-tripped into embedded-Concept form still plans as SKIP.

        FMR returns component concepts as embedded Concept(urn=..(ver)) while the
        local build uses versionless ItemReferences; that reference-form
        round-trip must not produce a spurious bump.
        """
        concept_urn = (
            "urn:sdmx:org.sdmx.infomodel.conceptscheme."
            "Concept=WB.TEST:CS_MAIN(1.0.0).FREQ"
        )
        comps = Components(
            [
                msgspec.structs.replace(c, concept=Concept(id="FREQ", urn=concept_urn))
                if c.id == "FREQ"
                else c
                for c in dsd_base.components
            ]
        )
        registry_copy = msgspec.structs.replace(dsd_base, components=comps)
        client = FakeFmrClient([registry_copy])
        plan = plan_publication(client, [dsd_base])
        assert _action_for(plan, "DSD_TEST").kind == PlannedActionKind.SKIP
