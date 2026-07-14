from fixtures.fxtr_fmr import FakeFmrClient

from tidysdmx.fmr.diff import compare_artefacts
from tidysdmx.fmr.publish import execute_plan, plan_publication
from tidysdmx.fmr.report import (
    diff_to_dataframe,
    plan_to_dataframe,
    report_to_dataframe,
)

DIFF_COLUMNS = [
    "SHORT_URN",
    "ARTEFACT_TYPE",
    "KIND",
    "IMPACT",
    "PATH",
    "MESSAGE",
    "OLD",
    "NEW",
]


class TestDiffToDataframe:
    def test_diff_to_dataframe_columns(self, codelist_base, codelist_item_removed):
        """The diff DataFrame has the documented columns."""
        diff = compare_artefacts(codelist_base, codelist_item_removed)
        df = diff_to_dataframe(diff)
        assert list(df.columns) == DIFF_COLUMNS
        assert len(df) == 1
        assert df.loc[0, "IMPACT"] == "breaking"

    def test_diff_to_dataframe_accepts_single_and_sequence(
        self, codelist_base, codelist_item_removed, codelist_item_added
    ):
        """A sequence of diffs concatenates all change rows."""
        diff1 = compare_artefacts(codelist_base, codelist_item_removed)
        diff2 = compare_artefacts(codelist_base, codelist_item_added)
        df = diff_to_dataframe([diff1, diff2])
        assert len(df) == 2
        assert set(df["KIND"]) == {"removed", "added"}

    def test_diff_to_dataframe_unchanged_is_empty(self, codelist_base):
        """Unchanged diffs contribute no rows but keep the columns."""
        diff = compare_artefacts(codelist_base, codelist_base)
        df = diff_to_dataframe(diff)
        assert df.empty
        assert list(df.columns) == DIFF_COLUMNS


class TestPlanToDataframe:
    def test_plan_to_dataframe_one_row_per_action(
        self, codelist_base, codelist_item_removed, dsd_base
    ):
        """The plan DataFrame carries one row per planned action."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed, dsd_base])
        df = plan_to_dataframe(plan)
        assert len(df) == 2
        update_row = df[df["ACTION"] == "update"].iloc[0]
        assert update_row["REGISTRY_VERSION"] == "1.0"
        assert update_row["PROPOSED_VERSION"] == "2.0"
        assert update_row["N_BREAKING"] == 1
        assert not update_row["BLOCKED"]

    def test_plan_to_dataframe_renders_issues(
        self, codelist_base, codelist_item_removed
    ):
        """Attached issues are rendered as 'RULE: message' strings."""
        client = FakeFmrClient([codelist_base])
        plan = plan_publication(client, [codelist_item_removed], allow_breaking=False)
        df = plan_to_dataframe(plan)
        assert "P005" in df.loc[0, "ISSUES"]
        assert df.loc[0, "BLOCKED"]


class TestReportToDataframe:
    def test_report_to_dataframe_statuses(self, fake_fmr_client, codelist_base):
        """The report DataFrame carries per-result statuses."""
        plan = plan_publication(fake_fmr_client, [codelist_base])
        report = execute_plan(fake_fmr_client, plan)
        df = report_to_dataframe(report)
        assert list(df.columns) == ["SHORT_URN", "ACTION", "STATUS", "ERROR"]
        assert df.loc[0, "STATUS"] == "published"
        assert df.loc[0, "ERROR"] is None
