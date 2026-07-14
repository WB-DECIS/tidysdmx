"""Integration tests for the tidysdmx.fmr publication workflow (opt-in).

Unlike ``test_fmr_integration.py`` (which replays the legacy fetch lane
against ``TIDYSDMX_TEST_FMR_URL`` cassettes), these tests exercise the
``tidysdmx.fmr`` subpackage — FmrClient, planning, and publishing —
against a live registry configured through the FmrClient runtime
environment variables. They are skipped unless the environment provides
a registry:

- ``TIDYSDMX_FMR_URL`` — enables the read-only tests;
- plus ``TIDYSDMX_FMR_USER``/``TIDYSDMX_FMR_PASSWORD`` (or
  ``TIDYSDMX_FMR_TOKEN``) and ``TIDYSDMX_FMR_TEST_AGENCY`` — enables
  the publish tests.

The publish tests create codelists named ``CL_TIDYSDMX_IT_<hex>`` under
the test agency; the FMR client exposes no delete operation, so expect
those to accumulate on the target (QA) registry.
"""

import os
import uuid

import msgspec
import pytest

from tidysdmx.artefact_builder import build_codelist
from tidysdmx.fmr import (
    ENV_FMR_PASSWORD,
    ENV_FMR_TOKEN,
    ENV_FMR_URL,
    ENV_FMR_USER,
    FmrClient,
    PlannedActionKind,
    plan_publication,
    publish,
)

pytestmark = pytest.mark.integration

_HAS_URL = bool(os.environ.get(ENV_FMR_URL))
_HAS_CREDS = bool(
    os.environ.get(ENV_FMR_TOKEN)
    or (os.environ.get(ENV_FMR_USER) and os.environ.get(ENV_FMR_PASSWORD))
)
_TEST_AGENCY = os.environ.get("TIDYSDMX_FMR_TEST_AGENCY", "")

requires_fmr = pytest.mark.skipif(not _HAS_URL, reason=f"{ENV_FMR_URL} is not set")
requires_write = pytest.mark.skipif(
    not (_HAS_URL and _HAS_CREDS and _TEST_AGENCY),
    reason="FMR write credentials or TIDYSDMX_FMR_TEST_AGENCY not set",
)


@pytest.fixture(scope="module")
def live_client():
    """FmrClient configured entirely from the environment."""
    return FmrClient()


@pytest.fixture
def unique_codelist():
    """A publish-ready codelist with a unique id for this test run."""
    from pysdmx.model import Code

    suffix = uuid.uuid4().hex[:8].upper()
    return build_codelist(
        id=f"CL_TIDYSDMX_IT_{suffix}",
        agency=_TEST_AGENCY,
        name="tidysdmx integration test codelist",
        codes=[Code(id="A", name="Alpha"), Code(id="B", name="Beta")],
    )


@requires_fmr
class TestIntegrationRead:
    def test_integration_client_lists_dataflows(self, live_client):
        """The read facade reaches the registry."""
        flows = live_client.get_dataflows()
        assert isinstance(flows, list | tuple)

    def test_integration_exists_is_false_for_unknown_artefact(self, live_client):
        """A random id does not exist on the registry."""
        ref = f"Codelist=TIDYSDMX:CL_{uuid.uuid4().hex[:10].upper()}(1.0)"
        assert live_client.exists(ref) is False


@requires_write
class TestIntegrationPublish:
    def test_integration_publish_create_then_skip(self, live_client, unique_codelist):
        """First publish creates; an identical re-plan is a SKIP."""
        report = publish(live_client, [unique_codelist])
        assert report.ok
        assert report.results[0].status == "published"

        plan = plan_publication(live_client, [unique_codelist])
        assert plan.actions[0].kind == PlannedActionKind.SKIP

    def test_integration_publish_update_bumps_version(
        self, live_client, unique_codelist
    ):
        """An additive change republishes at a bumped version."""
        from pysdmx.model import Code

        publish(live_client, [unique_codelist])
        grown = msgspec.structs.replace(
            unique_codelist,
            items=(*unique_codelist.items, Code(id="C", name="Gamma")),
        )
        plan = plan_publication(live_client, [grown])
        action = plan.actions[0]
        assert action.kind == PlannedActionKind.UPDATE
        assert action.proposed_version == "1.1"

        report = publish(live_client, [grown])
        assert report.ok
        stored = live_client.get_existing(unique_codelist)
        assert stored.version == "1.1"

    def test_integration_version_conflict_detected(self, live_client, unique_codelist):
        """A stale baseline against a moved-on registry is flagged."""
        from pysdmx.model import Code

        publish(live_client, [unique_codelist])
        grown = msgspec.structs.replace(
            unique_codelist,
            items=(*unique_codelist.items, Code(id="C", name="Gamma")),
        )
        publish(live_client, [grown])

        stale = msgspec.structs.replace(
            unique_codelist,
            items=(*unique_codelist.items, Code(id="D", name="Delta")),
        )
        plan = plan_publication(live_client, [stale])
        assert any(
            issue.rule_id == "P002"
            for action in plan.actions
            for issue in action.issues
        )
