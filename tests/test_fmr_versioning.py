import pytest
from pysdmx.util import is_final

from tidysdmx.fmr.diff import (
    ArtefactChange,
    ArtefactDiff,
    ChangeImpact,
    ChangeKind,
)
from tidysdmx.fmr.versioning import (
    DEFAULT_VERSION_POLICY,
    SdmxVersion,
    VersioningMode,
    VersionPolicy,
    VersionScheme,
    bump_version,
    compare_versions,
    is_fmr_publishable,
    parse_version,
    suggest_version,
)


def _diff(impact=None):
    """Build a minimal ArtefactDiff with 0 or 1 change."""
    changes = ()
    if impact is not None:
        changes = (
            ArtefactChange(
                kind=ChangeKind.MODIFIED,
                impact=impact,
                path="items.X",
                message="changed",
            ),
        )
    return ArtefactDiff(
        short_urn="Codelist=WB:CL(1.0)",
        artefact_type="Codelist",
        changes=changes,
    )


class TestParseVersion:
    def test_parse_version_two_part(self):
        """'1.0' parses as a two-part version."""
        v = parse_version("1.0")
        assert (v.major, v.minor, v.patch, v.extension) == (1, 0, None, None)
        assert v.scheme == VersionScheme.TWO_PART

    def test_parse_version_semver(self):
        """'1.2.3' parses as a semver version."""
        v = parse_version("1.2.3")
        assert (v.major, v.minor, v.patch, v.extension) == (1, 2, 3, None)
        assert v.scheme == VersionScheme.SEMVER

    def test_parse_version_draft_extension(self):
        """'1.0.0-draft' keeps its extension."""
        v = parse_version("1.0.0-draft")
        assert v.extension == "draft"
        assert not v.is_final

    @pytest.mark.parametrize(
        "bad", ["", "1", "abc", "1.0-draft", "1.0.0-", "1.a", "01.0.0", "1.0."]
    )
    def test_parse_version_invalid_raises(self, bad):
        """Malformed version strings raise ValueError."""
        with pytest.raises(ValueError, match="Invalid SDMX version"):
            parse_version(bad)

    def test_parse_version_roundtrips(self):
        """str() renders the canonical form back."""
        for text in ("1.0", "0.9", "1.2.3", "10.0.1-draft"):
            assert str(parse_version(text)) == text


class TestSdmxVersion:
    def test_sdmx_version_extension_requires_patch(self):
        """An extension without a patch segment is invalid."""
        with pytest.raises(ValueError, match="patch segment"):
            SdmxVersion(1, 0, None, "draft")

    def test_sdmx_version_negative_segment_rejected(self):
        """Negative segments are invalid."""
        with pytest.raises(ValueError, match="non-negative"):
            SdmxVersion(-1, 0)

    def test_sdmx_version_is_final_matches_pysdmx(self):
        """Finality agrees with pysdmx.util.is_final on samples."""
        samples = ["1.0", "1.0.0", "1.0.0-draft", "0.1.0", "2.3.4"]
        for text in samples:
            assert parse_version(text).is_final == is_final(text)


class TestCompareVersions:
    def test_compare_versions_orders_numerics(self):
        """Numeric segments dominate the ordering."""
        assert compare_versions("1.0", "2.0") == -1
        assert compare_versions("1.10.0", "1.9.0") == 1
        assert compare_versions("1.0.0", "1.0.0") == 0

    def test_compare_versions_orders_extensions_before_final(self):
        """A draft sorts before the final at equal numerics."""
        assert compare_versions("1.0.0-draft", "1.0.0") == -1
        assert compare_versions("1.0.0", "1.0.0-draft") == 1

    def test_compare_versions_two_part_before_semver(self):
        """'1.0' sorts before '1.0.0'."""
        assert compare_versions("1.0", "1.0.0") == -1

    def test_compare_versions_prerelease_numeric_identifiers(self):
        """Numeric extension identifiers compare numerically (semver §11)."""
        assert compare_versions("1.0.0-rc.2", "1.0.0-rc.10") == -1

    def test_compare_versions_prerelease_numeric_before_alpha(self):
        """Numeric identifiers precede alphanumeric ones."""
        assert compare_versions("1.0.0-1", "1.0.0-alpha") == -1

    def test_compare_versions_prerelease_shorter_set_first(self):
        """A shorter identifier list precedes a longer one on a tie."""
        assert compare_versions("1.0.0-alpha", "1.0.0-alpha.1") == -1


class TestBumpVersion:
    def test_bump_version_two_part_major(self):
        """Major bump on two-part zeroes the minor."""
        assert bump_version("1.4", "major") == "2.0"

    def test_bump_version_two_part_patch_collapses_to_minor(self):
        """Two-part versions have no patch slot."""
        assert bump_version("1.4", "patch") == "1.5"

    def test_bump_version_semver_patch(self):
        """Patch bump increments the last segment."""
        assert bump_version("1.2.3", "patch") == "1.2.4"

    def test_bump_version_drops_extension_and_zeroes_lower(self):
        """Bumping drops the extension and zeroes lower segments."""
        assert bump_version("1.2.3-draft", "minor") == "1.3.0"
        assert bump_version("1.2.3-draft", "major") == "2.0.0"


class TestSuggestVersion:
    def test_suggest_version_breaking_bumps_major(self):
        """Breaking changes bump the major segment."""
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.2.3") == "2.0.0"
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.2") == "2.0"

    def test_suggest_version_additive_bumps_minor(self):
        """Additive changes bump the minor segment."""
        assert suggest_version(_diff(ChangeImpact.ADDITIVE), "1.2.3") == "1.3.0"
        assert suggest_version(_diff(ChangeImpact.ADDITIVE), "1.2") == "1.3"

    def test_suggest_version_cosmetic_bumps_patch(self):
        """Cosmetic changes bump the patch segment."""
        assert suggest_version(_diff(ChangeImpact.COSMETIC), "1.2.3") == "1.2.4"

    def test_suggest_version_cosmetic_two_part_collapses_to_minor(self):
        """Two-part versions map the patch bump onto minor."""
        assert suggest_version(_diff(ChangeImpact.COSMETIC), "1.2") == "1.3"

    def test_suggest_version_unchanged_returns_current(self):
        """An empty diff keeps the current version verbatim."""
        assert suggest_version(_diff(), "1.2.3-draft") == "1.2.3-draft"

    def test_suggest_version_draft_finalize_strategy(self):
        """Under SDMX_3, the finalize strategy drops the draft with no bump."""
        policy = VersionPolicy(mode=VersioningMode.SDMX_3)
        assert (
            suggest_version(_diff(ChangeImpact.BREAKING), "1.0.1-draft", policy)
            == "1.0.1"
        )

    def test_suggest_version_draft_bump_strategy(self):
        """Under SDMX_3, the bump strategy bumps numerics and drops the ext."""
        policy = VersionPolicy(mode=VersioningMode.SDMX_3, draft_strategy="bump")
        assert (
            suggest_version(_diff(ChangeImpact.BREAKING), "1.0.1-draft", policy)
            == "2.0.0"
        )

    def test_suggest_version_replace_non_final_keeps_version(self):
        """Under SDMX_3, replace_non_final republishes non-final in place."""
        policy = VersionPolicy(mode=VersioningMode.SDMX_3, replace_non_final=True)
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.0", policy) == "1.0"
        assert (
            suggest_version(_diff(ChangeImpact.BREAKING), "1.0.0-draft", policy)
            == "1.0.0-draft"
        )

    def test_suggest_version_replace_non_final_still_bumps_final(self):
        """replace_non_final does not affect final versions."""
        policy = VersionPolicy(mode=VersioningMode.SDMX_3, replace_non_final=True)
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.0.0", policy) == "2.0.0"

    def test_suggest_version_semver_only_strips_draft_and_bumps(self):
        """SEMVER_ONLY (default) finalizes a draft, then bumps per impact."""
        assert suggest_version(_diff(ChangeImpact.COSMETIC), "1.0.0-draft") == "1.0.1"
        assert suggest_version(_diff(ChangeImpact.ADDITIVE), "1.0.0-draft") == "1.1.0"
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.0.0-draft") == "2.0.0"

    def test_suggest_version_semver_only_ignores_replace_non_final(self):
        """SEMVER_ONLY bumps even when replace_non_final is set (inert)."""
        policy = VersionPolicy(replace_non_final=True)
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.0", policy) == "2.0"
        assert (
            suggest_version(_diff(ChangeImpact.BREAKING), "1.0.0-draft", policy)
            == "2.0.0"
        )

    def test_suggest_version_semver_only_ignores_finalize(self):
        """SEMVER_ONLY bumps a draft rather than finalizing it in place."""
        assert suggest_version(_diff(ChangeImpact.BREAKING), "1.0.1-draft") == "2.0.0"

    def test_suggest_version_invalid_version_raises(self):
        """An unparseable current version propagates ValueError."""
        with pytest.raises(ValueError, match="Invalid SDMX version"):
            suggest_version(_diff(ChangeImpact.COSMETIC), "not-a-version")

    def test_suggest_version_custom_policy_levels(self):
        """Policy levels are honoured (additive → major here)."""
        policy = VersionPolicy(additive="major")
        assert suggest_version(_diff(ChangeImpact.ADDITIVE), "1.2.3", policy) == "2.0.0"

    def test_default_version_policy_values(self):
        """The default policy is breaking/additive/cosmetic + semver-only."""
        assert (
            VersionPolicy(
                breaking="major",
                additive="minor",
                cosmetic="patch",
                draft_strategy="finalize",
                replace_non_final=False,
                mode=VersioningMode.SEMVER_ONLY,
            )
            == DEFAULT_VERSION_POLICY
        )

    def test_default_version_policy_mode_is_semver_only(self):
        """The default policy defaults to SEMVER_ONLY."""
        assert VersionPolicy().mode == VersioningMode.SEMVER_ONLY


class TestIsFmrPublishable:
    def test_plain_semver_is_publishable(self):
        """Plain three-part semver is publishable."""
        assert is_fmr_publishable("1.0.0")

    def test_two_part_is_publishable(self):
        """Two-part (SDMX 2.1) versions are publishable."""
        assert is_fmr_publishable("1.0")

    def test_zero_major_is_publishable(self):
        """A 0.x.y version has no extension, so it is publishable."""
        assert is_fmr_publishable("0.1.0")

    def test_draft_is_not_publishable(self):
        """A prerelease/draft version is not publishable."""
        assert not is_fmr_publishable("1.0.0-draft")

    def test_wildcard_is_not_publishable(self):
        """An unparseable wildcard token is not publishable."""
        assert not is_fmr_publishable("~")
