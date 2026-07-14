"""Parsing, comparison, and automated bumping of SDMX artefact versions.

SDMX registries use two versioning conventions side by side:

- **Two-part** versions such as ``"1.0"`` (SDMX 2.1 style). Two-part
  versions are never *final* in the SDMX 3.0 sense.
- **Semver** versions such as ``"1.0.0"`` or ``"1.0.0-draft"``
  (SDMX 3.0 semantic versioning, where a hyphen extension marks a
  non-final version).

This module parses both schemes into :class:`SdmxVersion`, provides
total ordering, and computes the next version for an artefact from the
impact of a change (see :mod:`tidysdmx.fmr.diff`) via
:func:`suggest_version`. Version suggestions never migrate an artefact
from one scheme to the other: a two-part version stays two-part.
"""

import re
from dataclasses import dataclass
from enum import StrEnum
from functools import total_ordering
from typing import Literal

from pysdmx.util import is_final as _pysdmx_is_final
from typeguard import typechecked

from .diff import ArtefactDiff, ChangeImpact

_VERSION_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:\.(0|[1-9]\d*)(?:-([0-9A-Za-z][0-9A-Za-z.\-]*))?)?$"
)

BumpLevel = Literal["major", "minor", "patch"]


class VersionScheme(StrEnum):
    """The versioning convention used by a version string.

    Attributes:
        TWO_PART: SDMX 2.1 style ``"1.0"`` versions.
        SEMVER: SDMX 3.0 style ``"1.0.0"`` / ``"1.0.0-draft"`` versions.
    """

    TWO_PART = "two_part"
    SEMVER = "semver"


@total_ordering
@dataclass(frozen=True)
class SdmxVersion:
    """A parsed SDMX artefact version.

    Instances are totally ordered: numeric segments compare first, a
    missing patch segment sorts before patch ``0`` (``1.0 < 1.0.0``),
    and — per semver — a version with an extension sorts before the
    same numerics without one (``1.0.0-draft < 1.0.0``).

    Attributes:
        major: Major segment.
        minor: Minor segment.
        patch: Patch segment; ``None`` for two-part versions.
        extension: Hyphen extension (e.g. ``"draft"``); only valid on
            semver versions.
    """

    major: int
    minor: int
    patch: int | None = None
    extension: str | None = None

    def __post_init__(self) -> None:
        """Validate segment consistency."""
        if self.major < 0 or self.minor < 0:
            raise ValueError("Version segments must be non-negative.")
        if self.patch is None and self.extension is not None:
            raise ValueError(
                "A version extension requires a patch segment: "
                "extensions are only valid on semver (X.Y.Z) versions."
            )
        if self.patch is not None and self.patch < 0:
            raise ValueError("Version segments must be non-negative.")

    @property
    def scheme(self) -> VersionScheme:
        """The versioning scheme of this version."""
        return VersionScheme.TWO_PART if self.patch is None else VersionScheme.SEMVER

    @property
    def is_final(self) -> bool:
        """Whether this version is final per SDMX 3.0 semantics.

        Delegates to :func:`pysdmx.util.is_final`: a version is final
        only if it has three numeric segments and no extension. Note
        that two-part versions are therefore never final.
        """
        return _pysdmx_is_final(str(self))

    def bump(self, level: BumpLevel) -> "SdmxVersion":
        """Return the next version at the given bump level.

        Bumping always drops any extension and zeroes the lower
        segments. On two-part versions, which have no patch slot, a
        ``"patch"`` bump collapses to a minor bump.

        Args:
            level: ``"major"``, ``"minor"``, or ``"patch"``.

        Returns:
            The bumped :class:`SdmxVersion`, in the same scheme.
        """
        if self.scheme == VersionScheme.TWO_PART:
            if level == "major":
                return SdmxVersion(self.major + 1, 0)
            return SdmxVersion(self.major, self.minor + 1)
        if level == "major":
            return SdmxVersion(self.major + 1, 0, 0)
        if level == "minor":
            return SdmxVersion(self.major, self.minor + 1, 0)
        patch = self.patch if self.patch is not None else 0
        return SdmxVersion(self.major, self.minor, patch + 1)

    def _sort_key(self) -> tuple[int, int, int, bool, str]:
        return (
            self.major,
            self.minor,
            self.patch if self.patch is not None else -1,
            self.extension is None,
            self.extension or "",
        )

    def __lt__(self, other: "SdmxVersion") -> bool:
        """Order versions by numeric segments, extensions before final."""
        if not isinstance(other, SdmxVersion):
            return NotImplemented
        return self._sort_key() < other._sort_key()

    def __str__(self) -> str:
        """Render the canonical version string."""
        text = f"{self.major}.{self.minor}"
        if self.patch is not None:
            text += f".{self.patch}"
        if self.extension is not None:
            text += f"-{self.extension}"
        return text


@typechecked
def parse_version(version: str) -> SdmxVersion:
    """Parse an SDMX version string.

    Accepts two-part ``"N.N"``, semver ``"N.N.N"``, and extended semver
    ``"N.N.N-ext"`` forms. Extensions on two-part versions (e.g.
    ``"1.0-draft"``) are invalid per SDMX 3.0.

    Args:
        version: The version string to parse.

    Returns:
        The parsed :class:`SdmxVersion`.

    Raises:
        ValueError: If the string matches none of the accepted forms.

    Examples:
        >>> parse_version("1.0").scheme
        <VersionScheme.TWO_PART: 'two_part'>
        >>> parse_version("1.2.3-draft").extension
        'draft'
    """
    m = _VERSION_RE.match(version)
    if not m:
        raise ValueError(
            f"Invalid SDMX version string: {version!r}. Expected 'N.N' "
            "(two-part), 'N.N.N' (semver), or 'N.N.N-ext'."
        )
    major, minor, patch, extension = m.groups()
    return SdmxVersion(
        major=int(major),
        minor=int(minor),
        patch=int(patch) if patch is not None else None,
        extension=extension,
    )


@typechecked
def compare_versions(a: str, b: str) -> int:
    """Compare two SDMX version strings.

    Args:
        a: The first version string.
        b: The second version string.

    Returns:
        ``-1`` if ``a < b``, ``0`` if equal, ``1`` if ``a > b``.

    Raises:
        ValueError: If either string is not a valid SDMX version.
    """
    va, vb = parse_version(a), parse_version(b)
    if va._sort_key() == vb._sort_key():
        return 0
    return -1 if va < vb else 1


@typechecked
def bump_version(version: str, level: BumpLevel) -> str:
    """Bump an SDMX version string at the given level.

    Args:
        version: The current version string.
        level: ``"major"``, ``"minor"``, or ``"patch"``. On two-part
            versions, ``"patch"`` collapses to a minor bump.

    Returns:
        The bumped version string, in the same scheme.

    Raises:
        ValueError: If ``version`` is not a valid SDMX version.

    Examples:
        >>> bump_version("1.0", "major")
        '2.0'
        >>> bump_version("1.2.3-draft", "minor")
        '1.3.0'
        >>> bump_version("1.0", "patch")
        '1.1'
    """
    return str(parse_version(version).bump(level))


@dataclass(frozen=True)
class VersionPolicy:
    """Policy mapping change impact to version bumps.

    Attributes:
        breaking: Bump level for breaking changes (always ``"major"``).
        additive: Bump level for additive changes.
        cosmetic: Bump level for cosmetic changes. On two-part versions
            a ``"patch"`` bump collapses to minor (no patch slot).
        draft_strategy: How to version an artefact whose current version
            carries an extension (e.g. ``"1.0.1-draft"``):
            ``"finalize"`` drops the extension without a numeric bump
            (the draft *was* the staged next version), while ``"bump"``
            bumps the numerics per impact and drops the extension.
        replace_non_final: If ``True``, artefacts whose current version
            is not final are republished at the *same* version
            (in-place replace) instead of being bumped. Note that
            two-part versions are never final per SDMX 3.0 semantics,
            so this disables bumping entirely for two-part registries.
    """

    breaking: Literal["major"] = "major"
    additive: Literal["major", "minor"] = "minor"
    cosmetic: Literal["major", "minor", "patch"] = "patch"
    draft_strategy: Literal["finalize", "bump"] = "finalize"
    replace_non_final: bool = False


DEFAULT_VERSION_POLICY = VersionPolicy()


@typechecked
def suggest_version(
    diff: ArtefactDiff,
    current_version: str,
    policy: VersionPolicy = DEFAULT_VERSION_POLICY,
) -> str:
    """Suggest the next version for an artefact from its diff.

    The bump level is derived from the most severe change impact in
    ``diff`` via ``policy``. The suggestion never migrates versioning
    schemes: a two-part current version yields a two-part suggestion.

    Args:
        diff: The changes between the registry artefact and the update
            (see :func:`tidysdmx.fmr.diff.compare_artefacts`).
        current_version: The version currently in the registry.
        policy: The bump policy. Defaults to breaking→major,
            additive→minor, cosmetic→patch, finalize drafts.

    Returns:
        The suggested next version string. If the diff is empty, the
        current version is returned unchanged.

    Raises:
        ValueError: If ``current_version`` is not a valid SDMX version.

    Examples:
        >>> from tidysdmx.fmr.diff import (
        ...     ArtefactChange, ArtefactDiff, ChangeImpact, ChangeKind,
        ... )
        >>> breaking = ArtefactDiff(
        ...     short_urn="Codelist=WB:CL(1.0)", artefact_type="Codelist",
        ...     changes=(ArtefactChange(
        ...         kind=ChangeKind.REMOVED, impact=ChangeImpact.BREAKING,
        ...         path="items.A", message="Code 'A' removed."),),
        ... )
        >>> suggest_version(breaking, "1.0")
        '2.0'
        >>> suggest_version(breaking, "1.0.0")
        '2.0.0'
        >>> suggest_version(breaking, "1.0.1-draft")
        '1.0.1'
    """
    current = parse_version(current_version)
    if diff.is_unchanged:
        return current_version
    if policy.replace_non_final and not current.is_final:
        return current_version
    if current.extension is not None and policy.draft_strategy == "finalize":
        return str(SdmxVersion(current.major, current.minor, current.patch))
    impact = diff.impact
    if impact == ChangeImpact.BREAKING:
        level: BumpLevel = policy.breaking
    elif impact == ChangeImpact.ADDITIVE:
        level = policy.additive
    else:
        level = policy.cosmetic
    return str(current.bump(level))
