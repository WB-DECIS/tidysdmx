"""Shared helpers for FMR cassette fixtures.

Centralises the FMR test endpoint and the load/guard/fallback logic that was
previously duplicated across ``fxtr_schemas``, ``fxtr_mapping`` and
``test_pipeline_integration``.
"""

import os
import pickle as pkl
from pathlib import Path

# The FMR instance used to (re)generate cassettes and to run live integration
# tests. Overridable so a fork can point at its own registry without editing
# source.
FMR_TEST_URL = os.getenv(
    "TIDYSDMX_TEST_FMR_URL", "https://fmrqa.worldbank.org/FMR/sdmx/v2"
)


def load_pickle_cassette(
    path: str | Path, expected_type: type, regen_cmd: str
) -> object | None:
    """Load a committed pickle cassette, or signal that it is absent.

    Args:
        path: Path to the ``.pkl`` cassette.
        expected_type: The pysdmx type the cassette must unpickle to. A
            mismatch (e.g. after a pysdmx upgrade changed the class layout)
            raises, turning silent staleness into a loud failure.
        regen_cmd: Human-readable command for regenerating the cassette,
            surfaced in error messages.

    Returns:
        The unpickled object when the cassette exists; ``None`` when it is
        missing and the run is *not* under CI (so the caller may live-fetch or
        skip).

    Raises:
        RuntimeError: If the cassette is missing while running under CI (env
            var ``CI`` set) — refusing to silently live-fetch a missing or
            renamed cassette.
        TypeError: If the cassette unpickles to an unexpected type.
    """
    path = Path(path)
    if path.exists():
        with open(path, "rb") as f:
            obj = pkl.load(f)
        if not isinstance(obj, expected_type):
            raise TypeError(
                f"Cassette {path.name} unpickled to {type(obj).__name__}, "
                f"expected {expected_type.__name__}. Regenerate it: {regen_cmd}"
            )
        return obj
    if os.environ.get("CI"):
        raise RuntimeError(
            f"Cassette {path.name} missing under CI; refusing to live-fetch FMR. "
            f"Regenerate it locally and commit it: {regen_cmd}"
        )
    return None
