"""Print the versions installed in this environment: ``python -m benchmarks.probe``.

``benchmarks.run`` runs it inside each version's environment and records the
output, so the results say what every version ran with. Standard library only.
"""

import importlib.metadata
import json
import platform
import sys
from pathlib import Path

# pyarrow and psutil are absent from the mapping suite's environments (its
# tracemalloc figures depend on strings not living in Arrow buffers) and present
# in the pipeline suite's; recording them shows which was the case.
PACKAGES: tuple[str, ...] = (
    "tidysdmx",
    "pandas",
    "numpy",
    "pysdmx",
    "typeguard",
    "pyarrow",
    "psutil",
)


def installed() -> dict[str, str | None]:
    """Return the Python version and each package's installed version.

    Returns:
        ``{"python": ..., <package>: <version>}``, with ``None`` for a
        package that is not installed.
    """
    versions: dict[str, str | None] = {"python": platform.python_version()}
    for package in PACKAGES:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def assert_installed() -> None:
    """Refuse to measure a tidysdmx that is not installed in this environment.

    The case runners call this first: with the harness root as the working
    directory, ``benchmarks`` imports from the checkout, and tidysdmx must
    come from the environment of the version under test, not from a source
    tree that happens to be on ``sys.path``.

    Raises:
        ImportError: If tidysdmx was imported from outside ``sys.prefix``: the
            benchmark would measure the wrong code.
    """
    import tidysdmx

    location = Path(tidysdmx.__file__).resolve()
    prefix = Path(sys.prefix).resolve()
    if not location.is_relative_to(prefix):
        raise ImportError(
            f"tidysdmx was imported from {location}, outside this environment "
            f"({prefix}); the benchmark would measure the wrong code"
        )


if __name__ == "__main__":
    sys.stdout.write(json.dumps(installed()) + "\n")
