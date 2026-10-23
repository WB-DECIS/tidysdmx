"""Print the versions installed in this environment: ``python -m benchmarks.probe``.

``benchmarks.run`` runs it inside each version's environment and records the
output, so the results say what every version ran with. Standard library only.
"""

import importlib.metadata
import json
import platform
import sys

PACKAGES: tuple[str, ...] = ("tidysdmx", "pandas", "numpy", "pysdmx", "typeguard")


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


if __name__ == "__main__":
    sys.stdout.write(json.dumps(installed()) + "\n")
