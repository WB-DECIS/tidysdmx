"""
DEBUGGING SANITY CHECKS
----------------------
Remove or comment out once debugging is stable.
"""

import sys
import os
import tidysdmx

print("\n=== DEBUG ENVIRONMENT ===")
print("Python executable:")
print(" ", sys.executable)

print("\nFirst entries on sys.path:")
for p in sys.path[:5]:
    print(" ", p)

print("\nPackage resolution:")
print(" tidysdmx module file:")
print(" ", tidysdmx.__file__)

print(" tidysdmx is package:", hasattr(tidysdmx, "__path__"))

if hasattr(tidysdmx, "__path__"):
    print("\nFiles in tidysdmx package directory:")
    for pkg_path in tidysdmx.__path__:
        for name in os.listdir(pkg_path):
            print(" ", name)

print("\n=========================\n")
