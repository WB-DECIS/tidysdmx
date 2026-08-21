---
description: Run the test suite with optional flags
argument-hint: "[pytest flags, e.g. -k test_name, -m unit, --cov]"
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
---

Run the project tests:

```bash
uv run pytest $ARGUMENTS
```

If no arguments were given, run the default lane (excludes anything needing
network):

```bash
make test
```

Then:

- If tests **fail**, read the failing test and the code under test, diagnose the
  actual cause, and propose a fix. Do not adjust the test to match broken
  behaviour unless the test itself is what is wrong — say which it is.
- If tests **pass**, report the counts (passed / failed / skipped) and flag any
  skipped tests, since a skip is usually hidden debt.
