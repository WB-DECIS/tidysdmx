---
description: Run the test suite with optional flags
argument-hint: "[pytest flags, e.g. -k test_name, -m unit, --cov]"
allowed-tools:
  - Bash
---

Run the project tests using Poetry:

```bash
poetry run pytest $ARGUMENTS
```

If no arguments are given, run all tests excluding integration tests:

```bash
poetry run pytest -m "not integration"
```

After tests complete:
- If any tests FAIL, investigate the root cause and suggest a fix.
- If all tests PASS, report the summary (passed/failed/skipped counts).
