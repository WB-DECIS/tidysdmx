---
description: Run ruff linting and formatting checks
allowed-tools:
  - Bash
---

Run linting and format checks:

```bash
poetry run ruff check . && poetry run ruff format --check .
```

If there are violations:
1. Show a summary of the issues found.
2. Ask whether to auto-fix with `ruff check --fix . && ruff format .`
