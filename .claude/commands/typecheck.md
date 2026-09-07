---
description: Run mypy and help fix what it finds
allowed-tools:
  - Bash
  - Read
  - Edit
  - Grep
  - Glob
---

Run the type checker:

```bash
make typecheck
```

mypy is not in `--strict` mode yet: each module is exempted only from the
error codes it still trips, and the burn-down order is in `pyproject.toml`.
This package ships `py.typed`, so its
annotations are part of its public contract and a wrong one is a real bug for
downstream users.

If there are errors:

1. Group them by root cause rather than listing every line. One missing return
   type often produces several errors.
2. Fix by making the types correct, not by weakening them. Prefer a precise type
   over `Any`, and a narrower type over a broader one.
3. Only reach for `# type: ignore[code]` when the problem is genuinely outside
   this codebase (an untyped third-party package). Always scope it to the
   specific error code and add a comment explaining why.
4. If a fix would change runtime behaviour, say so explicitly before making it.
