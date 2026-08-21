---
description: Run ruff lint and format checks
allowed-tools:
  - Bash
  - Read
  - Edit
---

Check lint rules and formatting:

```bash
make lint
```

If there are violations:

1. Summarise them grouped by rule, with counts — not a raw dump.
2. Offer to auto-fix with `make fmt` (`ruff check --fix` + `ruff format`).
3. For anything ruff cannot fix automatically, explain what the rule is
   protecting against and fix it properly. Do not add `# noqa` to silence a real
   problem; if an exemption is genuinely right, add a comment saying why.
