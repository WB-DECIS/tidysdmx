---
description: Review the current branch's changes as a PR review
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
---

Review the changes on the current branch compared to master. Follow this checklist:

1. **Get the diff**: `git diff master...HEAD -- '*.py'`
2. **Read changed files** in full to understand context.
3. **Check each changed function** against these criteria:
   - **SDMX correctness**: artefacts conform to the SDMX information model
   - **pysdmx wrapping**: upstream is called where possible, not reimplemented
   - **Type safety**: `@typechecked` decorator and full type annotations present
   - **Google docstrings**: present, accurate, and complete (Args, Returns, Raises)
   - **Test coverage**: new/changed public functions have corresponding tests
   - **Code quality**: no bugs, edge cases handled, no unnecessary complexity
4. **Report findings** grouped by file, with severity (critical / suggestion).
