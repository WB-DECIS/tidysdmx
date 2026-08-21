---
description: Generate tests for new or changed functions
argument-hint: "[module or function name]"
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - Edit
  - Write
---

Write tests for `$ARGUMENTS`, or if no argument was given, for whatever changed
on this branch relative to its base:

```bash
base=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo dev)
git diff "$base...HEAD" --name-only -- 'src/**/*.py'
```

1. **Read the source** and any existing tests for that module, so you match the
   established style rather than inventing a second one.
2. **Check the conventions** in `.claude/rules/testing-conventions.md`. In short:
   - place tests in `tests/test_<module>.py`, mirroring the source module
   - group per function in a `class Test<FunctionName>:` block
   - name them `test_<function>_<scenario>`
   - one logical assertion per test
   - reuse fixtures from `tests/fixtures/`; add new shared data there rather than
     inlining it
   - mark anything needing network `@pytest.mark.integration`
3. **Cover all three**: the happy path, edge cases (empty, `None`, boundaries,
   duplicates), and the error paths with
   `pytest.raises(SomeError, match="...")` — always with `match=`, or the test
   passes even when the error is raised for the wrong reason.
4. **Run them**: `uv run pytest tests/test_<module>.py -v`, then `make cov` to
   confirm the gate still passes.

A test that cannot fail is worse than no test. If you cannot construct a case
that fails against broken code, say so instead of writing a placeholder.
