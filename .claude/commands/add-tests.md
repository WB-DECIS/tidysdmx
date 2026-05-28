---
description: Generate tests for new or changed functions
argument-hint: "[module name or function name]"
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - Edit
  - Write
---

Generate unit tests for the specified module or function following these rules:

1. **Find the target**: locate the module or function specified by `$ARGUMENTS`.
   If no argument given, find functions changed since master: `git diff master...HEAD --name-only -- 'src/**/*.py'`

2. **Read the source** and existing tests for context.

3. **Write tests** following the project conventions:
   - Place tests in `tests/test_<module>.py`, mirroring the source module name.
   - Use existing fixtures from `tests/fixtures/` — never create inline artefacts when a fixture exists.
   - Mark tests that need network/FMR with `@pytest.mark.integration`.
   - All other tests should be marked `@pytest.mark.unit`.
   - Test happy path, edge cases (empty DataFrames, missing columns), and error conditions.
   - Use descriptive test names: `test_<function>_<scenario>`.
   - Keep tests focused — one assertion per logical concern.

4. **Run the new tests** to verify they pass: `poetry run pytest <test_file> -v`
