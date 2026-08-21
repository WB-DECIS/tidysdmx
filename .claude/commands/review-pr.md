---
description: Review the current branch's changes as a PR review
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
---

Review the changes on this branch against its base branch.

1. **Find the base**: `gh pr view --json baseRefName -q .baseRefName` if a PR
   exists; otherwise assume `dev`, the integration branch feature branches
   target here (a release PR's base is `main`).
2. **Get the diff**: `git diff <base>...HEAD` (use `--stat` first to see the
   shape).
3. **Read the changed files in full**, not just the diff hunks — a diff hides the
   context that makes a change wrong.
4. **Assess each change** against:
   - **Correctness** — logic errors, unhandled edge cases, boundary and
     off-by-one conditions, error paths that cannot actually be reached.
   - **Type safety** — complete and accurate annotations; would mypy accept this
     without an ignore, and without widening a per-module exemption? Flag any new `Any`.
   - **Tests** — does every new or changed public function have tests, including
     its failure paths? Are new tests actually asserting something meaningful?
   - **Public API** — anything newly exported that should stay private? Is
     `__all__` still accurate? Any breaking signature change not marked `!`?
   - **Docstrings** — Google style, with `Args`/`Returns`/`Raises` that match the
     real signature and behaviour.
   - **Reuse** — duplicated logic, or something the stdlib or an existing helper
     in this repo already does.
   - **Commit messages** — do the types match what actually changed? A `feat`
     labelled `chore` silently withholds a release.
5. **Report** grouped by file, each finding marked **critical** (must fix) or
   **suggestion**. Include the concrete failure case for anything you call
   critical — if you cannot describe how it breaks, say so and downgrade it.

Do not comment on formatting or lint rules; ruff enforces those separately.
