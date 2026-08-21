---
description: Stage changes and write a Conventional Commit
argument-hint: "[optional: what the change is about]"
allowed-tools:
  - Bash
  - Read
---

Create a commit that satisfies this repo's `commit-msg` hook.

1. Review what changed: `git status` and `git diff` (plus `git diff --staged`).
2. Run `make check` if the change touches code, so you are not committing
   something broken.
3. Decide the Conventional Commit type **honestly** — it determines the version
   users receive:
   - new capability a caller can use → `feat`
   - previously-documented behaviour now works → `fix`
   - a caller's working code would now break → add `!` and a
     `BREAKING CHANGE:` footer explaining the migration
   - internal only, no observable change → `refactor` / `chore` / `test` / `docs`
   See `.claude/rules/commit-conventions.md` for the full mapping.
4. Write the message: imperative mood, lower case, no trailing full stop. Add a
   body when the *why* is not obvious from the diff.
5. Stage the relevant files and commit. Let the hooks run — do not use
   `--no-verify`.

If the hook rejects the message, fix the message rather than bypassing the hook.
