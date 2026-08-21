---
description: Walk the release runbook, dry run first
allowed-tools:
  - Bash
  - Read
---

Help cut a release. Read `RELEASING.md` first and follow it — this command is a
guide through it, not a replacement for it.

1. **Confirm the branch is releasable.** On `dev`, up to date, clean tree, and
   `make check` green.
2. **Dry run, always:**
   ```bash
   make release-dry
   ```
   Report the version it says it would produce.
3. **Sanity-check that version against the commits** since the last tag:
   ```bash
   git log $(git describe --tags --abbrev=0)..HEAD --oneline
   ```
   If the computed bump looks wrong, the cause is almost always a mislabelled
   commit — a feature committed as `chore:` produces no release at all. Say which
   commit is at fault rather than working around it.
4. **Explain what will happen** when this merges to `main`: version stamped in
   `pyproject.toml`, `CHANGELOG.md` updated, tagged, GitHub Release created,
   distributions built from the tag and attached.
5. **Stop there.** Releasing is a `dev` → `main` pull request merged by a human,
   with a merge commit and never a squash (see RELEASING.md). Do not
   push tags, do not edit the version by hand, and do not run
   `semantic-release version` without `--noop`.

If this is the project's first release, check the one-time setup boxes in
`TEMPLATE-NEXT-STEPS.md` are done — otherwise the release tags successfully and
then fails at the publish step.
