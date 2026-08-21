<!--
Title this PR as a Conventional Commit, e.g. "feat(mapping): add value map builder".
If you squash-merge, the PR title becomes the commit message that drives the
version bump — so `feat:` vs `fix:` vs `chore:` matters here.
-->

## What and why

<!-- What changes, and what problem it solves. Link the issue: Closes #123 -->

## Type of change

- [ ] `fix:` — bug fix (patch release)
- [ ] `feat:` — new feature (minor release)
- [ ] Breaking change (`!` or a `BREAKING CHANGE:` footer)
- [ ] `docs:` / `test:` / `chore:` / `refactor:` / `ci:` / `build:` — no release

## Checklist

- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] `make check` passes locally (lint, mypy strict, tests + coverage gate)
- [ ] New or changed public functions have tests, including failure paths
- [ ] New or changed public functions have Google-style docstrings
- [ ] Docs updated if behaviour or the public API changed
- [ ] Breaking changes are marked as such and explained above

## Notes for reviewers

<!-- Anything worth flagging: tradeoffs taken, areas you want scrutinised, or
     follow-up work deliberately left out of scope. -->
