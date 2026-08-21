# Commit Conventions

Commit messages are load-bearing in this repository: `python-semantic-release`
parses them to compute the next version and to generate `CHANGELOG.md`. A
`commit-msg` hook rejects anything that does not follow
[Conventional Commits](https://www.conventionalcommits.org/).

## Format

```
<type>[(optional scope)][!]: <description>

[optional body]

[optional footer(s)]
```

- Description in the imperative mood, lower case, no trailing full stop:
  "add value map builder", not "Added value map builder."
- Scope is the area touched, e.g. `feat(validation):`.
- `!` before the colon marks a breaking change.

## Types and their release effect

| Type | Version bump | Appears in CHANGELOG |
|---|---|---|
| `fix` | patch | yes |
| `perf` | patch | yes |
| `feat` | minor | yes |
| any type with `!`, or a `BREAKING CHANGE:` footer | minor while on 0.x, major after 1.0 | yes |
| `docs` | none | no |
| `test` | none | no |
| `refactor` | none | no |
| `style` | none | no |
| `chore` | none | no |
| `ci` | none | no |
| `build` | none | no |

Only `feat`, `fix` and `perf` reach the changelog — everything else is filtered
by `exclude_commit_patterns` so the changelog stays useful to users rather than
becoming a commit log.

## Choosing the type honestly

The type determines what version users receive, so it is a factual claim, not a
label:

- New capability a user can call → `feat`. Not `chore`.
- Behaviour a user relied on now differs → breaking, so `!`. Renaming or removing
  a public function, changing a default, tightening validation, or changing a
  return type all count.
- Internal restructuring with identical observable behaviour → `refactor`.
- If you are unsure between `fix` and `feat`: did it ever work as documented? If
  yes, `fix`. If the documented behaviour is new, `feat`.

## Breaking changes

```
feat!: rename greet() to salute()

BREAKING CHANGE: greet() is removed. Call salute(), which takes the same
arguments and returns the same type.
```

Always say in the footer what the caller must do differently. "This is breaking"
without a migration path pushes the work onto every user.

## Examples

```
feat(mapping): add value map builder
fix: handle empty input in summarise_lengths
fix(validation): guard against stale cached results
perf(mapping): avoid re-parsing the schema per row
docs: clarify the installation steps
test: cover the duplicate-key path
chore(deps): bump ruff to 0.16.3
ci: pin actions to commit SHAs
```

## Release commits

The release workflow itself commits `chore(release): X.Y.Z [skip ci]`. Never
create one of these by hand, and never hand-edit the version in
`pyproject.toml` — see `RELEASING.md`.
