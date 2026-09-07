# Contributing to tidysdmx

Thanks for helping out. This document covers the workflow; `RELEASING.md` covers
how versions get cut, and `CLAUDE.md` plus `docs/` carry the SDMX domain
background you will want before touching the mapping or validation code.

## Getting set up

You need [uv](https://docs.astral.sh/uv/getting-started/installation/) and
Python 3.11.9 or newer. uv creates and manages the project's virtual
environment itself — there is no conda or `venv` step.

```bash
git clone https://github.com/WB-DECIS/tidysdmx.git
cd tidysdmx
make install     # sync all dependency groups + install the git hooks
make check       # confirm a clean baseline before you change anything
```

`make install` installs three hook types: `pre-commit` and `pre-push` both run
the file fixers, ruff and mypy (`pre-push` adds the unit tests), and `commit-msg`
validates the message as a Conventional Commit. If you skip it, CI will catch
the same problems later and more slowly. *The git hooks* below has the details.

### `uv` has to be on your PATH

There is no path override anywhere in this project, by design: pre-commit runs a
hook entry without a shell, so a git hook has nothing to substitute a path into.
`git commit` and `git push` need `uv` on `PATH`. Putting it there needs no admin
rights, and two details trip people up:

- **`PATH` entries are directories.** Add the *folder that contains* `uv.exe`
  (`C:\WBG`), not `uv.exe` itself. With a file path there, `uv` does not resolve
  at all and `Get-Command uv` returns nothing. Edit it under
  *Settings → Edit environment variables for your account*; `setx` writes a
  `REG_SZ`, which silently breaks any existing entry that needs expansion.
- **Relaunch your editor completely.** A process inherits its environment when it
  starts and never re-reads it, and your editor spawns `git`, which spawns the
  hook — so all three keep the old `PATH`. Reloading the window is not enough.
  Confirm with `uv --version` in a fresh terminal.

## Everyday commands

Run `make help` for the full list. The ones you will use:

| Command | What it does |
|---|---|
| `make install` | sync every dependency group and install the git hooks |
| `make lint` | ruff lint + format check, no changes |
| `make fmt` | auto-fix lint violations and format |
| `make typecheck` | mypy (not strict yet — see the burn-down in `pyproject.toml`) |
| `make test` | unit tests, no coverage gate (so `-k` behaves) |
| `make cov` | unit tests with coverage, enforcing the gate |
| `make build` | build the sdist and wheel, check their metadata, import the wheel |
| `make docs` / `make docs-preview` | build / live-preview the documentation site |
| `make audit` | check locked dependencies for known vulnerabilities |
| `make release-dry` | show the version the next release would produce |
| `make check` | lint + typecheck + cov — CI runs these plus `make build` |

Without `make` (Windows), call the underlying commands directly — they are all
one line, e.g. `uv run python -m pytest -m "not integration"`,
`uv run python -m ruff check .`, `uv run python -m mypy`. Read the `Makefile`
for the exact recipe.

To run a single test:

```bash
uv run python -m pytest -k test_validate_dataset_local -v
```

Tests marked `integration` need an FMR and are excluded from the default lane;
run them with `uv run python -m pytest -m integration` when you have access.

## Adding a dependency

Always go through `uv`, so `uv.lock` moves with `pyproject.toml`:

```bash
uv add httpx                     # a runtime dependency
uv add --group dev respx         # a development-only one
uv add --group notebooks seaborn # something only the notebooks need
```

Commit `uv.lock` in the same commit as the `pyproject.toml` change. CI installs
with `uv sync --locked`, and a lockfile that lags the manifest fails every job
with *"The lockfile at `uv.lock` needs to be updated"*.

## Keeping up with the template

This project follows
[WB-DECIS/pypackage-template](https://github.com/WB-DECIS/pypackage-template),
and `.copier-answers.yml` records the answers and the template version it was
last reconciled with. To pull in later template improvements:

```bash
uvx copier update --trust
```

On a machine whose security policy refuses `uvx` (it runs a pip-generated
launcher), this form is identical:

```bash
uv run --with copier python -m copier update --trust
```

Copier re-applies the template at its newest tag and three-way merges the
result. Review `git diff`, resolve any `.rej` files it left, then re-sync — an
update can change dependency groups and hooks, and neither happens on its own:

```bash
uv sync --all-groups
uv run python -m pre_commit install --install-hooks
```

This repository diverges from the rendered template in most files (its own
modules, docs and notebooks, and the deliberate deviations listed in
`CLAUDE.md` under *Branching and Releases* and *Staying in Sync With the
Template*), so expect noise: an update is usually applied by hand, hunk by hunk,
and the deviations re-asserted afterwards. Commit the result as
`chore: update from pypackage-template` and bump `_commit` in
`.copier-answers.yml` only once every applicable hunk is in.

## The git hooks

`pre-commit` runs the file fixers, ruff, the notebook output stripper and mypy
on every commit; `pre-push` runs the same plus the unit tests; `commit-msg`
validates the message. To run every hook by hand — after a rejected commit, say,
or to see what a fixer changed:

```bash
uv run python -m pre_commit run --all-files
```

### Why every hook looks unusual

`.pre-commit-config.yaml` has no remote `repo:` entries: every hook is local and
every entry runs `uv run --no-sync python -m ...`. Please do not "simplify" it
back.

pip generates an `.exe` launcher for each hook entry point with the local
interpreter path baked in, so its hash is unique to one machine. Managed Windows
fleets running Microsoft Defender Attack Surface Reduction block exactly that
kind of executable — and the block denies *read*, so pre-commit dies with a
`PermissionError` traceback while resolving the hook rather than reporting a
failing hook. `python -m` launches only the interpreter, which is signed.

It costs nothing elsewhere: same packages, same versions, same entry points, and
running them out of the project environment means a hook and `make lint` can
never disagree about a tool's version. The one hook that says `--group notebooks`
instead of `--no-sync` does so because `notebooks` is not a default dependency
group, so it must be allowed to install itself the first time a notebook is
committed.

## Commit messages

Commit messages are **not** cosmetic here: `python-semantic-release` parses them
to compute the next version and to write `CHANGELOG.md`. The `commit-msg` hook
rejects anything that does not follow
[Conventional Commits](https://www.conventionalcommits.org/).

```
feat(mapping): add value map builder
fix(validation): guard against stale cached results
docs: clarify the installation steps
feat!: rename fetch_dsd_schema() to fetch_schema()
```

| Prefix | Release effect |
|---|---|
| `fix:` / `perf:` | patch |
| `feat:` | minor |
| `!` suffix or `BREAKING CHANGE:` footer | minor while on 0.x, major after 1.0 |
| `docs:` `test:` `chore:` `ci:` `build:` `refactor:` `style:` | none |

Only `feat`, `fix` and `perf` appear in the changelog — the rest are filtered out
so it stays useful to users. `.claude/rules/commit-conventions.md` has the
longer version.

## Code conventions

- **Type everything.** The package ships `py.typed`, so its annotations are part
  of its public contract. mypy runs over `src/` and `tests/` but is not yet in
  `--strict` mode: each module is exempted only from the error codes it still
  trips, and `pyproject.toml` records the burn-down order. Write as though it
  were strict. Static checking verifies the annotations are *consistent* — it
  says nothing about the values that arrive at runtime.
- **Runtime type checking.** Public functions carry typeguard's `@typechecked`,
  and `TypeCheckError` is part of the current public contract. Keep using it on
  new public functions; it goes in its own release once mypy is strict. See
  `.claude/rules/python-conventions.md`.
- **Google-style docstrings** on every public function, with accurate `Args:`,
  `Returns:` and `Raises:` sections. Ruff's `D` rules enforce the style; only you
  can enforce the accuracy.
- **Raise specific exceptions** (`ValueError`, `TypeError`), never bare
  `Exception`, and validate inputs at public API boundaries. Where an input is
  genuinely untrusted — a parsed Excel workbook, a JSON mapping file, an FMR
  response — annotate it `object` and narrow with `isinstance`, raising
  `TypeError`.
- **Return new objects** rather than mutating arguments in place.
- Private helpers are prefixed with `_` and are not exported in `__all__`. A new
  public name goes into `__all__` **and** `great-docs.yml`'s
  `reference.sections`, or the docs build fails.

## Tests

- Test files mirror source modules: `src/tidysdmx/foo.py` →
  `tests/test_foo.py`.
- Name tests `test_<function>_<scenario>`, and keep one logical assertion each.
- Cover the happy path, edge cases (empty input, `None`, boundaries) **and** the
  errors — use `pytest.raises(..., match=...)` so you assert the message too.
- Mark anything needing an FMR or the network `@pytest.mark.integration`; CI's
  default lane runs `-m "not integration"`. Cassettes under
  `tests/fixtures/cassettes/` are pickled pysdmx objects captured from FMR, and
  the tests that load them are integration tests too.
- Warnings are errors in the suite. If a test exercises a deprecated function on
  purpose, scope the suppression to that test or class with
  `pytest.mark.filterwarnings`.
- Every new public function needs at least one test. The coverage gate is a
  floor, not a target.

## Pull requests

1. Branch from `dev`.
   PRs target `dev`; `main` only ever receives release merges from `dev` — see
   `RELEASING.md`.
2. Make the change, with tests and docs updated alongside it.
3. Run `make check` — CI runs exactly this, plus a build and a multi-version test
   matrix.
4. Open the PR and fill in the template. Title it as a Conventional Commit: if
   the repo squash-merges, the PR title becomes the commit that drives the
   release. CI validates the title with the same checker as the commit-msg
   hook, so a malformed title fails the `All checks` gate instead of silently
   producing no release later.

Every PR runs lint, typing, tests on every supported Python version, a
packaging check, a dependency audit and a workflow security lint. A green board
is the bar for merging.

## Code of Conduct

This project is released with a
[Code of Conduct](https://github.com/WB-DECIS/tidysdmx/blob/main/CODE_OF_CONDUCT.md).
By contributing you agree to abide by its terms. (An absolute link on purpose:
great-docs renders this file into the docs site, where a relative link would
point nowhere.)
