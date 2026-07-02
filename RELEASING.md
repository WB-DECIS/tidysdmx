# Releasing tidysdmx

Releases are automated with [python-semantic-release](https://python-semantic-release.readthedocs.io/) (PSR)
and published to [PyPI](https://pypi.org/project/tidysdmx/) via GitHub Actions
(`.github/workflows/release.yml`).

## How it works

1. Work is merged into `dev` through PRs, using Conventional Commit messages.
2. When it is time to release, open a PR from `dev` to `master` and merge it —
   **the merge is the release**.
3. The push to `master` triggers the `Release` workflow, which:
   - computes the next version from the Conventional Commits since the last `v*` tag;
   - stamps `tool.poetry.version` in `pyproject.toml`;
   - inserts a generated section into `CHANGELOG.md` under the `<!-- version list -->` marker;
   - commits (`chore(release): X.Y.Z [skip ci]`), tags `vX.Y.Z`, and creates a GitHub Release;
   - builds the sdist + wheel with Poetry;
   - publishes to PyPI via [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC — no token).
4. After the release, **back-merge `master` into `dev` immediately, before merging
   any new work into `dev`**, so the release commit (version bump + changelog) reaches
   `dev`. Skipping it reintroduces version drift; delaying it can cause commits merged
   in the meantime to be attributed to the wrong release in the generated changelog.

If nothing since the last tag warrants a release (only `docs:`/`chore:`/`test:`… commits),
the workflow succeeds as a no-op and nothing is published.

The release commit itself triggers no CI: pushes made with `GITHUB_TOKEN` never start
workflows, and the `[skip ci]` marker is additionally
[honored natively by GitHub Actions](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/skip-workflow-runs)
for push/PR-triggered workflows (relevant if the PAT route is used).

## Commit messages → version bumps

| Commit type | Bump |
|---|---|
| `fix:`, `perf:` | patch (0.9.0 → 0.9.1) |
| `feat:` | minor (0.9.0 → 0.10.0) |
| `BREAKING CHANGE:` footer or `!` (e.g. `feat!:`) | minor while on 0.x (`major_on_zero = false`) |
| `docs:`, `test:`, `chore:`, `ci:`, `build:`, `refactor:`, `style:` | none |

Only `feat`/`fix`/`perf` entries appear in the generated changelog
(see `exclude_commit_patterns` in `pyproject.toml`). Hand-written history below the
`<!-- version list -->` marker is preserved; you may still edit it manually, but never
remove the marker itself and never hand-edit the version in `pyproject.toml`.

**Releasing 1.0.0:** the API is declared unstable, so breaking changes deliberately do
not leave 0.x. When the API is ready to be stabilised, set `major_on_zero = true` in
`pyproject.toml` — the next release containing a breaking change (or any release, if
you also remove `allow_zero_version`) becomes 1.0.0.

## One-time setup (record once done)

- [ ] **PyPI trusted publisher** — requires the *Owner* role on the PyPI project
      (maintainers cannot manage publishers). On pypi.org → *Your projects* →
      *tidysdmx* → *Manage* → *Publishing* → add a GitHub Actions publisher:
      owner `WB-DECIS`, repository `tidysdmx`, workflow `release.yml`, environment `pypi`.
- [ ] **GitHub environment** — repo *Settings → Environments* → create `pypi`.
      Optionally add required reviewers to gate PyPI uploads behind a manual approval.
- [ ] **Master push access for the workflow** — PSR pushes the release commit + tag to
      `master`, which is protected. Pick one:
      1. *Preferred:* put `master` under a ruleset and add the **GitHub Actions** app to
         its Bypass list (*Always allow*). `GITHUB_TOKEN` then pushes cleanly, and release
         commits trigger no recursive workflow runs.
      2. *Fallback:* a repo admin creates a fine-grained PAT (this repo only,
         *Contents: Read and write*), stores it as the `GH_RELEASE_TOKEN` secret, and
         uncomments the two token lines in `release.yml`.
      Trying plain `GITHUB_TOKEN` first is safe: a rejected push fails the run **before**
      anything reaches PyPI.

## Cutting a release

1. Ensure CI is green on `dev`.
2. (Optional) dry run — see below.
3. Open a PR `dev` → `master`; the diff is the release content. Merge it.
4. Watch *Actions → Release*. Verify afterwards:
   - PyPI shows the new version (with project links and classifiers);
   - `pip install tidysdmx==<version>` works;
   - the `vX.Y.Z` tag, GitHub Release (with dists attached), and CHANGELOG entry exist.
5. Back-merge `master` → `dev`.

## Dry runs

- **In CI** (second release onwards): *Actions → Release → Run workflow* with
  `noop: true` — prints what would be released without committing, tagging, or
  publishing. GitHub only lists a `workflow_dispatch` workflow in the Actions tab
  once its file exists on the default branch, so the *Release* entry (and its
  *Run workflow* button) only appears after the first release PR has been merged
  to `master`.
- **Locally** (works any time, including before the first release):
  semantic-release only computes releases on a branch named `master`/`main`, so
  running it on `dev` fails with "branch not in any release groups". Instead,
  simulate the post-merge state on a temporary **local** `master` — nothing is
  pushed at any point:

  ```bash
  git fetch origin
  git checkout -B master origin/dev     # local master at dev's tip = what master will contain after the release PR
  poetry run semantic-release -v --noop version
  git checkout dev
  git branch -f master origin/master    # restore your local master
  ```

  Expected output: `The next version is: X.Y.Z!` followed by `[NOP]` lines showing
  the commit, tag, and pushes it *would* perform.

## Troubleshooting & manual fallback

- **Push rejected (protected branch):** complete the "master push access" setup above,
  then re-run via *Actions → Release → Run workflow*. Nothing was tagged or published.
- **"No release will be made":** no `feat`/`fix`/`perf` commits since the last tag.
  Expected for doc/chore-only merges.
- **PyPI upload failed after the tag was created:** re-run the `publish-pypi` job
  (the built artifacts are stored on the run). As a last resort, publish manually from
  the tag with a project-scoped PyPI API token:

  ```bash
  git checkout vX.Y.Z
  poetry build
  poetry publish -u __token__ -p <pypi-token>
  ```

  PyPI versions are immutable — never delete and re-upload a version.
