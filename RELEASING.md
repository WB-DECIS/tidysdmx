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

## Commit messages → version bumps

| Commit type | Bump |
|---|---|
| `fix:`, `perf:` | patch (0.9.0 → 0.9.1) |
| `feat:` | minor (0.9.0 → 0.10.0) |
| `BREAKING CHANGE:` footer or `!` (e.g. `feat!:`) | major (0.x → 1.0.0) |
| `docs:`, `test:`, `chore:`, `ci:`, `build:`, `refactor:`, `style:` | none |

Only `feat`/`fix`/`perf` entries appear in the generated changelog
(see `exclude_commit_patterns` in `pyproject.toml`). Hand-written history below the
`<!-- version list -->` marker is preserved; you may still edit it manually, but never
remove the marker itself and never hand-edit the version in `pyproject.toml`.

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

- **In CI:** *Actions → Release → Run workflow* with `noop: true` — prints what would be
  released without committing, tagging, or publishing.
- **Locally:**

  ```bash
  git fetch origin master --tags
  poetry run semantic-release -v --noop version
  ```

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
