# Performance profile for `map_structures`: design

Status: approved 2026-10-23. Implementation plan:
`docs/superpowers/plans/2026-10-23-performance-profile.md`.

Amended after the whole-branch review: output fingerprints keep the comparison
like for like, the PR benchmark triggers on `pyproject.toml` rather than
`uv.lock`, and a few descriptions now match what was built.

## Why

`map_structures` performance now matters to the data pipelines that use
tidysdmx. The `perf/map-structures-large-data` work rewrote it to map distinct
values instead of rows. Nothing in the repository measures performance, though,
and nothing records it. The 2026-06 architecture review lists "performance
benchmarking" only as deferred work (§8).

This design adds:

- **A published profile per release.** It gives `map_structures` time and peak
  memory across dataset shapes and sizes, and compares them with the previous 3
  releases. Readers use it to size jobs ("how long, and how much RAM, for N
  rows?") and to see regressions.
- **A development view.** Every relevant pull request gets a comment comparing
  its head with its base branch and the latest release.

Nothing gates on the numbers. A person reads them and decides.

## Decisions

| Question | Decision |
|---|---|
| Purpose | Reader-facing first, with a cross-release comparison. No CI gating. |
| Scope | `map_structures` only. Adding another function means adding one scenario module. |
| Sizes | 1k, 10k, 100k and 1M rows. |
| Shapes | Typical indicator table (the headline), high cardinality, regex-heavy, wide. |
| Comparison | Each release re-measures the new version and the previous 3 releases on one runner. Results from different runs are never compared, and neither are results whose mapped output differs. |
| Dependencies | "As released": each version installs its own declared dependencies, resolved at benchmark time. |
| Store | A JSON file attached to the GitHub Release. |
| Release view | A Performance section on the great-docs site, which stays release-only. |
| Development view | A sticky PR comment. |

Some alternatives were considered and rejected:

- **asv.** It publishes its own site outside great-docs. Its `peakmem_` measures
  the whole process, including the 1M-row input built in setup, which hides what
  the mapping itself costs. It also needs a results branch, and its Windows
  support is fragile.
- **Accumulating one measurement per release.** GitHub-hosted runners vary by
  ±10–20% between runs, so comparing numbers measured months apart would report
  hardware changes as regressions.
- **Publishing a dev copy of the docs site.** Pages hosts one site per
  repository, so a dev copy would need a second host and a second deploy.

## Constraints from the codebase

- **One script fits every release, but not one meaning.** `map_structures(df,
  structure_map, verbose=False)` has had the same public signature since
  v0.8.0, but its semantics have not stayed the same. v0.8.0's
  `apply_component_map` is a literal dictionary lookup, so a `regex:` rule in a
  `ComponentMap`, including the `regex:.*` catch-all, never matches and leaves
  the value unmapped. The output has the same shape either way, so a version
  that skips that work looks several times faster. The output fingerprint
  (see the measurement protocol) is the guard: a version whose output differs
  from the target's on a case is marked and left out of the comparison.
- **One environment per version.** v0.8.0–v0.10.0 require `pandas<3`, and the
  perf branch requires `pandas>=3` and refuses to import below it.
- **No results on stdout.** v0.8.0 logs with `print()`, so results go to a file.
- **Targets never reuse source names.** Up to v0.10.0 each map reads the
  previous map's output; later versions read every source from the input. With
  distinct names, both behaviours produce the same columns.
- **A built-in noise check.** v0.9.0 and v0.10.0 have identical `mapping.py`, so
  any difference between them is noise.
- **`tracemalloc` covers the mapping's memory.** pyarrow is not installed, so
  numpy buffers, pandas hashtables and Python strings are all traced.
- **The Release is the only durable store.** Each Pages deploy replaces the whole
  site, and nothing persists between docs builds.
- **The page is generated whole.** A great-docs section turns every `.md`/`.qmd`
  under its directory into a page, and copies only asset *directories*
  (`great_docs/core.py`, `_process_sections`). So `performance/index.qmd` is
  generated in full, with its charts inlined, rather than including a separate
  generated file.

## Components

A top-level `benchmarks/` package. It is neither installed nor shipped, and it
is excluded from the sdist.

| File | Responsibility | Runs in |
|---|---|---|
| `benchmarks/records.py` | The results schema (`Record`, `VersionInfo`, `Results` TypedDicts; `Record.output_fingerprint` is optional, so files without it stay valid), `SCENARIOS`, `release_key()` and `empty_record()`. Stdlib only, so every other module can share it. | everywhere |
| `benchmarks/scenarios.py` | `build(scenario, rows, seed) -> (DataFrame, StructureMap)` and `targets(structure_map)`, the target columns the output fingerprint covers. Builds deterministic synthetic inputs with pysdmx constructors only, never tidysdmx builders (`build_multi_component_map` does not exist at v0.8.0). Works under pandas 2 and 3. | each version's venv, and the dev env for tests |
| `benchmarks/case.py` | `python -m benchmarks.case`: measures one (scenario, rows) case, fingerprints its output, and writes one JSON record to `--result`. Checks that `tidysdmx` was imported from site-packages, not a source tree. | each version's venv |
| `benchmarks/probe.py` | `python -m benchmarks.probe`: prints the Python and dependency versions installed in an env as JSON. | each version's venv |
| `benchmarks/run.py` | Orchestrator, stdlib only. Selects versions, builds the environments, runs the grid and writes the results JSON. | any Python with `git` and `uv` on PATH |
| `benchmarks/report.py` | Renders a results JSON as `page` (the docs page, or a placeholder when there are no results), `summary` (the job summary) or `pr-comment`. Imports matplotlib only to draw the page's charts. matplotlib is in the `dev` group: CI's test job renders the charts, and docs builds sync `dev` by default. | the dev env, or `--no-project` for the text formats |
| `benchmarks/page.qmd` | Page template: methodology and how to read the page, around a `<!-- RESULTS -->` marker. | n/a |

### Scenarios

Every scenario runs at every size with a fixed seed. Source columns use raw
names (`country`, `series`, …) and targets use SDMX ids (`REF_AREA`,
`INDICATOR`, …).

- **typical** (headline):
  - Six low-cardinality string dimensions: about 200 countries, 300 series, 3
    sexes, 10 ages, 5 units and 30 years.
  - Three `ComponentMap`s, one `ImplicitComponentMap` (year), two
    `FixedValueMap`s (`FREQ`, `UNIT_MULT`), one two-to-one `MultiComponentMap`
    with about 40 rules, and an implicit float `OBS_VALUE`.
- **high_cardinality**:
  - One source whose distinct values are about 10% of the row count.
  - A literal value map of about 10k entries, plus a `regex:.*` catch-all.
- **regex_heavy**:
  - Two sources of about 500 distinct values each.
  - About 20 regex rules plus a catch-all; half the values miss the literal
    lookup.
- **wide**:
  - 20 source columns of about 50 codes each.
  - 20 `ComponentMap`s of 50 entries.

### Measurement protocol (`case.py`)

1. **Build the inputs.** Record `input_bytes = df.memory_usage(deep=True).sum()`.
2. **Warm up.** Call once on `df.head(1000)`, to absorb typeguard
   instrumentation, imports and regex compilation.
3. **Time** with `time.perf_counter`:
   - Repeats: 7 at up to 10k rows, 5 at 100k, 3 at 1M.
   - Stop after one call that takes more than 30 s.
   - Record every sample, the median and the minimum.
4. **Fingerprint the output.**
   - After the first timed call, with the clock stopped, hash the columns
     `targets(structure_map)` of its result. Each column contributes its name, its
     null count and its non-null values as `str` with their counts, sorted by
     value. The first 16 hex digits of the SHA-256 digest are
     `output_fingerprint`.
   - The hash ignores row order, dtype (the `object`, `string` and `str` of
     pandas 2 and 3) and how a null is spelled (`None`, `NaN` or `pd.NA`), so
     versions that map a case the same way agree.
   - A version whose fingerprint differs from the target's on a case is not
     comparable there. A record without a fingerprint (an older file, or a case
     that did not run) is treated as comparable.
5. **Measure memory.**
   - Run `gc.collect()`, start `tracemalloc`, make one call, and record the peak
     as `peak_alloc_bytes`.
   - Tracing starts after the inputs exist, so this is what the call allocates,
     output included.
   - Skip this step when the median exceeds 60 s, because tracemalloc slows
     Python-level code.
6. **Record the outcome.** `status` is `ok`, `skipped` (with a reason) or
   `error` (with the message).

### Orchestration (`run.py`)

- **Version selection:**
  - `--target REF`: a git ref, or `WORKTREE` for uncommitted local code.
  - `--compare REF...`: extra named refs, such as a PR's base branch.
  - `--baselines N` (default 3): if the target is a tag, the N highest `v*`
    semver tags strictly below it; otherwise the N highest tags.
  - Refs that resolve to the same commit are measured once and listed under
    every label.
  - Selection is a pure function, so it is unit-tested.
- **Environments:**
  - `git archive <ref>` is extracted with `tarfile` (`filter="data"`) into
    `.bench/src/<label>/`. Worktrees are avoided so this works on Windows too.
  - `uv venv .bench/venv/<label> --python 3.11`, then
    `uv pip install --python <venv> <src>`.
  - The pandas, numpy, pysdmx and typeguard versions installed in each env are
    recorded.
- **Grid:**
  - Loop over scenario, then size (ascending), then version, with the version
    order rotated on each case.
  - Each case runs in its own subprocess, with the harness root as cwd and a
    15-minute timeout.
  - Once a (version, scenario) median exceeds 60 s, its larger sizes are
    recorded as `skipped` ("exceeds budget"). The expected case is v0.8.0's
    row-wise multi-map path at 1M rows.
- **Exit status:**
  - Non-zero only if the target fails to install, or every target case errors.
  - A failing case is recorded and the run continues.
- **Output:** `benchmarks-<target-label>.json` with `schema_version: 1`,
  containing:
  - `generated_at`;
  - `machine`: platform, CPU model, CPU count, total RAM, and the runner image
    when on Actions;
  - `python`;
  - `versions[label]`: `ref`, `commit`, `role` (`target`, `compare` or
    `baseline`) and `deps`;
  - `results[]`.

## Views

### Docs page (release-only)

`great-docs.yml` gains a Performance section placed after Reference:
`sections: [{title: Performance, dir: performance, navbar_after: Reference}]`.

`make docs` and `make docs-preview` run `report.py --format page` first, which
writes the gitignored `performance/index.qmd` from the template. The page has
five parts:

1. **Headline.** For the typical table at 1M rows on the runner: the median
   time, the extra memory, and that memory as a multiple of the input size.
2. **Sizing table.** For the profiled version: scenario × rows → median time,
   peak extra memory and input size.
3. **Comparison.**
   - Two figures, time and memory. Each is a 2×2 small multiple (one panel per
     scenario), log-log against rows, with one line per version.
   - The figures are drawn with matplotlib and inlined as SVG in a `{=html}` raw
     block, once for light mode and once for dark.
   - great-docs switches themes with its own toggle, which sets
     `html[data-bs-theme="dark"]`. It does not use Quarto's
     `.light-content`/`.dark-content` classes. The template therefore carries a
     small style block keyed on that attribute, which shows one variant at a
     time.
   - The target version is drawn as an accent-coloured, heavier line. Older
     versions are grey context lines, darker for newer releases, with distinct
     markers and dash styles. Every line is in the legend, and the tables carry
     the exact values.
   - A table at 1M rows gives the % change against the previous release; changes
     within ±10% read "≈ same".
   - A version whose output fingerprint differs from the target's on a case reads
     "≠ output" in the tables, with no change computed from it, and is left out
     of that case's chart lines. A footnote explains the mark, and appears only
     when there is one.
4. **Method and caveats.**
   - Hardware, Python, and each version's dependency versions, with the
     pandas 2 → 3 change called out.
   - What tracemalloc counts.
   - The noise floor.
5. **Raw data.** A link to the JSON on the Release.

Skipped or errored cells render as "—" with a footnote, never as zero. The
method section says that each case's output is fingerprinted and what "≠ output"
means.

### PR comment (development)

The comment is rendered as tables only, because comments cannot embed SVG.

- **Headline table**, at 1M rows, one row per scenario:
  - time and extra memory for the head, the base and the latest release;
  - % change of head against base (the main signal);
  - % change of head against the release.
- **Full grid**, inside a `<details>` block.
- **Footnote**: when a version's output differs from the head's on a case, that
  cell reads "≠ output" and a footnote says why its time is not comparable.
- **Footer**: runner, Python, the dependency versions of each version, the head
  SHA, the update time, and a link to the run.

The comment is found and edited in place by the hidden marker
`<!-- tidysdmx-benchmark -->`.

## CI

The benchmark is split across three workflow files.

**Why three files.** A called workflow's jobs cannot request more permissions
than the caller grants, even jobs that end up skipped. If one file held both the
release upload (`contents: write`) and the PR comment (`pull-requests: write`),
both callers would have to grant both. Each write therefore lives in its own
caller, next to the event that needs it, and the shared measuring step only ever
reads.

**Supply-chain rules** apply to all three files:

- Actions are pinned to full SHAs with `# vX.Y.Z` comments.
- Untrusted values reach scripts through `env:`, never by `${{ }}` interpolation
  inside `run:`.

### `.github/workflows/benchmarks.yml` (new, measure only)

- **Triggers:** `workflow_call` and `workflow_dispatch`, both with inputs `ref`,
  `target-label`, `compare`, `baselines` (3) and `sizes` (the full grid).
- **One job, `run`:**
  - Runs with `contents: read` on ubuntu-latest, with a 75-minute timeout. The
    checkout uses `fetch-depth: 0` so tags and branches are present.
  - Runs `uv run --no-project python -m benchmarks.run ...`.
  - Writes `report.py --format summary` to `$GITHUB_STEP_SUMMARY`, and renders
    `pr-comment.md`.
  - Uploads both, with the JSON, as the `benchmark-results` artifact.

### `.github/workflows/benchmarks-pr.yml` (new, PR view)

- **Trigger:** `pull_request` to `dev` or `main`, on `opened`, `synchronize`,
  `reopened`, `ready_for_review` and `labeled`.
- **Concurrency:** one group per PR number with `cancel-in-progress`. A
  `labeled` event for any other label gets a group of its own, so it cannot
  cancel a benchmark in flight.
- **Jobs.** PR code never runs while a write token is present:
  1. **`decide`** (`contents: read`). It follows `pr-review.yml`:
     - Fork PRs and drafts are skipped.
     - Dependabot-triggered runs are skipped too. Its uv PRs change `uv.lock`,
       which the benchmark never reads: `uv pip install <source>` resolves each
       version's declared dependencies afresh, so a lock-only change cannot change
       what is measured. Dependabot runs also get a read-only token.
     - A run starts if `git diff "origin/$BASE_REF...HEAD"` touches
       `src/tidysdmx/mapping.py`, `benchmarks/` or `pyproject.toml` (where the
       declared dependencies live), or if the PR carries the `benchmark` label.
     - A `labeled` event runs only when the label is `benchmark`.
     - The check lives in a job because an `on.pull_request.paths` filter would
       also drop the `labeled` event.
  2. **`benchmark`** calls `benchmarks.yml` with:
     - `ref` set to the head SHA, `target-label: head`;
     - `compare: base=origin/<base>`;
     - `baselines: 1`.
  3. **`comment`** (`pull-requests: write`, no checkout). Downloads the
     artifact, then creates or edits the sticky comment with `gh api`. Only a
     comment by `github-actions[bot]` that starts with the marker counts as the
     sticky one, so no one else can pre-empt it.

### `release.yml`

- **New `benchmark` job.**
  - It `needs: [release, build]` and carries the same `released`/`noop` guard.
  - It calls `benchmarks.yml` with `ref` set to the tag and `baselines: 3`, and
    runs in parallel with `publish-pypi`.
- **New `attach-benchmark` job** (`contents: write`).
  - It downloads the artifact and runs
    `gh release upload "$TAG" "benchmarks-$TAG.json" --clobber`.
- **`deploy-docs` changes.**
  - It also `needs: attach-benchmark`.
  - Its condition becomes
    `!cancelled() && needs.publish-pypi.result == 'success' && released && !noop`,
    so a failed benchmark never blocks the docs redeploy.
- **Cancellation window.** The benchmark jobs are outside the docs concurrency
  group, so the window described in the `deploy-docs` comment does not widen.

### `docs.yml`

- **New fetch step.** Before `make docs`, it walks `gh release list` newest
  first and downloads the first `benchmarks-*.json` it finds to
  `benchmarks/results/latest.json`. The step gets `GH_TOKEN` from
  `github.token`, plus `GH_REPO`.
- **When no release has the asset**, the step emits `::notice::` and the page
  renders a placeholder.
- **Which version the page shows:**
  - A release redeploy picks up the new release's profile.
  - If that release's benchmark failed, the page shows the previous release's
    profile and names it.

## Local use

- **`make bench`** runs
  `uv run --no-project --python 3.11 python -m benchmarks.run --target WORKTREE --baselines 3 --out benchmarks/results/latest.json $(BENCH_ARGS)`.
  `--python 3.11` makes a machine whose default Python is older fail clearly.
  Then `make docs-preview` shows the page.
- **For a quicker run,** use `BENCH_ARGS="--sizes 1000 10000 --baselines 1"`.
- **Laptop numbers** describe the laptop. They are not comparable with numbers
  from the CI runner.

## Testing

`tests/test_benchmarks.py` runs in the unit lane. Its fixture results JSON lives
in `tests/fixtures/fxtr_benchmarks.py` and is imported by `conftest.py`.

| Class | What it covers |
|---|---|
| `TestBuild` | Row and map counts, determinism, targets disjoint from sources, every scenario accepted by the installed `map_structures`. |
| `TestRunCase` | An in-process 1k-row case yields a well-formed record, with a fingerprint. |
| `TestOutputFingerprint` | Equal for frames that differ only in dtype, null spelling or row order; different for a changed value, count or null count, or a missing column. |
| `TestSelectVersions` | Baselines below a tag, baselines for a non-tag ref, de-duplication by commit. |
| `TestBudgetSkip` | Larger sizes are skipped once the budget is exceeded. |
| `TestRenderPage` | Tables, % change, the "≈ same" band, the placeholder, error cells, the "≠ output" mark and its footnote. |
| `TestRenderPrComment` | The marker, head-vs-base change, `<details>`, collapsed duplicate refs. |

The environment and subprocess plumbing is checked end to end by the
`benchmarks-pr.yml` run on the PR that adds it.

## Rollout

- The feature lands on `dev` and goes live with the next `dev` → `main` release.
  `main` does not yet have the release and docs wiring this depends on.
- That release publishes the first profile, against v0.10.0, v0.9.0 and v0.8.0.
- After `dev` gains the harness, merging `dev` into
  `perf/map-structures-large-data` gives that PR its before/after comment.
