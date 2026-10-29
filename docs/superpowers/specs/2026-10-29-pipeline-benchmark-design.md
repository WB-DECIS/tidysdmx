# Cross-version benchmark of the WB template pipeline: design

Status: approved 2026-10-29, implemented on `bench/wb-ids-mock-data`.
Extends `2026-10-23-performance-profile-design.md`, whose harness, views and
CI wiring it reuses; only what differs is described here.

## Why

The `map_structures` profile measures one function on synthetic inputs. The
work that made the WB template pipeline fit a 14 GB input (chunk-safe steps,
`DatasetValidator`, the lean load) changed `read_csv` options, `filter_rows`,
`standardize_output` and `validate_dataset_local` as much as the mapping, and
the notebook's wall time and peak memory are what the pipeline's owners see.
`benchmarks/wb_ids/pipeline.py` already measures that chain step by step for
one version. This design runs it for several versions on one machine, in the
places the `map_structures` numbers already go: the PR comment, the Release
asset and the Performance page.

## Decisions

| Question | Decision |
|---|---|
| What is measured | The WB template notebook's steps, as `benchmarks/wb_ids/pipeline.py` runs them: read the template, load, filter, build the structure map, `map_structures`, `standardize_output`, validate. Each step's own time, with the previous step's output in memory; the whole run's peak RSS. |
| Input | The mock WB_IDS bronze CSV (`benchmarks/wb_ids/generate`), seed 0, generated on the runner once per size and read by every version. |
| Configurations | Two per size. **whole**: the notebook's run, default load, every column. **chunked**: `chunk_rows = rows // 5`, `--skip-unused --categories`, the production loop of the "Large files" guide. The chunked configuration needs `DatasetValidator`, so a release without it records a skipped case and reads "—". |
| Sizes | 1M rows on pull requests; 1M and 10M on releases. |
| PR trigger | Any change under `src/tidysdmx/`, plus `benchmarks/`, `pyproject.toml` and the `benchmark` label. The `map_structures` suite keeps its narrower paths. |
| Comparison | The same run measures every version; results from different runs are never compared. A version whose standardised output differs from the target's is marked "≠ output" on every row; one whose validation found different errors is marked on the `validate` row alone. |
| Memory | Peak RSS sampled every 10 ms by psutil, not tracemalloc: the pipeline's cost is in Arrow string buffers and pandas' allocator caches, which tracemalloc does not see. pyarrow and psutil are therefore installed into this suite's environments, and never into the `map_structures` suite's, whose tracemalloc figures rely on pyarrow being absent. |
| Store | `pipeline-vX.Y.Z.json` on the GitHub Release, beside `benchmarks-vX.Y.Z.json`. |
| Views | A second section of the one sticky PR comment; a "WB template pipeline" section of the Performance page. Tables only, no chart yet. |
| The manual `pipeline-bench.yml` | Retired. The single-version runner stays as `make pipeline-profile` for sizes the cross-version run does not attempt (40M, 147M rows). |

## Constraints from the codebase

- **Every release since v0.8.0 runs the whole configuration.** v0.8.0 to
  v0.10.0 expose every function the runner calls with the keywords it passes.
  v0.7.5 rejects the structure-id keywords and records an error, which the
  report shows as "—" with the message.
- **No release has `DatasetValidator`.** The runner imports it inside the
  chunked path only, so the module imports everywhere; the case runner checks
  for the class before trying.
- **The harness root is the working directory of every case**, so
  `benchmarks.wb_ids` (the generator, the runner, the committed template and
  schema) comes from the harness's commit, not the version under test. The
  version under test supplies only `tidysdmx`.
- **Old releases import pyarrow eagerly nowhere, but the runner did.** Its
  version is now read from package metadata and recorded as `None` when the
  package is absent.
- **One results schema.** `PipelineRecord` extends `Record`: the inherited
  `scenario` holds the configuration name, so `run_grid`, the version
  selection and `is_comparable` are shared. `Results.suite` (absent means
  `mapping`) says which renderer applies, and `schema_version` stays 1 so
  every earlier file still loads.
- **`docs.yml` fetches by glob.** `benchmarks-v*.json` and `pipeline-v*.json`
  cannot match each other's asset.

## Components

| File | Change | Runs in |
|---|---|---|
| `benchmarks/records.py` | `Suite`, `PIPELINE_CONFIGS`, `PIPELINE_STEPS`, `ASSET_PREFIX`; `PipelineStep` and `PipelineRecord` (`chunk_rows`, `steps`, `peak_rss_bytes`, `validation`, `validation_fingerprint`); `empty_pipeline_record`, `suite_of`, `asset_name`. Stdlib only. | everywhere |
| `benchmarks/fingerprint.py` | `Fingerprint(columns)`: `update(df)` per frame or chunk, `digest()`. Text columns hash sorted `(str(value), count)` pairs with zero counts dropped, so a categorical's unused categories do not matter; numeric columns hash the count and a wraparound `uint64` sum of the `float64` bits, with `-0.0` folded, so 10M floats need no per-value counter. `case.output_fingerprint` delegates to it. `digest_counts` hashes the validation summary. | each version's venv |
| `benchmarks/wb_ids/pipeline.py` | `run_pipeline(..., on_output=)`: called once after `validate` (whole) or once per chunk after `DatasetValidator.update` (chunked), never inside a measured block. `DatasetValidator` imported lazily; pyarrow's version from metadata; `STEP_ORDER` replaced by `PIPELINE_STEPS`. | each version's venv |
| `benchmarks/pipeline_case.py` | `python -m benchmarks.pipeline_case --scenario whole|chunked --rows N --csv PATH --result FILE`. Runs `run_pipeline` `repeats_for(rows)` times (3 up to 1M rows, 1 above; stops after a run slower than 30 s), fingerprints the first run's output, takes per-step medians, the first run's per-step and overall peak RSS, the validation counts and their digest. A chunked case on a version without `DatasetValidator` is a skipped record; any exception is an error record. | each version's venv |
| `benchmarks/probe.py` | Reports pyarrow and psutil too; `assert_installed` (moved from `case.py`) guards both case runners. | each version's venv |
| `benchmarks/run.py` | `--suite {mapping,pipeline}` selects a `SuiteSpec`: the case module, the scenarios, the default sizes, the extra packages and whether inputs are generated. `create_env` installs the extras in the same resolution as the version. `generate_inputs` writes `wb_ids_<rows>.csv` under `.bench/pipeline/data/` with the target's environment. `measure_pipeline` shares the subprocess body with `measure`. `run_grid` takes skip rules: the time budget as before, plus `over_memory_budget`, which scales the previous size's peak RSS to the next size and skips it above 60% of the machine's memory. | any Python with git and uv |
| `benchmarks/report.py` | `render_pipeline_comparison`: per size, one table per configuration with a row per step, `total` and `peak RSS`, a column per version and a `Δ vs` column per reference; the largest size first and the rest in `<details>`; notes for unmeasured cells (skipped reasons and error messages), a line on the validation counts; the environment table gains a pyarrow column. `render_page` fills `<!-- PIPELINE-RESULTS -->` from `--pipeline-input`, or with a placeholder. | the dev env |
| `benchmarks/page.qmd` | A "WB template pipeline" section and a methodology list for both suites. | n/a |

### Measurement protocol (`pipeline_case.py`)

1. **Skip or run.** A chunked case on a version without `DatasetValidator`
   is recorded as skipped with the reason, before anything is imported.
2. **First run** with the fingerprint hook. The output's columns are the
   target schema's components, numeric where the component's data type is
   numeric. Validation errors are summarised by type and digested separately.
3. **Repeats.** Up to `repeats_for(rows)` runs in the same process, with
   `gc.collect()` between them; stop after a run slower than `SLOW_RUN_S`.
4. **Record.** Per step: every sample, the median, the first run's peak RSS
   and the rows it saw. Overall: the samples and median of the total, the first
   run's peak RSS, the CSV's size as `input_bytes`, the standardised frame's
   shape, both fingerprints, the validation counts and `chunk_rows`.

Per-step RSS is the first run's because later runs inherit the allocator's
caches and would read flat; per-step time is a median because the first run
pays for imports and JIT-like warm-up in typeguard and regex compilation.

### Orchestration (`run.py --suite pipeline`)

- Work goes under `.bench/pipeline/`, the mapping suite's under
  `.bench/mapping/`, so both can run locally side by side.
- Environments: `uv pip install <source> pyarrow psutil` in one resolution.
- Inputs: generated after the versions install, with the target's interpreter,
  every size, always regenerated (the generator takes 10 s per 10M rows and a
  stale file from another seed would silently change every number).
- Grid: configurations in `PIPELINE_CONFIGS` order, sizes ascending, versions
  rotated per case, as the mapping suite does. Skip rules: the time budget
  and the memory projection.
- Output: the mapping suite's JSON plus `"suite": "pipeline"`.

## CI

- `benchmarks.yml` takes `suite`; its artifact is `benchmark-results-<suite>`
  holding `<suite>-results.json` and `<suite>-summary.md`. `sizes` empty
  means the suite's default.
- `benchmarks-pr.yml`'s `decide` job emits one output per suite; the jobs
  `benchmark-mapping` and `benchmark-pipeline` run in parallel, and `comment`
  concatenates whichever summaries exist under the one marker.
- `release.yml` runs both suites in parallel; `attach-benchmark` uploads
  whichever results exist as `benchmarks-$TAG.json` and `pipeline-$TAG.json`.
- `docs.yml` fetches each asset from the newest release that carries it.
- Budget: a release run (four versions, 1M and 10M rows) is 12 to 20 minutes;
  a PR run (three versions, 1M rows) 6 to 8 minutes. The 75-minute timeout is
  the mapping suite's. No `--out-csv` in CI.

## Local use

- `make bench-pipeline` runs `benchmarks.run --suite pipeline --target WORKTREE
  --baselines 3 --out benchmarks/results/pipeline.json $(BENCH_ARGS)`.
  `BENCH_ARGS="--sizes 200000 --baselines 1"` for a quick run; a shallow clone
  needs `git fetch --tags` first.
- `make pipeline-profile` is the single-version runner, for 40M and 147M rows.
- `make docs-preview` renders both sections of the Performance page.

## Testing

`tests/test_benchmarks.py` gains, in the unit lane: `TestFingerprint` (equal to
`output_fingerprint`, chunks accumulate to the whole, unused categories
ignored, numeric text equals float, a changed float changes it),
`TestPipelineRepeats`, `TestPipelineOptions`, `TestPipelineRunCase` on the
4,000-row sample, `TestPipelineCaseMain`, `TestMeasurePipeline`,
`TestGenerateInputs`, `TestSkipRules`, a pipeline variant of `TestMain`,
`TestRenderPipelineComparison`, and `TestRenderPage` / `TestReportMain` cases
for the pipeline file. `tests/test_wb_ids.py` covers the `on_output` hook and
the lazy import. The fixture results live in `tests/fixtures/fxtr_benchmarks.py`.
