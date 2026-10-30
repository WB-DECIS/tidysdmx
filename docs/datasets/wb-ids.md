# WB_IDS bronze dataset

The World Bank's International Debt Statistics (IDS) bronze extract is the
largest input the WB template pipeline has been run on. It is a 15 GB CSV that
pandas cannot hold on most machines. The file cannot be committed, so this page
records its size and shape. `benchmarks/wb_ids/` holds a committed stand-in
that can rebuild a mock of any size, up to the full one, so the pipeline can be
run, measured and optimised without the real file.

Unless marked otherwise, every figure below comes from
`benchmarks/wb_ids/data/profile.json`. That profile was taken on 2026-10-28 on a
16-thread Intel machine with 63 GiB of RAM, running Windows 11, Python 3.11.9,
pandas 3.0.6 (Arrow-backed `str`), pyarrow 26.0.0 and numpy 2.4.6.

## The real inputs (not in git)

| File | Size | What it is |
|---|---:|---|
| `WB_IDS_BRONZE.csv` | 14,923,119,357 bytes | Bronze extract, one observation per row |
| `WB_IDS.xlsx` | 77 KB | WB mapping template. REP_MAPPING maps 574 series onto 8 target dimensions; COMP_MAPPING has 21 rows (20 with a rule); REQUESTED_IND_RAW lists 569 series |

- **Where they are kept:** locally under `_TMP/`, which is gitignored.
- **Target structure:** `WB.DEC.DG:DS_IDS(1.0.0)`, on the production FMR
  (`https://fmr.worldbank.org/FMR/sdmx/v2`). It is not on fmrqa.
- **Personal data:** the template's INFO sheet names its task team leader, with
  an email address. The committed copy is scrubbed.
- **Document properties:** the real workbook carries a Microsoft sensitivity
  label and SharePoint metadata, including the tenant's id, in its custom
  document properties. The INFO sheet classifies the content as public, and
  the committed copy has no custom properties.

## Size and shape

### File and columns

147,000,092 rows × 8 columns. Rows average 101.5 bytes. Every field is quoted.
Lines end in LF. The text is valid UTF-8: pyarrow's reader checks it.

| Column | Share of characters | Distinct | Empty | Mean / max length |
|---|---:|---:|---:|---:|
| `Series_Name` | 59.6% | 576 | 0 | 46.2 / 105 |
| `Series` | 20.6% | 576 | 0 | 16.0 / 19 |
| `Value` | 5.3% | — | 0 | 4.1 / 22 |
| `Year` | 5.2% | 63 | 0 | 4 / 4 |
| `Counterpart_Area_Code` | 5.0% | 304 | 0 | 3.9 / 11 |
| `Country_Code` | 3.9% | 134 | 0 | 3 / 3 |
| `country_series_metadata` | 0.4% | 130 | 99.89% | 0.34 / 650 |
| `footnote` | < 0.01% | 62 | 99.995% | 0.003 / 273 |

The shares count characters only. Quotes, commas and the newline add 24 bytes
to every row.

- **`Series_Name` is mostly waste.** It is 60% of the file's text, yet it is a
  1:1 label of `Series` that the pipeline never maps.

### Structure

- **Keys.** The four key columns (`Country_Code`, `Counterpart_Area_Code`,
  `Series`, `Year`) are unique: there are no duplicate rows.
- **Pairs.** Each of the 134 countries is paired with each of the 304
  counterparts: all 40,736 combinations occur.
- **Counterparts.**
  - 93 are institutions (`O_*`). The rest are ISO3 countries, historical
    countries (`GDR`, `GFR`, `SSR`, `CZCH`) and the aggregate `WLD`.
  - `WLD` is the largest at 2.59M rows. The median counterpart has 400k rows.
- **Series.**
  - There are 3,476,042 time series, each a (country, counterpart, series)
    triple.
  - Most pairs carry exactly 58 series: the median and both quartiles are 58.
  - The average is 85.3 series per pair and the maximum is 574. `WLD` carries
    the aggregate series.
- **Years.**
  - A series spans a median of 55 years, 1970–2024. 52% of series cover exactly
    that span. The mean span is 42.3 years.
  - Historical data, 1970–2024, has 2.0–3.0M rows a year (145.9M rows in all).
  - Projections, 2025–2032, have 0.11–0.15M rows a year (1.05M rows in all),
    across 149 series.
- **Row counts.**

  | Rows per | Minimum | Median | Maximum |
  |---|---:|---:|---:|
  | Country | 115k | 1.14M | 2.15M |
  | Counterpart | 381k | 400k | 2.59M |
  | Series | 108 | 38k | 2.04M |

### Order

The file is written as six partitions, one after another. Each covers an
alphabetical range of countries, and each runs from 1970 to 2032:

| Partition | Countries | Rows |
|---|---:|---:|
| AFG–DZA | 30 | 30,574,152 |
| EAP–KHM | 31 | 34,172,285 |
| LAC–MWI | 27 | 32,803,230 |
| NER–PRY | 9 | 10,727,526 |
| RWA–UZB | 28 | 29,600,901 |
| VCT–ZWE | 9 | 9,121,998 |

- **Within a partition,** rows are ordered by `Year`, then `Country_Code`, with
  no exceptions.
- **Counterparts** within a (year, country) are mostly in order, with 772,986
  descents.
- **The file as a whole is not ordered by year.** A reader that streams it
  cannot assume it has seen all of a year until the end.

### Values

- 83.2% of values are exactly zero and 2.5% are negative. None is missing or
  non-numeric.
- Non-zero magnitudes run from 10^0.2 to 10^10.1 (1st to 99th percentile), with
  a median of 10^6.8. The full range is −5.3e11 to 3.8e13.
- 99.2% are written with one decimal, e.g. `"2400000.0"`. The rest are other
  Python float reprs.

### Loading it

The profile loads the first 2M rows each way and extrapolates linearly:

| How | In-memory bytes per row | Whole file | 2M rows took |
|---|---:|---:|---:|
| `pd.read_csv(path)`: pandas 3 defaults, Arrow-backed `str` | 133 | 19.6 GB | 4.5 s |
| … without `Series_Name` (`usecols`) | 79 | 11.7 GB | 3.8 s |
| … with the code columns and `Year` as `category` | 33 | 4.8 GB | 3.6 s |
| A pyarrow `Table`, no dictionary encoding | 109 | 16.0 GB | — |

- **Peak memory.** `read_csv` peaks at about twice its result while it parses;
  see the pipeline table below. The pyarrow row has no time because the
  streaming reader's read-ahead distorts it.
- **A warning.** With defaults, pandas warns `DtypeWarning: Columns (7:
  footnote) have mixed types` on this file. `footnote` is empty for whole
  chunks, so pandas guesses float there and `str` elsewhere.

## What the pipeline costs

`python -m benchmarks.wb_ids.pipeline` runs the WB template notebook's steps
with the mock template and schema (`benchmarks.pipeline_case` wraps it to
compare versions; see "Reproducing and regenerating"):

1. read the template
2. `read_csv`
3. keep the REQUESTED_IND_RAW series
4. build the structure map
5. `map_structures`
6. `standardize_output`
7. `validate_dataset_local`

Each step frees its input once its output exists. The real-data runs take the
first N rows of the real file (`head`). The mock runs use
`generate --rows N --seed 0`. All runs were on the machine above.

| Rows | Data | Load s | Filter s | Map s | Standardize s | Validate s | Total s | Peak RSS |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1M | real | 1.08 | 0.21 | 0.77 | 0.07 | 0.82 | 3.7 | 552 MB |
| 1M | mock | 1.12 | 0.21 | 0.71 | 0.08 | 0.83 | 3.7 | 624 MB |
| 10M | real | 10.52 | 2.12 | 7.19 | 0.71 | 9.39 | 30.7 | 4.20 GB |
| 10M | mock | 11.00 | 2.11 | 7.20 | 0.68 | 9.48 | 31.2 | 4.20 GB |
| 40M | real | 33.2 | 6.3 | 22.3 | 2.2 | 30.2 | 94.9 | 16.4 GB |
| 40M | mock | 34.8 | 6.2 | 22.1 | 2.1 | 29.2 | 95.0 | 16.3 GB |

At 10M rows the frame peaks at 3.4 GB after mapping, when it has 28 columns.
Memory then peaks at 4.2 GB during validation.

- **Memory grows linearly with rows: about 410 bytes per input row.**
  Extrapolated to the whole file, 147M rows, the pipeline as it stood needed
  about **60 GB** and **6 to 7.5 minutes**. That is beyond this 63 GiB machine
  once the OS is counted, and far beyond a 16 GB one. The next section is
  what the October 2026 changes did about it; this table is their baseline.
- **The mock is calibrated.** At equal row counts it matches the real data
  step by step:
  - time within 5% at every size
  - peak memory within 1% at 10M and 40M rows
  - at 1M rows, peak memory 13% higher, where fixed overheads weigh more

  Each step costs what its row count and columns cost, and the mock reproduces
  both.
- **Run comparisons back to back.** Timings on this machine vary between runs.
  A first pair of 40M runs, while the machine was busier, took 136 s (real) and
  150 s (mock); the table shows the repeat. Peak memory stays stable.
- **What the comparison cannot show.** Real slices from `head` are partition 1's
  early years, while the mock samples whole series across the file. So it
  cannot show effects of row order or of the real data's joint structure.

### After the October 2026 changes

Every step is now chunk-safe and cheaper per row, so the pipeline can run as
a `pd.read_csv(chunksize=...)` loop whose memory is bounded by the chunk (the
user guide's "Large files" page has the loop). What changed in the library:
`map_structures` groups each source column into its distinct values once per
call instead of once per map, and keeps a categorical source column
categorical (the eight `Series` targets cost 1 byte a row instead of 11–16);
`standardize_output` no longer deep-copies the frame; the validation checks
judge each distinct value once rather than converting every row to text; and
`DatasetValidator` accumulates the checks across chunks. The runner grew
`--chunk-rows`, `--skip-unused` (do not read `Series_Name`) and
`--categories` (code columns and `Year` as `category`).

Measured on the 10M-row mock in a 4-core, 15 GB Linux container (uv-managed
Python 3.11, pandas 3.0.6, pyarrow 26.0.0), the same machine back to back:

| Run | Load s | Filter s | Map s | Standardize s | Validate s | Total s | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Before, whole frame | 25.3 | 7.9 | 7.2 | 4.0 | 10.7 | 56.2 | 4.31 GB |
| After, whole frame | 23.7 | 6.9 | 2.8 | 2.7 | 7.0 | 43.9 | 4.31 GB |
| After, whole frame, lean load | 7.9 | 0.6 | 1.3 | 2.5 | 7.9 | 21.3 | 3.31 GB |
| After, chunks of 2M rows | 12.9 | 2.2 | 4.2 | 1.5 | 5.5 | 28.4 | 1.50 GB |
| After, chunks of 2M rows, lean load | 8.2 | 0.9 | 2.4 | 2.1 | 7.5 | 23.1 | 0.90 GB |
| After, chunks of 5M rows, lean load | 8.1 | 2.1 | 3.9 | 2.7 | 8.4 | 26.6 | 1.98 GB |

"Lean load" is `--skip-unused --categories`. Every run reports the same 113
codelist errors, and the chunked lean run's output file is byte for byte the
whole-frame run's (`tests/test_wb_ids.py` checks this on the sample; a
10M-row `cmp` confirmed it). Writing the 5M-chunk run's output with
`DataFrame.to_csv` took another 49 s, 5 µs a row, more than the rest of the
pipeline together.

- **Run whole, the peak did not move.** It is set by two frame-sized
  transients the changes do not touch: the copy boolean filtering makes
  (3.5 GB at the `filter` step) and `DataFrame.duplicated` over the 11
  dimensions inside validation (4.3 GB). Mapping itself fell from 7.2 to
  2.8 s and the mapped frame from 345 to 131 bytes a row with the lean load.
- **Run in chunks, the peak is the chunk's working set: about 0.37 KB per
  chunk row with the lean load, 0.7 KB with the default one.** 2M-row chunks
  peak at 0.9 GB, 5M-row chunks at 2.0 GB, whatever the file's size. The peak
  falls inside validation, in `duplicated` over the chunk's dimensions.
- **Time barely changes.** Chunking costs nothing measurable; the lean load
  saves a third of the load time (one column fewer and category inference) and
  most of the filter time.

Larger runs in the same container, `--chunk-rows 5000000` with the lean load
unless marked otherwise; the whole-frame runs could not be repeated here
(16.4 GB at 40M rows on the 63 GiB machine):

| Rows | Chunks | Load s | Filter s | Map s | Standardize s | Validate s | Total s | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 40M, default load | 8 | 52.5 | 9.2 | 14.5 | 12.3 | 39.7 | 131 | 3.50 GB |
| 40M | 8 | 30.9 | 4.8 | 6.1 | 9.6 | 35.5 | 90 | 1.98 GB |
| 147,000,092 (`--scale 1`, 15.0 GB of CSV) | 30 | 135.5 | 25.1 | 66.6 | 45.0 | 100.6 | 383 | 1.99 GB |

- **The whole file runs in 2.0 GB and 6.4 minutes**, against the 60 GB and
  6 to 7.5 minutes extrapolated for the whole-frame pipeline, and the peak is
  the same at 10M, 40M and 147M rows: only the chunk's size sets it. The
  run found the same 113 codelist errors as the smaller mocks.
- **Writing is the long pole.** The full run also wrote its output with
  `DataFrame.to_csv`, to `/dev/null` for want of 17 GB of disk, and that took
  another 623 s, 4.2 µs a row, for a total of 1,006 s.

## The mock (`benchmarks/wb_ids/`)

| File | What it is |
|---|---|
| `data/profile.json` | The real file's profile: aggregates only, about 350 KB |
| `data/WB_IDS_mock.xlsx` | The real template, with personal data replaced and two typos fixed (see below) |
| `data/ds_ids_schema.json` | DS_IDS, with each codelist trimmed to the codes the pipeline can write (`codes_total` keeps the real size) |
| `data/WB_IDS_BRONZE_mock.csv` | `generate --rows 4000 --seed 0`, small enough for git. `tests/test_wb_ids.py` checks it is reproduced byte for byte |
| `profile.py`, `generate.py`, `template.py`, `schema.py`, `pipeline.py` | The tools that make and use the files above |

### How the mock is built

The mock's unit is the time series. `generate`:

1. builds a universe of (country, counterpart, series) triples the size of the
   real one: every country × counterpart pair, with series counts drawn from
   the real distribution. The largest counts go to pairs whose country and
   counterpart have the most rows.
2. takes triples in a fixed random order until their year spans, also drawn
   from the real distribution, add up to exactly N rows.
3. picks each triple's years from its series' and country's real year profiles.
4. writes the real partitions in order, each ordered by year, country and
   counterpart.

Values, metadata and footnotes are synthetic, drawn from the profile's
distributions. Keys are unique by construction.

A small mock is a random sample of whole series. The 4,000-row sample spans 65
countries, 73 counterparts, 75 series and all 63 years. At full size the mock is
the whole universe.

**What it reproduces.** Checked on a 200k-row mock against the real profile:

- the column layout, quoting and line endings, byte for byte in style
- bytes per row: 102.3, against 101.5 real
- the share of zeros: 0.832, against 0.832 real
- the metadata and footnote density
- all 134 countries, 304 counterparts and 63 years
- all 6 partitions, with real-data row counts correlating 0.98 by year, 0.94 by
  series, 0.90 by counterpart and 0.76 by country
- `WLD` as the largest counterpart

**At full size** (`--scale 1`), profiled the same way as the real file:

| | Real | Mock |
|---|---:|---:|
| Rows | 147,000,092 | 147,000,092 |
| Bytes | 14,923,119,357 | 15,042,309,747 (+0.8%) |
| Distinct countries / counterparts / series / years | 134 / 304 / 576 / 63 | 134 / 304 / 576 / 63 |
| Pairs / series / duplicate keys | 40,736 / 3,476,042 / 0 | 40,736 / 3,462,763 / 0 |
| Partition rows (millions) | 30.6, 34.2, 32.8, 10.7, 29.6, 9.1 | 30.0, 34.9, 33.2, 9.9, 30.1, 8.9 |
| Zero / negative values | 122.36M / 3.64M | 122.41M / 3.63M |
| Non-empty metadata / footnote rows | 167,947 / 7,477 | 167,999 / 7,647 |
| pandas bytes per row: default / no `Series_Name` / category | 133.3 / 79.4 / 32.6 | 134.3 / 79.0 / 32.4 |

Rows per year, country, counterpart and series correlate with the real ones at
0.98, 0.90, 0.98 and 0.97.

**What it does not reproduce:**

- Which series go with which counterpart. The real file has 58 bilateral series
  for most pairs and the aggregates on `WLD`; the mock draws series by overall
  frequency.
- Values that evolve over time. Mock values are drawn independently.
- The 772,986 counterpart descents. The mock's counterparts are strictly
  ordered.
- The spread of metadata. The mock puts the real number of metadata rows on
  fewer (country, series) keys: 86 distinct texts at full size, against 130.

None of these affects what the pipeline's steps cost, as the calibration table
shows. Revisit that if an optimisation starts to depend on them, for example
grouping by series per counterpart.

## Problems in the real inputs

Building the mock turned up the following. The first two silently lose data;
the mock template fixes them, so the mock pipeline maps everything it is meant
to. The rest are left as they are, so the mock reproduces the real validation
errors.

1. **`Country_code` is spelled with a lowercase c.** COMP_MAPPING's SOURCE is
   `Country_code`, but the CSV column is `Country_Code`. `map_structures`
   matches sources exactly, so the implicit REF_AREA map is skipped, with only a
   log message, and validation later reports REF_AREA missing.
2. **Three series codes end in non-breaking spaces (U+00A0).** They are
   `DT_DOD_VPPG_CD`, `DT_INT_VTOT_CD` and `DT_INT_VPPG_CD`, in both REP_MAPPING
   and REQUESTED_IND_RAW. They never equal the data's codes. The REQUESTED
   filter drops them, or, if the filter strips whitespace, all 8 of their
   targets map to NaN. Those NaNs then surface as duplicate keys and missing
   values in validation.
3. **No INDICATOR code is in DS_IDS's codelist.** The template writes codes such
   as `DT_DOD_DSTC`. DS_IDS's INDICATOR codelist,
   `WB.DATA360:CL_D360_INDICATOR(1.0)` with 11,743 codes, prefixes them:
   `WB_IDS_DT_DOD_DSTC`. All 111 template codes are missing, so every row fails
   the codelist check on INDICATOR.
4. **Six counterparts are not in `CL_IDS_COUNTERPART_AREA`:** `CZCH`, `GDR`,
   `GFR`, `SSR`, `O_ECGLC` and `O_FCW`, about 4.0M rows.
5. **Other codes the template writes are not in the codelists:**
   - STOCK_FLOW `CAB`, `GNI` and `POP`
   - CREDITOR `_Z`
   - CONCESSIONALITY `_T` and `_Z` (its codelist has only `C` and `NC`)
6. **Some rows are filtered out by design.** Two series in the data are not in
   REP_MAPPING, and seven mapped series are not requested.

## Optimisation leads

Done in October 2026 (see "After the October 2026 changes" above): the file
is read in chunks; `Series_Name` is not loaded and the code columns are
categories, which `map_structures` keeps; the eight maps on `Series` share one
grouping; `standardize_output` no longer copies its input; validation judges
distinct values rather than rows and accumulates across chunks.

What the measurements still point at:

- **Writing.** `DataFrame.to_csv` costs 5 µs a row, about 12 minutes for the
  whole file, more than reading, mapping and validating together. pyarrow's
  `csv.write_csv` on `pa.Table.from_pandas(chunk)` is the candidate; pyarrow
  is a development dependency only, so this belongs in the runner or the
  caller, not the library.
- **The duplicates check sets each chunk's peak.** `DataFrame.duplicated` over
  the 11 dimensions holds about 150 bytes a row in int64 labels and a group
  index. A 64-bit hash of the key columns would halve that, and would also
  let keys shared by two chunks be found, which the chunked run does not do
  today by choice (the registry's key check covers it).
- **Constants.** The six fixed values and the three reference columns are
  `str` columns of 9–14 bytes a row each, 60–100 bytes a row in all. As
  single-category categoricals they would be 1 byte a row, but that changes
  the output's dtypes for every caller, so it was not done.
- **Loading.** pandas' C parser reads about 1.2M rows a second here on one
  core. pyarrow's streaming CSV reader is multithreaded and can return
  dictionary-encoded (categorical) columns directly.

## Reproducing and regenerating

Anywhere, with no real data:

```bash
uv run python -m benchmarks.wb_ids.generate --rows 10000000 --out mock.csv   # ~1 GB
uv run python -m benchmarks.wb_ids.pipeline --csv mock.csv --out run.json
make pipeline-profile WB_IDS_ROWS=10000000                                   # both steps
# The same, in chunks and with the lean load; --out-csv writes the output too.
make pipeline-profile WB_IDS_ROWS=10000000 \
  PIPELINE_ARGS="--chunk-rows 5000000 --skip-unused --categories"
```

- **Against earlier versions:** `make bench-pipeline` runs the same pipeline,
  whole and in five chunks with the lean load, on the working tree and the
  last three releases, each in its own environment, and writes one JSON for
  the Performance page (`benchmarks/results/pipeline.json`). It generates the
  mock itself, 1M rows by default; `BENCH_ARGS="--sizes 200000 --baselines 1"`
  for a quick run. A shallow clone needs `git fetch --tags` first. Design:
  `docs/superpowers/specs/2026-10-29-pipeline-benchmark-design.md`.
- **In CI:** every pull request that touches `src/tidysdmx/` gets that
  comparison at 1M rows in its benchmark comment, and every release at 1M and
  10M rows, attached to the GitHub Release as `pipeline-vX.Y.Z.json` and
  rendered on the Performance page. A standard GitHub runner has about 16 GB
  of RAM and 14 GB of free disk: run whole, 10M rows is the practical ceiling;
  run in chunks, only the disk limits the size. The orchestrator skips a size
  whose projected peak RSS would exceed 60% of the machine's memory.
- **Generating** streams to disk year block by year block, so memory stays
  bounded whatever the size:

  | Rows | Time | Peak RSS | File |
  |---:|---:|---:|---:|
  | 10M | 10 s | 0.65 GB | 1.02 GB |
  | 40M | 36 s | 0.81 GB | 4.10 GB |
  | 147,000,092 (`--scale 1`) | 141 s | 1.01 GB | 15.04 GB, against 14.92 GB real |

With the real files (in `_TMP/`) and network access to FMR:

```bash
# Re-profile the real file: about 10 minutes, peaking near 5 GB.
uv run python -m benchmarks.wb_ids.profile --csv _TMP/WB_IDS_BRONZE.csv
# Rebuild the mock template from the real one.
uv run python -m benchmarks.wb_ids.template --real _TMP/WB_IDS.xlsx
# Recapture DS_IDS. This needs the profile and template above.
uv run python -m benchmarks.wb_ids.schema
# Refresh the committed sample. tests/test_wb_ids.py checks it.
uv run python -m benchmarks.wb_ids.generate --rows 4000 --out benchmarks/wb_ids/data/WB_IDS_BRONZE_mock.csv
```

After re-profiling, re-run the calibration (real `head` slices against the mock
at the same sizes) and update the table above.
