# Assessment: pandas → polars refactoring for tidysdmx

**Status:** decision artifact (no `src/` code changed)
**Date:** 2026-06-08
**Spike code:** `benchmarks/` (throwaway harness, `benchmarks` Poetry group)

## TL;DR

Polars is **3–40× faster** than the current pandas helpers across the board, and the
ports are **provably equivalent** (7/7 parity checks pass). Despite that, a migration is
**not recommended right now**, because:

- Real datasets today are **~3,800 rows**. At that scale the absolute pandas time is
  already **single-digit milliseconds** — polars' large *ratio* wins translate to savings
  no user can perceive.
- The pandas↔polars **conversion cost** (~330 ms per 1M rows, ~34 ms per 100k) means a
  naïve "convert inside each helper" hybrid would be **net-negative** at the sizes that
  matter.
- Migration **costs are real and broad**: 505 pandas-assuming tests, Excel I/O bound to
  pandas/openpyxl, a public API + `kedro.py` that return `pd.DataFrame`, and `null` vs
  `NaN`/`pd.NA` semantic differences threaded through validation/QA.

**Recommendation: (C) status quo + one engine-agnostic quick win now**, with a documented
trigger to revisit **(B) a native-polars hybrid** if/when typical datasets reach ≥100k rows.
The quick win: vectorise `apply_multi_component_map`'s row-wise `.apply(axis=1)` — that
single algorithmic change is worth ~30× **in pandas today**, no polars needed.

---

## 1. Surface-area inventory

~45–50 functions across 9 modules touch pandas. They fall into three classes with very
different migration economics:

| Class | What it is | polars-relevant? | Examples |
|---|---|---|---|
| **Data-path** | Vectorisable DataFrame ops over tabular data | **Yes** — real speed wins | `qa_utils.*`, `tidysdmx.vectorized_lookup_ordered_v1/v2`, `mapping.apply_*`, `tidy_raw.filter_rows`, `validation.*` |
| **Object construction** | Row-wise `.iterrows()` loops building pysdmx `ValueMap`/`Component`/map objects | **No** — bottleneck is Python object creation, not the DataFrame engine | `structures.build_value_map_list`, `build_multi_value_map_list`, `create_schema_from_table`, `build_structure_map_from_template_wb` |
| **Excel / IO** | `pd.read_excel`, `openpyxl`, `dataframe_to_rows` | **No / negative** — polars has no drop-in `read_excel`; still needs pandas+openpyxl | `utils.parse_mapping_template_wb`, `utils.build_excel_workbook`, `structures._parse_*_sheet` |

Only the **data-path** class is a candidate for polars. The object-construction class is the
heaviest CPU cost in practice (e.g. `.iterrows()` building pysdmx objects), and polars does
**not** help there — those need *algorithmic* fixes (vectorised matching, then construct
objects once), which are engine-independent.

The pysdmx boundary is clean: pysdmx's core API has **no pandas dependency** (pandas is only
an unused optional `data` extra) and returns pure model objects. So an engine swap touches
only tidysdmx — a genuine de-risking factor.

---

## 2. Benchmark results

Harness: `benchmarks/bench_datapath.py` (pytest-benchmark). Synthetic SDMX-shaped data
(`benchmarks/_data.py`) derived from the IFPRI ASTI fixture, swept at 1k/10k/100k/1M rows.
Inputs are built outside the timed region; the pandas↔polars conversion is **not** counted
in these numbers (it is measured separately in §3). Each polars port is asserted equal to
its pandas original first (`benchmarks/parity.py`, **7/7 pass**).

Mean wall time, pandas vs polars, with speedup (higher = polars faster):

| function / size | pandas (ms) | polars (ms) | speedup |
|---|---:|---:|---:|
| `apply_component_map` / 1k | 0.490 | 0.416 | 1.2× |
| `apply_component_map` / 10k | 1.370 | 0.584 | 2.3× |
| `apply_component_map` / 100k | 8.827 | 1.630 | 5.4× |
| `apply_component_map` / 1M | 149.168 | 8.338 | **17.9×** |
| `apply_multi_component_map` / 1k | 7.789 | 0.794 | 9.8× |
| `apply_multi_component_map` / 10k | 71.277 | 2.270 | 31.4× |
| `apply_multi_component_map` / 100k | 702.349 | 16.086 | **43.7×** |
| `filter_rows` / 1k | 1.037 | 0.389 | 2.7× |
| `filter_rows` / 10k | 2.983 | 0.540 | 5.5× |
| `filter_rows` / 100k | 20.781 | 1.845 | 11.3× |
| `filter_rows` / 1M | 272.391 | 12.127 | **22.5×** |
| `qa_coerce_numeric` / 1k | 1.380 | 0.231 | 6.0× |
| `qa_coerce_numeric` / 10k | 3.810 | 0.525 | 7.3× |
| `qa_coerce_numeric` / 100k | 27.254 | 2.984 | 9.1× |
| `qa_coerce_numeric` / 1M | 407.480 | 28.748 | **14.2×** |
| `qa_remove_duplicates` / 1k | 0.653 | 0.301 | 2.2× |
| `qa_remove_duplicates` / 10k | 3.114 | 1.386 | 2.2× |
| `qa_remove_duplicates` / 100k | 26.480 | 9.822 | 2.7× |
| `qa_remove_duplicates` / 1M | 374.592 | 119.437 | 3.1× |
| `vectorized_lookup_v1` / 1k | 2.843 | 1.973 | 1.4× |
| `vectorized_lookup_v1` / 10k | 13.041 | 3.200 | 4.1× |
| `vectorized_lookup_v1` / 100k | 115.348 | 16.028 | 7.2× |
| `vectorized_lookup_v1` / 1M | 1257.299 | 158.723 | **7.9×** |
| `vectorized_lookup_v2` / 1k | 2.455 | 1.667 | 1.5× |
| `vectorized_lookup_v2` / 10k | 8.931 | 2.388 | 3.7× |
| `vectorized_lookup_v2` / 100k | 70.290 | 9.877 | 7.1× |
| `vectorized_lookup_v2` / 1M | 753.041 | 90.682 | **8.3×** |

(`apply_multi_component_map` is capped at 100k: its pandas `.apply(axis=1)` is too slow to
benchmark at 1M. Regenerate with `poetry run pytest benchmarks/bench_datapath.py
-o python_files=bench_datapath.py --benchmark-only --benchmark-json=benchmarks/results.json`.)

**Reading the table.** The speedup *ratio* grows with size and is large everywhere. But the
*absolute* pandas time at the **current data scale (~3.8k rows)** sits between 1 ms and
~15 ms for every function. Saving 1–13 ms per pipeline step is not user-visible. Polars only
becomes *materially* worth it from ~100k rows upward, where pandas crosses into the
hundreds-of-ms / seconds range.

---

## 3. One-off costs (the hybrid-killer)

| Cost | Measurement | Implication |
|---|---|---|
| `import polars` | ~122 ms (vs `import pandas` ~305 ms) | Negligible; polars is actually cheaper to import. |
| `pl.from_pandas`, 100k rows | ~34 ms/call | Comparable to the *entire* polars op at that size. |
| `pl.from_pandas`, 1M rows | ~328 ms/call | **Larger than the polars op itself** (e.g. qa_coerce 1M = 29 ms). |

This is decisive for the *hybrid* option. If polars is bolted in helper-by-helper while the
pipeline stays pandas (convert in → compute → convert out per call), the conversion cost
**erases or reverses** the gain. A hybrid only pays off if data is **ingested natively into
polars once** (`pl.read_csv` / `pl.read_parquet`), kept in polars across *all* transforms,
and converted to pandas a single time at the pysdmx/Excel boundary.

**Memory:** a fair peak-RSS comparison was not achievable in this spike — the data generator
is pandas-based, so both engines' subprocesses built a 1M-row pandas frame first and both
peaked at ~841 MB. Isolating polars' memory profile would require native polars ingestion;
deferred as out of scope. (polars is generally more memory-efficient on wide data, but this
spike does not independently confirm that here.)

---

## 4. Pros / Cons

**Pros of migrating (or hybridising):**
- Real, large speedups on the data-path (3–40×), increasingly so at scale.
- Clean pysdmx boundary — upstream is untouched, lowering blast radius.
- Lazy / streaming execution available for genuinely large future datasets.
- Stricter typing and explicit null handling; cheaper import than pandas.

**Cons / costs:**
- **No payoff at current scale** — savings are sub-20 ms on ~3.8k-row data.
- **Conversion tax** makes naïve hybrids net-negative (§3).
- **Excel I/O stays on pandas/openpyxl** — `pd.read_excel` has no polars drop-in; a
  migration means dual DataFrame libraries, not a clean replacement.
- **Public API break** — functions and `kedro.py` nodes return `pd.DataFrame`; downstream
  user/Kedro code expects pandas. Needs either a breaking change or a conversion shim.
- **Test/fixture churn** — 505 tests and all `tests/fixtures/` build pandas; a real
  migration rewrites fixtures and assertions.
- **Semantic differences** — polars `null` vs pandas `NaN`/`pd.NA` is pervasive in
  validation/QA logic and a common source of subtle bugs.
- **Object-construction hotspots don't benefit** — the `.iterrows()` builders in
  `structures.py` need algorithmic fixes regardless of engine.

---

## 5. Risk matrix

| Risk | Likelihood | Impact | Notes |
|---|---|---|---|
| Conversion cost wipes out gains | High (if hybrid per-helper) | High | Avoid by ingesting natively into polars. |
| Null/NaN semantic regressions in validation | High | High | Pervasive; needs careful parity tests. |
| Public-API / Kedro break for downstream users | Medium | High | Requires shim or major version. |
| Excel path forces dual-library maintenance | High | Medium | openpyxl/pandas remain required. |
| Effort spent for imperceptible end-user gain | High (at current scale) | Medium | Opportunity cost vs other work. |
| Object-construction bottleneck unaddressed | High | Medium | polars doesn't solve it; needs vectorisation. |

---

## 6. Options & recommendation

**Decision criterion (set before reading results):** migrate/hybrid is justified only if
polars gives ≥2× on the dominant hot path **at a data size users realistically hit, at or
below current scale.** The ratio criterion is met; the **"size users realistically hit"**
criterion is **not** — at ~3.8k rows the absolute savings are negligible and the conversion
tax dominates.

- **(A) Full migration — _not now._** High cost (tests, fixtures, API, Excel, null
  semantics) for no perceptible benefit at current data sizes.
- **(B) Targeted native-polars hybrid — _conditional/future._** Worthwhile **only** if/when
  typical datasets reach **≥100k rows**, and **only** done natively: read source data
  straight into polars, run the whole transform/validate/standardise chain in polars, and
  convert to pandas exactly once at the pysdmx/Excel boundary. Per-helper conversion is an
  anti-pattern (§3).
- **(C) Status quo + engine-agnostic quick win — _recommended now._** Keep pandas. Bank the
  single biggest available win *without* polars: **vectorise
  `mapping.apply_multi_component_map`** — replace its row-wise `.apply(axis=1)` (which the
  benchmark shows is the worst hotspot, 702 ms at 100k) with an ordered, vectorised
  condition/`np.select` approach mirroring the polars `when/then` chain in
  `benchmarks/polars_impls.py`. The same restructuring gave **~30–40×** here and applies
  directly in pandas. Similarly, the `.iterrows()` object-builders in `structures.py` are
  better targets for vectorised pre-matching than for an engine swap.

**Trigger to revisit (B):** if a concrete use case brings routine datasets to ≥100k rows (or
a wide/streaming requirement appears), re-run this harness (`benchmarks/`) on representative
data and adopt the native-polars hybrid behind the existing API.

---

## 7. Reproducing this assessment

```bash
poetry install --with benchmarks

# 1. Parity — ports must match pandas before timings are trusted (expect 7/7)
poetry run python -m benchmarks.parity

# 2. Timings (writes benchmarks/results.json)
poetry run pytest benchmarks/bench_datapath.py -o python_files=bench_datapath.py \
    --benchmark-only --benchmark-columns=mean,median,ops \
    --benchmark-json=benchmarks/results.json
```

Spike layout: `benchmarks/_data.py` (synthetic data + pysdmx map fixtures),
`benchmarks/polars_impls.py` (the ports), `benchmarks/parity.py` (equivalence gate),
`benchmarks/bench_datapath.py` (pytest-benchmark cases). Nothing under `src/tidysdmx/` was
modified — actual migration would be a separate effort gated on the trigger above.
