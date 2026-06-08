"""Polars ports of the tidysdmx data-path functions under assessment.

Each function mirrors the behaviour of its pandas counterpart in ``src/tidysdmx``
closely enough that ``benchmarks/parity.py`` can assert output equality. These
are throwaway reference ports for the benchmark spike, not production code.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
from pysdmx.model.map import ComponentMap, MultiComponentMap


def qa_coerce_numeric_pl(df: pl.DataFrame, numeric_columns: list[str]) -> pl.DataFrame:
    """Polars port of ``qa_utils.qa_coerce_numeric``.

    Casts each target column to Float64 (non-castable values -> null) and drops
    rows that became null, matching the pandas ``to_numeric(errors="coerce")``
    + ``dropna`` behaviour.
    """
    cols = [c for c in numeric_columns if c in df.columns]
    if not cols:
        return df
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in cols])
    return df.drop_nulls(subset=cols)


def qa_remove_duplicates_pl(df: pl.DataFrame) -> pl.DataFrame:
    """Polars port of ``qa_utils.qa_remove_duplicates`` (keep first, stable order)."""
    return df.unique(keep="first", maintain_order=True)


def vectorized_lookup_ordered_v1_pl(
    series: pl.Series, mapping_df: pd.DataFrame
) -> pl.Series:
    """Polars port of ``tidysdmx.vectorized_lookup_ordered_v1``.

    Rules are tried longest-SOURCE-first; the first matching regex wins and the
    original value is kept on no match. Implemented as an ordered
    when/then/otherwise chain (first true branch wins).
    """
    s = series.cast(pl.Utf8).rename("v")
    if mapping_df.empty:
        return series
    m = mapping_df.copy()
    m["_len"] = m["SOURCE"].str.len()
    m = m.sort_values("_len", ascending=False)

    frame = pl.DataFrame({"v": s})
    expr = None
    for _, row in m.iterrows():
        cond = pl.col("v").str.contains(row["SOURCE"])
        expr = (
            pl.when(cond).then(pl.lit(row["TARGET"]))
            if expr is None
            else expr.when(cond).then(pl.lit(row["TARGET"]))
        )
    expr = expr.otherwise(pl.col("v"))
    return frame.select(expr.alias("v")).to_series().rename(series.name or "")


def vectorized_lookup_ordered_v2_pl(
    series: pl.Series, mapping_df: pd.DataFrame
) -> pl.Series:
    """Polars port of ``tidysdmx.vectorized_lookup_ordered_v2`` (regex or exact)."""
    s = series.cast(pl.Utf8).rename("v")
    if mapping_df.empty:
        return series
    m = mapping_df.copy()
    m["_len"] = m["SOURCE"].str.len()
    m = m.sort_values("_len", ascending=False)

    frame = pl.DataFrame({"v": s})
    expr = None
    for _, row in m.iterrows():
        if row["IS_REGEX"]:
            cond = pl.col("v").str.contains(row["SOURCE"])
        else:
            cond = pl.col("v") == row["SOURCE"]
        expr = (
            pl.when(cond).then(pl.lit(row["TARGET"]))
            if expr is None
            else expr.when(cond).then(pl.lit(row["TARGET"]))
        )
    expr = expr.otherwise(pl.col("v"))
    return frame.select(expr.alias("v")).to_series().rename(series.name or "")


def apply_component_map_pl(
    df: pl.DataFrame, component_map: ComponentMap
) -> pl.DataFrame:
    """Polars port of ``mapping.apply_component_map`` (dict map -> new column)."""
    source_col = component_map.source
    target_col = component_map.target
    mapping = {vm.source: vm.target for vm in component_map.values.maps}
    if source_col not in df.columns:
        raise KeyError(f"Source column '{source_col}' not found in DataFrame.")
    return df.with_columns(
        pl.col(source_col)
        .replace_strict(mapping, default=None, return_dtype=pl.Utf8)
        .alias(target_col)
    )


def apply_multi_component_map_pl(
    df: pl.DataFrame, multi_component_map: MultiComponentMap
) -> pl.DataFrame:
    """Polars port of ``mapping.apply_multi_component_map``.

    The pandas version applies a python function row-wise; this expresses the
    same ordered, first-match-wins rules as a vectorised when/then chain, where
    each rule is a conjunction of per-column predicates. ``regex:`` patterns use
    ``re.fullmatch`` semantics, reproduced by anchoring with ``^(?:...)$``.
    """
    source_cols = list(multi_component_map.source)
    target_col = multi_component_map.target[0]
    missing = [c for c in source_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing source columns: {missing}")

    rules = [
        {"patterns": list(mv.source), "target": mv.target[0]}
        for mv in multi_component_map.values.maps
    ]

    expr = None
    for rule in rules:
        conds = []
        for col, pattern in zip(source_cols, rule["patterns"], strict=True):
            col_expr = pl.col(col).cast(pl.Utf8)
            if pattern.startswith("regex:"):
                rgx = pattern.removeprefix("regex:")
                conds.append(col_expr.str.contains(f"^(?:{rgx})$"))
            else:
                conds.append(col_expr == pattern)
        cond = conds[0]
        for c in conds[1:]:
            cond = cond & c
        expr = (
            pl.when(cond).then(pl.lit(rule["target"]))
            if expr is None
            else expr.when(cond).then(pl.lit(rule["target"]))
        )
    if expr is None:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(target_col))
    expr = expr.otherwise(pl.lit(None, dtype=pl.Utf8))
    return df.with_columns(expr.alias(target_col))


def filter_rows_pl(
    df: pl.DataFrame, codelist_ids: dict[str, list[str]]
) -> pl.DataFrame:
    """Polars port of ``tidy_raw.filter_rows``.

    Keeps a row when, for every constrained column, the value is in the allowed
    set OR is null (out-of-list non-null values cause the row to be dropped).
    """
    if not codelist_ids:
        return df
    keep = pl.lit(True)
    for col, allowed in codelist_ids.items():
        if col not in df.columns:
            continue
        allowed_str = [str(a) for a in allowed]
        keep = keep & (
            pl.col(col).cast(pl.Utf8).is_in(allowed_str) | pl.col(col).is_null()
        )
    return df.filter(keep)
