"""Performance benchmarks for tidysdmx.

Not part of the installed package. ``run`` measures several tidysdmx versions
side by side in one of two suites, and ``report`` renders the results:

- ``mapping``: ``map_structures`` alone, on generated dataset shapes. ``case``
  measures one case inside one version's environment. Design:
  docs/superpowers/specs/2026-10-23-performance-profile-design.md.
- ``pipeline``: the whole WB template pipeline, step by step, on a mock of the
  WB_IDS bronze file (``wb_ids``). ``pipeline_case`` measures one case inside
  one version's environment. Design:
  docs/superpowers/specs/2026-10-29-pipeline-benchmark-design.md.
"""
