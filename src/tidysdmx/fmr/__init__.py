"""Enhanced FMR client layer built on top of pysdmx.

This subpackage removes the friction of real-world FMR (Fusion Metadata
Registry) publication workflows. On top of pysdmx's CRUD clients it adds:

- :class:`~tidysdmx.fmr.client.FmrClient` — a unified read/write facade.
- :func:`~tidysdmx.fmr.diff.compare_artefacts` — change detection between
  an existing (registry) artefact and an updated local one.
- :mod:`~tidysdmx.fmr.versioning` — parsing, comparison, and automated
  bumping of SDMX artefact versions.
- :func:`~tidysdmx.fmr.publish.plan_publication` /
  :func:`~tidysdmx.fmr.publish.publish` — a dry-run-able upsert workflow
  (fetch → diff → skip unchanged → bump versions → submit).
- :mod:`~tidysdmx.fmr.report` — pandas DataFrame views of diffs and plans.

The core modules depend only on pysdmx, the standard library, and
typeguard (plus :mod:`tidysdmx.artefact_validation`, itself pysdmx-only),
so the subpackage stays extractable into a standalone distribution.
pandas is used only by :mod:`tidysdmx.fmr.report`.
"""
