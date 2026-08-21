---
description: Build the documentation site and report what broke
allowed-tools:
  - Bash
  - Read
  - Edit
  - Grep
  - Glob
---

Build the docs:

```bash
make docs
```

The site is built by [great-docs](https://posit-dev.github.io/great-docs/) on top
of Quarto. Both arrive via the `docs` dependency group, so no separate install is
needed.

If the build fails, the usual causes in this repo are:

1. **A name in `great-docs.yml` under `reference.sections.contents` no longer
   exists** — this is the most common failure, and it happens whenever a public
   function is renamed or removed. Reconcile that list with
   `src/tidysdmx/__init__.py`'s `__all__`.
2. **A malformed docstring** — great-docs parses Google style; a mis-indented
   `Args:` block or a broken code fence will surface here.
3. **A broken cross-link.** Note that great-docs rewrites paths: `user_guide/`
   becomes `user-guide/` in the output, and numeric ordering prefixes are
   stripped (`01-installation.qmd` → `installation.html`). Link to the *output*
   paths.

On success, report where the site was written and confirm that
`great-docs/_site/reference/` contains a page per public function. To review it
in a browser, use `make docs-preview`.
