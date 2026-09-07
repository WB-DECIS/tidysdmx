# AGENTS.md

The instructions for AI agents working in this repository live in
[**CLAUDE.md**](CLAUDE.md), which is kept as the single source of truth so the two
files cannot drift apart.

Read `CLAUDE.md` before making changes. It covers the environment (`uv`), the
`make` targets that CI also uses, the repository layout, the coding and testing
conventions, the Conventional Commits requirement, and a list of things not to do.

Detailed conventions are in `.claude/rules/`:

- `python-conventions.md` — function design, naming, docstrings, error handling
- `testing-conventions.md` — test structure, markers, fixtures, coverage
- `commit-conventions.md` — Conventional Commits and how they map to releases
