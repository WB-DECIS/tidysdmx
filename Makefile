# Single source of truth for project commands.
#
# CI (ci.yml, docs.yml, security.yml), CLAUDE.md and the .claude/commands/*
# slash commands all call these targets, so each command is defined once
# instead of drifting across four places. `check` is what a contributor runs;
# CI runs the same targets one job each, plus `build`.
# Windows contributors without `make` can read the recipes below and run the
# `uv run ...` line directly; CONTRIBUTING.md lists the equivalents.
#
# `uv` must be on PATH — see CONTRIBUTING.md. Every tool that lives in the
# project environment is invoked as `python -m`, so no pip-generated `.exe`
# launcher is ever executed; .pre-commit-config.yaml explains why that matters.
# The exceptions are the docs tools: neither `great-docs` nor `quarto` ships a
# module entry point, so those two keep their console scripts.

.DEFAULT_GOAL := help
.PHONY: help install lint fmt typecheck test cov build docs docs-preview audit release-dry check

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependency groups, the optional extras and the pre-commit hooks
	uv sync --all-groups --all-extras
	uv run python -m pre_commit install --install-hooks

lint: ## Check lint rules and formatting (no changes)
	uv run python -m ruff check .
	uv run python -m ruff format --check .

fmt: ## Auto-fix lint violations and format
	uv run python -m ruff check --fix .
	uv run python -m ruff format .

typecheck: ## Run mypy (not strict yet — see the burn-down in pyproject.toml)
	uv run python -m mypy

test: ## Run unit tests (no coverage gate, so -k works as expected)
	uv run python -m pytest -m "not integration"

cov: ## Run unit tests with coverage and enforce the gate
	uv run python -m pytest -m "not integration" --cov --cov-report=term-missing --cov-report=xml

# `dist/` is cleared first so the wheel-import step below cannot pick up a
# stale build. twine lives in the `release` group and runs as `python -m` like
# every other tool. The import check installs the wheel into an isolated
# environment: it is the only way to catch a src-layout wheel that builds fine
# and ships nothing importable — an editable install always "works".
build: ## Build the sdist and wheel, check their metadata, and import the wheel
	rm -rf dist
	uv build
	uv run --group release python -m twine check dist/*
	uv run --isolated --no-project --with dist/*.whl \
		python -c "import tidysdmx; print(tidysdmx.__version__)"

# great-docs has no `python -m` entry point (its CLI is a bare click group with
# no __main__), and neither has quarto, which great-docs shells out to. Both
# therefore stay console scripts. This is a docs-only path — it never runs on
# the commit or push path, so it cannot block a commit.
docs: ## Build the documentation site
	uv run --group docs great-docs build

docs-preview: ## Serve the documentation site locally with live reload
	uv run --group docs great-docs preview

# Advisory IDs the audit may skip, space-separated (PYSEC-... or GHSA-...).
# Only for a vulnerability this project cannot fix: typically a tool in a
# dependency group that caps the vulnerable package below the patched version,
# so no bump here can resolve it. Committed on purpose so CI honours it — but
# record why and when next to each entry, and re-check on every release
# whether the cap upstream has moved. See "Suppressing an advisory" in
# SECURITY.md.
PIP_AUDIT_IGNORE ?=

# --locked so this audits the committed uv.lock rather than a fresh resolution.
# The export goes to a real file rather than a pipe: /dev/stdin does not exist
# on native Windows. --no-hashes because pip-audit rejects a requirements set
# that mixes hashed and unhashed entries, and --strict so a dependency that
# cannot be audited fails instead of being skipped silently. security.yml runs
# this same target, so the local and CI audits cannot drift.
audit: ## Audit locked dependencies for known vulnerabilities
	uv export --locked --format requirements-txt --no-emit-project --all-groups --all-extras \
		--no-hashes --output-file requirements-audit.txt
	uv run --group security python -m pip_audit --requirement requirements-audit.txt \
		--strict $(foreach id,$(PIP_AUDIT_IGNORE),--ignore-vuln $(id))

release-dry: ## Show the version the next release would produce, changing nothing
	uv run --group release python -m semantic_release -v --noop version

check: lint typecheck cov ## Lint, typecheck and gated tests — CI runs these plus `build`
