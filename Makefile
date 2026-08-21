# Single source of truth for project commands.
#
# CI, CLAUDE.md and the .claude/commands/* slash commands all call these targets,
# so each command is defined once instead of drifting across four places.
# Windows contributors without `make` can read the recipes below and run the
# `$(UV) run ...` line directly; CONTRIBUTING.md lists the equivalents.
#
# UV is overridable for anyone whose uv is not on PATH:
#     make install UV=C:/WBG/uv.exe
# `?=` defers to the environment too, so `export UV=...` works for a whole shell
# session. Deliberately not baked in at generation time — this file is committed,
# and one contributor's path must not become everyone's.
#
# `$(UV)` is quoted in every recipe because make expands variables without
# quoting, so an unquoted path containing a space would split into two words.
# UV is therefore a path, not a command line: it cannot carry its own arguments.
UV ?= uv

.DEFAULT_GOAL := help
.PHONY: help install lint fmt typecheck test cov docs docs-preview audit release-dry check

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependency groups and the pre-commit hooks
	"$(UV)" sync --all-groups
	"$(UV)" run pre-commit install --install-hooks

lint: ## Check lint rules and formatting (no changes)
	"$(UV)" run ruff check .
	"$(UV)" run ruff format --check .

fmt: ## Auto-fix lint violations and format
	"$(UV)" run ruff check --fix .
	"$(UV)" run ruff format .

typecheck: ## Run mypy in strict mode
	"$(UV)" run mypy

test: ## Run unit tests (no coverage gate, so -k works as expected)
	"$(UV)" run pytest -m "not integration"

cov: ## Run unit tests with coverage and enforce the gate
	"$(UV)" run pytest -m "not integration" --cov --cov-report=term-missing --cov-report=xml

docs: ## Build the documentation site
	"$(UV)" run --group docs great-docs build

docs-preview: ## Serve the documentation site locally with live reload
	"$(UV)" run --group docs great-docs preview

audit: ## Audit locked dependencies for known vulnerabilities
	"$(UV)" export --format requirements-txt --no-emit-project --all-groups \
		| "$(UV)" run --group security pip-audit --requirement /dev/stdin

release-dry: ## Show the version the next release would produce, changing nothing
	"$(UV)" run --group release semantic-release -v --noop version

check: lint typecheck cov ## Everything CI runs, in one command
