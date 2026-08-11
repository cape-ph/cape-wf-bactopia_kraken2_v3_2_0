---
type: concept
title: Dev Environment and Tooling
created: 2026-08-11
updated: 2026-08-11
---

# Dev Environment and Tooling

How to work on this repo so future sessions match the toolchain.

## Toolchain versions (`mise.toml` / `.tool-versions`)

- Python 3.10 (code targets 3.10-3.12: `python = ">=3.10,<3.13"`).
- Poetry 1.8 (dependency management only; `package-mode = false`).
- pre-commit 3.
- opencode 1.
- `mise.toml` sets `_.python.venv = ".venv"`; `poetry.toml` creates an in-project `.venv`.

## Setup

```bash
mise install       # Python, Poetry, pre-commit, opencode
poetry install     # deps for type checking
pre-commit install # git hooks
```

## Formatters and linters (config-backed)

- **Black** - line length 80 (`[tool.black]` in pyproject.toml; pinned `24.8.0` in pre-commit).
- **isort** - `profile = "black"`, `line_length = 80` (pinned `5.13.2`).
- **Ruff** - `line-length = 80` (`[tool.ruff]`). A `ruff 0.15.20` binary is on PATH globally.
- **typos** - spell check (pinned `v1.24.6`).
- **pre-commit hooks** also run: mixed-line-ending, check-json, check-yaml, check-toml, check-merge-conflict.
- `.editorconfig` - 4-space indent, max line length 80, tabs only in Makefiles.

Generated Python must be Black + isort + Ruff clean at 80 columns.

## Type checking

- **Pyright** in `basic` mode, `reportMissingTypeStubs: false`, `autoImportCompletions: true` (`pyrightconfig.json`).
- As of the #9-era changes, `pyright = "^1.1.410"` is a declared Poetry dev dependency in `pyproject.toml`/`poetry.lock`, so `poetry install` provides it locally (matching the CI check).

## LSP / pi-lens status

- pi-lens Python diagnostics **work** here: `lsp_diagnostics` on the DAG returns "Primary LSP (python): confirmed clean." pi-lens supplies its own Python language server; no separate install is required.
- Local-workstation note: in the current session `pyright`, `black`, `isort`, and `typos` are not yet in `.venv/bin`; they land after running `poetry install` (pyright) / `pre-commit install` (the pre-commit-managed formatters). `ruff` is available globally. Until then, run checks via `poetry run` / `pre-commit run`.

## Build / run / test

- **Build/distribution**: no build step. Release = GitHub release zip of `bactopia_kraken2_v3_2_0.py` + `meta.json` (see [[concepts/release-and-distribution]]).
- **Run**: not runnable standalone; the DAG executes inside a CAPE Airflow 3.1+ deployment, triggered via the CAPE API.
- **Test**: no unit tests and no pytest (`pytest: false` in CI). Validation is static checks + integration testing inside the CAPE environment.

## Quality-check commands (from README/AGENTS)

```bash
black bactopia_kraken2_v3_2_0.py
isort bactopia_kraken2_v3_2_0.py
pyright bactopia_kraken2_v3_2_0.py
typos
```

## CI

- `.github/workflows/cape.yml` - reusable `poetry_python_checks.yml` (pyright, black, isort; pytest/sphinx off) + `general_checks.yml`.
- `.github/workflows/release.yml` - `semantic_release.yml` (release-please, `release-type: python`) plus an artifact-attach job that zips the two workflow files. See [[concepts/release-and-distribution]].

Related: [[concepts/coding-style-and-conventions]].
