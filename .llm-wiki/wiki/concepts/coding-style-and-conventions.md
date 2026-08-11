---
type: concept
title: Coding Style and Conventions
created: 2026-08-11
updated: 2026-08-11
---

# Coding Style and Conventions

Conventions to match when generating code for this repo.

## Language and formatting

- Python 3.10+, formatted with Black + isort (Black profile) + Ruff, all at **80-column** line length.
- 4-space indentation (`.editorconfig`).
- Keep imports sorted per isort/Black profile.

## Structure

- Single-file Airflow DAG. Do not split into packages; the release ships only `bactopia_kraken2_v3_2_0.py` + `meta.json`.
- Use the Airflow **TaskFlow API**: `@dag` decorator, `@task` functions, operator/sensor task objects, and `chain(...)` to express ordering.
- Pass data between tasks via **XCom** (`ti.xcom_pull(task_ids=...)` in templated operator kwargs); a validation `@task` reshapes input config and downstream operators pull from it.
- Register reusable Jinja helpers with `@dag(user_defined_filters={...})` (e.g. `extract_s3_bucket`).
- Module-level helper functions that are DAG-agnostic (e.g. `nextflow_options_to_cli_string`, `extract_s3_bucket_name`) are written with future extraction into a shared DAG library in mind; keep them free of DAG-scoped globals where practical (a `TODO` notes `EXPECTED_PIPELINES` is still assumed in scope).
- Uppercase module-level constants for DAG identity, versions, infrastructure names, config expectations, and key fragments (`DAG_ID`, `BACTOPIA_VERSION`, `WORKFLOW_QUEUE_NAME`, `EXPECTED_PIPELINES`, `K2_INCLUDE_PREFIX`, ...).
- Runtime config arrives via `dag_run.conf` (Airflow API), consumed with Jinja templating inside operator kwargs.

## Docstrings and comments

- Modules and non-trivial functions/tasks use full docstrings (module summary + Args/Returns/Raises/Examples), as in the #9 rework. Prefer this for new library-bound code.
- Inline `TODO:`/`NOTE:` blocks capture assumptions, open design questions, and planned refactors (often referencing issue numbers). Preserve this reasoning-first style; explain constraints over narrating syntax.

## Naming

- `snake_case` for task ids and variables; task ids describe the action (`submit_bactopia_batch_job`, `wait_for_kraken_2_include_file`).
- Constant names encode operational role.

## Comments and TODOs

- The file uses substantial header comments capturing assumptions ("Assumes: ...") and open design questions. Preserve this reasoning-first comment style; prefer explaining constraints/assumptions over narrating syntax.
- Open work is tracked inline with `TODO:` blocks. Keep that convention for known-incomplete areas.

## Error handling / robustness

- The validation task raises `ValueError` with descriptive messages for missing pipelines / required fields; a `fail_on_any_error` flag (default `True`) toggles raise-vs-warn for future library reuse.
- Stage coordination uses S3 sentinel files + `S3KeySensor` polling rather than implicit ordering (`poke_interval` tuned explicitly).
- `extract_s3_bucket_name` does lenient validation and raises `ValueError` on malformed `s3://` paths.

## Commits and releases

- **Conventional Commits** required (`feat:`, `fix:`, `feat!:`/`BREAKING CHANGE:`). release-please computes version bumps and changelog from them.
- PR titles are validated against conventional-commit format.
- No em/en dashes in commits, PRs, or docs (repo + global policy).

## Testing pattern

- No local unit tests by design. Correctness is established via CI static checks and integration testing in the CAPE environment. Do not add pytest scaffolding unless explicitly requested.

Related: [[concepts/dev-environment-and-tooling]], [[syntheses/architecture-overview]].
