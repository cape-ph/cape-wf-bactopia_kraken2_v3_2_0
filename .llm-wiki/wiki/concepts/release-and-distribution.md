---
type: concept
title: Release and Distribution
created: 2026-08-11
updated: 2026-08-11
---

# Release and Distribution

How this workflow is versioned and shipped.

## Model

- **No packaging, no PyPI.** `pyproject.toml` sets `package-mode = false`; Poetry is used only for CI type-checking dependencies.
- Distribution is a **GitHub release zip** containing exactly `bactopia_kraken2_v3_2_0.py` and `meta.json`. That two-file shape is the archive contract consumed by cape-cod-env's workflow deploy (see [[concepts/workflow-deployment-pipeline]]); the release is inert until an operator deploys it.
- Artifact naming: `cape-wf-bactopia_kraken2_v3_2_0-<tag>.zip` (e.g. `...-v1.2.3.zip`).

## Automated release flow (release-please)

1. Branch from `main`, commit with Conventional Commits, open a PR.
2. `.github/workflows/release.yml` uses the shared `cape-ph/.github` `semantic_release.yml` (`release-type: python`).
3. Merge to `main` -> version calc, CHANGELOG update, git tag, GitHub release.
4. An `attach-artifacts` job finds the release for the current commit (within a 5-minute window) and uploads the zip.

Version bumps: `feat:` -> minor, `fix:` -> patch, `feat!:`/`BREAKING CHANGE:` -> major.

## Manual recovery

- `workflow_dispatch` on release.yml with a tag input re-attaches the artifact if automated upload failed.
- OPERATIONS.md documents deleting/rolling forward bad releases (prefer forward-fixing over deleting used releases).

## Current version

- `0.1.3` (see CHANGELOG.md). Early history is dominated by release-plumbing and metadata fixes.

Related: [[concepts/dev-environment-and-tooling]], [[syntheses/architecture-overview]].
