---
type: source
title: "Observation: CAPE bactopia+kraken2 workflow primed with project wiki"
tags:
  - cape
  - airflow
  - wiki
  - priming
  - bactopia
  - kraken2
status: observation
created: 2026-08-11
updated: 2026-08-11
slug: obs-2026-08-11-cape-bactopia-kraken2-workflow-primed-with-project-wiki
relevance: high
observed_at: 2026-08-11T17:43:41.237Z
source_context: "Priming session: analyze repo and build project wiki"
---

# ⭐ Observation: CAPE bactopia+kraken2 workflow primed with project wiki

Initialized a company-mode llm-wiki vault at the repo root of cape-wf-bactopia_kraken2_v3_2_0 and authored 9 cross-linked pages (architecture-overview synthesis; bactopia-kraken2-dag entity; workflow-task-flow, features-and-entry-points, metajson-and-parameters, external-dependencies-and-boundaries, cape-platform-integration, dev-environment-and-tooling, coding-style-and-conventions, release-and-distribution concepts). Wired commit hygiene: added the pi-llm-wiki .gitignore block (ignoring .llm-wiki/meta,raw,outputs,.discoveries and wiki/.obsidian) and a self-contained '## Project Wiki' section in AGENTS.md. Repo is a single-file Airflow 3.1+ TaskFlow DAG (bactopia_kraken2_v3_2_0.py) plus meta.json, shipped as a GitHub release zip; deps in pyproject.toml are CI-type-check-only. No project code changed this pass. pi-lens Python diagnostics work (LSP clean); gap: pyright/black/isort/typos are not installed locally (not on PATH or in .venv), only ruff 0.15.20 is global.

*Relevance: high*
*Context: Priming session: analyze repo and build project wiki*
*Tags: cape airflow wiki priming bactopia kraken2*

---
*Observed: 2026-08-11T17:43:41.237Z*
