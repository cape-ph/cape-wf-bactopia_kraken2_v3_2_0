---
type: synthesis
title: Architecture Overview
created: 2026-08-11
updated: 2026-08-11
---

# Architecture Overview

This repository is a **single-file Apache Airflow DAG** that orchestrates a two-stage bacterial genomics pipeline inside the CAPE platform. It contains no application runtime of its own: it is a workflow *definition* that CAPE deploys into a pre-built Airflow environment.

## What it is

- A CAPE workflow in the `cape-wf-*` family (see [[concepts/cape-platform-integration]]).
- Defined as one Airflow 3.1+ DAG using the TaskFlow API in [[entities/bactopia-kraken2-dag]].
- Runs two Nextflow pipelines back-to-back on **AWS Batch**: Bactopia v3.2.0, then Kraken2 (as a Bactopia tool) v3.2.0.
- Distributed as a GitHub release zip containing only `bactopia_kraken2_v3_2_0.py` and `meta.json` (see [[concepts/release-and-distribution]]).

## How the pieces fit

1. CAPE frontend collects parameters described by [[concepts/metajson-and-parameters]] and triggers the DAG via the CAPE API.
2. The DAG submits jobs to AWS Batch and coordinates them through S3 sentinel files. The task graph lives in [[concepts/workflow-task-flow]].
3. External systems (AWS Batch queues, S3, Nextflow, Kraken2 DB mount) are documented in [[concepts/external-dependencies-and-boundaries]].

## Boundaries

- Dependencies in `pyproject.toml` exist **only for CI type checking**; nothing here is bundled or installed at runtime. The real dependencies (Airflow, providers, capepy) are supplied by the CAPE environment.
- No tests, no packaging, no PyPI publish. Validation is integration testing inside CAPE plus static checks in CI (see [[concepts/dev-environment-and-tooling]]).

## Current maturity

The DAG has moved past its initial scaffold: the config format is now defined (`dag_run.conf.pipelineConfigs`) with a validation task, and helper functions are staged for extraction into a shared DAG library. Remaining `TODO`s: hard-coded Batch queue / job-definition names, sourcing pipeline schema/project/version from the CAPE `/workflows/pipelineprofiles` endpoint (issue #10), missing report generation, and possible XCom size limits for large configs. Still validated by integration testing in CAPE rather than local tests.

## Related

- [[concepts/features-and-entry-points]]
- [[concepts/coding-style-and-conventions]]
- [[concepts/cape-platform-integration]]
