---
type: entity
title: cape-cod
created: 2026-08-11
updated: 2026-08-11
---

# cape-cod

**External, separately-maintained repository.** The CAPE Pulumi Infrastructure-as-Code. It defines the AWS environment this workflow runs in. Details here describe its *role and the contracts we depend on*; internal module/class names are illustrative and may change independently of this repo.

## Role

- Pulumi (Python) IaC for the "Center For Applied Pathogen Genomics" platform: a data lake of configurable domains ("Tributaries"), plus the pipeline-execution infrastructure.
- Stands up the pieces this DAG needs at runtime: an Apache Airflow (MWAA) instance that loads DAGs from an S3 `airflow/dags` prefix, AWS Batch compute environments / job queues / job definitions for Nextflow, ECR, IAM, and the DynamoDB registries.

## Registries it owns (the contract surface we care about)

- **Analysis Pipeline Registry (DAP registry)** - DynamoDB table keyed by `pipeline_id`, with a `pipeline_name`+`version` secondary index. Loaded at deploy time from DAP fixture/profile JSON (under its `assets/analysis-pipelines`). Each entry carries `pipelineName`, `pipelineId`, `version`, `project`, `pipelineType`, a `parametersSchema` (JSON Schema the frontend uses to collect user params), and pipeline-framework config. Our `meta.json` `pipeline_ids` must match `pipeline_id`s registered here.
- **Workflow Meta Registry** - DynamoDB table keyed by `dag_id`, mapping a DAG to its `pipeline_ids`. cape-cod creates the *table*; the *entries* are written by cape-cod-env at workflow-deploy time. Its own docstring notes: "Workflows themselves are deployed outside the pulumi flow (in the cape-cod-env ansible repo)."

## Relationship to this repo

- Owns the Batch queue names and Nextflow job definition the DAG references (today hard-coded here).
- Owns the S3 bucket and `airflow/dags` prefix the deployed DAG lands in.
- Defines the `pipeline_id`s that our `pipeline_ids` point at.

See [[syntheses/cape-system-topology]] and [[concepts/external-dependencies-and-boundaries]].
