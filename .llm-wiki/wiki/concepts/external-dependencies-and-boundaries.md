---
type: concept
title: External Dependencies and Boundaries
created: 2026-08-11
updated: 2026-08-11
---

# External Dependencies and Boundaries

The DAG has no runtime dependencies of its own; it drives external systems provided by the CAPE environment.

## External systems

- **AWS Batch** - executes both pipeline stages.
  - `WORKFLOW_QUEUE_NAME` (`ccd-pvsl-workflows-btch-jobq-e326d2f`): Nextflow head jobs.
  - `JOB_QUEUE_NAME` (`ccd-pvsl-analysis-btch-jobq-0a107a5`): pipeline task jobs.
  - `NEXTFLOW_JOB_DEFINITION` (`ccd-pvsl-nextflow-jobdef`).
  - All three are hard-coded and flagged as TODO to parameterize.
- **AWS S3** - output storage and stage-coordination sentinel files.
- **Nextflow** - the pipeline engine invoked inside Batch containers via `NF_OPTS`.
- **Bactopia v3.2.0 / Kraken2** - the bioinformatics pipelines themselves (`bactopia/bactopia`).
- **Kraken2 database** - mounted read-only at `/mnt/nextflow_shared_data/kraken2`; conda at `/opt/conda:/mnt/conda` via `--aws_volumes`.

## Python imports (satisfied by the CAPE runtime, not this repo)

- `airflow.providers.amazon.aws.operators.batch` - `BatchOperator`, `BatchCreateComputeEnvironmentOperator`.
- `airflow.providers.amazon.aws.operators.ecs` - `EcsDeregisterTaskDefinitionOperator`.
- `airflow.providers.amazon.aws.operators.s3` - `S3CreateObjectOperator`.
- `airflow.providers.amazon.aws.sensors.batch` / `.s3` - `BatchSensor`, `S3KeySensor`, etc.
- `airflow.sdk` - `chain`, `dag`, `task`.
- `boto3`, standard lib `io`, `logging`, `datetime`.
- A code `TODO` notes several imports were copied from the Airflow batch example and may be removable.

## CI-only dependencies

`pyproject.toml` declares `boto3` and `apache-airflow[amazon]` (pinned `3.0.6`) purely for CI type checking. `capepy` is a planned future import (TODO). Nothing here is packaged or shipped; see [[concepts/release-and-distribution]].

## Boundaries

- Runtime environment: pre-built CAPE Airflow (3.1+). This repo does not manage those dependencies.
- Trust boundary: the CAPE API supplies params (see [[concepts/metajson-and-parameters]]); AWS credentials/queues come from the deployment.
