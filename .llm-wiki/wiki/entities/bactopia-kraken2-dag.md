---
type: entity
title: bactopia-kraken2-dag
created: 2026-08-11
updated: 2026-08-11
---

# bactopia-kraken2-dag

The workflow definition file: `bactopia_kraken2_v3_2_0.py`. This is the sole code artifact in the repo and the primary entry point for all real work here.

## Identity

- `DAG_ID = "bactopia_kraken2_v3_2_0"` (must match `meta.json` `dag_id`; a past bug from a mismatch was fixed in v0.1.3).
- Display name: "Bactopia v3.2.0 and Kraken2 (through Bactopia) v3.2.0".
- Decorated DAG function: `bactopia_and_kraken2_v3_2_0()`, using the Airflow TaskFlow `@dag` decorator.

## DAG configuration

- `schedule="@once"` - triggered, not scheduled.
- `start_date=datetime.now()`.
- `catchup=False` - a newer run does not supersede an already-scheduled one.

## Version constants

- `BACTOPIA_PROJ = "bactopia/bactopia"`, `BACTOPIA_VERSION = "v3.2.0"`.
- `KRAKEN2_PROJ = "bactopia/bactopia"`, `KRAKEN2_VERSION = "v3.2.0"` (Kraken2 runs as a Bactopia tool, same project).
- Flagged for removal in issue #10, once project/version come from the CAPE `/workflows/pipelineprofiles` endpoint.

## Imports (as of the #9 rework)

Trimmed to what the DAG actually uses: `BatchOperator`, `S3CreateObjectOperator`, `S3KeySensor`, and `chain`/`dag`/`task` from `airflow.sdk`, plus stdlib `logging`/`datetime`. The earlier copied-from-example imports (compute-environment operator, ECS deregister, batch sensors, `boto3`, `io`) were removed.

## Config, helpers, and validation

- `EXPECTED_PIPELINES` - dict mapping stage key (`bactopia`/`kraken2`) to accepted `pipeline_ids` and `required_fields`. See [[concepts/metajson-and-parameters]].
- `K2_INCLUDE_PREFIX` / `K2_INCLUDE_SUFFIX` - build the kraken2 include-file S3 key.
- Module-level helpers intended for a future shared DAG library: `nextflow_options_to_cli_string(options_dict)` and `extract_s3_bucket_name(s3_path)`.
- `@task validate_and_extract_nextflow_configs(...)` - validates `dag_run.conf` and publishes reshaped configs via XCom.
- The DAG registers `extract_s3_bucket_name` as a Jinja filter via `@dag(..., user_defined_filters={"extract_s3_bucket": extract_s3_bucket_name})`.

## Hard-coded infrastructure (flagged TODO)

- `WORKFLOW_QUEUE_NAME = "ccd-pvsl-workflows-btch-jobq-e326d2f"` - queue for Nextflow head jobs.
- `NEXTFLOW_JOB_DEFINITION = "ccd-pvsl-nextflow-jobdef"`.
- `JOB_QUEUE_NAME = "ccd-pvsl-analysis-btch-jobq-0a107a5"` - queue for pipeline tasks.

These are marked as needing to be provided rather than hard-coded.

## Task graph

Defined via `chain(...)`; see [[concepts/workflow-task-flow]] for the validation task plus four operator tasks and their ordering. Fits into the wider system per [[syntheses/architecture-overview]].
