---
type: synthesis
title: CAPE System Topology
created: 2026-08-11
updated: 2026-08-11
---

# CAPE System Topology

Where this repo sits in the larger CAPE system, and the stable contracts that connect it to its sibling repositories. This page is intentionally kept at the level of **responsibilities and contracts**, because the sibling repos evolve independently; treat their internal file/class names as illustrative, not load-bearing.

## The three repos

- **This repo** (`cape-wf-bactopia_kraken2_v3_2_0`) - authors one Airflow DAG and its `meta.json`, and publishes them as a release zip. See [[syntheses/architecture-overview]].
- **cape-cod** - the Pulumi Infrastructure-as-Code that *defines* the AWS environment the DAG runs in (Airflow/MWAA, AWS Batch queues + job definitions, S3 buckets, the DynamoDB registries). External repo: [[entities/cape-cod]].
- **cape-cod-env** - the Ansible repo that *deploys* workflow archives onto that infrastructure (uploads the DAG to S3, registers metadata in DynamoDB). External repo: [[entities/cape-cod-env]].

Division of labor: cape-cod builds the stage, cape-cod-env puts the play on it, this repo writes one play.

## The end-to-end flow

1. cape-cod (Pulumi) stands up the environment: an Airflow instance that reads DAGs from an S3 `airflow/dags` prefix, AWS Batch queues/job-definitions for Nextflow, and two DynamoDB registries (analysis-pipeline registry keyed by `pipeline_id`; workflow-meta registry keyed by `dag_id`).
2. This repo releases a zip of `<dag>.py` + `meta.json`.
3. cape-cod-env fetches that release (or a local copy), validates it, uploads the DAG to `s3://<bucket>/airflow/dags/<dag_id>.py`, and writes `{dag_id, pipeline_ids}` to the workflow-meta registry. Details: [[concepts/workflow-deployment-pipeline]].
4. The CAPE frontend/backend uses `pipeline_ids` to look up each pipeline's `parametersSchema` in the analysis-pipeline registry, collects user parameters, and triggers the DAG by `dag_id`.
5. The running DAG submits Nextflow jobs to the Batch queues cape-cod created. See [[concepts/workflow-task-flow]].

## The stable contracts (what must not drift silently)

- **Archive shape**: exactly two flat files, one `.py` DAG and one `meta.json` (`{dag_id, pipeline_ids}`). Enforced by cape-cod-env's validator.
- **`dag_id`** is the join key in three places: the S3 object name (`<dag_id>.py`), the Airflow DAG id, and the workflow-meta registry hash key. A mismatch between `DAG_ID` in the DAG and `dag_id` in `meta.json` breaks discovery (this caused the v0.1.3 fix).
- **`pipeline_ids`** must correspond to `pipeline_id` entries defined in cape-cod's analysis-pipeline registry, or the frontend cannot resolve parameter schemas.
- **Infrastructure names** the DAG references (Batch queue names, Nextflow job definition) are owned by cape-cod. Today they are hard-coded in the DAG (a flagged TODO) rather than injected, so an infra rename in cape-cod would require a change here.

## Related

- [[concepts/cape-platform-integration]]
- [[concepts/external-dependencies-and-boundaries]]
- [[concepts/metajson-and-parameters]]
